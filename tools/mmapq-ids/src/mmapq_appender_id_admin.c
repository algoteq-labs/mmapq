#include "mmapq_appender_id_admin.h"

/* Single source of truth for the pool size and bitmap layout: the library's
 * internal header. Compile-time only — no function from it is called, so the
 * tool still links no mmapq object code. */
#include "internal/appender_id_pool.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#if !defined(__linux__) || __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "mmapq ID administration requires little-endian Linux"
#endif

/* What this tool's logic relies on, pinned against the header (CAND-50 owner
 * correction 2026-10-22 — an assert set that never mentions a constant would
 * let a perturbed value build silently). */
_Static_assert(APPENDER_ID_POOL_WORD_COUNT * 32 == MAX_APPENDERS,
               "pool bitmap must exactly cover the appender ID space");
_Static_assert(MAX_APPENDERS == MMAPQ_APPENDER_ID_ADMIN_COUNT,
               "tool fact array must cover every appender ID");
_Static_assert(APPENDER_ID_POOL_WORD_COUNT * 4 <= APPENDER_ID_POOL_REGION_SIZE,
               "bitmap words must fit inside the pool region");
/* 64 bytes is the Java binary-compatibility contract
 * (src/internal/appender_id_pool.h) — never "fix" this pin. */
_Static_assert(APPENDER_ID_POOL_REGION_SIZE == 64,
               "pool region size is the Java binary-compat contract");
_Static_assert(sizeof(_Atomic uint32_t) == 4, "pool word layout mismatch");
_Static_assert(sizeof(unsigned int) == 4 && ATOMIC_INT_LOCK_FREE == 2,
               "cross-process pool operations require lock-free 32-bit atomics");

typedef struct {
    int directory;
    int pool;
    void *mapping;
} Pool;

static int close_pool(Pool *pool, int error)
{
    if (pool->mapping != MAP_FAILED && munmap(pool->mapping, APPENDER_ID_POOL_REGION_SIZE) != 0 && !error)
        error = errno;
    if (pool->pool >= 0 && close(pool->pool) != 0 && !error) error = errno;
    if (pool->directory >= 0 && close(pool->directory) != 0 && !error) error = errno;
    return error;
}

static int open_pool(const char *directory, const char *queue, int writable, Pool *pool)
{
    *pool = (Pool){ .directory = -1, .pool = -1, .mapping = MAP_FAILED };
    if (!directory || !*directory || !queue || !*queue || strchr(queue, '/') ||
        !strcmp(queue, ".") || !strcmp(queue, "..")) return EINVAL;

    int base = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (base < 0) return errno;
    /* Resolve children relative to one directory descriptor. Refusing symlinks
     * prevents a lane or pool name from redirecting inspection or mutation. */
    pool->directory = openat(base, queue, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    int error = pool->directory < 0 ? errno : 0;
    if (close(base) != 0 && !error) error = errno;
    if (error) return error;
    pool->pool = openat(pool->directory, APPENDER_ID_POOL_FILE_NAME,
        (writable ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (pool->pool < 0) return errno;
    struct stat st;
    if (fstat(pool->pool, &st) != 0) return errno;
    if (!S_ISREG(st.st_mode) || st.st_size != APPENDER_ID_POOL_REGION_SIZE) return EINVAL;
    pool->mapping = mmap(NULL, APPENDER_ID_POOL_REGION_SIZE, PROT_READ | (writable ? PROT_WRITE : 0),
                         MAP_SHARED, pool->pool, 0);
    return pool->mapping == MAP_FAILED ? errno : 0;
}

/* Parse canonical decimal components without sscanf's accepted suffixes,
 * signs, whitespace, leading zeroes, or overflowing numeric conversions. */
static int number(const char **cursor, uint64_t limit, uint64_t *value)
{
    const char *p = *cursor;
    if (*p < '0' || *p > '9') return 0;
    if (*p == '0' && p[1] >= '0' && p[1] <= '9') return 0;
    uint64_t n = 0;
    while (*p >= '0' && *p <= '9') {
        unsigned digit = (unsigned)(*p++ - '0');
        if (n > (limit - digit) / 10) return 0;
        n = n * 10 + digit;
    }
    *value = n;
    *cursor = p;
    return 1;
}

static int lane_name(const char *name, unsigned *id, int *meta)
{
    const char *p;
    if (!strncmp(name, "appender_meta_", 14)) {
        p = name + 14;
        *meta = 1;
    } else if (!strncmp(name, "payload_", 8)) {
        p = name + 8;
        *meta = 0;
    } else return 0;
    uint64_t lane, file;
    if (!number(&p, 255, &lane)) return 0;
    if (!*meta && (*p++ != '_' || !number(&p, INT64_MAX, &file))) return 0;
    if (*p) return 0;
    *id = (unsigned)lane;
    return 1;
}

static int collect_facts(int directory, MmapqAppenderIdFacts *facts)
{
    int copy = fcntl(directory, F_DUPFD_CLOEXEC, 0);
    if (copy < 0) return errno;
    DIR *stream = fdopendir(copy);
    if (!stream) { int error = errno; close(copy); return error; }
    int error = 0;
    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(stream);
        if (!entry) { error = errno; break; }
        unsigned id;
        int meta;
        if (!lane_name(entry->d_name, &id, &meta)) continue;
        struct stat st;
        if (fstatat(directory, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            error = errno; break;
        }
        if (!S_ISREG(st.st_mode) || st.st_size < 0) { error = EINVAL; break; }
        MmapqAppenderIdFacts *fact = &facts[id];
        if (meta) fact->has_meta = 1;
        else {
            if (fact->payload_files == UINT64_MAX ||
                (uint64_t)st.st_size > UINT64_MAX - fact->payload_bytes) {
                error = EOVERFLOW; break;
            }
            fact->payload_files++;
            fact->payload_bytes += (uint64_t)st.st_size;
        }
        if (!fact->has_mtime || st.st_mtime > fact->latest_mtime)
            fact->latest_mtime = st.st_mtime;
        fact->has_mtime = 1;
    }
    if (closedir(stream) != 0 && !error) error = errno;
    return error;
}

int mmapq_appender_id_admin_inspect(const char *directory, const char *queue,
                                  MmapqAppenderIdFacts *facts)
{
    if (!facts) return EINVAL;
    memset(facts, 0, sizeof(*facts) * MMAPQ_APPENDER_ID_ADMIN_COUNT);
    MmapqAppenderIdFacts result[MMAPQ_APPENDER_ID_ADMIN_COUNT] = {0};
    Pool pool;
    int error = open_pool(directory, queue, 0, &pool);
    if (!error) {
        const _Atomic uint32_t *words = pool.mapping;
        for (int w = 0; w < APPENDER_ID_POOL_WORD_COUNT; w++) {
            /* Same acquire load as the pool implementation. Each word is read
             * once, but this is not a globally consistent pool snapshot. */
            uint32_t bits = atomic_load_explicit(&words[w], memory_order_acquire);
            for (int b = 0; b < 32; b++) result[w * 32 + b].taken = (bits >> b) & 1u;
        }
        error = collect_facts(pool.directory, result);
    }
    error = close_pool(&pool, error);
    if (!error) memcpy(facts, result, sizeof(result));
    return error;
}

int mmapq_appender_id_admin_release(const char *directory, const char *queue,
                                  unsigned int id, int *released)
{
    if (!released) return EINVAL;
    *released = 0;
    if (id >= MMAPQ_APPENDER_ID_ADMIN_COUNT) return EINVAL;
    Pool pool;
    int error = open_pool(directory, queue, 1, &pool);
    if (!error) {
        _Atomic uint32_t *word = (_Atomic uint32_t *)pool.mapping + id / 32;
        uint32_t mask = UINT32_C(1) << (id % 32);
        uint32_t current = atomic_load_explicit(word, memory_order_acquire);
        while (current & mask) {
            /* Match the library's release operation. CAS retries preserve
             * concurrent changes to other IDs within this same word. */
            if (atomic_compare_exchange_weak_explicit(word, &current, current & ~mask,
                    memory_order_acq_rel, memory_order_acquire)) {
                *released = 1;
                if (msync(pool.mapping, APPENDER_ID_POOL_REGION_SIZE, MS_SYNC) != 0) error = errno;
                break;
            }
        }
    }
    return close_pool(&pool, error);
}
