#include "internal/file_mapper.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/platform_internal.h"
#include "internal/test_hooks.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    char            *file_path;
    int64_t          max_size;
    FileInitialiser  initialiser;
    void            *initialiser_ctx;
    int              fd;               /* -1 when not open */
    int              no_create;        /* CAND-59: open existing only (never O_CREAT) */
    int64_t          length_cache;     /* cached file length to avoid stat() on hot path */
} SingleFileRwImpl;

/* ── Forward declarations ────────────────────────────────────────────────── */

static uintptr_t rw_map  (FileMapper *self, int64_t position, int32_t length);
static void      rw_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length);
static void      rw_close(FileMapper *self);

/* ── File length helpers ─────────────────────────────────────────────────── */

static int64_t rw_file_length(SingleFileRwImpl *impl)
{
    struct stat st;
    if (fstat(impl->fd, &st) != 0) {
        return 0;
    }
    return st.st_size;
}

static int rw_extend_file_length(SingleFileRwImpl *impl, int64_t min_length)
{
    MMAP_ASSERT(min_length <= impl->max_size,
        "rw_extend_file_length: exceeded max_size — configuration error");

    int64_t current = rw_file_length(impl);
    if (current >= min_length) {
        impl->length_cache = current;
        return 1;
    }

    if (ftruncate(impl->fd, (off_t)min_length) != 0) {
        return 0;
    }
    impl->length_cache = min_length;
    return 1;
}

/* ── Pre-touch newly extended pages ─────────────────────────────────────── */

/**
 * Pre-touch newly mapped pages by reading and writing each page boundary.
 * Forces the OS to allocate physical pages now rather than on first access,
 * eliminating page fault latency from the hot path.
 *
 * extended_bytes: number of newly extended bytes
 * addr:           start of the mapped region
 * offset:         offset within the region where new bytes begin
 */
static void rw_pretouch(uintptr_t addr, int32_t region_length,
                         int32_t offset, int32_t extended_bytes)
{
    if (extended_bytes < (int32_t)sizeof(int64_t)) {
        return;
    }

    long ps = page_size();
    int32_t remaining = extended_bytes;
    uintptr_t current_addr = addr + offset;

    while (remaining >= (int32_t)sizeof(int64_t)) {
        _Atomic int64_t *p = (_Atomic int64_t *)current_addr;
        int64_t expected = 0;
        /* 
         * Atomic CAS (0 -> 0) forces a Read-For-Ownership (RFO) and marks 
         * the page as dirty in the hardware and kernel. This ensures the
         * background thread takes the page fault and dirty-bit penalty.
         */
        atomic_compare_exchange_strong(p, &expected, 0);

        current_addr += ps;
        remaining -= (int32_t)ps;
    }
    (void)region_length;
}

/* CAND-41 test hook — see internal/test_hooks.h. Atomic: it is armed on
 * the test's thread and taken on the async mapper thread (rw_map runs
 * there with asynchronous mapping). */
#ifdef MMAPQ_TEST_HOOKS
static _Atomic int wpe_reserve_enospc_remaining;
void mmapq_test_hook_set_wpe_reserve_enospc(int n) { atomic_store(&wpe_reserve_enospc_remaining, n); }
int mmapqi_wpe_reserve_enospc_take(void)
{
    int n = atomic_load(&wpe_reserve_enospc_remaining);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&wpe_reserve_enospc_remaining, &n, n - 1)) return 1;
    }
    return 0;
}
#endif

/* ── Reserve the blocks behind a mapping (CAND-41) ──────────────────────── */

/* Returns 0, or the errno of a failed reservation.
 *
 * ftruncate only sets a file's length: the file is sparse, and the blocks
 * are allocated when a page is first written. On a full filesystem that
 * first write — the pre-touch below, or a later append — raises SIGBUS and
 * kills the process. fallocate allocates the same blocks up front and
 * reports ENOSPC instead (it uses no more space: the pre-touch writes every
 * newly extended page anyway). It runs on every read-write map, not only
 * when the file was extended now, because a map that follows a failed
 * reservation finds the file already long enough; on a range that is
 * already allocated it changes nothing. Once per region map, never per
 * message. A filesystem without fallocate (EOPNOTSUPP) keeps the previous
 * behaviour. */
static int rw_reserve(SingleFileRwImpl *impl, int64_t position, int32_t length)
{
#ifdef MMAPQ_TEST_HOOKS
    if (mmapqi_wpe_reserve_enospc_take()) return ENOSPC;
#endif
    if (fallocate(impl->fd, 0, (off_t)position, (off_t)length) == 0) return 0;
    int err = errno;
    if (err == EOPNOTSUPP || err == ENOSYS) return 0;
    return err != 0 ? err : EIO;
}

/* ── Lazy init — open (or create) the file on first map() call ──────────── */

/* Returns 1 when the file is open, 0 when it does not exist and this mapper
 * is open-existing-only (no_create: ENOENT), or the negated errno of the
 * failure otherwise (EIO if the call left errno 0).
 *
 * CAND-59: a no_create mapper never brings a file into existence — the
 * commit path's header mapper is no_create, so a header file that was
 * reclaimed is never silently re-created as zeros; frontier creation is
 * the explicit mmapqi_file_mapper_rolled_create_file (O_CREAT|O_EXCL).
 * A missing file is retried on the next map() (fd stays -1). */
static int rw_init(SingleFileRwImpl *impl)
{
    if (impl->fd >= 0) {
        return 1;  /* already open */
    }

    int fd = open(impl->file_path,
                  impl->no_create ? O_RDWR : (O_RDWR | O_CREAT),
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (fd < 0) {
        int err = errno;
        if (impl->no_create && err == ENOENT) return 0;   /* missing */
        return err != 0 ? -err : -EIO;
    }

    /* Run initialiser (typically mmapqi_file_init_zeros for new files) */
    if (impl->initialiser != NULL) {
        struct stat st;
        fstat(fd, &st);
        if (st.st_size == 0 && impl->initialiser(fd, (size_t)impl->max_size,
                                                   impl->initialiser_ctx) != 0) {
            int err = errno;   /* saved before close() can change it */
            close(fd);
            return err != 0 ? -err : -EIO;
        }
    }

    impl->fd           = fd;
    impl->length_cache = 0;
    return 1;
}

/* The sentinel for a failed open/create/extend/map (mmapqi_write_errno_rc).
 * A transient errno (EINTR, EAGAIN, EMFILE, ENFILE, EBUSY, ENOLCK) stays
 * FILE_MAPPER_NULL_ADDRESS, which the region retries until its timeout.
 * Anything else (EACCES, EROFS, ENOSPC, ENODEV, ...) is FILE_MAPPER_IO_ERROR,
 * which the region reports at once. Before 0.2.1 every failure was
 * NULL_ADDRESS and lost its errno, so a permission error at a file boundary
 * cost one full timeout per append and read as "retry" (CAND-41). */
static uintptr_t rw_fail(int err)
{
    return mmapqi_write_errno_rc(err != 0 ? err : EIO) == MMAPQ_ETIMEDOUT
        ? FILE_MAPPER_NULL_ADDRESS : FILE_MAPPER_IO_ERROR;
}

/* ── map ─────────────────────────────────────────────────────────────────── */

static uintptr_t rw_map(FileMapper *self, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL, "rw_map: self is NULL");
    MMAP_ASSERT(length > 0,   "rw_map: length must be > 0");

    SingleFileRwImpl *impl = (SingleFileRwImpl *)self->impl;

    /* Init is expected to be called from the region mapper thread.
     * CAND-59: the result is checked (it was ignored since 91d37a9, which
     * made a missing file indistinguishable from an I/O error). */
    int irc = rw_init(impl);
    if (irc == 0) return FILE_MAPPER_FILE_NOT_FOUND;
    if (irc < 0)  return rw_fail(-irc);

    int64_t min_length = position + length;

    /* Check cache first — avoid stat on hot path */
    int32_t extended_bytes = 0;
    if (impl->length_cache < min_length) {
        int64_t old_length = rw_file_length(impl);
        if (old_length < min_length) {
            if (!rw_extend_file_length(impl, min_length)) {
                return rw_fail(errno);   /* ftruncate's errno: nothing runs after it */
            }
            extended_bytes = (int32_t)(min_length - old_length);
        } else {
            impl->length_cache = old_length;
        }
    }

    int rerr = rw_reserve(impl, position, length);
    if (rerr != 0) return rw_fail(rerr);   /* ENOSPC: MMAPQ_EIO, not SIGBUS */

    void *addr = mmap(NULL, (size_t)length,
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_POPULATE, impl->fd, (off_t)position);
    if (addr == MAP_FAILED) {
        return rw_fail(errno);
    }

    huge_pages_hint(addr, (size_t)length);

    /* Pre-touch newly extended pages — cap to mapped region
     * to avoid negative offset when file is extended by more than
     * region_size (e.g. first map at a high start_index). */
    if (extended_bytes >= (int32_t)sizeof(int64_t)) {
        int32_t pb = extended_bytes > length ? length : extended_bytes;
        rw_pretouch((uintptr_t)addr, length, length - pb, pb);
    }

    return (uintptr_t)addr;
}

/* ── unmap ───────────────────────────────────────────────────────────────── */

static void rw_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL,  "rw_unmap: self is NULL");
    MMAP_ASSERT(address != 0, "rw_unmap: address is NULL");
    MMAP_ASSERT(length > 0,   "rw_unmap: length must be > 0");
    (void)position;

    if (munmap((void *)address, (size_t)length) != 0) {
        return;
    }
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void rw_close(FileMapper *self)
{
    MMAP_ASSERT(self != NULL, "rw_close: self is NULL");

    SingleFileRwImpl *impl = (SingleFileRwImpl *)self->impl;
    if (impl->fd >= 0) {
        close(impl->fd);
        impl->fd = -1;
    }
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapqi_file_mapper_single_rw_create(
    const char      *file_path,
    int64_t          max_size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    int              no_create,
    FileMapper     **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_path == NULL) return MMAPQ_EINVAL;
    if (max_size <= 0) return MMAPQ_EINVAL;

    SingleFileRwImpl *impl = calloc(1, sizeof(SingleFileRwImpl));
    if (impl == NULL) return MMAPQ_ENOMEM;

    impl->file_path       = strdup(file_path);
    if (impl->file_path == NULL) { free(impl); return MMAPQ_ENOMEM; }
    impl->max_size        = max_size;
    impl->initialiser     = initialiser;
    impl->initialiser_ctx = initialiser_ctx;
    impl->fd              = -1;
    impl->no_create       = no_create ? 1 : 0;
    impl->length_cache    = 0;

    FileMapper *fm = calloc(1, sizeof(FileMapper));
    if (fm == NULL) { free(impl); return MMAPQ_ENOMEM; }

    fm->map   = rw_map;
    fm->unmap = rw_unmap;
    fm->close = rw_close;
    fm->impl  = impl;

    *out = fm;
    return 0;
}
