#include "internal/appender_id_pool.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/file_initialiser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <sys/mman.h>

/* ── File path ───────────────────────────────────────────────────────────── */

static void pool_file_path(const char *directory, const char *queue_name,
                            char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/%s/%s", directory, queue_name,
             APPENDER_ID_POOL_FILE_NAME);
}

/* ── mmapqi_appender_id_pool_open ───────────────────────────────────────────────── */

int mmapqi_appender_id_pool_open(
    const char *directory,
    const char *queue_name,
    AppenderIdPool **out_pool)
{
    if (out_pool == NULL) return MMAPQ_EINVAL;
    *out_pool = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    char path[4096];
    pool_file_path(directory, queue_name, path, sizeof(path));

    /* mmapqi_file_init_zeros initialises all bits to 0 (all IDs free) on first creation */
    RegionAccessor *region = NULL;
    int fsrerr = mmapqi_fixed_size_region_create(
        path,
        APPENDER_ID_POOL_REGION_SIZE,
        mmapqi_file_init_zeros,
        NULL,
        &region);

    if (fsrerr != 0) return fsrerr;

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    if (r != WRAP_SUCCESS) {
        mmapqi_fixed_size_region_destroy(region);
        return MMAPQ_ENOMEM;
    }

    AppenderIdPool *pool = calloc(1, sizeof(AppenderIdPool));
    if (pool == NULL) {
        mmapqi_fixed_size_region_destroy(region);
        return MMAPQ_ENOMEM;
    }

    pool->region = region;
    pool->base   = buf.data;

    *out_pool = pool;
    return 0;
}

/* ── Bitset helpers ──────────────────────────────────────────────────────── */

/*
 * The bitset is 8 x uint32 = 256 bits in shared memory.
 * For CAS we need atomic access. We treat each 4-byte word as _Atomic uint32_t.
 * This is valid in C11 — _Atomic uint32_t has the same size/alignment as uint32_t
 * and the memory is page-aligned from mmap.
 */

static inline _Atomic uint32_t *word_ptr(AppenderIdPool *pool, int word_index)
{
    return (_Atomic uint32_t *)(pool->base + word_index * sizeof(uint32_t));
}

static inline int word_index(uint8_t id)  { return id / 32; }
static inline int bit_position(uint8_t id) { return id % 32; }

/* ── mmapqi_appender_id_pool_acquire ────────────────────────────────────────────── */

int mmapqi_appender_id_pool_acquire(AppenderIdPool *pool)
{
    if (pool == NULL) return -1;

    for (int retry = 0; retry < APPENDER_ID_POOL_MAX_RETRIES; retry++) {
        for (int w = 0; w < APPENDER_ID_POOL_WORD_COUNT; w++) {
            _Atomic uint32_t *ap = word_ptr(pool, w);
            uint32_t current = atomic_load_explicit(ap, memory_order_acquire);

            /* Scan bits in this word */
            for (int bit = 0; bit < 32; bit++) {
                if (current & (1u << bit)) continue;  /* bit set = ID taken */

                uint32_t updated = current | (1u << bit);
                if (atomic_compare_exchange_strong_explicit(
                        ap, &current, updated,
                        memory_order_acq_rel, memory_order_acquire)) {
                    return w * 32 + bit;
                }
                /* CAS failed — another process grabbed it; re-read and retry */
                current = atomic_load_explicit(ap, memory_order_acquire);
            }
        }
    }

    return -1;  /* pool exhausted */
}

/* ── mmapqi_appender_id_pool_try_acquire ────────────────────────────────────────── */

int mmapqi_appender_id_pool_try_acquire(AppenderIdPool *pool, uint8_t id)
{
    if (pool == NULL) return 0;

    _Atomic uint32_t *ap = word_ptr(pool, word_index(id));
    int bit = bit_position(id);

    uint32_t current;
    uint32_t updated;

    do {
        current = atomic_load_explicit(ap, memory_order_acquire);
        if (current & (1u << bit)) {
            return 0;  /* already acquired */
        }
        updated = current | (1u << bit);
    } while (!atomic_compare_exchange_weak_explicit(
                 ap, &current, updated,
                 memory_order_acq_rel, memory_order_acquire));

    return 1;  /* acquired */
}

/* ── mmapqi_appender_id_pool_release ────────────────────────────────────────────── */

int mmapqi_appender_id_pool_release(AppenderIdPool *pool, uint8_t id)
{
    MMAP_ASSERT(pool != NULL, "mmapqi_appender_id_pool_release: pool is NULL");

    _Atomic uint32_t *ap = word_ptr(pool, word_index(id));
    int bit = bit_position(id);

    uint32_t current;
    uint32_t updated;

    do {
        current = atomic_load_explicit(ap, memory_order_acquire);
        if (!(current & (1u << bit))) {
            return 0;  /* not held — double-release guard */
        }
        updated = current & ~(1u << bit);
    } while (!atomic_compare_exchange_weak_explicit(
                 ap, &current, updated,
                 memory_order_acq_rel, memory_order_acquire));

    /* Force sync to ensure bit clearance is persistent */
    msync(pool->base, APPENDER_ID_POOL_REGION_SIZE, MS_SYNC);

    return 1;  /* released */
}

/* ── mmapqi_appender_id_pool_close ──────────────────────────────────────────────── */

void mmapqi_appender_id_pool_close(AppenderIdPool *pool)
{
    if (pool == NULL) return;
    if (pool->region != NULL) {
        mmapqi_fixed_size_region_destroy(pool->region);
        pool->region = NULL;
    }
    free(pool);
}
