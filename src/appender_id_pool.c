#include "appender_id_pool.h"
#include "mmap_assert.h"
#include "file_initialiser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* ── File path ───────────────────────────────────────────────────────────── */

static void pool_file_path(const char *directory, const char *queue_name,
                            char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/%s/%s", directory, queue_name,
             APPENDER_ID_POOL_FILE_NAME);
}

/* ── appender_id_pool_open ───────────────────────────────────────────────── */

AppenderIdPool *appender_id_pool_open(
    const char *directory,
    const char *queue_name)
{
    MMAP_ASSERT(directory  != NULL, "appender_id_pool_open: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "appender_id_pool_open: queue_name is NULL");

    char path[4096];
    pool_file_path(directory, queue_name, path, sizeof(path));

    /* file_init_zeros initialises all bits to 0 (all IDs free) on first creation */
    RegionAccessor *region = fixed_size_region_create(
        path,
        APPENDER_ID_POOL_REGION_SIZE,
        file_init_zeros,
        NULL);

    MMAP_ASSERT(region != NULL, "appender_id_pool_open: failed to create region");

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    MMAP_ASSERT(r == WRAP_SUCCESS, "appender_id_pool_open: failed to wrap region");

    AppenderIdPool *pool = calloc(1, sizeof(AppenderIdPool));
    MMAP_ASSERT(pool != NULL, "appender_id_pool_open: out of memory");

    pool->region = region;
    pool->base   = buf.data;

    return pool;
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

/* ── appender_id_pool_acquire ────────────────────────────────────────────── */

int appender_id_pool_acquire(AppenderIdPool *pool)
{
    MMAP_ASSERT(pool != NULL, "appender_id_pool_acquire: pool is NULL");

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

/* ── appender_id_pool_try_acquire ────────────────────────────────────────── */

int appender_id_pool_try_acquire(AppenderIdPool *pool, uint8_t id)
{
    MMAP_ASSERT(pool != NULL, "appender_id_pool_try_acquire: pool is NULL");

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

/* ── appender_id_pool_release ────────────────────────────────────────────── */

int appender_id_pool_release(AppenderIdPool *pool, uint8_t id)
{
    MMAP_ASSERT(pool != NULL, "appender_id_pool_release: pool is NULL");

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

    return 1;  /* released */
}

/* ── appender_id_pool_close ──────────────────────────────────────────────── */

void appender_id_pool_close(AppenderIdPool *pool)
{
    if (pool == NULL) return;
    if (pool->region != NULL) {
        fixed_size_region_destroy(pool->region);
        pool->region = NULL;
    }
    free(pool);
}
