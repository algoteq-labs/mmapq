#ifndef APPENDER_ID_POOL_H
#define APPENDER_ID_POOL_H

#include <stdint.h>
#include "mmap_assert.h"
#include "fixed_size_region.h"
#include "header_codec.h"

/**
 * AppenderIdPool — shared memory bitset for appender ID allocation.
 *
 * File: "<directory>/<queue_name>/appender_id_pool"
 * Size: APPENDER_ID_POOL_REGION_SIZE = 32 bytes = 256 bits
 *
 * Layout: 8 x uint32 packed bitset.
 *   bit i = 1 → appender ID i is currently acquired.
 *   bit i = 0 → appender ID i is free.
 *
 * Allocation via CAS on uint32 words — lock-free, multi-process safe.
 * Each word covers 32 appender IDs. With MAX_APPENDERS = 256, we need
 * 256 / 32 = 8 words = 32 bytes.
 *
 * The pool is backed by a fixed-size memory-mapped file so allocations
 * persist across process restarts. An appender that crashes without
 * releasing its ID will hold the bit until manually cleared or the
 * pool is reset.
 *
 * acquire():
 *   Scan words for a free bit. CAS 0→1.
 *   Retry up to APPENDER_ID_POOL_MAX_RETRIES times before failing.
 *   Returns the acquired ID, or -1 on failure.
 *
 * try_acquire(id):
 *   Attempt to acquire a specific ID. Returns 1 if acquired, 0 if already taken.
 *
 * release(id):
 *   CAS 1→0 for the bit. Returns 1 if released, 0 if not held.
 */

/*
 * The bitset needs 256/8 = 32 bytes of data, but fixed_size_region_create
 * requires a multiple of page_size() (4096 bytes on Linux, 16384 on macOS M1).
 * We use page_size() at runtime so this works on both platforms.
 */
#include "mmap_platform.h"
static inline int32_t appender_id_pool_region_size(void)
{
    return (int32_t)page_size();
}
#define APPENDER_ID_POOL_REGION_SIZE  appender_id_pool_region_size()
#define APPENDER_ID_POOL_WORD_COUNT   ((int32_t)(MAX_APPENDERS / 32))
#define APPENDER_ID_POOL_MAX_RETRIES  ((int32_t)100)
#define APPENDER_ID_POOL_FILE_NAME    "appender_id_pool"

typedef struct {
    RegionAccessor *region;   /* FixedSizeRegion backing the bitset */
    uint8_t        *base;     /* base pointer into mapped memory    */
} AppenderIdPool;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * appender_id_pool_open — open or create the appender ID pool.
 *
 * Creates the file if it does not exist, initialised to all zeros (all IDs free).
 * The file is opened read-write since both readers and writers acquire IDs.
 *
 * Returns a heap-allocated AppenderIdPool. Aborts on failure.
 */
AppenderIdPool *appender_id_pool_open(
    const char *directory,
    const char *queue_name);

/* ── Operations ──────────────────────────────────────────────────────────── */

/**
 * appender_id_pool_acquire — allocate the next free appender ID.
 *
 * Scans the bitset for a free slot and CAS-acquires it.
 * Retries up to APPENDER_ID_POOL_MAX_RETRIES times.
 *
 * Returns the acquired appender ID (0–255), or -1 if all IDs are in use.
 */
int appender_id_pool_acquire(AppenderIdPool *pool);

/**
 * appender_id_pool_try_acquire — attempt to acquire a specific appender ID.
 *
 * id: the specific ID to acquire (0–255).
 * Returns 1 if acquired successfully, 0 if already held by another appender.
 *
 * Used when an appender wants to resume with its previous ID after restart.
 */
int appender_id_pool_try_acquire(AppenderIdPool *pool, uint8_t id);

/**
 * appender_id_pool_release — release a previously acquired appender ID.
 *
 * Returns 1 if released, 0 if the ID was not held (double-release guard).
 */
int appender_id_pool_release(AppenderIdPool *pool, uint8_t id);

/**
 * appender_id_pool_close — unmap and free an AppenderIdPool.
 *
 * Does NOT release any acquired IDs. The caller must call release() for
 * each held ID before calling close(), or accept that the IDs remain
 * held until another process clears them.
 */
void appender_id_pool_close(AppenderIdPool *pool);

#endif /* APPENDER_ID_POOL_H */
