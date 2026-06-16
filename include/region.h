#ifndef REGION_H
#define REGION_H

#include <stdint.h>
#include "region_accessor.h"
#include "file_mapper.h"

/**
 * Region — a single memory-mapped file window.
 *
 * Extends RegionAccessor with map/unmap operations. The RegionRingAccessor
 * calls map() to initiate mapping and unmap() to release a region when the
 * ring advances past it.
 *
 * Two implementations are provided:
 *   SyncRegion  — map/unmap happen synchronously on the calling thread.
 *   AsyncRegion — map/unmap are requested and processed by the RegionMapper
 *                 thread via a volatile state machine.
 *
 * map():
 *   Request that the region at the given position be mapped.
 *   position must be aligned to region size.
 *   Returns 1 if the region is mapped after the call, 0 otherwise.
 *   Sync: always returns 1 (maps immediately).
 *   Async: returns 0 if mapping is in progress; 1 if already mapped.
 *
 * unmap():
 *   Request that the currently mapped region be unmapped.
 *   Returns 1 if unmapped after the call, 0 otherwise.
 *   Sync: always returns 1 (unmaps immediately).
 *   Async: returns 0 if unmapping is in progress.
 *
 * wrap() and wrap_no_premap() are inherited from RegionAccessor.
 * close() waits for any in-progress mapping to complete then unmaps.
 *
 * INVARIANT: position passed to map() must be a multiple of region size.
 *            Enforced with MMAP_ASSERT in debug builds.
 *
 * LAYOUT NOTE: RegionAccessor is embedded as the FIRST member so that
 *   (RegionAccessor *)region_ptr is always valid — the vtable pointers
 *   and impl field are at identical offsets in both structs.
 */
typedef struct Region {
    /* RegionAccessor embedded first — layout must match exactly */
    RegionAccessor base;

    /* Region-specific operations */
    int (*map)  (struct Region *self, int64_t position);
    int (*unmap)(struct Region *self);
} Region;

/* ── Convenience wrappers ────────────────────────────────────────────────── */

static inline int region_map(Region *r, int64_t position)
{
    return r->map(r, position);
}

static inline int region_unmap(Region *r)
{
    return r->unmap(r);
}

static inline WrapResult region_wrap(Region *r, int64_t position, Buffer *buffer)
{
    return r->base.wrap(&r->base, position, buffer);
}

static inline WrapResult region_wrap_no_premap(Region *r, int64_t position, Buffer *buffer)
{
    return r->base.wrap_no_premap(&r->base, position, buffer);
}

static inline void region_close(Region *r)
{
    r->base.close(&r->base);
}

static inline int32_t region_size(Region *r)
{
    return r->base.size(&r->base);
}

/* ── Factory functions ───────────────────────────────────────────────────── */

/**
 * Sync region — all mapping operations happen on the calling thread.
 *
 * map() calls FileMapper.map() directly and blocks until complete.
 * Appropriate for testing, simple use cases, or when async premap
 * is not required.
 *
 * timeout_ns: maximum nanoseconds to spin waiting for map to succeed.
 *             If exceeded, wrap() returns WRAP_MAP_TIMED_OUT.
 *
 * max_file_not_found_errors:   retries before returning WRAP_FILE_NOT_FOUND.
 * max_beyond_file_size_errors: retries before returning WRAP_BEYOND_FILE_SIZE.
 *
 * INVARIANT: region_size must be a power of two (MMAP_ASSERT enforced).
 */
Region *region_sync_create(
    FileMapper *file_mapper,
    int32_t     region_size,
    uint64_t    timeout_ns,
    int         max_file_not_found_errors,
    int         max_beyond_file_size_errors);

/**
 * Async region — mapping operations are performed by the RegionMapper thread.
 *
 * map() transitions state to MAP_REQUESTED and returns immediately.
 * The RegionMapper thread calls process_request() to perform the actual mmap.
 * wrap() spins until the region reaches MAPPED state or timeout expires.
 *
 * process_request():
 *   Called exclusively by the RegionMapper thread.
 *   Drives the state machine forward by one transition.
 *   Returns 1 if a state change occurred, 0 if no pending request.
 *
 * unmanage():
 *   Called when the RegionMapper is shutting down.
 *   Transitions to UNMANAGED state — subsequent wrap() calls will
 *   fail fast with MMAP_ASSERT rather than spinning indefinitely.
 *
 * INVARIANT: process_request() must only be called from the RegionMapper thread.
 * INVARIANT: region_size must be a power of two (MMAP_ASSERT enforced).
 */
Region *region_async_create(
    FileMapper *file_mapper,
    int32_t     region_size,
    uint64_t    timeout_ns,
    int         max_file_not_found_errors,
    int         max_beyond_file_size_errors);

/**
 * Async-region-specific operations.
 * Called only by the RegionMapper thread — not part of the Region vtable
 * to keep the hot path interface clean.
 */
int  region_async_process_request(Region *r);  /* returns 1 if work done */
void region_async_unmanage(Region *r);

/**
 * Destroy a Region and free all associated memory.
 * Calls close() then frees the Region struct and its impl.
 */
void region_destroy(Region *r);

/* ── Default error thresholds (mirrors Java IRegionFactory constants) ────── */
#define REGION_MAX_FILE_NOT_FOUND_ERRORS    3
#define REGION_MAX_BEYOND_FILE_SIZE_ERRORS  3

#endif /* REGION_H */
