#ifndef REGION_RING_H
#define REGION_RING_H

#include <stdint.h>
#include "region.h"
#include "file_mapper.h"
#include "mmap_assert.h"

/**
 * RegionRing — a fixed-size circular array of Region instances.
 *
 * Manages the lifecycle of N regions (typically 4, must be power of two)
 * over a contiguous logical address space backed by rolled files.
 *
 * The ring does not perform mapping decisions itself — that is the
 * responsibility of RegionRingAccessor. The ring simply owns the Region
 * array and the FileMapper, and coordinates shutdown.
 *
 * Sync ring:
 *   All mapping operations happen on the calling thread.
 *   close() unmaps all regions and closes the FileMapper immediately.
 *
 * Async ring:
 *   Mapping is driven by a RegionMapper registered with a Runtime.
 *   close() blocks until all regions reach UNMAPPED state (driven by
 *   the RegionMapper thread), then schedules deregistration of the
 *   RegionMapper from the Runtime via a DeregisterExecutable.
 *   This preserves the Runtime threading contract: unregister() must
 *   only be called from the Runtime thread.
 *
 * INVARIANT: ring_size must be a power of two (MMAP_ASSERT enforced).
 * INVARIANT: region_size must be a power of two (MMAP_ASSERT enforced).
 */
typedef struct RegionRing {
    void    (*close)(struct RegionRing *self);
    Region **regions;     /* array of ring_size Region pointers              */
    int      ring_size;   /* number of regions — power of two                */
    int      is_async;    /* 1 = async regions, 0 = sync regions             */
    void    *impl;
} RegionRing;

static inline void region_ring_close(RegionRing *ring)
{
    ring->close(ring);
}

/* ── Factory functions ───────────────────────────────────────────────────── */

/* Forward declaration — Runtime defined in runtime.h */
typedef struct Runtime Runtime;

/**
 * Create a synchronous RegionRing.
 *
 * All mapping operations are performed synchronously on the calling thread.
 * No Runtime is required.
 *
 * ring_size:   number of regions in the ring (power of two, typically 4)
 * region_size: size of each region in bytes (power of two, page-aligned)
 * file_mapper: the FileMapper backing all regions in this ring
 * timeout_ns:  per-wrap timeout in nanoseconds
 * out:         out-parameter for the created RegionRing (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure. Sets *out to NULL on failure.
 */
int region_ring_sync_create(
    int         ring_size,
    int32_t     region_size,
    FileMapper *file_mapper,
    uint64_t    timeout_ns,
    RegionRing **out);

/**
 * Create an asynchronous RegionRing.
 *
 * Mapping is performed by the RegionMapper thread registered with runtime.
 * The ring registers an AsyncMappingExecutable with the Runtime on creation.
 * On close(), regions are unmapped (blocking on calling thread) and a
 * DeregisterExecutable is registered to remove the AsyncMappingExecutable
 * from the Runtime on the Runtime thread.
 *
 * INVARIANT: max_cache_size in the rolled FileMapper must be >= ring_size.
 *            The caller is responsible for enforcing this at construction.
 *
 * out:         out-parameter for the created RegionRing (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure. Sets *out to NULL on failure.
 */
int region_ring_async_create(
    int         ring_size,
    int32_t     region_size,
    FileMapper *file_mapper,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    RegionRing **out);

/**
 * Destroy a RegionRing and free all associated memory.
 * Calls close() then frees the RegionRing struct, impl, and all Region instances.
 */
void region_ring_destroy(RegionRing *ring);

/* ── Default ring configuration ─────────────────────────────────────────── */
#define REGION_RING_DEFAULT_SIZE          4   /* power of two */
#define REGION_RING_DEFAULT_PREMAP_AHEAD  1   /* map 1 region ahead */

#endif /* REGION_RING_H */
