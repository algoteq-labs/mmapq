#ifndef REGION_RING_ACCESSOR_H
#define REGION_RING_ACCESSOR_H

#include <stdint.h>
#include "region_accessor.h"
#include "region_ring.h"
#include "file_mapper.h"
#include "mmap_assert.h"

/**
 * RegionRingAccessor — sliding window view over a RegionRing.
 *
 * Implements RegionAccessor. This is the object owned by appenders,
 * pollers, and readers. It translates logical positions into ring slot
 * indices, triggers premap of upcoming regions, and handles region
 * boundary crossing transparently.
 *
 * Premap strategy:
 *   When the position within the current region exceeds premap_trigger_offset
 *   (computed as region_size * premap_trigger_ratio), premap is requested
 *   for the next N regions (regions_to_map_ahead, typically 1).
 *   This ensures the next region is already MAPPED when the boundary is crossed,
 *   making the crossing zero-latency on the hot path.
 *
 * Forward vs backward premap:
 *   wrap()           — sequential forward access. Premaps ahead.
 *   wrap_no_premap() — random access. No premap triggered.
 *                      Also used for reverse traversal where forward
 *                      premap would be wasteful or incorrect.
 *
 * Owns its RegionRing. close() closes the ring and releases all resources.
 *
 * INVARIANT: regions_to_map_ahead < ring_size (MMAP_ASSERT enforced).
 * INVARIANT: region_size must be a power of two (MMAP_ASSERT enforced).
 * INVARIANT: premap_trigger_ratio must be in (0.0, 1.0] (MMAP_ASSERT enforced).
 */
/**
 * Create a RegionRingAccessor.
 *
 * ring:                   the RegionRing to wrap (ownership transferred)
 * region_size:            size of each region in bytes (power of two)
 * regions_to_map_ahead:   number of regions to premap ahead (typically 1).
 *                         Automatically forced to 0 when ring->is_async == 0:
 *                         sync regions map on demand so premapping ahead
 *                         has no benefit — it would just block the calling
 *                         thread earlier with no latency hiding.
 * premap_trigger_ratio:   fraction of region consumed before premap fires.
 *                         0.0 = trigger immediately on entering a new region
 *                         (used for writers at the frontier).
 *                         0.5 = trigger at halfway point (used for readers).
 *                         Ignored when effective regions_to_map_ahead == 0.
 *
 * Returns a RegionAccessor* — callers use the RegionAccessor interface only.
 * The underlying RegionRingAccessor is hidden behind the interface.
 */
RegionAccessor *region_ring_accessor_create(
    RegionRing *ring,
    int32_t     region_size,
    int         regions_to_map_ahead,
    double      premap_trigger_ratio);

/**
 * Destroy a RegionRingAccessor created by region_ring_accessor_create().
 *
 * Calls close() on the accessor (which closes the underlying ring),
 * then frees all memory. After destroy(), the pointer must not be used.
 *
 * Note: pass the RegionAccessor* returned by region_ring_accessor_create().
 */
void region_ring_accessor_destroy(RegionAccessor *acc);

/* ── Default configuration ───────────────────────────────────────────────── */
#define REGION_RING_ACCESSOR_DEFAULT_PREMAP_RATIO   0.5
#define REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD  1

#endif /* REGION_RING_ACCESSOR_H */
