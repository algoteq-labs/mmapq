#include "region_ring_accessor.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <math.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    RegionRing *ring;
    int32_t     region_size;
    int         regions_to_map_ahead;
    int64_t     premap_trigger_offset;  /* bytes into region before premap fires */
    int32_t     regions_length_mask;    /* ring_size - 1, for fast modulo         */
    int         shift_for_region_size;  /* log2(region_size), for fast division   */
    int64_t     region_mask;            /* region_size - 1, for fast offset       */
    int64_t     current_absolute_index; /* last region index where premap fired   */
} RingAccessorImpl;

/* ── Premap helper ───────────────────────────────────────────────────────── */

/**
 * trigger_premap_forward — request mapping of the next N regions ahead.
 *
 * Called when the position within the current region exceeds
 * premap_trigger_offset. Requests map() on upcoming regions so they
 * are ready before the boundary is crossed.
 *
 * This is idempotent — region_async_map() on an already-mapped or
 * already-requested region is a no-op.
 */
static void trigger_premap_forward(RingAccessorImpl *impl, int64_t absolute_index)
{
    int64_t forward_bound = absolute_index + impl->regions_to_map_ahead;
    for (int64_t map_index = absolute_index + 1; map_index <= forward_bound; map_index++) {
        int ring_slot = (int)(map_index & impl->regions_length_mask);
        int64_t map_position = map_index << impl->shift_for_region_size;
        region_map(impl->ring->regions[ring_slot], map_position);
    }
    impl->current_absolute_index = absolute_index;
}

/**
 * trigger_premap_backward — request mapping of the previous N regions.
 *
 * Called when traversing backwards (reverse replay, random access
 * going back). Premaps behind the current position.
 */
static void trigger_premap_backward(RingAccessorImpl *impl, int64_t absolute_index)
{
    int64_t backward_bound = absolute_index - impl->regions_to_map_ahead;
    if (backward_bound < 0) backward_bound = 0;
    for (int64_t map_index = absolute_index - 1; map_index >= backward_bound; map_index--) {
        int ring_slot = (int)(map_index & impl->regions_length_mask);
        int64_t map_position = map_index << impl->shift_for_region_size;
        region_map(impl->ring->regions[ring_slot], map_position);
    }
    impl->current_absolute_index = absolute_index;
}

/* ── wrap — sequential access with premap ────────────────────────────────── */

static WrapResult ring_accessor_wrap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self   != NULL, "ring_accessor_wrap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "ring_accessor_wrap: buffer is NULL");
    MMAP_ASSERT(position >= 0,  "ring_accessor_wrap: position must be >= 0");

    RingAccessorImpl *impl = (RingAccessorImpl *)self->impl;

    int64_t absolute_index = position >> impl->shift_for_region_size;
    int     ring_slot      = (int)(absolute_index & impl->regions_length_mask);

    WrapResult result = region_wrap(impl->ring->regions[ring_slot], position, buffer);

    if (result == WRAP_SUCCESS) {
        /* Trigger premap only for async regions (regions_to_map_ahead > 0) */
        if (impl->regions_to_map_ahead > 0) {
            int64_t offset_in_region = position & impl->region_mask;
            if (offset_in_region >= impl->premap_trigger_offset) {
                if (impl->current_absolute_index < absolute_index) {
                    trigger_premap_forward(impl, absolute_index);
                } else if (impl->current_absolute_index > absolute_index) {
                    trigger_premap_backward(impl, absolute_index);
                }
            }
        }
    }

    return result;
}

/* ── wrap_no_premap — random access, no premap ───────────────────────────── */

static WrapResult ring_accessor_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self   != NULL, "ring_accessor_wrap_no_premap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "ring_accessor_wrap_no_premap: buffer is NULL");
    MMAP_ASSERT(position >= 0,  "ring_accessor_wrap_no_premap: position must be >= 0");

    RingAccessorImpl *impl = (RingAccessorImpl *)self->impl;

    int64_t absolute_index = position >> impl->shift_for_region_size;
    int     ring_slot      = (int)(absolute_index & impl->regions_length_mask);

    return region_wrap_no_premap(impl->ring->regions[ring_slot], position, buffer);
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void ring_accessor_close(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "ring_accessor_close: self is NULL");

    RingAccessorImpl *impl = (RingAccessorImpl *)self->impl;
    region_ring_close(impl->ring);
}

static int32_t ring_accessor_size(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "ring_accessor_size: self is NULL");
    return ((RingAccessorImpl *)self->impl)->region_size;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

RegionAccessor *region_ring_accessor_create(
    RegionRing *ring,
    int32_t     region_size,
    int         regions_to_map_ahead,
    double      premap_trigger_ratio)
{
    MMAP_ASSERT(ring != NULL,              "region_ring_accessor_create: ring is NULL");
    MMAP_ASSERT(region_size > 0,           "region_ring_accessor_create: region_size must be > 0");
    MMAP_ASSERT((region_size & (region_size - 1)) == 0,
        "region_ring_accessor_create: region_size must be a power of two");
    MMAP_ASSERT(regions_to_map_ahead >= 0, "region_ring_accessor_create: regions_to_map_ahead must be >= 0");
    MMAP_ASSERT(regions_to_map_ahead < ring->ring_size,
        "region_ring_accessor_create: regions_to_map_ahead must be < ring_size");
    MMAP_ASSERT(premap_trigger_ratio >= 0.0 && premap_trigger_ratio <= 1.0,
        "region_ring_accessor_create: premap_trigger_ratio must be in [0.0, 1.0]");

    /*
     * Premap is only meaningful for async regions.
     * Sync regions map on demand on the calling thread — requesting ahead
     * would just block the thread earlier with no latency benefit.
     */
    int effective_regions_ahead = ring->is_async ? regions_to_map_ahead : 0;

    RingAccessorImpl *impl = calloc(1, sizeof(RingAccessorImpl));
    MMAP_ASSERT(impl != NULL, "region_ring_accessor_create: out of memory");

    impl->ring                  = ring;
    impl->region_size           = region_size;
    impl->regions_to_map_ahead  = effective_regions_ahead;
    impl->premap_trigger_offset = effective_regions_ahead > 0
        ? (int64_t)(region_size * premap_trigger_ratio)
        : 0;
    impl->regions_length_mask   = ring->ring_size - 1;
    impl->shift_for_region_size = __builtin_ctz((unsigned)region_size);
    impl->region_mask           = region_size - 1;
    impl->current_absolute_index = -1;

    RegionAccessor *acc = calloc(1, sizeof(RegionAccessor));
    MMAP_ASSERT(acc != NULL, "region_ring_accessor_create: out of memory");

    acc->wrap           = ring_accessor_wrap;
    acc->wrap_no_premap = ring_accessor_wrap_no_premap;
    acc->close          = ring_accessor_close;
    acc->size           = ring_accessor_size;
    acc->impl           = impl;

    return acc;
}

/* ── Destroy ─────────────────────────────────────────────────────────────── */

void region_ring_accessor_destroy(RegionAccessor *acc)
{
    if (acc == NULL) return;
    region_accessor_close(acc);

    RingAccessorImpl *impl = (RingAccessorImpl *)acc->impl;
    region_ring_destroy(impl->ring);

    free(impl);
    free(acc);
}
