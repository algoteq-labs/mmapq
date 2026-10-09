#include "internal/region_ring_accessor.h"
#include "internal/mmap_assert.h"
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
    if (position < 0) return WRAP_MAP_TIMED_OUT;

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

        self->mapped_capacity = buffer->capacity;
        /* Update fast-path cache */
        self->active_address = buffer->data;
        self->active_base    = position;
        if (impl->regions_to_map_ahead > 0 && impl->current_absolute_index != absolute_index) {
            /* Premap not yet fired for this region — fast path only up to trigger point */
            self->fast_path_end = (absolute_index << impl->shift_for_region_size) + impl->premap_trigger_offset;
        } else {
            /* Premap already fired or not needed — fast path until end of region */
            self->fast_path_end = position + buffer->capacity;
        }
    } else {
        /* The failed wrap may have unmapped the cached region (same-slot remap) */
        region_accessor_invalidate_fast_path(self);
    }

    return result;
}

/* ── wrap_no_premap — random access, no premap ───────────────────────────── */

static WrapResult ring_accessor_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self   != NULL, "ring_accessor_wrap_no_premap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "ring_accessor_wrap_no_premap: buffer is NULL");
    if (position < 0) return WRAP_MAP_TIMED_OUT;

    RingAccessorImpl *impl = (RingAccessorImpl *)self->impl;

    int64_t absolute_index = position >> impl->shift_for_region_size;
    int     ring_slot      = (int)(absolute_index & impl->regions_length_mask);

    WrapResult result = region_wrap_no_premap(impl->ring->regions[ring_slot], position, buffer);
    if (result == WRAP_SUCCESS) {
        self->active_address = buffer->data;
        self->active_base    = position;
        self->fast_path_end = position + buffer->capacity;
        self->mapped_capacity = buffer->capacity;
    } else {
        /* The failed wrap may have unmapped the cached region (same-slot remap) */
        region_accessor_invalidate_fast_path(self);
    }
    return result;
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void ring_accessor_close(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "ring_accessor_close: self is NULL");

    RingAccessorImpl *impl = (RingAccessorImpl *)self->impl;
    region_ring_close(impl->ring);
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapqi_region_ring_accessor_create(
    RegionRing    *ring,
    int32_t        region_size,
    int            regions_to_map_ahead,
    double         premap_trigger_ratio,
    RegionAccessor **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (ring == NULL) return MMAPQ_EINVAL;
    if (region_size <= 0) return MMAPQ_EINVAL;
    if ((region_size & (region_size - 1)) != 0) return MMAPQ_EINVAL;
    if (regions_to_map_ahead < 0) return MMAPQ_EINVAL;
    if (regions_to_map_ahead >= ring->ring_size) return MMAPQ_EINVAL;
    if (premap_trigger_ratio < 0.0 || premap_trigger_ratio > 1.0) return MMAPQ_EINVAL;
    /*
     * Sync regions map on demand on the calling thread — requesting ahead
     * would just block the thread earlier with no latency benefit.
     */
    int effective_regions_ahead = ring->is_async ? regions_to_map_ahead : 0;

    RingAccessorImpl *impl = calloc(1, sizeof(RingAccessorImpl));
    if (impl == NULL) return MMAPQ_ENOMEM;

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
    if (acc == NULL) { free(impl); return MMAPQ_ENOMEM; }

    acc->wrap           = ring_accessor_wrap;
    acc->wrap_no_premap = ring_accessor_wrap_no_premap;
    acc->close          = ring_accessor_close;
    acc->region_size    = region_size;
    acc->impl           = impl;

    *out = acc;
    return 0;
}

/* ── Destroy ─────────────────────────────────────────────────────────────── */

void mmapqi_region_ring_accessor_destroy(RegionAccessor *acc)
{
    if (acc == NULL) return;
    region_accessor_close(acc);

    RingAccessorImpl *impl = (RingAccessorImpl *)acc->impl;
    mmapqi_region_ring_destroy(impl->ring);

    free(impl);
    free(acc);
}
