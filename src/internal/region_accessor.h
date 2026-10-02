#ifndef REGION_ACCESSOR_H
#define REGION_ACCESSOR_H

#include <stdint.h>
#include <stddef.h>

/**
 * WrapResult — outcome of a region wrap operation.
 *
 * Mirrors Java WrapResult enum exactly.
 * The caller MUST check for SUCCESS before using the buffer.
 * Using a buffer after any non-SUCCESS result is undefined behaviour.
 */
typedef enum {
    WRAP_SUCCESS,          /* buffer wrapped, buffer.capacity valid, safe to use   */
    WRAP_MAP_TIMED_OUT,    /* async mapping did not complete within timeout         */
    WRAP_FILE_NOT_FOUND,   /* backing file does not exist yet                      */
    WRAP_BEYOND_FILE_SIZE, /* position exceeds current file size                   */
    WRAP_UNWRAPPED,        /* buffer explicitly unwrapped (non-queue contexts)      */
} WrapResult;

/**
 * Buffer — flyweight view over a memory-mapped region.
 *
 * Owned by the appender, poller, or reader — NOT by the region.
 * The region populates data and capacity on a successful wrap.
 *
 * capacity: available bytes from data to end of the mapped region.
 *           Equivalent to Java DirectBuffer.capacity() after wrap.
 *           Set by the region on WRAP_SUCCESS only.
 *           May be less than the full region size if the logical position
 *           falls near the end of a region — the queue must skip in this case.
 *
 * data:     pointer into mapped memory at the logical position's offset.
 *           Valid only while the region remains mapped.
 *           Accessing data after a subsequent wrap() call on the same
 *           RegionAccessor is undefined behaviour.
 *
 * CONTRACT (enforced by MMAP_ASSERT in debug builds):
 *   A RegionAccessor must not have two simultaneously valid buffers.
 *   Calling wrap() again invalidates any previously returned buffer.
 *   The owner must not use a buffer after calling wrap() again.
 */
typedef struct {
    uint8_t *data;        /* pointer into mapped memory — NULL until WRAP_SUCCESS */
    int32_t  capacity;    /* available bytes from data — 0 until WRAP_SUCCESS     */
} Buffer;

/**
 * RegionAccessor — the sole contract between the region layer and all
 * higher structures (queue, broadcast ring, metrics ring, etc).
 *
 * Higher structures interact with memory only through this interface.
 * They never see FileMapper, mmap, or region lifecycle details.
 *
 * Two wrap variants:
 *
 *   wrap()           — sequential access. Wraps buffer and triggers
 *                      premap of the next region when approaching a
 *                      boundary. Use for appenders and pollers moving
 *                      forward sequentially.
 *
 *   wrap_no_premap() — random access. Wraps buffer only, no premap.
 *                      Use for random-access readers traversing the
 *                      queue in non-sequential order or in reverse.
 *
 * close() — releases all resources. After close(), wrap() and
 *           wrap_no_premap() must not be called.
 *
 * size() — the region size in bytes. All regions in a ring have the
 *          same size. The queue uses this to compute region boundaries.
 */
typedef struct RegionAccessor {
    WrapResult (*wrap)          (struct RegionAccessor *self, int64_t position, Buffer *buffer);
    WrapResult (*wrap_no_premap)(struct RegionAccessor *self, int64_t position, Buffer *buffer);
    void       (*close)         (struct RegionAccessor *self);
    int32_t    region_size;
    int32_t    mapped_capacity;
    void       *impl;

    /* Fast-path cache fields */
    uint8_t   *active_address;
    int64_t    active_base;
    int64_t    fast_path_end;
} RegionAccessor;

/* ── Convenience wrappers ────────────────────────────────────────────────── */

static inline WrapResult region_accessor_wrap(
    RegionAccessor *acc,
    int64_t         position,
    Buffer         *buffer)
{
    if (__builtin_expect(position >= acc->active_base && position < acc->fast_path_end, 1)) {
        int64_t offset = position - acc->active_base;
        buffer->data = acc->active_address + offset;
        buffer->capacity = acc->mapped_capacity - (int32_t)offset;
        return WRAP_SUCCESS;
    }
    return acc->wrap(acc, position, buffer);
}

static inline WrapResult region_accessor_wrap_no_premap(
    RegionAccessor *acc,
    int64_t         position,
    Buffer         *buffer)
{
    if (__builtin_expect(position >= acc->active_base && position < acc->fast_path_end, 1)) {
        int64_t offset = position - acc->active_base;
        buffer->data = acc->active_address + offset;
        buffer->capacity = acc->mapped_capacity - (int32_t)offset;
        return WRAP_SUCCESS;
    }
    return acc->wrap_no_premap(acc, position, buffer);
}

static inline void region_accessor_close(RegionAccessor *acc)
{
    acc->close(acc);
}

static inline int32_t region_accessor_size(RegionAccessor *acc)
{
    return acc->region_size;
}

#endif /* REGION_ACCESSOR_H */
