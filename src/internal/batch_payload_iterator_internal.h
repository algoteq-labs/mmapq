#ifndef BATCH_PAYLOAD_ITERATOR_INTERNAL_H
#define BATCH_PAYLOAD_ITERATOR_INTERNAL_H

/**
 * Internal (private) header — full definition of MmapqBatchPayloadIterator.
 *
 * This header exposes the struct layout and inline accessors for C code
 * inside this library. Bindings (Rust FFI, Python cffi) MUST NOT include
 * this file — they see only the opaque typedef in the public header.
 *
 * Include from .c files only, never from public headers.
 */

#include "batch_payload_iterator.h"
#include "internal/region_accessor.h"
#include "header_codec.h"

/* Complete the forward declaration from batch_payload_iterator.h */
struct MmapqBatchPayloadIterator {
    const char     *queue_name;
    RegionAccessor *payload_acc;
    Buffer          payload_buf;

    int64_t         batch_offset;
    int32_t         message_count;

    int32_t         current_msg_index;
    int64_t         current_msg_offset;
    int32_t         current_msg_length;
    int64_t         next_message_position;

    int             exhausted;
    int             last_error;

    /* CAND-58: when set (the writer walks' iterators only), a wrap that
     * misses the file reports MMAPQI_WALK_MAP_MISS (walk_classify.h) instead
     * of collapsing to MMAPQ_ECORRUPT — the walk's caller classifies. Never
     * set on reader/poller iterators; their miss-as-ECORRUPT is unchanged. */
    int             walk_mode;
};

static inline int batch_iterator_has_current(const MmapqBatchPayloadIterator *it)
{
    return it->current_msg_index >= 0 && !it->exhausted;
}

static inline const void *batch_iterator_current_data(const MmapqBatchPayloadIterator *it)
{
    return it->payload_buf.data + PAYLOAD_DATA_OFFSET;
}

static inline int32_t batch_iterator_current_length(const MmapqBatchPayloadIterator *it)
{
    return it->current_msg_length;
}

static inline int64_t batch_iterator_next_payload_position(const MmapqBatchPayloadIterator *it)
{
    return it->next_message_position;
}

#endif /* BATCH_PAYLOAD_ITERATOR_INTERNAL_H */
