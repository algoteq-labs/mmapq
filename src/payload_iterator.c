#include "payload_iterator.h"
#include "payload_walk.h"
#include "mmap_assert.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── payload_iterator_create ─────────────────────────────────────────────── */

PayloadIterator *payload_iterator_create(
    const char     *queue_name,
    RegionAccessor *payload_accessor,
    int64_t       (*start_of_stream_offset)(void *ctx),
    int64_t       (*end_of_stream_offset)(void *ctx),
    void           *stream_ctx,
    int             fix_end_of_stream)
{
    if (queue_name == NULL) return NULL;
    if (payload_accessor == NULL) return NULL;
    if (start_of_stream_offset == NULL) return NULL;
    if (end_of_stream_offset == NULL) return NULL;

    PayloadIterator *it = calloc(1, sizeof(PayloadIterator));
    if (it == NULL) return NULL;

    it->queue_name             = queue_name;
    it->payload_accessor       = payload_accessor;
    it->start_of_stream_offset = start_of_stream_offset;
    it->end_of_stream_offset   = end_of_stream_offset;
    it->stream_ctx             = stream_ctx;
    it->fix_end_of_stream      = fix_end_of_stream;

    /* Mark as uninitialised */
    it->next_probing_offset    = PAYLOAD_ITERATOR_NOT_SET;
    it->current_message_offset = PAYLOAD_ITERATOR_NOT_SET;
    it->current_message_length = (int32_t)PAYLOAD_ITERATOR_NOT_SET;
    it->start_offset           = PAYLOAD_ITERATOR_NOT_SET;
    it->end_offset             = PAYLOAD_ITERATOR_NOT_SET;
    it->end_of_stream_at_init  = PAYLOAD_ITERATOR_NOT_SET;

    return it;
}

/* ── payload_iterator_init ───────────────────────────────────────────────── */

PayloadIterator *payload_iterator_init(
    PayloadIterator *it,
    int64_t          start_offset,
    int64_t          end_offset)
{
    MMAP_ASSERT(it != NULL, "payload_iterator_init: it is NULL");

    int64_t start_of_stream = it->start_of_stream_offset(it->stream_ctx);

    /* Resolve start_offset */
    if (start_offset == PAYLOAD_ITERATOR_NOT_SET) {
        it->start_offset = start_of_stream;
    } else {
        if (start_offset < start_of_stream) { free(it); return NULL; }
        it->start_offset = start_offset;
    }

    /* Validate end_offset */
    if (end_offset != PAYLOAD_ITERATOR_NOT_SET) {
        if (end_offset < start_of_stream) { free(it); return NULL; }
    }
    it->end_offset = end_offset;

    /* Capture end-of-stream snapshot for fix_end_of_stream mode */
    it->end_of_stream_at_init = it->end_of_stream_offset(it->stream_ctx);

    /* Reset traversal state */
    it->next_probing_offset    = it->start_offset;
    it->current_message_offset = PAYLOAD_ITERATOR_NOT_SET;
    it->current_message_length = (int32_t)PAYLOAD_ITERATOR_NOT_SET;

    return it;
}

/* ── Effective end of stream ─────────────────────────────────────────────── */

static inline int64_t effective_end(const PayloadIterator *it)
{
    return it->fix_end_of_stream
        ? it->end_of_stream_at_init
        : it->end_of_stream_offset(it->stream_ctx);
}

/* ── payload_iterator_has_next ───────────────────────────────────────────── */

int payload_iterator_has_next(const PayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "payload_iterator_has_next: it is NULL");

    if (it->next_probing_offset == PAYLOAD_ITERATOR_NOT_SET) return 0;
    if (it->next_probing_offset >= effective_end(it)) return 0;
    if (it->end_offset != PAYLOAD_ITERATOR_NOT_SET &&
        it->next_probing_offset > it->end_offset) return 0;
    return 1;
}

/* ── payload_iterator_advance ────────────────────────────────────────────── */

int payload_iterator_advance(PayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "payload_iterator_advance: it is NULL");

    if (!payload_iterator_has_next(it)) return 0;

    int64_t  message_offset = PAYLOAD_ITERATOR_NOT_SET;
    int32_t  length;

    length = payload_walk_advance(
        it->queue_name,
        &it->next_probing_offset,
        &message_offset,
        it->payload_accessor,
        &it->payload_buf);

    if (length == 0) {
        /* End of stream within the region */
        return 0;
    }

    MMAP_ASSERT(length > 0,
        "payload_iterator_advance: payload_walk returned negative length");

    it->current_message_offset = message_offset;
    it->current_message_length = length;

    /* Point message_buf at the payload bytes (after the 4-byte length prefix) */
    it->message_buf.data     = it->payload_buf.data + PAYLOAD_DATA_OFFSET;
    it->message_buf.capacity = length;

    return 1;
}

/* ── payload_iterator_current ────────────────────────────────────────────── */

Buffer *payload_iterator_current(PayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "payload_iterator_current: it is NULL");
    MMAP_ASSERT(it->current_message_length != (int32_t)PAYLOAD_ITERATOR_NOT_SET,
        "payload_iterator_current: no current message — call advance() first");

    /* message_buf was set by advance(); return pointer to it */
    return &it->message_buf;
}

/* ── payload_iterator_rewind ─────────────────────────────────────────────── */

PayloadIterator *payload_iterator_rewind(PayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "payload_iterator_rewind: it is NULL");
    MMAP_ASSERT(it->start_offset != PAYLOAD_ITERATOR_NOT_SET,
        "payload_iterator_rewind: iterator not yet initialised");

    it->next_probing_offset    = it->start_offset;
    it->current_message_offset = PAYLOAD_ITERATOR_NOT_SET;
    it->current_message_length = (int32_t)PAYLOAD_ITERATOR_NOT_SET;

    return it;
}

/* ── payload_iterator_destroy ────────────────────────────────────────────── */

void payload_iterator_destroy(PayloadIterator *it)
{
    if (it == NULL) return;
    /* payload_accessor is NOT owned by the iterator */
    free(it);
}
