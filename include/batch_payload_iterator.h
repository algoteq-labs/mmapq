#ifndef BATCH_PAYLOAD_ITERATOR_H
#define BATCH_PAYLOAD_ITERATOR_H

#include <stdint.h>
#include <stddef.h>
#include "region_accessor.h"
#include "header_codec.h"

/**
 * BatchPayloadIterator — zero-copy flyweight iterator over a single batch
 * in the payload region of an indexed queue.
 *
 * Wire format (per batch in the payload stream):
 *
 *   Offset  Size  Field
 *   ------  ----  -----
 *   +0      4     int32 message_count   — number of messages in the batch
 *   +4      ...   framed messages, each:
 *                   +0  4  int32 length   — payload byte count
 *                   +4  N  payload bytes
 *
 * The iterator provides sequential, zero-copy access to messages. It holds a
 * flyweight Buffer into the mapped payload region — the data pointer returned
 * by batch_iterator_current_data() is valid only until the next call to
 * batch_iterator_advance() or any re-wrap of the underlying region.
 *
 * batch_iterator_current_data() and batch_iterator_current_length() are only
 * valid after a successful call to batch_iterator_advance() (returns 1).
 *
 * WARNING: The iterator struct is designed to be embedded inside pollers,
 * appenders, and reading contexts to achieve zero heap allocation per message.
 * The payload_acc and queue_name fields must be set by the owning object
 * before calling batch_iterator_init().
 *
 * Mirrors Java IBatch / BatchPayloadIterator.
 */

/** Offset of the message count field at the start of each batch. */
#define BATCH_MSG_COUNT_OFFSET    ((int32_t)0)

/** Size in bytes of the message count field. */
#define BATCH_MSG_COUNT_SIZE      ((int32_t)4)

/** Offset of the first message within the batch (immediately after count). */
#define BATCH_FIRST_MSG_OFFSET    ((int32_t)4)

/** Sentinel value returned by batch_iterator_probe_batch when no valid batch found. */
#define BATCH_ITERATOR_INVALID_POSITION ((int64_t)-1)

typedef struct {
    const char     *queue_name;   /* for diagnostic messages */
    RegionAccessor *payload_acc;  /* payload region — set by owning object */
    Buffer          payload_buf;  /* flyweight view into mapped region */

    /* Current batch state */
    int64_t         batch_offset;    /* absolute payload offset of this batch */
    int32_t         message_count;   /* total messages in the batch */

    /* Cursor state — valid after a successful batch_iterator_advance() */
    int32_t         current_msg_index;     /* zero-based index of current message */
    int64_t         current_msg_offset;    /* absolute payload offset of current message */
    int32_t         current_msg_length;    /* payload bytes in current message */
    int64_t         next_message_position; /* absolute offset of the next message */

    int             exhausted; /* 1 after all messages have been visited */
} BatchPayloadIterator;

/**
 * Initialise the iterator for a known batch.
 *
 * Must be called before advancing. payload_acc and queue_name must already
 * be set by the owning object.
 *
 * @param it               the iterator to initialise
 * @param batch_offset     absolute payload offset where the batch begins
 * @param message_count    total messages in the batch; pass -1 to read from disk
 * @param start_msg_index  zero-based message index to start iteration from
 * @return @p it (for chaining)
 */
BatchPayloadIterator *batch_iterator_init(
    BatchPayloadIterator *it,
    int64_t               batch_offset,
    int32_t               message_count,
    int32_t               start_msg_index);

/**
 * Read the message count from the batch header on disk.
 *
 * Used during initialisation when the count is not already known (e.g.
 * in the reader after looking up the header entry).
 *
 * @param it           the iterator (payload_acc must be set)
 * @param batch_offset absolute payload offset of the batch to probe
 * @return the message count, or -1 if the region cannot be mapped
 */
int32_t batch_iterator_read_message_count(BatchPayloadIterator *it, int64_t batch_offset);

/**
 * Check whether a valid batch exists at @p offset, skipping short-padding markers.
 *
 * Used during initialisation of the append position on restart to locate
 * the last committed batch without scanning all headers.
 *
 * @param it      the iterator
 * @param offset  payload offset to probe
 * @return the resolved batch offset, or BATCH_ITERATOR_INVALID_POSITION if none found
 */
int64_t batch_iterator_probe_batch(BatchPayloadIterator *it, int64_t offset);

/**
 * Advance to the next message in the batch.
 *
 * On the first call after batch_iterator_init(), moves to message 0 (or
 * start_msg_index if non-zero). On subsequent calls, moves to the next message.
 *
 * After a successful call, batch_iterator_current_data() and
 * batch_iterator_current_length() return valid values for the current message.
 *
 * @return 1 if advanced successfully; 0 if the batch is exhausted
 */
int batch_iterator_advance(BatchPayloadIterator *it);

/**
 * Return 1 if the iterator is positioned on a valid message (advance() returned 1).
 */
static inline int batch_iterator_has_current(const BatchPayloadIterator *it)
{
    return it->current_msg_index >= 0 && !it->exhausted;
}

/**
 * Return a pointer to the current message's payload bytes.
 *
 * Flyweight — valid only until the next batch_iterator_advance() call or
 * any re-wrap of the underlying region. Do not retain across iterations.
 *
 * Only valid after a successful batch_iterator_advance() call.
 */
static inline const void *batch_iterator_current_data(const BatchPayloadIterator *it)
{
    return it->payload_buf.data + PAYLOAD_DATA_OFFSET;
}

/**
 * Return the byte count of the current message's payload.
 *
 * Only valid after a successful batch_iterator_advance() call.
 */
static inline int32_t batch_iterator_current_length(const BatchPayloadIterator *it)
{
    return it->current_msg_length;
}

/**
 * Skip all remaining messages in the batch, leaving the iterator exhausted.
 *
 * Used to skip remaining messages and position next_message_position at
 * the start of the following batch.
 */
void batch_iterator_exhaust(BatchPayloadIterator *it);

/**
 * Return the byte offset immediately after the last message in the batch.
 *
 * Valid after batch_iterator_exhaust() or after batch_iterator_advance()
 * returns 0. This is the position where the next batch begins in the
 * payload stream.
 *
 * Mirrors Java BatchPayloadIterator.nextPayloadPosition().
 */
static inline int64_t batch_iterator_next_payload_position(const BatchPayloadIterator *it)
{
    return it->next_message_position;
}

#endif /* BATCH_PAYLOAD_ITERATOR_H */
