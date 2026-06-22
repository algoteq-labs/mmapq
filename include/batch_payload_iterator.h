#ifndef BATCH_PAYLOAD_ITERATOR_H
#define BATCH_PAYLOAD_ITERATOR_H

#include <stdint.h>
#include <stddef.h>
#include "region_accessor.h"
#include "header_codec.h"

/**
 * BatchPayloadIterator — zero-copy flyweight iterator over a single batch in an indexed queue.
 *
 * framing (per batch):
 *   +0: int32_t message_count
 *   +4: framed messages...
 *
 * Each message within the batch has the standard framing:
 *   +0: int32_t length
 *   +4: payload bytes...
 *
 * Reusable and designed to be embedded inside Poller/Appender for zero allocation.
 */

#define BATCH_MSG_COUNT_OFFSET    ((int32_t)0)
#define BATCH_MSG_COUNT_SIZE      ((int32_t)4)
#define BATCH_FIRST_MSG_OFFSET    ((int32_t)4)

#define BATCH_ITERATOR_INVALID_POSITION ((int64_t)-1)

typedef struct {
    const char     *queue_name;
    RegionAccessor *payload_acc;
    Buffer          payload_buf;
    
    /* Current batch state */
    int64_t         batch_offset;
    int32_t         message_count;
    
    /* Cursor state */
    int32_t         current_msg_index;
    int64_t         current_msg_offset;
    int32_t         current_msg_length;
    int64_t         next_message_position;
    
    int             exhausted;
} BatchPayloadIterator;

/**
 * batch_iterator_init — initialize iterator for a known batch.
 *
 * batch_offset: absolute offset in the payload stream where the batch begins.
 * message_count: total messages in the batch (if known, else pass -1 to read from disk).
 * start_msg_index: index to start iteration from (typically 0).
 */
BatchPayloadIterator *batch_iterator_init(
    BatchPayloadIterator *it,
    int64_t               batch_offset,
    int32_t               message_count,
    int32_t               start_msg_index);

/**
 * batch_iterator_read_message_count — read count from batch head on disk.
 */
int32_t batch_iterator_read_message_count(BatchPayloadIterator *it, int64_t batch_offset);

/**
 * batch_iterator_probe_batch — check if a valid batch exists at offset.
 * Returns the resolved batch offset, or BATCH_ITERATOR_INVALID_POSITION.
 */
int64_t batch_iterator_probe_batch(BatchPayloadIterator *it, int64_t offset);

/**
 * batch_iterator_advance — move to the next message in the batch.
 * Returns 1 if successful, 0 if exhausted.
 */
int batch_iterator_advance(BatchPayloadIterator *it);

/**
 * batch_iterator_has_current — true if advance() was successful.
 */
static inline int batch_iterator_has_current(const BatchPayloadIterator *it)
{
    return it->current_msg_index >= 0 && !it->exhausted;
}

/**
 * batch_iterator_current_data — pointer to current message payload.
 */
static inline const void *batch_iterator_current_data(const BatchPayloadIterator *it)
{
    return it->payload_buf.data + PAYLOAD_DATA_OFFSET;
}

/**
 * batch_iterator_current_length — length of current message payload.
 */
static inline int32_t batch_iterator_current_length(const BatchPayloadIterator *it)
{
    return it->current_msg_length;
}

/**
 * batch_iterator_exhaust — skip all remaining messages in the batch.
 */
void batch_iterator_exhaust(BatchPayloadIterator *it);

#endif /* BATCH_PAYLOAD_ITERATOR_H */
