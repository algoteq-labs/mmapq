#ifndef MMAPQ_BATCH_PAYLOAD_ITERATOR_H
#define MMAPQ_BATCH_PAYLOAD_ITERATOR_H

#include <stdint.h>
#include <stddef.h>

/**
 * MmapqBatchPayloadIterator — zero-copy flyweight iterator over a single batch
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
 * The type is opaque to binding code. Access through the out-of-line
 * functions declared below. Internal C code may include the private
 * header src/internal/batch_payload_iterator_internal.h for zero-cost
 * inline access.
 *
 * mmapq_batch_iterator_current_data_ffi() and mmapq_batch_iterator_current_length_ffi()
 * are only valid after a successful call to mmapq_batch_iterator_advance() (returns 1).
 *
 * Mirrors Java IBatch / MmapqBatchPayloadIterator.
 */

/** Offset of the message count field at the start of each batch. */
#define MMAPQ_BATCH_MSG_COUNT_OFFSET    ((int32_t)0)

/** Size in bytes of the message count field. */
#define MMAPQ_BATCH_MSG_COUNT_SIZE      ((int32_t)4)

/** Offset of the first message within the batch (immediately after count). */
#define MMAPQ_BATCH_FIRST_MSG_OFFSET    ((int32_t)4)

/** Sentinel value returned by mmapq_batch_iterator_probe_batch when no valid batch found. */
#define MMAPQ_BATCH_ITERATOR_INVALID_POSITION ((int64_t)-1)

typedef struct MmapqBatchPayloadIterator MmapqBatchPayloadIterator;

/**
 * Initialise the iterator for a known batch.
 *
 * Must be called before advancing. payload_acc and queue_name must already
 * be set by the owning object.
 *
 * it:              the iterator to initialise
 * batch_offset:    absolute payload offset where the batch begins
 * message_count:   total messages in the batch; pass -1 to read from disk
 * start_msg_index: zero-based message index to start iteration from
 * Returns: @p it (for chaining)
 */
MmapqBatchPayloadIterator *mmapq_batch_iterator_init(
    MmapqBatchPayloadIterator *it,
    int64_t               batch_offset,
    int32_t               message_count,
    int32_t               start_msg_index);

/**
 * Read the message count from the batch header on disk.
 *
 * Used during initialisation when the count is not already known (e.g.
 * in the reader after looking up the header entry).
 *
 * it:          the iterator (payload_acc must be set)
 * batch_offset: absolute payload offset of the batch to probe
 * Returns: the message count, or -1 if the region cannot be mapped
 */
int32_t mmapq_batch_iterator_read_message_count(MmapqBatchPayloadIterator *it, int64_t batch_offset);

/**
 * Check whether a valid batch exists at @p offset, skipping short-padding markers.
 *
 * Used during initialisation of the append position on restart to locate
 * the last committed batch without scanning all headers.
 *
 * it:     the iterator
 * offset: payload offset to probe
 * Returns: the resolved batch offset, or MMAPQ_BATCH_ITERATOR_INVALID_POSITION if none found
 */
int64_t mmapq_batch_iterator_probe_batch(MmapqBatchPayloadIterator *it, int64_t offset);

/**
 * Advance to the next message in the batch.
 *
 * Boolean stepping API retained for compatibility and internal C callers.
 * Unlike mmapq_batch_iterator_next(), this function returns 0 for both
 * exhaustion and failure; it never returns a negative error code.
 *
 * On the first call after mmapq_batch_iterator_init(), moves to message 0 (or
 * start_msg_index if non-zero). On subsequent calls, moves to the next message.
 *
 * After a successful call, mmapq_batch_iterator_current_data_ffi() and
 * mmapq_batch_iterator_current_length_ffi() return valid values for the current message.
 *
 * Returns: 1 if advanced successfully; 0 if exhausted or invalid data/mapping
 * prevented advancement. A reader's last_error reports iteration failures;
 * a poller reports them after its handler returns. Discard partially read
 * batches on error.
 */
int mmapq_batch_iterator_advance(MmapqBatchPayloadIterator *it);

/**
 * Advance once and return 1 for a message, 0 for exhaustion, or an MMAPQ_*
 * error. Errors remain sticky until reinitialisation. Outputs are cleared
 * on exhaustion/error. A successful payload remains borrowed until another
 * advance, reinitialisation, accessor reposition, or owner close.
 *
 * Checked alternative to mmapq_batch_iterator_advance(), intended for callers
 * that need to distinguish exhaustion from failure directly, including FFI
 * bindings. Uses the same advancement logic and supplies the borrowed data
 * and length in one call, without payload copying or allocation. Do not call
 * advance() first: this function itself advances to the next message.
 * Test the return value explicitly: 1 is a message, 0 is exhaustion, and a
 * negative value is an error, not a successful step.
 *
 * iterator: iterator owned by the reader or poller
 * data:     required output pointer to borrowed payload bytes
 * length:   required output pointer to positive payload length
 */
int mmapq_batch_iterator_next(MmapqBatchPayloadIterator *iterator,
                             const void **data, int32_t *length);

/**
 * Return 1 if the iterator is positioned on a valid message (advance() returned 1).
 */
int mmapq_batch_iterator_has_current_ffi(const MmapqBatchPayloadIterator *it);

/**
 * Return a pointer to the current message's payload bytes.
 *
 * Flyweight — valid only until the next mmapq_batch_iterator_advance() call or
 * any re-wrap of the underlying region. Do not retain across iterations.
 *
 * Only valid after a successful mmapq_batch_iterator_advance() call.
 */
const void *mmapq_batch_iterator_current_data_ffi(const MmapqBatchPayloadIterator *it);

/**
 * Return the byte count of the current message's payload.
 *
 * Only valid after a successful mmapq_batch_iterator_advance() call.
 */
int32_t mmapq_batch_iterator_current_length_ffi(const MmapqBatchPayloadIterator *it);

/**
 * Return the byte offset immediately after the last message in the batch.
 *
 * Valid after mmapq_batch_iterator_exhaust() or after mmapq_batch_iterator_advance()
 * returns 0. This is the position where the next batch begins in the
 * payload stream.
 *
 * Mirrors Java MmapqBatchPayloadIterator.nextPayloadPosition().
 */
int64_t mmapq_batch_iterator_next_payload_position_ffi(const MmapqBatchPayloadIterator *it);

/**
 * Skip all remaining messages in the batch, leaving the iterator exhausted.
 *
 * Used to skip remaining messages and position next_message_position at
 * the start of the following batch.
 */
void mmapq_batch_iterator_exhaust(MmapqBatchPayloadIterator *it);

#endif /* MMAPQ_BATCH_PAYLOAD_ITERATOR_H */
