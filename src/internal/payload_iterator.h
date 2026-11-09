#ifndef PAYLOAD_ITERATOR_H
#define PAYLOAD_ITERATOR_H

#include <stdint.h>
#include "internal/region_accessor.h"
#include "header_codec.h"
#include "mmap_assert.h"

/**
 * MmapqPayloadIterator — zero-copy sequential iterator over a payload stream.
 *
 * Flyweight pattern — the Buffer returned by
 * current() points directly into mapped memory and is invalidated by the
 * next advance() call.
 *
 * Lifecycle:
 *   1. mmapqi_payload_iterator_init()     — set start/end offsets, reset state
 *   2. mmapqi_payload_iterator_advance()  — move to next message, returns 1 if valid
 *   3. mmapqi_payload_iterator_current()  — get flyweight buffer for current message
 *   4. mmapqi_payload_iterator_rewind()   — reset to initial start_offset
 *   5. payload_iterator_close()    — release resources
 *
 * Thread confinement:
 *   Not thread-safe. One iterator per thread.
 *
 * Live vs fixed end-of-stream (fix_end_of_stream flag):
 *   fix_end_of_stream = 0: advance() sees new messages appended after init().
 *   fix_end_of_stream = 1: advance() is bounded to end_of_stream at init time.
 *                          Used by MmapqUncommittedPayloadIterator for deterministic
 *                          recovery — does not observe new commits after init.
 *
 * NOT_SET sentinel: use PAYLOAD_ITERATOR_NOT_SET (-1) for unset offsets.
 *   Passing NOT_SET as start_offset defaults to start_of_stream_offset.
 *   Passing NOT_SET as end_offset defaults to live end-of-stream.
 */

#define PAYLOAD_ITERATOR_NOT_SET  ((int64_t)-1)

/* Completes the forward declaration from include/indexed_appender.h */
typedef struct MmapqPayloadIterator {
    /* Configuration — set at construction */
    const char     *queue_name;
    RegionAccessor *payload_accessor;
    int             fix_end_of_stream;   /* 1 = snapshot at init, 0 = live */

    /* Callbacks into the owning context */
    int64_t (*start_of_stream_offset)(void *ctx); /* earliest valid offset */
    int64_t (*end_of_stream_offset)(void *ctx);   /* current append position */
    void    *stream_ctx;

    /* State — reset by init() and rewind() */
    int64_t start_offset;             /* effective start (after NOT_SET resolved) */
    int64_t end_offset;               /* caller-supplied end, or NOT_SET          */
    int64_t end_of_stream_at_init;    /* snapshot of end-of-stream at init time   */
    int64_t next_probing_offset;      /* position of next message probe            */
    int64_t current_message_offset;   /* position of current message length prefix */
    int32_t current_message_length;   /* length of current message payload         */

    /* Flyweight buffers — owned by caller, filled by advance() */
    Buffer  payload_buf;              /* scratch buffer for region access          */
    Buffer  message_buf;              /* view of current message (subset of payload_buf) */
} MmapqPayloadIterator;

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapqi_payload_iterator_create — allocate and zero a MmapqPayloadIterator.
 *
 * queue_name:              used in error messages
 * payload_accessor:        RegionAccessor for this appender's payload file
 * start_of_stream_offset:  callback returning earliest valid payload position
 * end_of_stream_offset:    callback returning current append position
 * stream_ctx:              opaque context for both callbacks
 * fix_end_of_stream:       1 = snapshot end-of-stream at init, 0 = live
 * out:                     out-parameter for the created iterator (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL args,
 * MMAPQ_ENOMEM on allocation failure. Sets *out to NULL on failure.
 */
int mmapqi_payload_iterator_create(
    const char     *queue_name,
    RegionAccessor *payload_accessor,
    int64_t       (*start_of_stream_offset)(void *ctx),
    int64_t       (*end_of_stream_offset)(void *ctx),
    void           *stream_ctx,
    int             fix_end_of_stream,
    MmapqPayloadIterator **out);

/**
 * mmapqi_payload_iterator_init — initialise the iterator to a range.
 *
 * start_offset: first position to probe, or PAYLOAD_ITERATOR_NOT_SET
 *               to start from start_of_stream_offset().
 * end_offset:   last position to probe (inclusive), or PAYLOAD_ITERATOR_NOT_SET
 *               to iterate to current end-of-stream.
 *
 * After init, call advance() to position at the first message.
 *
 * Returns 0 on success, MMAPQ_EINVAL if it is NULL or an explicit offset
 * is below start_of_stream_offset(). On failure the iterator is left
 * unchanged and remains owned by the caller — init never frees it and
 * never takes ownership of it.
 */
int mmapqi_payload_iterator_init(
    MmapqPayloadIterator *it,
    int64_t          start_offset,
    int64_t          end_offset);

/**
 * mmapqi_payload_iterator_advance — move to the next message in the stream.
 *
 * Three-way contract (D8):
 *   Returns 1 if a message is available (current_message_length > 0).
 *   Returns 0 if end of stream reached.
 *   Returns a negative MMAPQ_E* code propagated from the payload walk —
 *   MMAPQ_ETIMEDOUT on a transient mapping timeout (retryable; the iterator
 *   state is unchanged and the next advance() retries the same position),
 *   MMAPQ_ECORRUPT on corrupt on-disk data. A negative is truthy in C:
 *   callers must compare explicitly, never `if (advance(...))`.
 *
 * After a successful advance():
 *   mmapqi_payload_iterator_current() returns the message buffer.
 *   it->current_message_length holds the payload byte count.
 *   it->current_message_offset holds the length-prefix position.
 *   it->next_probing_offset holds the position after this message.
 */
int mmapqi_payload_iterator_advance(MmapqPayloadIterator *it);

/**
 * mmapqi_payload_iterator_has_next — returns 1 if advance() would succeed.
 *
 * Does NOT consume the message. Use this for hasNext() checks.
 */
int mmapqi_payload_iterator_has_next(const MmapqPayloadIterator *it);

/**
 * mmapqi_payload_iterator_current — get the current message as a Buffer.
 *
 * Valid only after a successful advance(). The buffer points directly
 * into mapped memory and is invalidated by the next advance() call.
 *
 * Returns a pointer to the internal message_buf.
 * Aborts if no current message exists.
 */
Buffer *mmapqi_payload_iterator_current(MmapqPayloadIterator *it);

/**
 * mmapqi_payload_iterator_rewind — reset to the state after the last init() call.
 *
 * Allows re-traversal of the same range without re-initialising.
 *
 * Returns 0 on success, MMAPQ_EINVAL if it is NULL or not yet initialised.
 */
int mmapqi_payload_iterator_rewind(MmapqPayloadIterator *it);

/**
 * mmapqi_payload_iterator_destroy — free a MmapqPayloadIterator.
 *
 * Does NOT close the payload_accessor — that is owned by QueueRegionAccessor.
 */
void mmapqi_payload_iterator_destroy(MmapqPayloadIterator *it);

#endif /* PAYLOAD_ITERATOR_H */
