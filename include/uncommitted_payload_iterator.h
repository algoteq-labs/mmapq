#ifndef UNCOMMITTED_PAYLOAD_ITERATOR_H
#define UNCOMMITTED_PAYLOAD_ITERATOR_H

#include <stdint.h>
#include "region_accessor.h"
#include "payload_iterator.h"
#include "header_codec.h"
#include "mmap_assert.h"

/**
 * UncommittedPayloadIterator — iterate only uncommitted (unconfirmed) messages.
 *
 * Mirrors Java UncommittedPayloadIterator exactly.
 *
 * Used during appender restart recovery. After a crash, the appender's payload
 * file may contain messages that were written but never committed into the header.
 * This iterator skips those that WERE committed (visible in the header) and
 * yields only those that were NOT.
 *
 * ALGORITHM:
 *   At init():
 *     1. Scan the header from index 0 forward.
 *     2. For each header entry matching this appender_id, decode payloadPosition.
 *     3. Walk the payload stream to build contiguous committed RANGES:
 *        [start_offset, end_offset) where all bytes are part of committed messages.
 *     4. Committed ranges are stored in a sorted array (by start_offset).
 *     5. The inner PayloadIterator traverses GAPS between committed ranges.
 *
 *   At advance():
 *     1. Try to advance the inner PayloadIterator in the current gap.
 *     2. If the message end reaches the next committed range start, skip
 *        past that range (and any adjacent ranges) to the next gap.
 *     3. Return 1 if an uncommitted message was found, 0 if exhausted.
 *
 * FROZEN STATE:
 *   end_of_stream is captured at init() time. New appends or commits after
 *   init() are NOT visible. This ensures deterministic recovery behaviour.
 *
 * COMMITTED RANGE STRUCTURE:
 *   Simple dynamic array of (start, end) pairs, sorted by start.
 *   Adjacent ranges are merged during the header scan.
 *   Pairs with end <= start_offset are pruned at init time.
 *
 * CONTRACT:
 *   No message may OVERLAP a committed span. If encountered, abort.
 *   (This would indicate corruption.)
 *
 * Thread confinement: not thread-safe. One iterator per thread.
 */

/* ── Committed range entry ────────────────────────────────────────────────── */

typedef struct {
    int64_t start;  /* first byte of committed region (inclusive) */
    int64_t end;    /* first byte AFTER committed region (exclusive) */
} CommittedRange;

/* ── UncommittedPayloadIterator ──────────────────────────────────────────── */

typedef struct {
    const char     *queue_name;
    uint8_t         appender_id;

    /* Callbacks for stream boundaries */
    int64_t       (*start_of_stream_offset)(void *ctx);
    int64_t       (*end_of_stream_offset)(void *ctx);
    void           *stream_ctx;

    /* Header accessors for committed range detection */
    RegionAccessor *header_accessor;
    Buffer          header_buf;

    /* Inner iterator — walks gaps between committed ranges */
    PayloadIterator *inner;

    /* Committed ranges — sorted ascending by start */
    CommittedRange *ranges;
    int             range_count;
    int             range_capacity;
    int             current_range;  /* index into ranges[] of next committed span */

    /* Snapshot of end-of-stream at init() */
    int64_t         fixed_end_of_stream;

    /* Saved init parameters for rewind() */
    int64_t         init_start_offset;
    int64_t         init_end_offset;
    int             initialised;
} UncommittedPayloadIterator;

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * uncommitted_iterator_create — allocate and configure.
 *
 * queue_name:              diagnostic name
 * appender_id:             which appender's committed spans to skip
 * start_of_stream_offset:  callback returning earliest valid payload offset
 * end_of_stream_offset:    callback returning current append position
 * stream_ctx:              context for both callbacks
 * header_accessor:         RO accessor for the queue header file
 * inner:                   PayloadIterator for gap traversal (ownership NOT taken;
 *                          caller must keep it alive for the iterator's lifetime)
 */
int uncommitted_iterator_create(
    const char     *queue_name,
    uint8_t         appender_id,
    int64_t       (*start_of_stream_offset)(void *ctx),
    int64_t       (*end_of_stream_offset)(void *ctx),
    void           *stream_ctx,
    RegionAccessor *header_accessor,
    PayloadIterator *inner,
    UncommittedPayloadIterator **out);

/**
 * uncommitted_iterator_init — scan header and initialise gap traversal.
 *
 * start_offset: payload start, or PAYLOAD_ITERATOR_NOT_SET
 * end_offset:   payload end (inclusive), or PAYLOAD_ITERATOR_NOT_SET
 *
 * Performs the full header scan at each call. Rewind is equivalent to
 * calling init() again with the same parameters.
 *
 * Returns the iterator (same pointer).
 */
UncommittedPayloadIterator *uncommitted_iterator_init(
    UncommittedPayloadIterator *it,
    int64_t                     start_offset,
    int64_t                     end_offset);

/**
 * uncommitted_iterator_advance — move to the next uncommitted message.
 *
 * Returns 1 if an uncommitted message is available, 0 if exhausted.
 *
 * After a successful advance():
 *   uncommitted_iterator_current() returns the message buffer.
 *   it->inner->current_message_length holds the payload length.
 *   it->inner->current_message_offset holds the length-prefix position.
 */
int uncommitted_iterator_advance(UncommittedPayloadIterator *it);

/**
 * uncommitted_iterator_has_next — returns 1 if advance() would succeed.
 */
int uncommitted_iterator_has_next(const UncommittedPayloadIterator *it);

/**
 * uncommitted_iterator_current — get the current message buffer.
 *
 * Valid only after a successful advance(). Delegates to inner iterator.
 * Aborts if no current message.
 */
Buffer *uncommitted_iterator_current(UncommittedPayloadIterator *it);

/**
 * uncommitted_iterator_rewind — re-init with same parameters.
 *
 * Aborts if not yet initialised.
 */
UncommittedPayloadIterator *uncommitted_iterator_rewind(UncommittedPayloadIterator *it);

/**
 * uncommitted_iterator_destroy — free the iterator and its range array.
 *
 * Does NOT destroy the inner PayloadIterator or header_accessor —
 * those are owned by the caller.
 */
void uncommitted_iterator_destroy(UncommittedPayloadIterator *it);

#endif /* UNCOMMITTED_PAYLOAD_ITERATOR_H */
