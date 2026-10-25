#ifndef UNCOMMITTED_PAYLOAD_ITERATOR_H
#define UNCOMMITTED_PAYLOAD_ITERATOR_H

#include <stdint.h>
#include "internal/region_accessor.h"
#include "internal/committed_ranges.h"
#include "payload_iterator.h"
#include "header_codec.h"
#include "mmap_assert.h"

/**
 * MmapqUncommittedPayloadIterator — iterate only uncommitted (unconfirmed) messages.
 *
 * Mirrors Java MmapqUncommittedPayloadIterator exactly.
 *
 * Used during appender restart recovery. After a crash, the appender's payload
 * file may contain messages that were written but never committed into the header.
 * This iterator skips those that WERE committed (visible in the header) and
 * yields only those that were NOT.
 *
 * ALGORITHM:
 *   At init():
 *     1. Acquire-load the queue's start index FIRST, then the appender's
 *        archive line (appender_meta archive_offset) — the D-G publication
 *        rule (CAND-49 §3.9; doc/design/archive-boundaries.md). Reading the
 *        line before si is the defect. A zero line (< INITIAL_PAYLOAD_POSITION)
 *        means a corrupt appender meta: init refuses with MMAPQ_ECORRUPT
 *        (pre-0.2.0 queues are refused at open with MMAPQ_ESCHEMA).
 *     2. Scan the header from the frozen start index forward (D8/S3 —
 *        entries below it are archived by definition and their files may
 *        have been reclaimed).
 *     3. For each header entry matching this appender_id, decode payloadPosition.
 *     4. Walk the payload stream to measure contiguous committed RANGES:
 *        [start_offset, end_offset) where all bytes are part of committed messages.
 *        Spans are appended raw (header order is commit order — D-B), then
 *        sort+merged once (mmapqi_ranges_sort_merge).
 *     5. The inner MmapqPayloadIterator traverses GAPS between committed ranges,
 *        starting at the archive line (never below it — a caller-supplied
 *        start_offset below the line clamps up to the line).
 *
 *   At advance():
 *     1. Bounded-gap pre-check: next_probing_offset == current_gap_end means
 *        the gap is exhausted — switch gaps without probing at the committed
 *        range's start (D-A).
 *     2. Try to advance the inner MmapqPayloadIterator in the current gap.
 *     3. A yielded message must sit cleanly before the next committed range
 *        (overshoot or overlap is MMAPQ_ECORRUPT — stricter than Java, which
 *        silently skips); a bounded gap that ends short of current_gap_end
 *        is torn framing, also MMAPQ_ECORRUPT (previously silent EOS).
 *     4. On a yield, refill the owning appender's outstanding set via the
 *        refill callback (the appender-level helper skips the flag-tracked
 *        tail, §3.3). ENOMEM leaves the iterator parked at the same message:
 *        the next advance re-walks and re-yields it (ruling 5).
 *     5. Return 1 if an uncommitted message was found, 0 if exhausted.
 *
 * FROZEN STATE:
 *   end_of_stream, the queue start index, and the archive line are captured
 *   at init() time. New appends or commits after init() are NOT visible.
 *   This ensures deterministic recovery behaviour.
 *
 * COMMITTED RANGE STRUCTURE:
 *   Dynamic array of (start, end) pairs (CommittedRanges, shared with the
 *   batch walker), sorted ascending by start after the init-time
 *   sort+merge. Pairs with end <= effective start are pruned at init time.
 *
 * CONTRACT:
 *   No message may OVERLAP a committed span. If encountered, return
 *   MMAPQ_ECORRUPT (both build configs — a runtime check, not an assert).
 *
 * Thread confinement: not thread-safe. One iterator per thread.
 */

/* ── MmapqUncommittedPayloadIterator ──────────────────────────────────────────── */
/* Completes the forward declaration from include/indexed_appender.h */

typedef struct MmapqUncommittedPayloadIterator {
    const char     *queue_name;
    uint8_t         appender_id;

    /* Callbacks for stream boundaries */
    int64_t       (*start_of_stream_offset)(void *ctx);
    int64_t       (*end_of_stream_offset)(void *ctx);
    void           *stream_ctx;

    /* Archive line supplier (CAND-49 §3.9): appender_meta archive_offset.
     * Read at init AFTER queue_start_index — the D-G publication rule. */
    int64_t       (*archive_offset)(void *ctx);

    /* Queue start index supplier (D8/S3): the header scan's floor. Read once
     * per init, BEFORE archive_offset, and frozen into the scan. */
    int64_t       (*queue_start_index)(void *ctx);
    void           *queue_start_index_ctx;

    /* Refill callback (CAND-49 §3.3): invoked with each yielded offset to
     * make it committable; 0 or MMAPQ_ENOMEM. NULL-agnostic (create
     * validates). */
    int           (*refill)(void *ctx, int64_t offset);
    void           *refill_ctx;

    /* Header accessors for committed range detection */
    RegionAccessor *header_accessor;
    Buffer          header_buf;

    /* Inner iterator — walks gaps between committed ranges */
    MmapqPayloadIterator *inner;

    /* Committed ranges — sorted ascending by start (sort+merged at init) */
    CommittedRanges ranges;
    int             current_range;  /* index into ranges.v[] of next committed span */

    /* Explicit bounded-gap boundary (CAND-49 §3.2): the exclusive end of the
     * current gap; PAYLOAD_ITERATOR_NOT_SET for the trailing gap (ends at
     * fixed_end_of_stream). The inner iterator never probes at this offset. */
    int64_t         current_gap_end;

    /* Snapshot of the archive line at init() — the gap walk's floor */
    int64_t         archive_line;

    /* Snapshot of end-of-stream at init() */
    int64_t         fixed_end_of_stream;

    /* Saved init parameters for rewind() */
    int64_t         init_start_offset;
    int64_t         init_end_offset;
    int             initialised;
} MmapqUncommittedPayloadIterator;

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapqi_uncommitted_iterator_create — allocate and configure.
 *
 * queue_name:              diagnostic name
 * appender_id:             which appender's committed spans to skip
 * start_of_stream_offset:  callback returning earliest valid payload offset
 * end_of_stream_offset:    callback returning current append position
 * stream_ctx:              context for both callbacks and archive_offset
 * archive_offset:          callback returning the appender's archive line
 *                          (appender_meta archive_offset, CAND-49 §3.9)
 * header_accessor:         RO accessor for the queue header file
 * inner:                   MmapqPayloadIterator for gap traversal (ownership NOT taken;
 *                          caller must keep it alive for the iterator's lifetime)
 * queue_start_index:       callback returning the queue's current start index —
 *                          the header scan's floor (D8/S3)
 * queue_start_index_ctx:   context for queue_start_index
 * refill:                  callback making a yielded offset committable
 *                          (0 or MMAPQ_ENOMEM); the appender-level helper
 *                          skips the flag-tracked tail (§3.3)
 * refill_ctx:              context for refill
 *
 * Retry note (D8): after a FAILED init (e.g. a transient MMAPQ_ETIMEDOUT),
 * rewind() returns MMAPQ_EINVAL — the retry path is a NEW factory call
 * (mmapq_indexed_appender_uncommitted_iterator_checked), not rewind.
 */
int mmapqi_uncommitted_iterator_create(
    const char     *queue_name,
    uint8_t         appender_id,
    int64_t       (*start_of_stream_offset)(void *ctx),
    int64_t       (*end_of_stream_offset)(void *ctx),
    void           *stream_ctx,
    int64_t       (*archive_offset)(void *ctx),
    RegionAccessor *header_accessor,
    MmapqPayloadIterator *inner,
    int64_t       (*queue_start_index)(void *ctx),
    void           *queue_start_index_ctx,
    int           (*refill)(void *ctx, int64_t offset),
    void           *refill_ctx,
    MmapqUncommittedPayloadIterator **out);

/**
 * mmapqi_uncommitted_iterator_init — scan header and initialise gap traversal.
 *
 * start_offset: payload start, or PAYLOAD_ITERATOR_NOT_SET
 * end_offset:   payload end (inclusive), or PAYLOAD_ITERATOR_NOT_SET
 *
 * Snapshot discipline (D-G, CAND-49 §3.9): acquire-loads the queue start
 * index FIRST, then reads the archive line (never start_offset); the gap
 * walk starts at the archive line — a caller-supplied start_offset below
 * the line clamps up to the line.
 *
 * Performs the full header scan at each call. Rewind is equivalent to
 * calling init() again with the same parameters.
 *
 * Returns 0 on success; MMAPQ_EINVAL if it is NULL, an explicit offset
 * is below start_of_stream_offset(), or start_offset > end_offset when both
 * are set (Java init parity); MMAPQ_ETIMEDOUT on a transient mapping
 * timeout during the scan (retryable — via a NEW factory call, see create);
 * MMAPQ_ECORRUPT if the header scan was genuinely truncated (a committed
 * header whose payload would not walk for a non-timeout reason) or the
 * archive line is zero — a corrupt appender meta (pre-0.2.0 queues are
 * refused at open with MMAPQ_ESCHEMA). On failure
 * the iterator is left uninitialised; the caller-owned inner iterator is
 * never freed and remains valid.
 */
int mmapqi_uncommitted_iterator_init(
    MmapqUncommittedPayloadIterator *it,
    int64_t                     start_offset,
    int64_t                     end_offset);

/**
 * mmapqi_uncommitted_iterator_advance — move to the next uncommitted message.
 *
 * Three-way contract (D8):
 *   Returns 1 if an uncommitted message is available.
 *   Returns 0 if exhausted (no more gaps).
 *   Returns a negative MMAPQ_E* code propagated from the inner payload
 *   iterator — MMAPQ_ETIMEDOUT on a transient mapping timeout (retryable:
 *   iterator state is unchanged, the next advance() retries), MMAPQ_ENOMEM
 *   on a refill allocation failure (state unchanged: the next advance()
 *   re-yields the same message), or MMAPQ_ECORRUPT on gap-boundary
 *   corruption (overshoot, overlap, or torn framing — CAND-49 §3.2).
 *   A negative is truthy in C: callers must compare explicitly, never
 *   `while (advance(...))`.
 *
 * After a successful advance():
 *   mmapqi_uncommitted_iterator_current() returns the message buffer.
 *   it->inner->current_message_length holds the payload length.
 *   it->inner->current_message_offset holds the length-prefix position.
 */
int mmapqi_uncommitted_iterator_advance(MmapqUncommittedPayloadIterator *it);

/**
 * mmapqi_uncommitted_iterator_has_next — returns 1 if advance() would succeed.
 */
int mmapqi_uncommitted_iterator_has_next(const MmapqUncommittedPayloadIterator *it);

/**
 * mmapqi_uncommitted_iterator_current — get the current message buffer.
 *
 * Valid only after a successful advance(). Delegates to inner iterator.
 * Aborts if no current message.
 */
Buffer *mmapqi_uncommitted_iterator_current(MmapqUncommittedPayloadIterator *it);

/**
 * mmapqi_uncommitted_iterator_rewind — re-init with same parameters.
 *
 * Returns 0 on success; MMAPQ_EINVAL if it is NULL or not yet initialised;
 * otherwise the result of the re-run init(). After a FAILED init the
 * iterator is not initialised, so rewind returns MMAPQ_EINVAL — retry
 * after a transient MMAPQ_ETIMEDOUT via a NEW factory call, not rewind
 * (D8, probe P5).
 */
int mmapqi_uncommitted_iterator_rewind(MmapqUncommittedPayloadIterator *it);

/**
 * mmapqi_uncommitted_iterator_destroy — free the iterator and its range array.
 *
 * Does NOT destroy the inner MmapqPayloadIterator or header_accessor —
 * those are owned by the caller.
 */
void mmapqi_uncommitted_iterator_destroy(MmapqUncommittedPayloadIterator *it);

#endif /* UNCOMMITTED_PAYLOAD_ITERATOR_H */
