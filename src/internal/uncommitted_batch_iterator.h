#ifndef UNCOMMITTED_BATCH_ITERATOR_H
#define UNCOMMITTED_BATCH_ITERATOR_H

#include <stdint.h>
#include "internal/region_accessor.h"
#include "internal/committed_ranges.h"
#include "internal/payload_iterator.h"   /* PAYLOAD_ITERATOR_NOT_SET */
#include "internal/batch_payload_iterator_internal.h"
#include "header_codec.h"
#include "mmap_assert.h"

/**
 * MmapqUncommittedBatchIterator — iterate only uncommitted BATCHES
 * (CAND-49 §3.6; C-first — the Java batch appender has no uncommitted
 * iterator).
 *
 * The batch-lane analogue of MmapqUncommittedPayloadIterator: scan the
 * header to learn committed batch spans, walk the gaps batch by batch from
 * KNOWN batch boundaries only (the archive line or a committed range's end —
 * record boundaries). probe_batch is NOT used (the CAND-23 ambiguity cannot
 * arise at a known boundary).
 *
 * The scan skeleton mirrors uncommitted_payload_iterator.c per-format by
 * design (the single-message contiguous fast path has no batch analogue —
 * committed batches are measured one at a time via their counts); the two
 * scans carry this cross-reference. Do not unify them.
 *
 * ALGORITHM:
 *   At init():
 *     1. Acquire-load the queue start index FIRST, then the archive line
 *        (the D-G publication rule, §3.9); a zero line (<
 *        INITIAL_PAYLOAD_POSITION) means a corrupt appender meta:
 *        MMAPQ_ECORRUPT (pre-0.2.0 queues are refused at open with
 *        MMAPQ_ESCHEMA).
 *     2. Scan headers from the frozen si (D8/S3 floor); for each entry of
 *        this appender, measure the committed batch's span by reading its
 *        count (trusted as written, CAND-23 docs) and walking its messages
 *        (the advance_buffer framing rules incl. short/long padding);
 *        raw-append the span, then sort+merge once (D-B).
 *     3. The gap walk starts at the archive line (the batch walk takes no
 *        start/end parameters).
 *   At next():
 *     1. Bounded-gap pre-check: position == current_gap_end ⇒ gap exhausted
 *        — never probe at a committed range's start.
 *     2. At position p: remaining region capacity <= MMAPQ_BATCH_MSG_COUNT_SIZE
 *        ⇒ skip to the next region (the CAND-51e unified tail rule: <= 4
 *        bytes left means padding; a batch count slot never occupies a
 *        region tail). Read the count. count == 0 ends the gap walk (a count is
 *        written only by complete/complete_uncommitted/non-empty discard/
 *        join completion, so count 0 can exist only at the true tail); in a
 *        bounded gap, count 0 with p != current_gap_end ⇒ MMAPQ_ECORRUPT.
 *        count < 0 is the §3.8 boundary padding marker: legal region-skip
 *        iff its extent is exactly the rest of the region, else ECORRUPT.
 *     3. count > 0: yield (p, count) with msgs re-initialised to (p, count,
 *        0); refill the owning appender's outstanding set (ENOMEM ⇒ the
 *        iterator state is unchanged — the next next() re-yields the same
 *        batch).
 *     4. The NEXT next() advances p past the yielded batch by walking count
 *        messages; bounded gap: batch end <= current_gap_end, and the gap
 *        walk stops only with p == current_gap_end exactly — overshoot or
 *        undershoot ⇒ MMAPQ_ECORRUPT.
 *
 * Wrap failures propagate MMAPQ_ETIMEDOUT / MMAPQ_ECORRUPT (three-way,
 * never collapsed into exhaustion).
 *
 * Thread confinement: not thread-safe. One iterator per thread.
 */

typedef struct MmapqUncommittedBatchIterator {
    const char     *queue_name;
    uint8_t         appender_id;

    /* Stream boundary callbacks */
    int64_t       (*start_of_stream_offset)(void *ctx);
    int64_t       (*end_of_stream_offset)(void *ctx);
    void           *stream_ctx;

    /* Archive line supplier (CAND-49 §3.9); read at init AFTER
     * queue_start_index — the D-G publication rule. */
    int64_t       (*archive_offset)(void *ctx);

    /* CAND-58: the classification's payload floor — the archive line
     * re-read with seq_cst AFTER a walk failure (never start_offset, L(iv)).
     * Shares stream_ctx. */
    int64_t       (*archive_offset_sc)(void *ctx);

    /* Queue start index supplier (D8/S3 floor) — read once per init, first. */
    int64_t       (*queue_start_index)(void *ctx);
    void           *queue_start_index_ctx;

    /* Refill callback (§3.3): 0 or MMAPQ_ENOMEM. */
    int           (*refill)(void *ctx, int64_t offset);
    void           *refill_ctx;

    /* Header accessor for the committed-span scan */
    RegionAccessor *header_accessor;
    Buffer          header_buf;

    /* Payload accessor (the appender's) for the scan/walk wraps */
    RegionAccessor *payload_acc;

    /* Committed batch spans — sorted ascending after the init sort+merge */
    CommittedRanges ranges;
    int             current_range;

    /* Explicit bounded-gap boundary (§3.2 batch analogue):
     * PAYLOAD_ITERATOR_NOT_SET for the trailing gap. */
    int64_t         current_gap_end;

    int64_t         archive_line;
    int64_t         fixed_end_of_stream;

    /* CAND-58: the floor that classified the current overtaken failure,
     * with its domain (walk_classify.h's MMAPQI_WALK_DOM_*) — the restart
     * loop's no-progress record. */
    int64_t         walk_ovt_floor;
    int             walk_ovt_domain;

    /* Walk position: the next batch boundary to probe. When
     * pending_advance is set, the previously yielded batch at
     * yielded_offset/yielded_count has not been walked past yet — the next
     * next() walks it first (this is what makes a refill ENOMEM retryable:
     * the iterator state is unchanged on the failed yield). */
    int64_t         gap_pos;
    int64_t         yielded_offset;
    int32_t         yielded_count;
    int             pending_advance;

    /* The yielded batch's messages — embedded, re-initialised per yield,
     * invalidated by the next next(). */
    MmapqBatchPayloadIterator msgs;

    int             initialised;
} MmapqUncommittedBatchIterator;

/**
 * mmapqi_uncommitted_batch_iterator_create — allocate and configure.
 *
 * queue_name:              diagnostic name
 * appender_id:             which appender's committed spans to skip
 * start_of_stream_offset:  callback returning earliest valid payload offset
 * end_of_stream_offset:    callback returning current append position
 * stream_ctx:              context for both callbacks and archive_offset
 * archive_offset:          callback returning the appender's archive line
 * archive_offset_sc:       the archive line re-read seq_cst AFTER a walk
 *                          failure — the CAND-58 classification floor (§4.2)
 * header_accessor:         the walk accessor's RO header side (CAND-58 §4.1)
 * payload_acc:             the walk accessor's RO, size-checked payload side
 * queue_start_index:       callback returning the queue's current start index
 * queue_start_index_ctx:   context for queue_start_index
 * refill:                  callback making a yielded batch offset committable
 * refill_ctx:              context for refill
 *
 * Returns 0, MMAPQ_EINVAL for NULL args, MMAPQ_ENOMEM on allocation failure.
 */
int mmapqi_uncommitted_batch_iterator_create(
    const char     *queue_name,
    uint8_t         appender_id,
    int64_t       (*start_of_stream_offset)(void *ctx),
    int64_t       (*end_of_stream_offset)(void *ctx),
    void           *stream_ctx,
    int64_t       (*archive_offset)(void *ctx),
    int64_t       (*archive_offset_sc)(void *ctx),
    RegionAccessor *header_accessor,
    RegionAccessor *payload_acc,
    int64_t       (*queue_start_index)(void *ctx),
    void           *queue_start_index_ctx,
    int           (*refill)(void *ctx, int64_t offset),
    void           *refill_ctx,
    MmapqUncommittedBatchIterator **out);

/**
 * mmapqi_uncommitted_batch_iterator_init — scan headers, start at the line.
 *
 * Returns 0 on success; MMAPQ_ETIMEDOUT on a transient scan timeout
 * (retryable — a NEW factory call); MMAPQ_ECORRUPT on genuine scan
 * truncation or a zero archive line — a corrupt appender meta
 * (pre-0.2.0 queues are refused at open with MMAPQ_ESCHEMA);
 * MMAPQ_EINVAL if it is NULL.
 */
int mmapqi_uncommitted_batch_iterator_init(MmapqUncommittedBatchIterator *it);

/**
 * mmapqi_uncommitted_batch_iterator_next — move to the next uncommitted batch.
 *
 * Returns 1 (batch available — *batch_offset / *message_count set, *msgs
 * re-initialised to the batch's messages), 0 when exhausted, or a negative
 * MMAPQ_E* (ETIMEDOUT retryable / ECORRUPT / ENOMEM on refill failure, the
 * iterator state unchanged). A negative is truthy in C: compare explicitly.
 */
int mmapqi_uncommitted_batch_iterator_next(
    MmapqUncommittedBatchIterator *it,
    int64_t     *batch_offset,
    int32_t     *message_count,
    MmapqBatchPayloadIterator **msgs);

/**
 * mmapqi_uncommitted_batch_iterator_destroy — free the iterator.
 * The accessors are NOT owned.
 */
void mmapqi_uncommitted_batch_iterator_destroy(MmapqUncommittedBatchIterator *it);

#endif /* UNCOMMITTED_BATCH_ITERATOR_H */
