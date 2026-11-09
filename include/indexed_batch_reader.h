#ifndef MMAPQ_INDEXED_BATCH_READER_H
#define MMAPQ_INDEXED_BATCH_READER_H

#include <stdint.h>
#include "runtime.h"
#include "batch_payload_iterator.h"

/**
 * MmapqIndexedBatchReader — random-access reader for an indexed batch queue.
 *
 * Provides direct access to committed batches by index, without advancing
 * a sequential cursor. Unlike MmapqIndexedBatchPoller, which drives a message-at-
 * a-time cursor forward through the queue, MmapqIndexedBatchReader can read any
 * committed batch by index at any time.
 *
 * Typical use cases:
 *   - Snapshot lookup: read the last committed batch (read_last)
 *   - Recovery: read the first batch after a known checkpoint (read)
 *   - Presence check: has_entry() before committing to a full read
 *
 * The caller owns a MmapqBatchReadingContext and passes it to each read call.
 * The context is reused across calls — no heap allocation per read.
 * The MmapqBatchPayloadIterator pointed to by the context is valid only until
 * the next call to any read function on this reader.
 *
 * Thread safety: one reader per consumer thread. Must not be shared.
 *
 * MmapqBatchEntryStatus mirrors MmapqEntryStatus (indexed_reader.h) — defined here
 * independently of MmapqIndexedReader to keep the batch layer self-contained.
 */

typedef struct MmapqIndexedBatchReader MmapqIndexedBatchReader;

/**
 * MmapqBatchEntryStatus — availability status of a batch at a given index.
 *
 */
typedef enum {
    /**
     * The batch exists and is readable. The MmapqBatchPayloadIterator in the
     * context is valid and positioned at the first message.
     */
    MMAPQ_BATCH_ENTRY_AVAILABLE,

    /**
     * The batch has not been committed yet. The queried index is beyond the
     * last committed batch, or the header file is absent below an unmoved
     * start index (the written frontier). Once a batch is committed at this
     * index it will become MMAPQ_BATCH_ENTRY_AVAILABLE. Never terminal on
     * an archivable queue — re-read; a later call returns ARCHIVED if the
     * index was archived meanwhile.
     */
    MMAPQ_BATCH_ENTRY_END_OF_STREAM,

    /**
     * The batch was previously available but has been archived and is no
     * longer accessible. Its index is below start_index() — including
     * after a failed read whose index the archiver moved below
     * start_index mid-call (the failure-path re-check).
     */
    MMAPQ_BATCH_ENTRY_ARCHIVED,

    /**
     * A mapping, I/O, corruption, or invalid-argument error occurred whose
     * index is still at or above start_index(). Inspect last_error to
     * distinguish a retryable timeout from invalid stored data or an
     * invalid index (MMAPQ_EINVAL); a failure whose index is now below
     * start_index is reported as MMAPQ_BATCH_ENTRY_ARCHIVED.
     */
    MMAPQ_BATCH_ENTRY_ERROR
} MmapqBatchEntryStatus;

/**
 * MmapqBatchReadingContext — caller-owned result of a read operation.
 *
 * Passed to mmapq_indexed_batch_reader_read() and friends. Reused across calls —
 * allocate once per reader, not once per read.
 *
 * Fields are valid when status == MMAPQ_BATCH_ENTRY_AVAILABLE:
 *   - index:          the batch index that was read
 *   - batch_offset:   payload file offset of the batch header
 *   - message_count:  number of messages in the batch
 *   - appender_id:    appender ID that wrote this batch
 *   - iterator:       pointer to a MmapqBatchPayloadIterator positioned before
 *                     the first message; call mmapq_batch_iterator_advance() to
 *                     move to each message
 *
 * When status != MMAPQ_BATCH_ENTRY_AVAILABLE, the iterator is in an invalid state
 * and must not be used.
 *
 * The MmapqBatchPayloadIterator is owned by the reader or poller that produced
 * this context. It is valid only until the next read call on that reader,
 * or until the poll handler returns. Do not retain the pointer, and do not
 * free it.
 */
typedef struct {
    int64_t               index;
    MmapqBatchEntryStatus      status;
    int64_t               batch_offset;
    int32_t               message_count;
    uint8_t               appender_id;
    uint8_t               reserved[3];
    MmapqBatchPayloadIterator *iterator;
} MmapqBatchReadingContext;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create a reader for the named queue.
 *
 * The queue does not need to exist at creation time — the reader opens it
 * lazily on the first read. Reads before the queue exists return
 * MMAPQ_BATCH_ENTRY_END_OF_STREAM.
 *
 * directory:  path to the directory containing queue files
 * queue_name: logical name of the queue
 * ring_size:  number of simultaneously mapped regions
 * timeout_ns: region-mapping timeout in nanoseconds
 * runtime:    background region-mapper runtime
 * Returns: a new reader; never NULL (asserts on allocation failure)
 */
int mmapq_indexed_batch_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedBatchReader **out);

/* ── Random-access reads ─────────────────────────────────────────────────── */

/**
 * Read the batch at the given index.
 *
 * Populates @p ctx with the result. If the batch is available, ctx->status
 * is MMAPQ_BATCH_ENTRY_AVAILABLE and ctx->iterator points to a MmapqBatchPayloadIterator
 * positioned before the first message (call mmapq_batch_iterator_advance() to
 * begin iteration).
 *
 * Iteration contract (CAND-51a): mmapq_batch_iterator_advance is unchecked
 * (0 = exhaustion OR error). After the message loop the caller MUST check
 * mmapq_indexed_batch_reader_last_error — it reflects the iterator's live
 * error channel — before trusting a complete message list; a mid-batch
 * mapping timeout or corruption otherwise looks like a shorter batch. The
 * checked mmapq_batch_iterator_next step needs no such post-check.
 *
 * If the batch is not available, ctx->status indicates why:
 *   MMAPQ_BATCH_ENTRY_END_OF_STREAM — index beyond the last committed
 *                                     batch, or a missing header file
 *                                     below an unmoved start index
 *   MMAPQ_BATCH_ENTRY_ARCHIVED      — index below start_index(), including
 *                                     after a failed header/payload read
 *                                     whose index the archiver moved below
 *                                     start_index mid-call (the
 *                                     failure-path re-check)
 *   MMAPQ_BATCH_ENTRY_ERROR         — mapping timeout, I/O failure, or
 *                                     invalid index (last_error == MMAPQ_EINVAL)
 *                                     whose index is still at or above
 *                                     start_index
 *
 * r:     the reader
 * index: zero-based batch index to read (must be in [0, 2^56 - 1]; beyond
 *        2^56 - 1 indices would alias already-written slots)
 * ctx:   caller-owned context to populate; returned for convenience
 * Returns: @p ctx (same pointer passed in)
 */
MmapqBatchReadingContext *mmapq_indexed_batch_reader_read(
    MmapqIndexedBatchReader  *r,
    int64_t              index,
    MmapqBatchReadingContext *ctx);

/**
 * Read the first unarchived batch (at start_index()).
 *
 * If the queue is empty or not yet created, ctx->status is
 * MMAPQ_BATCH_ENTRY_END_OF_STREAM.
 *
 * r:   the reader
 * ctx: caller-owned context to populate
 * Returns: @p ctx
 */
MmapqBatchReadingContext *mmapq_indexed_batch_reader_read_first(
    MmapqIndexedBatchReader  *r,
    MmapqBatchReadingContext *ctx);

/**
 * Read the last committed batch (at last_index()).
 *
 * If the queue is empty, ctx->status is MMAPQ_BATCH_ENTRY_END_OF_STREAM and
 * ctx->index is -1 (HEADER_NULL_INDEX).
 *
 * r:   the reader
 * ctx: caller-owned context to populate
 * Returns: @p ctx
 */
MmapqBatchReadingContext *mmapq_indexed_batch_reader_read_last(
    MmapqIndexedBatchReader  *r,
    MmapqBatchReadingContext *ctx);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * Return the index of the last committed batch.
 *
 * Returns HEADER_NULL_INDEX (-1) if no batches have been committed yet or
 * discovery fails. Inspect last_error to distinguish these outcomes.
 * The result is cached and updated incrementally on each call.
 */
int64_t mmapq_indexed_batch_reader_last_index(MmapqIndexedBatchReader *r);

/**
 * Return the first unarchived batch index — the logical start of the queue.
 *
 * Any batch with an index below this value has been archived and is no longer
 * accessible. Returns QUEUE_META_INITIAL_START_INDEX if the queue does not
 * yet exist.
 */
int64_t mmapq_indexed_batch_reader_start_index(MmapqIndexedBatchReader *r);

/**
 * Return the availability status of the batch at the given index.
 *
 * A lightweight check that does not populate a MmapqBatchReadingContext.
 * Equivalent to reading the header entry at @p index without loading payload.
 *
 * r:     the reader
 * index: zero-based batch index to check; above 2^56 - 1 returns
 *        MMAPQ_BATCH_ENTRY_ERROR with last_error == MMAPQ_EINVAL (such
 *        indices would alias already-written slots); has_entry inherits 0
 * Returns: MMAPQ_BATCH_ENTRY_AVAILABLE, MMAPQ_BATCH_ENTRY_END_OF_STREAM,
 *          MMAPQ_BATCH_ENTRY_ARCHIVED, or MMAPQ_BATCH_ENTRY_ERROR
 *
 * A header read that fails is classified like the read path: a mapping
 * timeout is MMAPQ_BATCH_ENTRY_ERROR + last_error (not END_OF_STREAM),
 * unless start_index has moved past the index (the archiver reclaimed the
 * file mid-call), which is MMAPQ_BATCH_ENTRY_ARCHIVED; a missing header
 * file below an unmoved start index is MMAPQ_BATCH_ENTRY_END_OF_STREAM.
 * has_entry is unchanged: 0 for END_OF_STREAM, ERROR and ARCHIVED alike.
 */
MmapqBatchEntryStatus mmapq_indexed_batch_reader_entry_status(MmapqIndexedBatchReader *r, int64_t index);

/**
 * Convenience predicate: return 1 if a batch is available at @p index, 0 otherwise.
 *
 * Equivalent to:
 *   mmapq_indexed_batch_reader_entry_status(r, index) == MMAPQ_BATCH_ENTRY_AVAILABLE
 */
static inline int mmapq_indexed_batch_reader_has_entry(MmapqIndexedBatchReader *r, int64_t index)
{
    return mmapq_indexed_batch_reader_entry_status(r, index) == MMAPQ_BATCH_ENTRY_AVAILABLE;
}

/* ── Error accessor ────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_batch_reader_last_error — error from the most recent read or last_index.
 *
 * Zero means no error. Reads and last_index clear this value on entry;
 * inspect it immediately after MMAPQ_BATCH_ENTRY_ERROR or a last_index failure.
 */
int mmapq_indexed_batch_reader_last_error(const MmapqIndexedBatchReader *r);

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Close the reader and release all associated resources.
 *
 * After this call the pointer must not be used.
 */
void mmapq_indexed_batch_reader_close(MmapqIndexedBatchReader *r);

#endif /* MMAPQ_INDEXED_BATCH_READER_H */
