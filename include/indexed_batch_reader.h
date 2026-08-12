#ifndef INDEXED_BATCH_READER_H
#define INDEXED_BATCH_READER_H

#include <stdint.h>
#include "runtime.h"
#include "batch_payload_iterator.h"

/**
 * IndexedBatchReader — random-access reader for an indexed batch queue.
 *
 * Provides direct access to committed batches by index, without advancing
 * a sequential cursor. Unlike IndexedBatchPoller, which drives a message-at-
 * a-time cursor forward through the queue, IndexedBatchReader can read any
 * committed batch by index at any time.
 *
 * Typical use cases:
 *   - Snapshot lookup: read the last committed batch (read_last)
 *   - Recovery: read the first batch after a known checkpoint (read)
 *   - Presence check: has_entry() before committing to a full read
 *
 * The caller owns a BatchReadingContext and passes it to each read call.
 * The context is reused across calls — no heap allocation per read.
 * The BatchPayloadIterator pointed to by the context is valid only until
 * the next call to any read function on this reader.
 *
 * Thread safety: one reader per consumer thread. Must not be shared.
 *
 * Mirrors Java IIndexedBatchReader. BatchEntryStatus mirrors
 * IIndexedBatchReader.EntryStatus — defined here independently of
 * IndexedReader to keep the batch layer self-contained.
 */

typedef struct IndexedBatchReader IndexedBatchReader;

/**
 * BatchEntryStatus — availability status of a batch at a given index.
 *
 * Mirrors Java IIndexedBatchReader.EntryStatus.
 */
typedef enum {
    /**
     * The batch exists and is readable. The BatchPayloadIterator in the
     * context is valid and positioned at the first message.
     * Mirrors Java EntryStatus.AVAILABLE.
     */
    BATCH_ENTRY_AVAILABLE,

    /**
     * The batch has not been committed yet. The queried index is beyond the
     * last committed batch. Once a batch is committed at this index it will
     * become BATCH_ENTRY_AVAILABLE.
     * Mirrors Java EntryStatus.END_OF_STREAM.
     */
    BATCH_ENTRY_END_OF_STREAM,

    /**
     * The batch was previously available but has been archived and is no
     * longer accessible. Its index is below start_index().
     * Mirrors Java EntryStatus.ARCHIVED.
     */
    BATCH_ENTRY_ARCHIVED,

    /**
     * A mapping or I/O error occurred. The application should retry.
     * Mirrors Java EntryStatus.ERROR.
     */
    BATCH_ENTRY_ERROR
} BatchEntryStatus;

/**
 * BatchReadingContext — caller-owned result of a read operation.
 *
 * Passed to indexed_batch_reader_read() and friends. Reused across calls —
 * allocate once per reader, not once per read.
 *
 * Fields are valid when status == BATCH_ENTRY_AVAILABLE:
 *   - index:          the batch index that was read
 *   - batch_offset:   payload file offset of the batch header
 *   - message_count:  number of messages in the batch
 *   - appender_id:    appender ID that wrote this batch
 *   - iterator:       pointer to a BatchPayloadIterator positioned before
 *                     the first message; call batch_iterator_advance() to
 *                     move to each message
 *
 * When status != BATCH_ENTRY_AVAILABLE, the iterator is in an invalid state
 * and must not be used.
 *
 * The BatchPayloadIterator is owned by the reader or poller that produced
 * this context. It is valid only until the next read call on that reader,
 * or until the poll handler returns. Do not retain the pointer, and do not
 * free it.
 */
typedef struct {
    int64_t               index;
    BatchEntryStatus      status;
    int64_t               batch_offset;
    int32_t               message_count;
    uint8_t               appender_id;
    uint8_t               reserved[3];
    BatchPayloadIterator *iterator;
} BatchReadingContext;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create a reader for the named queue.
 *
 * The queue does not need to exist at creation time — the reader opens it
 * lazily on the first read. Reads before the queue exists return
 * BATCH_ENTRY_END_OF_STREAM.
 *
 * directory:  path to the directory containing queue files
 * queue_name: logical name of the queue
 * ring_size:  number of simultaneously mapped regions
 * timeout_ns: region-mapping timeout in nanoseconds
 * runtime:    background region-mapper runtime
 * Returns: a new reader; never NULL (asserts on allocation failure)
 */
int indexed_batch_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    IndexedBatchReader **out);

/* ── Random-access reads ─────────────────────────────────────────────────── */

/**
 * Read the batch at the given index.
 *
 * Populates @p ctx with the result. If the batch is available, ctx->status
 * is BATCH_ENTRY_AVAILABLE and ctx->iterator points to a BatchPayloadIterator
 * positioned before the first message (call batch_iterator_advance() to
 * begin iteration).
 *
 * If the batch is not available, ctx->status indicates why:
 *   BATCH_ENTRY_END_OF_STREAM — index beyond the last committed batch
 *   BATCH_ENTRY_ARCHIVED      — index below start_index()
 *   BATCH_ENTRY_ERROR         — mapping timeout or I/O failure
 *
 * r:     the reader
 * index: zero-based batch index to read (must be >= 0)
 * ctx:   caller-owned context to populate; returned for convenience
 * Returns: @p ctx (same pointer passed in)
 */
BatchReadingContext *indexed_batch_reader_read(
    IndexedBatchReader  *r,
    int64_t              index,
    BatchReadingContext *ctx);

/**
 * Read the first unarchived batch (at start_index()).
 *
 * If the queue is empty or not yet created, ctx->status is
 * BATCH_ENTRY_END_OF_STREAM.
 *
 * r:   the reader
 * ctx: caller-owned context to populate
 * Returns: @p ctx
 */
BatchReadingContext *indexed_batch_reader_read_first(
    IndexedBatchReader  *r,
    BatchReadingContext *ctx);

/**
 * Read the last committed batch (at last_index()).
 *
 * If the queue is empty, ctx->status is BATCH_ENTRY_END_OF_STREAM and
 * ctx->index is -1 (HEADER_NULL_INDEX).
 *
 * r:   the reader
 * ctx: caller-owned context to populate
 * Returns: @p ctx
 */
BatchReadingContext *indexed_batch_reader_read_last(
    IndexedBatchReader  *r,
    BatchReadingContext *ctx);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * Return the index of the last committed batch.
 *
 * Returns HEADER_NULL_INDEX (-1) if no batches have been committed yet.
 * The result is cached and updated incrementally on each call.
 */
int64_t indexed_batch_reader_last_index(IndexedBatchReader *r);

/**
 * Return the first unarchived batch index — the logical start of the queue.
 *
 * Any batch with an index below this value has been archived and is no longer
 * accessible. Returns QUEUE_META_INITIAL_START_INDEX if the queue does not
 * yet exist.
 */
int64_t indexed_batch_reader_start_index(IndexedBatchReader *r);

/**
 * Return the availability status of the batch at the given index.
 *
 * A lightweight check that does not populate a BatchReadingContext.
 * Equivalent to reading the header entry at @p index without loading payload.
 *
 * r:     the reader
 * index: zero-based batch index to check
 * Returns: BATCH_ENTRY_AVAILABLE, BATCH_ENTRY_END_OF_STREAM, or BATCH_ENTRY_ARCHIVED
 */
BatchEntryStatus indexed_batch_reader_entry_status(IndexedBatchReader *r, int64_t index);

/**
 * Convenience predicate: return 1 if a batch is available at @p index, 0 otherwise.
 *
 * Equivalent to:
 *   indexed_batch_reader_entry_status(r, index) == BATCH_ENTRY_AVAILABLE
 */
static inline int indexed_batch_reader_has_entry(IndexedBatchReader *r, int64_t index)
{
    return indexed_batch_reader_entry_status(r, index) == BATCH_ENTRY_AVAILABLE;
}

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Close the reader and release all associated resources.
 *
 * After this call the pointer must not be used.
 */
void indexed_batch_reader_close(IndexedBatchReader *r);

#endif /* INDEXED_BATCH_READER_H */
