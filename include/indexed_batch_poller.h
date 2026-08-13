#ifndef MMAPQ_INDEXED_BATCH_POLLER_H
#define MMAPQ_INDEXED_BATCH_POLLER_H

#include <stdint.h>
#include "poller.h"
#include "runtime.h"
#include "indexed_batch_reader.h"

/**
 * MmapqIndexedBatchPoller — sequential batch consumer.
 *
 * Delivers one complete batch per poll() call. The handler receives the
 * batch index and a MmapqBatchReadingContext whose iterator field points to a
 * MmapqBatchPayloadIterator positioned before the first message. The handler
 * calls mmapq_batch_iterator_advance() to iterate messages, then returns a
 * MmapqNextMove directive to control cursor movement.
 *
 * This is a per-batch poller — the atomic unit of delivery is a batch,
 * matching the atomic unit of commit. There is no per-message granularity.
 * Consumers that need per-message random access should use MmapqIndexedBatchReader.
 *
 * POLL OPERATION:
 *   1. If queue not yet created → MMAPQ_POLL_END_OF_STREAM.
 *   2. If currentBatchIndex < startIndex → MMAPQ_POLL_ARCHIVED.
 *   3. Read header at currentBatchIndex.
 *   4. If NULL_HEADER → MMAPQ_POLL_END_OF_STREAM.
 *   5. Decode appenderId + payloadPosition from header.
 *   6. Read message count from payload, init iterator before message 0.
 *   7. Populate MmapqBatchReadingContext with batch info + iterator pointer.
 *   8. Call handler(batch_index, &context, ctx).
 *   9. Apply the returned directive:
 *        MMAPQ_NEXT_ADVANCE  → currentBatchIndex++
 *        MMAPQ_NEXT_STOP     → cursor held
 *        MMAPQ_NEXT_RETREAT  → currentBatchIndex-- (if > startIndex)
 *  10. Return the poll result.
 *
 * CURSOR POSITIONING:
 *   move_to_index():               jump to a specific batch index
 *   move_to_end():                 jump past the last committed batch
 *   move_to_last_existing_batch(): jump to the last committed batch
 *   move_to_start():               jump to the earliest unarchived batch
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One MmapqIndexedBatchPoller per consumer thread.
 */

typedef struct MmapqIndexedBatchPoller MmapqIndexedBatchPoller;

/**
 * BatchPollResult — alias for MmapqPollResult, returned by batch poll().
 *
 * Has the same variants as MmapqPollResult (MMAPQ_POLL_END_OF_STREAM, MMAPQ_POLL_ADVANCED,
 * MMAPQ_POLL_STOPPED, MMAPQ_POLL_RETREATED, MMAPQ_POLL_ARCHIVED, MMAPQ_POLL_ERROR).
 */
typedef MmapqPollResult BatchPollResult;

/**
 * MmapqBatchHandler — callback invoked once per poll() call, for one batch.
 *
 * The MmapqBatchReadingContext's iterator is positioned BEFORE message 0.
 * Handlers MUST call mmapq_batch_iterator_advance() before the first call to
 * mmapq_batch_iterator_current_data_ffi() / mmapq_batch_iterator_current_length_ffi().
 *
 * The context and its iterator are flyweight views into mapped memory —
 * valid only for the duration of this call. Do not retain the pointer.
 *
 * batch_index: zero-based index of the delivered batch
 * context:     batch context with iterator positioned before msg 0
 * ctx:         caller-supplied context pointer
 * Returns:     cursor directive: ADVANCE, STOP, or RETREAT
 */
typedef MmapqNextMove (*MmapqBatchHandler)(
    int64_t              batch_index,
    MmapqBatchReadingContext *context,
    void                *ctx);

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create a poller for the named queue, positioned at start_index.
 *
 * The poller may be created before the queue exists — poll() will return
 * MMAPQ_POLL_END_OF_STREAM until batches are committed.
 *
 * directory:  path to the directory containing queue files
 * queue_name: logical name of the queue
 * ring_size:  number of simultaneously mapped regions
 * timeout_ns: region-mapping timeout in nanoseconds
 * runtime:    background region-mapper runtime
 * Returns: a new poller; never NULL (asserts on allocation failure)
 */
int mmapq_indexed_batch_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedBatchPoller **out);

/* ── Polling ─────────────────────────────────────────────────────────────── */

/**
 * Poll the next batch, delivering it to @p handler.
 *
 * The handler is called at most once per poll() invocation. If no committed
 * batch exists at the current index, returns MMAPQ_POLL_END_OF_STREAM without
 * invoking the handler.
 *
 * p:      the poller
 * handler: callback to receive the batch context
 * ctx:    caller context passed through to the handler
 * Returns: the result of the poll operation
 */
BatchPollResult mmapq_indexed_batch_poller_poll(
    MmapqIndexedBatchPoller *p,
    MmapqBatchHandler        handler,
    void               *ctx);

/* ── Cursor positioning ──────────────────────────────────────────────────── */

/**
 * Move the cursor to a specific batch index.
 *
 * Returns 1 if the index is valid (>= start_index and not too far ahead).
 * Returns 0 if the index is archived or unreachable.
 */
int mmapq_indexed_batch_poller_move_to_index(MmapqIndexedBatchPoller *p, int64_t batch_index);

/**
 * Move the cursor to one past the last committed batch.
 *
 * Subsequent poll() calls will return MMAPQ_POLL_END_OF_STREAM until a new
 * batch is appended.
 *
 * Returns: 1 always
 */
int mmapq_indexed_batch_poller_move_to_end(MmapqIndexedBatchPoller *p);

/**
 * Move the cursor to the last committed batch.
 *
 * Unlike move_to_end(), which positions past the last batch, this positions
 * AT the last valid batch so it can be polled immediately.
 *
 * Returns: 1 if a committed batch exists; 0 if the queue is empty
 */
int mmapq_indexed_batch_poller_move_to_last_existing_batch(MmapqIndexedBatchPoller *p);

/**
 * Move the cursor to the first unarchived batch index.
 *
 * This is the lowest index still available for reading.
 *
 * Returns: 1 always
 */
int mmapq_indexed_batch_poller_move_to_start(MmapqIndexedBatchPoller *p);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * Return the current batch index (cursor position).
 */
int64_t mmapq_indexed_batch_poller_current_batch_index(const MmapqIndexedBatchPoller *p);

/**
 * Return the first unarchived batch index (logical start of readable queue).
 */
int64_t mmapq_indexed_batch_poller_start_index(MmapqIndexedBatchPoller *p);

/**
 * Return 1 if a committed batch exists at @p batch_index, 0 otherwise.
 *
 * Does not alter the poller's cursor.
 */
int mmapq_indexed_batch_poller_exists(MmapqIndexedBatchPoller *p, int64_t batch_index);

/* ── Error accessor ────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_batch_poller_last_error — MMAPQ_* code from the most recent MMAPQ_POLL_ERROR.
 *
 * Returns 0 if the last poll did not return MMAPQ_POLL_ERROR. The value is cleared
 * to 0 at the start of each poll() call, so it always describes the most
 * recent call. Meaningful only immediately after a poll that returned MMAPQ_POLL_ERROR.
 */
int mmapq_indexed_batch_poller_last_error(const MmapqIndexedBatchPoller *p);

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Close the poller and release all associated resources.
 *
 * After this call the pointer must not be used. Idempotent.
 */
void mmapq_indexed_batch_poller_close(MmapqIndexedBatchPoller *p);

#endif /* MMAPQ_INDEXED_BATCH_POLLER_H */
