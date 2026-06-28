#ifndef INDEXED_BATCH_POLLER_H
#define INDEXED_BATCH_POLLER_H

#include <stdint.h>
#include "poller.h"
#include "runtime.h"
#include "indexed_batch_reader.h"

/**
 * IndexedBatchPoller — sequential batch consumer.
 *
 * Delivers one complete batch per poll() call. The handler receives the
 * batch index and a BatchReadingContext containing a BatchPayloadIterator
 * positioned before the first message. The handler calls
 * batch_iterator_advance() to iterate messages, then returns a NextMove
 * directive to control cursor movement.
 *
 * This is a per-batch poller — the atomic unit of delivery is a batch,
 * matching the atomic unit of commit. There is no per-message granularity.
 * Consumers that need per-message random access should use IndexedBatchReader.
 *
 * POLL OPERATION:
 *   1. If queue not yet created → POLL_END_OF_STREAM.
 *   2. If currentBatchIndex < startIndex → POLL_ARCHIVED.
 *   3. Read header at currentBatchIndex.
 *   4. If NULL_HEADER → POLL_END_OF_STREAM.
 *   5. Decode appenderId + payloadPosition from header.
 *   6. Read message count from payload, init iterator before message 0.
 *   7. Populate BatchReadingContext with batch info + iterator.
 *   8. Call handler(batch_index, &context, ctx).
 *   9. Apply the returned directive:
 *        NEXT_ADVANCE  → currentBatchIndex++
 *        NEXT_STOP     → cursor held
 *        NEXT_RETREAT  → currentBatchIndex-- (if > startIndex)
 *  10. Return the poll result.
 *
 * CURSOR POSITIONING:
 *   move_to_index():               jump to a specific batch index
 *   move_to_end():                 jump past the last committed batch
 *   move_to_last_existing_batch(): jump to the last committed batch
 *   move_to_start():               jump to the earliest unarchived batch
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One IndexedBatchPoller per consumer thread.
 */

typedef struct IndexedBatchPoller IndexedBatchPoller;

/**
 * BatchPollResult — alias for PollResult, returned by batch poll().
 *
 * Has the same variants as PollResult (POLL_END_OF_STREAM, POLL_ADVANCED,
 * POLL_STOPPED, POLL_RETREATED, POLL_ARCHIVED, POLL_ERROR).
 */
typedef PollResult BatchPollResult;

/**
 * BatchHandler — callback invoked once per poll() call, for one batch.
 *
 * The BatchReadingContext's batch_iterator is positioned BEFORE message 0.
 * Handlers MUST call batch_iterator_advance() before the first call to
 * batch_iterator_current_data() / batch_iterator_current_length().
 *
 * The context and its iterator are flyweight views into mapped memory —
 * valid only for the duration of this call. Do not retain the pointer.
 *
 * @param batch_index  zero-based index of the delivered batch
 * @param context      batch context with iterator positioned before msg 0
 * @param ctx          caller-supplied context pointer
 * @return             cursor directive: ADVANCE, STOP, or RETREAT
 */
typedef NextMove (*BatchHandler)(
    int64_t              batch_index,
    BatchReadingContext *context,
    void                *ctx);

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create a poller for the named queue, positioned at start_index.
 *
 * The poller may be created before the queue exists — poll() will return
 * POLL_END_OF_STREAM until batches are committed.
 *
 * @param directory   path to the directory containing queue files
 * @param queue_name  logical name of the queue
 * @param ring_size   number of simultaneously mapped regions
 * @param timeout_ns  region-mapping timeout in nanoseconds
 * @param runtime     background region-mapper runtime
 * @return a new poller; never NULL (asserts on allocation failure)
 */
IndexedBatchPoller *indexed_batch_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime);

/* ── Polling ─────────────────────────────────────────────────────────────── */

/**
 * Poll the next batch, delivering it to @p handler.
 *
 * The handler is called at most once per poll() invocation. If no committed
 * batch exists at the current index, returns POLL_END_OF_STREAM without
 * invoking the handler.
 *
 * @param p       the poller
 * @param handler callback to receive the batch context
 * @param ctx     caller context passed through to the handler
 * @return the result of the poll operation
 */
BatchPollResult indexed_batch_poller_poll(
    IndexedBatchPoller *p,
    BatchHandler        handler,
    void               *ctx);

/* ── Cursor positioning ──────────────────────────────────────────────────── */

/**
 * Move the cursor to a specific batch index.
 *
 * Returns 1 if the index is valid (>= start_index and not too far ahead).
 * Returns 0 if the index is archived or unreachable.
 */
int indexed_batch_poller_move_to_index(IndexedBatchPoller *p, int64_t batch_index);

/**
 * Move the cursor to one past the last committed batch.
 *
 * Subsequent poll() calls will return POLL_END_OF_STREAM until a new
 * batch is appended.
 *
 * @return 1 always
 */
int indexed_batch_poller_move_to_end(IndexedBatchPoller *p);

/**
 * Move the cursor to the last committed batch.
 *
 * Unlike move_to_end(), which positions past the last batch, this positions
 * AT the last valid batch so it can be polled immediately.
 *
 * @return 1 if a committed batch exists; 0 if the queue is empty
 */
int indexed_batch_poller_move_to_last_existing_batch(IndexedBatchPoller *p);

/**
 * Move the cursor to the first unarchived batch index.
 *
 * This is the lowest index still available for reading.
 *
 * @return 1 always
 */
int indexed_batch_poller_move_to_start(IndexedBatchPoller *p);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * Return the current batch index (cursor position).
 */
int64_t indexed_batch_poller_current_batch_index(const IndexedBatchPoller *p);

/**
 * Return the first unarchived batch index (logical start of readable queue).
 */
int64_t indexed_batch_poller_start_index(IndexedBatchPoller *p);

/**
 * Return 1 if a committed batch exists at @p batch_index, 0 otherwise.
 *
 * Does not alter the poller's cursor.
 */
int indexed_batch_poller_exists(IndexedBatchPoller *p, int64_t batch_index);

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Close the poller and release all associated resources.
 *
 * After this call the pointer must not be used. Idempotent.
 */
void indexed_batch_poller_close(IndexedBatchPoller *p);

#endif /* INDEXED_BATCH_POLLER_H */
