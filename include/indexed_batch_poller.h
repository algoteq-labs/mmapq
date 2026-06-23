#ifndef INDEXED_BATCH_POLLER_H
#define INDEXED_BATCH_POLLER_H

#include <stdint.h>
#include "runtime.h"

/**
 * IndexedBatchPoller — sequential poller for an indexed, batched message queue.
 *
 * Provides message-at-a-time delivery from committed batches. For each call to
 * indexed_batch_poller_poll(), one message in the current batch is delivered to
 * a BatchMessageHandler. The handler's returned BatchNextMove controls how the
 * poller advances its cursor.
 *
 * Typical usage patterns:
 *   - Loop until POLL_BATCH_END_OF_STREAM: consume all available messages
 *   - Time-sliced polling: call poll() a fixed number of times per tick
 *   - Reverse scan: use BATCH_RETREAT_BATCH to locate historical batches
 *
 * The poller maintains its cursor across calls — batch_index and message_index
 * persist between invocations. Use the move_to_* functions to reposition.
 *
 * Thread safety: one poller per consumer. Must not be shared across threads.
 *
 * Mirrors Java IIndexedBatchPoller. BatchNextMove mirrors IBatchMessageHandler.NextMove.
 * Both are defined here independently of IndexedPoller — the batch and single-message
 * layers are parallel, not layered.
 */

typedef struct IndexedBatchPoller IndexedBatchPoller;

/**
 * BatchNextMove — returned by BatchMessageHandler to control the poller cursor.
 *
 * Mirrors Java IBatchMessageHandler.NextMove exactly.
 */
typedef enum {
    /**
     * Advance to the next message in the current batch.
     * If at the last message, advance to the next batch (index++, message_index=0).
     * Equivalent to Java NextMove.ADVANCE.
     */
    BATCH_ADVANCE,

    /**
     * Skip any remaining messages in the current batch and advance to the next batch.
     * Equivalent to Java NextMove.ADVANCE_BATCH.
     */
    BATCH_ADVANCE_BATCH,

    /**
     * Stop at the current message. The same message will be offered again on the
     * next poll() call. Equivalent to Java NextMove.STOP.
     */
    BATCH_STOP,

    /**
     * Move to the previous batch index. Useful for reverse scans, such as
     * locating the most recent snapshot. Equivalent to Java NextMove.RETREAT_BATCH.
     */
    BATCH_RETREAT_BATCH
} BatchNextMove;

/**
 * BatchPollResult — indicates how the poller adjusted its position after a poll().
 *
 * Works in tandem with BatchNextMove: maps handler feedback into high-level
 * poller state changes. Mirrors Java IIndexedBatchPoller.Result.
 */
typedef enum {
    /**
     * No batch is available at the current index. The poller remains at the
     * same position; a subsequent poll() will retry once data arrives.
     * Mirrors Java Result.END_OF_STREAM.
     */
    POLL_BATCH_END_OF_STREAM,

    /**
     * One message was processed and the cursor advanced to the next message
     * within the same batch. Mirrors Java Result.ADVANCED_MESSAGE.
     */
    POLL_BATCH_ADVANCED_MESSAGE,

    /**
     * One message was processed and the cursor advanced to the next batch
     * (either because the batch ended or BATCH_ADVANCE_BATCH was returned).
     * Mirrors Java Result.ADVANCED_BATCH.
     */
    POLL_BATCH_ADVANCED_BATCH,

    /**
     * The handler returned BATCH_STOP. The cursor remains at the current
     * message; the same message will be offered on the next poll().
     * Mirrors Java Result.STOPPED.
     */
    POLL_BATCH_STOPPED,

    /**
     * The handler returned BATCH_RETREAT_BATCH and the cursor moved to
     * the previous batch index. Mirrors Java Result.RETREATED_BATCH.
     */
    POLL_BATCH_RETREATED_BATCH,

    /**
     * The batch at the current index has been archived and is no longer
     * accessible. The poller remains at the same index.
     * Mirrors Java Result.ARCHIVED.
     */
    POLL_BATCH_ARCHIVED
} BatchPollResult;

/**
 * BatchMessageHandler — callback invoked once per poll() call, for one message.
 *
 * @param batch_index    zero-based index of the current batch
 * @param message_index  zero-based index of the message within the batch
 * @param message_count  total number of messages in the current batch
 * @param data           pointer to the raw message payload (flyweight — do not retain)
 * @param length         length of the message payload in bytes
 * @param ctx            caller-supplied context pointer
 * @return               how the poller should move its cursor after this message
 */
typedef BatchNextMove (*BatchMessageHandler)(
    int64_t     batch_index,
    int32_t     message_index,
    int32_t     message_count,
    const void *data,
    int32_t     length,
    void       *ctx);

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create a poller for the named queue, positioned at batch index 0.
 *
 * The queue must have been created by an appender before any batches can
 * be polled, but the poller may be created before the queue exists — it
 * will return POLL_BATCH_END_OF_STREAM until data arrives.
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
 * Poll one message from the current batch, delivering it to @p handler.
 *
 * The handler is called at most once per poll() invocation. The handler's
 * returned BatchNextMove determines how the cursor advances. The resulting
 * BatchPollResult describes the new cursor state.
 *
 * If no committed batch exists at the current index, returns
 * POLL_BATCH_END_OF_STREAM without invoking the handler.
 *
 * @param p       the poller
 * @param handler callback to receive the message
 * @param ctx     caller context passed through to the handler unchanged
 * @return the result of the poll operation
 */
BatchPollResult indexed_batch_poller_poll(
    IndexedBatchPoller *p,
    BatchMessageHandler handler,
    void               *ctx);

/* ── Cursor positioning ──────────────────────────────────────────────────── */

/**
 * Move the cursor to the start of the given batch (message_index = 0).
 *
 * - If the batch exists and is committed, polling begins at message 0.
 * - If the batch does not yet exist but the previous batch does, the move
 *   is accepted and poll() will return END_OF_STREAM until data arrives.
 * - If the batch is archived, or no previous batch exists, the move is
 *   rejected and the cursor is unchanged.
 *
 * @param p           the poller
 * @param batch_index zero-based target batch index (must be >= 0)
 * @return 1 if the move succeeded; 0 if the batch is archived or out of range
 */
int indexed_batch_poller_move_to_index(IndexedBatchPoller *p, int64_t batch_index);

/**
 * Move the cursor to a specific message within a batch.
 *
 * - If the batch exists, message_index must be in [0, message_count).
 * - If the batch does not yet exist, the move is accepted only if
 *   message_index == 0 and the previous batch exists.
 * - If the batch is archived, or message_index is out of range, returns 0.
 *
 * @param p             the poller
 * @param batch_index   zero-based target batch index
 * @param message_index zero-based target message index within the batch
 * @return 1 if the move succeeded; 0 otherwise
 */
int indexed_batch_poller_move_to(IndexedBatchPoller *p, int64_t batch_index, int32_t message_index);

/**
 * Move the cursor to one past the last committed batch.
 *
 * Subsequent poll() calls will return END_OF_STREAM until a new batch is
 * appended. Equivalent to Java moveToEnd().
 *
 * @return 1 if the move succeeded; 0 on error
 */
int indexed_batch_poller_move_to_end(IndexedBatchPoller *p);

/**
 * Move the cursor to the last existing (committed) batch.
 *
 * Unlike move_to_end(), which positions past the last batch, this positions
 * AT the last valid batch so it can be polled immediately. Useful for
 * reverse scanning or resuming from the most recent known data.
 *
 * If the queue is empty, the cursor is unchanged and 0 is returned.
 *
 * @return 1 if the move succeeded; 0 if the queue is empty
 */
int indexed_batch_poller_move_to_last_existing_batch(IndexedBatchPoller *p);

/**
 * Move the cursor to the first unarchived batch index (the logical start).
 *
 * This is the lowest index still available for reading. Batches below this
 * index have been permanently removed. If the queue has no committed batches
 * yet, positions at the first writable index (equivalent to move_to_end()).
 *
 * @return 1 if the move succeeded; 0 on error
 */
int indexed_batch_poller_move_to_start(IndexedBatchPoller *p);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * Return the current batch index.
 *
 * This is the batch the poller is positioned at, whether committed or not.
 * If at the end of the queue, this is the index at which the next commit
 * is expected.
 */
int64_t indexed_batch_poller_current_batch_index(const IndexedBatchPoller *p);

/**
 * Return the current message index within the current batch.
 *
 * If the current batch is not yet committed, returns 0 (the default start).
 */
int32_t indexed_batch_poller_current_message_index(const IndexedBatchPoller *p);

/**
 * Return the first unarchived batch index — the logical start of the readable queue.
 *
 * Any batch with an index below this value has been permanently removed and
 * cannot be accessed. The batch at this index may not yet be committed.
 */
int64_t indexed_batch_poller_start_index(IndexedBatchPoller *p);

/**
 * Check whether a message exists at the specified batch and message index.
 *
 * Returns 1 only if the batch is committed, not archived, and message_index
 * is within [0, message_count). Does not alter the poller's cursor.
 *
 * @param p             the poller
 * @param batch_index   zero-based batch index to check
 * @param message_index zero-based message index within the batch
 * @return 1 if the message exists and is readable; 0 otherwise
 */
int indexed_batch_poller_exists(IndexedBatchPoller *p, int64_t batch_index, int32_t message_index);

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Close the poller and release all associated resources.
 *
 * After this call the pointer must not be used.
 */
void indexed_batch_poller_close(IndexedBatchPoller *p);

#endif /* INDEXED_BATCH_POLLER_H */
