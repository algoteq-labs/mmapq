#ifndef INDEXED_BATCH_POLLER_H
#define INDEXED_BATCH_POLLER_H

#include <stdint.h>
#include "indexed_poller.h"
#include "runtime.h"

/**
 * IndexedBatchPoller — polls batches of messages from an indexed queue.
 */

typedef struct IndexedBatchPoller IndexedBatchPoller;

typedef enum {
    POLL_BATCH_END_OF_STREAM,
    POLL_BATCH_ADVANCED_MESSAGE,
    POLL_BATCH_ADVANCED_BATCH,
    POLL_BATCH_STOPPED,
    POLL_BATCH_RETREATED_BATCH,
    POLL_BATCH_ARCHIVED
} BatchPollResult;

typedef MessageHandlerDirective (*BatchMessageHandler)(
    int64_t batch_index,
    int32_t message_index,
    int32_t message_count,
    const void *data,
    int32_t length,
    void *ctx);

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedBatchPoller *indexed_batch_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime);

/* ── Polling ─────────────────────────────────────────────────────────────── */

BatchPollResult indexed_batch_poller_poll(
    IndexedBatchPoller *p,
    BatchMessageHandler handler,
    void               *ctx);

/* ── Cursor Positioning ──────────────────────────────────────────────────── */

int indexed_batch_poller_move_to_index(IndexedBatchPoller *p, int64_t batch_index);

int indexed_batch_poller_move_to(IndexedBatchPoller *p, int64_t batch_index, int32_t message_index);

int indexed_batch_poller_move_to_end(IndexedBatchPoller *p);

int indexed_batch_poller_move_to_last_existing_batch(IndexedBatchPoller *p);

int indexed_batch_poller_move_to_start(IndexedBatchPoller *p);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

int64_t indexed_batch_poller_current_batch_index(const IndexedBatchPoller *p);

int32_t indexed_batch_poller_current_message_index(const IndexedBatchPoller *p);

int64_t indexed_batch_poller_start_index(IndexedBatchPoller *p);

int indexed_batch_poller_exists(IndexedBatchPoller *p, int64_t batch_index, int32_t message_index);

void indexed_batch_poller_close(IndexedBatchPoller *p);

#endif /* INDEXED_BATCH_POLLER_H */
