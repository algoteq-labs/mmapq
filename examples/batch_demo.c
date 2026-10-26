/*
 * batch_demo.c — mmapq indexed batch queue demonstration
 *
 * Runs a single-process producer/consumer loop using the batch queue API.
 * The appender writes 3 batches (1, 2, and 3 messages each); the poller
 * reads them all using mmapq_batch_iterator_advance().
 *
 * The queue files are written to /tmp/mmap_batch_demo/ and cleaned up
 * on exit.
 */

#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "runtime.h"
#include "mmap_platform.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define QUEUE_DIR        "/tmp"
#define QUEUE_NAME       "mmap_batch_demo"
#define REGION_SIZE      (4096 * 1024)        /* 4 MB per region          */
#define RING_SIZE        4
#define MAX_FILE_SIZE    ((int64_t)REGION_SIZE * 256)  /* ~1 GB max        */
#define TIMEOUT_NS       5000000000ULL         /* 5 second mapping timeout */

static MmapqNextMove batch_handler(
    int64_t                   batch_index,
    MmapqBatchReadingContext       *ctx,
    void                      *user_data)
{
    int64_t *total_messages = (int64_t *)user_data;
    int32_t  msg_count      = ctx->message_count;
    int32_t  seen           = 0;

    printf("  batch[%ld] — %d message(s):\n", batch_index, msg_count);

    while (mmapq_batch_iterator_advance(ctx->iterator)) {
        const void *data = batch_iterator_current_data(ctx->iterator);
        int32_t length   = batch_iterator_current_length(ctx->iterator);
        printf("    msg[%d]: %.*s\n", seen, length, (const char *)data);
        seen++;
        (*total_messages)++;
    }

    return MMAPQ_NEXT_ADVANCE;
}

int main(void)
{
    printf("mmapq indexed batch queue demo\n");
    printf("  region size: %d KB\n", REGION_SIZE / 1024);
    printf("  queue dir:   %s/%s\n\n", QUEUE_DIR, QUEUE_NAME);

    /* Cleanup previous run */
    {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", QUEUE_DIR, QUEUE_NAME);
        if (system(cmd) != 0) { /* best-effort cleanup */ }
    }

    /* Create runtime */
    MmapqRuntime *rt = NULL;
    int rc = mmapq_runtime_region_mapper(&rt);
    if (rc != 0) { fprintf(stderr, "mmapq_runtime_region_mapper failed\n"); return 1; }

    /* Create the batch queue */
    MmapqIndexedBatchQueue *q = NULL;
    rc = mmapq_indexed_batch_queue_create(
        QUEUE_DIR, QUEUE_NAME,
        REGION_SIZE, RING_SIZE, 1,
        MAX_FILE_SIZE, 0, TIMEOUT_NS, 0,
        rt, rt, &q);
    if (rc != 0) { fprintf(stderr, "mmapq_indexed_batch_queue_create failed (rc=%d)\n", rc); return 1; }

    /* Create appender and poller */
    MmapqIndexedBatchAppender *appender = NULL;
    rc = mmapq_indexed_batch_queue_create_appender(q, &appender);
    if (rc != 0) { fprintf(stderr, "create_appender failed (rc=%d)\n", rc); return 1; }

    MmapqIndexedBatchPoller *poller = NULL;
    rc = mmapq_indexed_batch_queue_create_poller(q, &poller);
    if (rc != 0) { fprintf(stderr, "create_poller failed (rc=%d)\n", rc); return 1; }

    printf("appender ID: %d\n\n", mmapq_indexed_batch_appender_id(appender));

    /* Produce 3 batches with increasing message counts */
    printf("Producing batches...\n");

    /* Batch 0: 1 message */
    {
        MmapqMutableBatch *b = NULL;
        if (mmapq_indexed_batch_appender_start_batch_checked(appender, &b) != 0) { fprintf(stderr, "start_batch failed\n"); return 1; }
        if (mmapq_mutable_batch_append(b, "hello", 5) < 0) { fprintf(stderr, "append failed\n"); return 1; }
        int64_t off = mmapq_mutable_batch_complete(b);
        if (off < 0) { fprintf(stderr, "complete failed\n"); return 1; }
        int64_t idx = mmapq_indexed_batch_appender_commit_batch(appender, off);
        if (idx < 0) { fprintf(stderr, "commit_batch failed\n"); return 1; }
        printf("  batch[%ld] committed (1 message)\n", idx);
    }

    /* Batch 1: 2 messages */
    {
        MmapqMutableBatch *b = NULL;
        if (mmapq_indexed_batch_appender_start_batch_checked(appender, &b) != 0) { fprintf(stderr, "start_batch failed\n"); return 1; }
        if (mmapq_mutable_batch_append(b, "foo", 3) < 0) { fprintf(stderr, "append failed\n"); return 1; }
        if (mmapq_mutable_batch_append(b, "bar", 3) < 0) { fprintf(stderr, "append failed\n"); return 1; }
        int64_t off = mmapq_mutable_batch_complete(b);
        if (off < 0) { fprintf(stderr, "complete failed\n"); return 1; }
        int64_t idx = mmapq_indexed_batch_appender_commit_batch(appender, off);
        if (idx < 0) { fprintf(stderr, "commit_batch failed\n"); return 1; }
        printf("  batch[%ld] committed (2 messages)\n", idx);
    }

    /* Batch 2: 3 messages */
    {
        MmapqMutableBatch *b = NULL;
        if (mmapq_indexed_batch_appender_start_batch_checked(appender, &b) != 0) { fprintf(stderr, "start_batch failed\n"); return 1; }
        if (mmapq_mutable_batch_append(b, "alpha", 5) < 0) { fprintf(stderr, "append failed\n"); return 1; }
        if (mmapq_mutable_batch_append(b, "beta", 4) < 0) { fprintf(stderr, "append failed\n"); return 1; }
        if (mmapq_mutable_batch_append(b, "gamma", 5) < 0) { fprintf(stderr, "append failed\n"); return 1; }
        int64_t off = mmapq_mutable_batch_complete(b);
        if (off < 0) { fprintf(stderr, "complete failed\n"); return 1; }
        int64_t idx = mmapq_indexed_batch_appender_commit_batch(appender, off);
        if (idx < 0) { fprintf(stderr, "commit_batch failed\n"); return 1; }
        printf("  batch[%ld] committed (3 messages)\n", idx);
    }

    /* Consume all 3 batches via sequential poll */
    printf("\nConsuming batches...\n");
    int64_t total_messages = 0;

    for (int i = 0; i < 3; i++) {
        MmapqPollResult r = mmapq_indexed_batch_poller_poll(poller, batch_handler, &total_messages);
        if (r == MMAPQ_POLL_END_OF_STREAM) {
            printf("  end of stream (batch %d)\n", i);
            break;
        }
    }

    /* Verify */
    printf("\n── Results ───────────────────────────────────────\n");
    printf("  batches produced: 3\n");
    printf("  total messages:   %ld\n", total_messages);

    if (total_messages == 6) {
        printf("OK — all batches and messages received\n");
    } else {
        printf("FAIL — expected 6 messages, got %ld\n", total_messages);
    }

    /* Cleanup */
    mmapq_indexed_batch_appender_close(appender);
    mmapq_indexed_batch_poller_close(poller);
    mmapq_indexed_batch_queue_close(q);

    {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", QUEUE_DIR, QUEUE_NAME);
        if (system(cmd) != 0) { /* best-effort cleanup */ }
    }

    return (total_messages == 6) ? 0 : 1;
}
