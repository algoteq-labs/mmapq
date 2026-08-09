/*
 * demo.c — mmapq indexed queue demonstration
 *
 * Runs a single-process producer/consumer loop using the indexed queue.
 * The appender writes 1,000,000 messages; the poller reads them all.
 *
 * Build:
 *   make demo
 *
 * Run:
 *   ./demo
 *
 * The queue files are written to /tmp/mmap_demo_queue/ and cleaned up
 * on exit. To inspect the raw files while the demo runs, open a second
 * terminal and ls -lh /tmp/mmap_demo_queue/.
 */

#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "runtime.h"
#include "mmap_platform.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/stat.h>

/* ── Configuration ───────────────────────────────────────────────────────── */

#define QUEUE_DIR        "/tmp"
#define QUEUE_NAME       "mmap_demo_queue"
#define REGION_SIZE      (4096 * 1024)        /* 4 MB per region          */
#define RING_SIZE        4
#define MAX_FILE_SIZE    ((int64_t)REGION_SIZE * 256)  /* ~1 GB max        */
#define TIMEOUT_NS       5000000000ULL         /* 5 second mapping timeout */
#define MSG_COUNT        1000000
#define REPORT_INTERVAL  100000

/* ── Message format ──────────────────────────────────────────────────────── */

typedef struct {
    int64_t  sequence;     /* monotonically increasing */
    uint64_t timestamp_ns; /* monotonic nanoseconds at send time */
    char     payload[48];  /* fixed-size payload */
} Message;

/* ── Consumer thread ─────────────────────────────────────────────────────── */

typedef struct {
    IndexedPoller *poller;
    int64_t        expected_sequence;
    int64_t        messages_received;
    uint64_t       total_latency_ns;
    uint64_t       max_latency_ns;
    int            errors;
    int            done;       /* set to 1 when MSG_COUNT messages consumed */
} ConsumerCtx;

static NextMove consumer_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    ConsumerCtx *c   = (ConsumerCtx *)ctx;
    Message m;
    (void)appender_id;

    /* Verify length */
    if (length != (int32_t)sizeof(Message)) {
        fprintf(stderr, "ERROR: unexpected message length %d at index %ld\n",
                length, index);
        c->errors++;
        return NEXT_ADVANCE;
    }

    /* Safe copy instead of unsafe cast to avoid alignment violations */
    memcpy(&m, data, sizeof(Message));

    /* Verify sequence */
    if (m.sequence != c->expected_sequence) {
        fprintf(stderr, "ERROR: sequence gap — expected %ld got %ld at index %ld\n",
                c->expected_sequence, m.sequence, index);
        c->errors++;
    }

    /* Measure latency */
    uint64_t now_ns  = monotonic_nanos();
    uint64_t lat_ns  = now_ns - m.timestamp_ns;
    c->total_latency_ns += lat_ns;
    if (lat_ns > c->max_latency_ns) c->max_latency_ns = lat_ns;

    c->expected_sequence = m.sequence + 1;
    c->messages_received++;

    if (c->messages_received == MSG_COUNT) {
        c->done = 1;
    }

    return NEXT_ADVANCE;
}

static void *consumer_thread_fn(void *arg)
{
    ConsumerCtx *ctx = (ConsumerCtx *)arg;

    while (!ctx->done) {
        PollResult r = indexed_poller_poll(ctx->poller, consumer_handler, ctx);
        if (r == POLL_END_OF_STREAM) {
            /* Spin-wait until the producer writes one.
             * Burns CPU intentionally for minimum latency.
             * Use a sleeping poller or idle strategy in
             * throughput-oriented applications. */
            spin_hint();
        }
    }

    return NULL;
}

/* ── Cleanup ─────────────────────────────────────────────────────────────── */

static void cleanup(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", QUEUE_DIR, QUEUE_NAME);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("mmapq indexed queue demo\n");
    printf("  messages:    %d\n", MSG_COUNT);
    printf("  region size: %d KB\n", REGION_SIZE / 1024);
    printf("  queue dir:   %s/%s\n\n", QUEUE_DIR, QUEUE_NAME);

    cleanup();

    /* Create runtime — the background region mapping thread */
    Runtime *rt = runtime_region_mapper();

    /* Create the queue */
    IndexedQueue *q = indexed_queue_create(
        QUEUE_DIR, QUEUE_NAME,
        REGION_SIZE, RING_SIZE,
        1,            /* regions_to_map_ahead — async premap depth */
        MAX_FILE_SIZE, 0,
        TIMEOUT_NS, /* archivable = */ 0, rt, rt);

    /* Create appender and poller */
    IndexedAppender *appender = NULL;
    int rc_appender = indexed_queue_create_appender(q, &appender);
    TEST_ASSERT_EQUAL_INT(0, rc_appender);
    TEST_ASSERT_NOT_NULL(appender);
    IndexedPoller *poller = NULL;
    int rc_poller = indexed_queue_create_poller(q, &poller);
    TEST_ASSERT_EQUAL_INT(0, rc_poller);
    TEST_ASSERT_NOT_NULL(poller);

    printf("appender ID: %d\n", indexed_appender_id(appender));

    /* Start consumer thread */
    ConsumerCtx ctx = {
        .poller            = poller,
        .expected_sequence = 0,
        .messages_received = 0,
        .total_latency_ns  = 0,
        .max_latency_ns    = 0,
        .errors            = 0,
        .done              = 0,
    };

    pthread_t consumer;
    pthread_create(&consumer, NULL, consumer_thread_fn, &ctx);

    /* Produce messages
     * produce_start: wall time when production began (after consumer thread
     * is ready).  produce_end: wall time when the last message was committed.
     * consume_end: wall time when the last message was delivered. */
    printf("producing %d messages...\n", MSG_COUNT);
    uint64_t produce_start = monotonic_nanos();

    Message msg;
    memset(&msg, 0, sizeof(msg));
    snprintf(msg.payload, sizeof(msg.payload), "hello from mmapq");

    for (int i = 0; i < MSG_COUNT; i++) {
        msg.sequence     = i;
        msg.timestamp_ns = monotonic_nanos();

        int64_t idx = indexed_appender_append(appender, &msg, sizeof(msg));
        (void)idx; /* commit succeeded — idx is the assigned queue index */

        if ((i + 1) % REPORT_INTERVAL == 0) {
            printf("  produced %d / %d\n", i + 1, MSG_COUNT);
        }
    }

    uint64_t produce_end = monotonic_nanos();

    /* Wait for consumer to finish */
    pthread_join(consumer, NULL);
    uint64_t consume_end = monotonic_nanos();

    /* ── Results ─────────────────────────────────────────────────────────── */

    double produce_ms   = (double)(produce_end - produce_start) / 1e6;
    double total_ms     = (double)(consume_end - produce_start) / 1e6;
    double avg_lat_us   = ctx.messages_received > 0
        ? (double)ctx.total_latency_ns / ctx.messages_received / 1e3
        : 0.0;
    double max_lat_us   = (double)ctx.max_latency_ns / 1e3;
    double throughput   = MSG_COUNT / (total_ms / 1000.0);

    printf("\n── Results ───────────────────────────────────────\n");
    printf("  messages produced:    %d\n",     MSG_COUNT);
    printf("  messages consumed:    %ld\n",    ctx.messages_received);
    printf("  errors:               %d\n",     ctx.errors);
    printf("  produce time:         %.1f ms\n", produce_ms);
    printf("  total time:           %.1f ms\n", total_ms);
    printf("  throughput:           %.0f msg/s\n", throughput);
    printf("  avg latency:          %.2f µs\n",  avg_lat_us);
    printf("  max latency:          %.2f µs\n",  max_lat_us);
    printf("  last index:           %ld\n",    indexed_appender_current_index(appender) - 1);
    printf("──────────────────────────────────────────────────\n");

    if (ctx.errors == 0 && ctx.messages_received == (int64_t)MSG_COUNT) {
        printf("OK — all messages received in order\n");
    } else {
        printf("FAIL — errors=%d received=%ld/%d\n",
               ctx.errors, ctx.messages_received, MSG_COUNT);
    }

    /* Cleanup */
    indexed_appender_close(appender);
    indexed_poller_close(poller);
    indexed_queue_close(q);
    cleanup();

    return (ctx.errors == 0 && ctx.messages_received == (int64_t)MSG_COUNT) ? 0 : 1;
}
