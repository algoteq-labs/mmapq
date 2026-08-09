/*
 * consumer.c — multiprocess test: consumer side
 *
 * Reads N messages from the queue, verifying sequence (per appender) and checksum.
 * Usage: consumer <queue_dir> <queue_name> <message_count> <timeout_ms>
 *
 * Exits 0 if all N messages were received correctly within timeout_ms.
 * Exits 1 on any error (wrong sequence, wrong checksum, timeout).
 */

#include "indexed_queue.h"
#include "indexed_poller.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "header_codec.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define REGION_SIZE   (256 * 1024)
#define RING_SIZE     4
#define MAX_FILE_SIZE ((int64_t)REGION_SIZE * 64)
#define TIMEOUT_NS    5000000000ULL

typedef struct {
    int64_t  sequence;
    uint32_t checksum;
    char     tag[20];
} TestMessage;

static uint32_t checksum_of(int64_t seq)
{
    return (uint32_t)((uint64_t)seq * 0xDEADBEEFULL);
}

/* ── Poll state ──────────────────────────────────────────────────────────── */

typedef struct {
    int64_t  expected_seq[MAX_APPENDERS];  /* per-appender sequence counters */
    int64_t  received;
    int64_t  target;
    int      errors;
} PollState;

static NextMove verify_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    PollState        *s = (PollState *)ctx;
    TestMessage m;

    /* Length check */
    if (length != (int32_t)sizeof(TestMessage)) {
        fprintf(stderr, "consumer: wrong length %d at index %ld\n", length, index);
        s->errors++;
        return NEXT_ADVANCE;
    }

    /* Safe copy instead of unsafe cast to avoid alignment violations */
    memcpy(&m, data, sizeof(TestMessage));

    /* Tag check */
    if (strncmp(m.tag, "mmapq-producer", sizeof("mmapq-producer") - 1) != 0) {
        fprintf(stderr, "consumer: bad tag at index %ld\n", index);
        s->errors++;
        return NEXT_ADVANCE;
    }

    /* Per-appender sequence check */
    int64_t expected = s->expected_seq[appender_id];
    if (m.sequence != expected) {
        fprintf(stderr,
            "consumer: sequence gap for appender %d at index %ld — "
            "expected %ld got %ld\n",
            appender_id, index, expected, m.sequence);
        s->errors++;
        s->expected_seq[appender_id] = m.sequence + 1;
        s->received++;
        return NEXT_ADVANCE;
    }

    /* Checksum check */
    uint32_t expected_cs = checksum_of(m.sequence);
    if (m.checksum != expected_cs) {
        fprintf(stderr,
            "consumer: checksum mismatch at index %ld seq %ld appender %d — "
            "expected 0x%08x got 0x%08x\n",
            index, m.sequence, appender_id, expected_cs, m.checksum);
        s->errors++;
    }

    s->expected_seq[appender_id] = m.sequence + 1;
    s->received++;
    return NEXT_ADVANCE;
}

int main(int argc, char *argv[])
{
    if (argc != 5) {
        fprintf(stderr,
            "usage: %s <queue_dir> <queue_name> <message_count> <timeout_ms>\n",
            argv[0]);
        return 1;
    }

    const char *dir        = argv[1];
    const char *name       = argv[2];
    int64_t     count      = atoll(argv[3]);
    int64_t     timeout_ms = atoll(argv[4]);

    if (count <= 0 || timeout_ms <= 0) {
        fprintf(stderr, "message_count and timeout_ms must be > 0\n");
        return 1;
    }

    Runtime      *rt = NULL;
    if (runtime_region_mapper(&rt) != 0) { fprintf(stderr, "runtime_region_mapper failed\n"); return 1; }
    IndexedQueue *q  = NULL;

    uint64_t start_ns    = monotonic_nanos();
    uint64_t deadline_ns = start_ns + (uint64_t)timeout_ms * 1000000ULL;

    int rc;
    while (q == NULL) {
        rc = indexed_queue_create(
            dir, name,
            REGION_SIZE, RING_SIZE, 1,
            MAX_FILE_SIZE, 0,
            TIMEOUT_NS, 0, rt, rt, &q);

        if (rc != 0) {
            q = NULL;
            if (monotonic_nanos() >= deadline_ns) {
                fprintf(stderr, "consumer: timed out waiting for queue to appear\n");
                return 1;
            }
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
            nanosleep(&ts, NULL);
        }
    }

    IndexedPoller *p = NULL;
    if (indexed_queue_create_poller(q, &p) != 0) {
        fprintf(stderr, "consumer: create poller failed\n");
        return 1;
    }

    fprintf(stderr, "consumer: reading %ld messages (timeout %ld ms)\n",
            count, timeout_ms);

    PollState state;
    memset(&state, 0, sizeof(state));
    state.target = count;

    while (state.received < count) {
        if (monotonic_nanos() >= deadline_ns) {
            fprintf(stderr, "consumer: timeout — received %ld / %ld\n",
                    state.received, count);
            indexed_poller_close(p);
            indexed_queue_close(q);
            return 1;
        }

        PollResult r = indexed_poller_poll(p, verify_handler, &state);
        if (r == POLL_END_OF_STREAM) {
            spin_hint();
        }
    }

    fprintf(stderr, "consumer: done — received %ld messages, errors %d\n",
            state.received, state.errors);

    indexed_poller_close(p);
    indexed_queue_close(q);

    return (state.errors == 0) ? 0 : 1;
}
