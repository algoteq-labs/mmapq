#include "unity.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "runtime.h"
#include "header_codec.h"
#include "header_reader.h"
#include "indexed_batch_queue.h"
#include "queue_meta.h"
#include <stdio.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

/* ── Test configuration ──────────────────────────────────────────────────── */

static const char    *DIR        = "/tmp";
static const char    *QUEUE      = "mmap_test_iq";
static const int32_t  REGION_SZ  = 4096;
static const int      RING_SZ    = 16;
static const int64_t  MAX_FILE   = 4096LL * 64;   /* small — triggers rolling */
static const uint64_t TIMEOUT_NS = 5000000000ULL;  /* 5 seconds */

static Runtime       *rt = NULL;
static IndexedQueue  *q  = NULL;

/* ── File cleanup ────────────────────────────────────────────────────────── */

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
}

/* ── setUp / tearDown ────────────────────────────────────────────────────── */

void setUp(void)
{
    cleanup();
    mkdir("/tmp/mmap_test_iq", 0775);
    int rc_rt = runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
    int rc_q  = indexed_queue_create(DIR, QUEUE,
                                      REGION_SZ, RING_SZ, 1,
                                      MAX_FILE, 0,
                                      TIMEOUT_NS, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
}

void tearDown(void)
{
    if (q) { indexed_queue_close(q); q = NULL; }
    cleanup();
}

/* ── Poll handler helpers ────────────────────────────────────────────────── */

typedef struct {
    char     data[1100];   /* large enough for 1000-byte test messages */
    int32_t  length;
    int64_t  index;
    uint8_t  appender_id;
    int      call_count;
    NextMove directive;
} PollCapture;

static NextMove capture_handler(
    int64_t index, uint8_t appender_id,
    const void *data, int32_t length, void *ctx)
{
    PollCapture *c = (PollCapture *)ctx;
    c->index       = index;
    c->appender_id = appender_id;
    c->length      = length;
    c->call_count++;
    if (length > 0 && length < (int32_t)sizeof(c->data)) {
        memcpy(c->data, data, (size_t)length);
        c->data[length] = '\0';
    }
    return c->directive;
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

/* ── Single appender, single poller round-trip ───────────────────────────── */

void test_append_and_poll_single_message(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    const char *msg = "hello queue";
    int64_t idx = indexed_appender_append(a, msg, (int32_t)strlen(msg));
    TEST_ASSERT_EQUAL_INT64(0, idx);

    PollCapture cap = { .directive = NEXT_ADVANCE };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT64(0, cap.index);
    TEST_ASSERT_EQUAL_INT32((int32_t)strlen(msg), cap.length);
    TEST_ASSERT_EQUAL_STRING(msg, cap.data);

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poll_end_of_stream_when_empty(void)
{
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    PollCapture cap = { .directive = NEXT_ADVANCE };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, r);
    TEST_ASSERT_EQUAL_INT(0, cap.call_count);

    indexed_poller_close(p);
}

void test_append_multiple_and_poll_all(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    const char *msgs[] = { "alpha", "beta", "gamma", "delta" };
    int n = 4;

    for (int i = 0; i < n; i++) {
        int64_t idx = indexed_appender_append(a, msgs[i], (int32_t)strlen(msgs[i]));
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    for (int i = 0; i < n; i++) {
        PollCapture cap = { .directive = NEXT_ADVANCE };
        PollResult r = indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
        TEST_ASSERT_EQUAL_INT64(i, cap.index);
        TEST_ASSERT_EQUAL_STRING(msgs[i], cap.data);
    }

    /* Next poll should be end of stream */
    PollCapture cap = { .directive = NEXT_ADVANCE };
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM,
        indexed_poller_poll(p, capture_handler, &cap));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

/* ── Random access reader ────────────────────────────────────────────────── */

void test_reader_read_at_index(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedReader *r = NULL;
    int rc_r = indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    indexed_appender_append(a, "first",  5);
    indexed_appender_append(a, "second", 6);
    indexed_appender_append(a, "third",  5);

    ReadingContext ctx = {0};
    indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(6, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("second", ctx.data, 6);

    indexed_appender_close(a);
    indexed_reader_close(r);
}

void test_reader_end_of_stream_beyond_last(void)
{
    IndexedReader *r = NULL;
    int rc_r = indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    ReadingContext ctx = {0};
    indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_END_OF_STREAM, ctx.status);
    indexed_reader_close(r);
}

void test_reader_last_index(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedReader *r = NULL;
    int rc_r = indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, indexed_reader_last_index(r));

    indexed_appender_append(a, "msg", 3);
    indexed_appender_append(a, "msg", 3);
    indexed_appender_append(a, "msg", 3);

    TEST_ASSERT_EQUAL_INT64(2, indexed_reader_last_index(r));

    indexed_appender_close(a);
    indexed_reader_close(r);
}

void test_reader_read_first_and_last(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedReader *r = NULL;
    int rc_r = indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    indexed_appender_append(a, "first",  5);
    indexed_appender_append(a, "middle", 6);
    indexed_appender_append(a, "last",   4);

    ReadingContext ctx = {0};

    indexed_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_MEMORY("first", ctx.data, 5);

    indexed_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(2, ctx.index);
    TEST_ASSERT_EQUAL_MEMORY("last", ctx.data, 4);

    indexed_appender_close(a);
    indexed_reader_close(r);
}

/* ── Poller cursor movement ──────────────────────────────────────────────── */

void test_poller_move_to_end(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    indexed_appender_append(a, "a", 1);
    indexed_appender_append(a, "b", 1);
    indexed_appender_append(a, "c", 1);

    indexed_poller_move_to_end(p);
    TEST_ASSERT_EQUAL_INT64(3, indexed_poller_current_index(p));

    PollCapture cap = { .directive = NEXT_ADVANCE };
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM,
        indexed_poller_poll(p, capture_handler, &cap));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poller_move_to_start(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    indexed_appender_append(a, "x", 1);
    indexed_appender_append(a, "y", 1);

    indexed_poller_move_to_end(p);
    indexed_poller_move_to_start(p);
    TEST_ASSERT_EQUAL_INT64(0, indexed_poller_current_index(p));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poller_move_to_index(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    for (int i = 0; i < 5; i++)
        indexed_appender_append(a, "x", 1);

    int ok = indexed_poller_move_to_index(p, 3);
    TEST_ASSERT_EQUAL_INT(1, ok);
    TEST_ASSERT_EQUAL_INT64(3, indexed_poller_current_index(p));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poller_handler_stop(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    indexed_appender_append(a, "stop_me", 7);

    PollCapture cap = { .directive = NEXT_STOP };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(POLL_STOPPED, r);
    TEST_ASSERT_EQUAL_INT64(0, indexed_poller_current_index(p));  /* cursor didn't move */

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poller_handler_retreat(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    indexed_appender_append(a, "a", 1);
    indexed_appender_append(a, "b", 1);

    /* Advance to index 1 */
    PollCapture cap = { .directive = NEXT_ADVANCE };
    indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT64(1, indexed_poller_current_index(p));

    /* Retreat */
    cap.directive = NEXT_RETREAT;
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_RETREATED, r);
    TEST_ASSERT_EQUAL_INT64(0, indexed_poller_current_index(p));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poller_retreat_at_start_stays(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    indexed_appender_append(a, "x", 1);

    /* Retreat from index 0 — should stop, not go negative */
    PollCapture cap = { .directive = NEXT_RETREAT };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_STOPPED, r);
    TEST_ASSERT_EQUAL_INT64(0, indexed_poller_current_index(p));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

/* ── Multiple appenders ──────────────────────────────────────────────────── */

void test_multiple_appenders_interleaved(void)
{
    IndexedAppender *a0 = NULL;
    int rc_a0 = indexed_queue_create_appender(q, &a0);
    TEST_ASSERT_EQUAL_INT(0, rc_a0);
    TEST_ASSERT_NOT_NULL(a0);
    IndexedAppender *a1 = NULL;
    int rc_a1 = indexed_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Interleave appends from two appenders */
    indexed_appender_append(a0, "from_a0_0", 9);
    indexed_appender_append(a1, "from_a1_0", 9);
    indexed_appender_append(a0, "from_a0_1", 9);
    indexed_appender_append(a1, "from_a1_1", 9);

    /* Poll all 4 in order — verify appender IDs alternate */
    uint8_t id0 = indexed_appender_id(a0);
    uint8_t id1 = indexed_appender_id(a1);

    for (int i = 0; i < 4; i++) {
        PollCapture cap = { .directive = NEXT_ADVANCE };
        PollResult r = indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);

        if (i == 0 || i == 2)
            TEST_ASSERT_EQUAL_UINT8(id0, cap.appender_id);
        else
            TEST_ASSERT_EQUAL_UINT8(id1, cap.appender_id);
    }

    indexed_appender_close(a0);
    indexed_appender_close(a1);
    indexed_poller_close(p);
}

/* ── Region boundary (padding) ───────────────────────────────────────────── */

void test_large_message_sequence_crossing_regions(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /*
     * Write messages large enough to force region crossings.
     * Region size = 4096 bytes. Each message = 1000 bytes.
     * ~4 messages per region. After ~16 messages we've crossed 4 regions.
     */
    char msg[1000];
    memset(msg, 'X', sizeof(msg));

    int count = 20;
    for (int i = 0; i < count; i++) {
        msg[0] = (char)('A' + i);  /* unique first byte per message */
        int64_t idx = indexed_appender_append(a, msg, sizeof(msg));
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    /* Poll all messages and verify first byte */
    for (int i = 0; i < count; i++) {
        PollCapture cap = { .directive = NEXT_ADVANCE };
        PollResult r = indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT_MESSAGE(POLL_ADVANCED, r, "expected ADVANCED");
        TEST_ASSERT_EQUAL_INT32(1000, cap.length);
        TEST_ASSERT_EQUAL_CHAR((char)('A' + i), cap.data[0]);
    }

    indexed_appender_close(a);
    indexed_poller_close(p);
}

/* ── Restart recovery ────────────────────────────────────────────────────── */

void test_appender_recovers_position_after_close(void)
{
    /* Phase 1: append some messages and close the appender */
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    uint8_t id = indexed_appender_id(a);

    indexed_appender_append(a, "msg_0", 5);
    indexed_appender_append(a, "msg_1", 5);
    int64_t before_close = indexed_appender_current_index(a);
    indexed_appender_close(a);

    /* Phase 2: reopen with same ID — should resume at index 2 */
    IndexedAppender *a2 = NULL;
    int rc_a2 = indexed_appender_create_with_id(
        DIR, QUEUE, REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT_NS, 0, rt, id, 0, &a2);
    TEST_ASSERT_EQUAL_INT(0, rc_a2);
    TEST_ASSERT_NOT_NULL(a2);

    TEST_ASSERT_EQUAL_INT64(before_close, indexed_appender_current_index(a2));

    /* Phase 3: append more messages — should get index 2, 3 */
    int64_t idx2 = indexed_appender_append(a2, "msg_2", 5);
    int64_t idx3 = indexed_appender_append(a2, "msg_3", 5);
    TEST_ASSERT_EQUAL_INT64(2, idx2);
    TEST_ASSERT_EQUAL_INT64(3, idx3);

    /* Phase 4: poll all 4 messages */
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    const char *expected[] = { "msg_0", "msg_1", "msg_2", "msg_3" };

    for (int i = 0; i < 4; i++) {
        PollCapture cap = { .directive = NEXT_ADVANCE };
        PollResult r = indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
        TEST_ASSERT_EQUAL_STRING(expected[i], cap.data);
    }

    indexed_appender_close(a2);
    indexed_poller_close(p);
}

/* ── Stale appender catch-up ─────────────────────────────────────────────── */

static void test_stale_appender_catch_up(void)
{
    /*
     * Regression: when an appender has not written while another appender
     * has advanced the queue significantly, its first append must not
     * linearly scan all intervening headers. Bound the scan and fall back
     * to jump-based search.
     *
     * Create two appenders, fill 10 header files with appender1, then
     * append with appender2 and verify the returned index is correct.
     */
    IndexedAppender *a1 = NULL;
    int rc_a1 = indexed_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    IndexedAppender *a2 = NULL;
    int rc_a2 = indexed_queue_create_appender(q, &a2);
    TEST_ASSERT_EQUAL_INT(0, rc_a2);
    TEST_ASSERT_NOT_NULL(a2);
    TEST_ASSERT_NOT_NULL(a2);

    int header_files_to_fill = 10;
    int64_t msgs = (header_files_to_fill * MAX_FILE) / HEADER_SIZE;

    const char *payload = "x";
    int32_t     plen    = 1;

    for (int64_t i = 0; i < msgs; i++) {
        int64_t idx = indexed_appender_append(a1, payload, plen);
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    int64_t idx = indexed_appender_append(a2, payload, plen);
    clock_gettime(CLOCK_MONOTONIC, &ts1);

    TEST_ASSERT_EQUAL_INT64(msgs, idx);

    uint64_t dt_us = (uint64_t)((ts1.tv_sec - ts0.tv_sec) * 1000000LL
                              + (ts1.tv_nsec - ts0.tv_nsec) / 1000LL);
    (void)dt_us;

    indexed_appender_close(a1);
    indexed_appender_close(a2);
}

/* ── Uncommitted append + commit ─────────────────────────────────────────── */

void test_uncommitted_append_and_commit(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Append uncommitted — not visible to poller yet */
    int64_t offset = indexed_appender_append_uncommitted(a, "deferred", 8);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, offset);

    PollCapture cap = { .directive = NEXT_ADVANCE };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, r);  /* not visible */

    /* Commit — now visible */
    int64_t idx = indexed_appender_commit(a, offset);
    TEST_ASSERT_EQUAL_INT64(0, idx);

    r = indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT32(8, cap.length);
    TEST_ASSERT_EQUAL_MEMORY("deferred", cap.data, 8);

    indexed_appender_close(a);
    indexed_poller_close(p);
}

/* ── Uncommitted iterator ────────────────────────────────────────────────── */

void test_uncommitted_iterator_finds_uncommitted(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    /* Append one committed, one uncommitted */
    indexed_appender_append(a, "committed", 9);
    int64_t uncommitted_off = indexed_appender_append_uncommitted(a, "uncommitted", 11);

    /* The uncommitted iterator should find exactly one message */
    UncommittedPayloadIterator *it =
        indexed_appender_uncommitted_iterator(a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    int found = 0;
    while (uncommitted_iterator_advance(it)) {
        found++;
        Buffer *buf = uncommitted_iterator_current(it);
        TEST_ASSERT_EQUAL_INT32(11, buf->capacity);
        TEST_ASSERT_EQUAL_MEMORY("uncommitted", buf->data, 11);
    }
    TEST_ASSERT_EQUAL_INT(1, found);

    /* Now commit and check again — should find nothing */
    indexed_appender_commit(a, uncommitted_off);

    it = indexed_appender_uncommitted_iterator(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);
    found = 0;
    while (uncommitted_iterator_advance(it)) found++;
    TEST_ASSERT_EQUAL_INT(0, found);

    indexed_appender_close(a);
}

/* ── Reader entry_status ─────────────────────────────────────────────────── */

void test_reader_entry_status(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedReader *r = NULL;
    int rc_r = indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    indexed_appender_append(a, "x", 1);

    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE,     indexed_reader_entry_status(r, 0));
    TEST_ASSERT_EQUAL_INT(ENTRY_END_OF_STREAM, indexed_reader_entry_status(r, 1));
    TEST_ASSERT_TRUE(indexed_reader_has_entry(r, 0));
    TEST_ASSERT_FALSE(indexed_reader_has_entry(r, 1));

    indexed_appender_close(a);
    indexed_reader_close(r);
}

/* ── Poller exists ───────────────────────────────────────────────────────── */

void test_poller_exists(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_FALSE(indexed_poller_exists(p, 0));
    indexed_appender_append(a, "y", 1);
    TEST_ASSERT_TRUE(indexed_poller_exists(p, 0));
    TEST_ASSERT_FALSE(indexed_poller_exists(p, 1));

    indexed_appender_close(a);
    indexed_poller_close(p);
}

void test_poller_automatically_discovers_region_size(void)
{
    /* Phase 1: Create a queue with 8MB regions and write a message */
    int32_t LARGE_REGION = 8 * 1024 * 1024;
    IndexedQueue *q1 = NULL;
    int rc_q1 = indexed_queue_create(DIR, QUEUE, LARGE_REGION, RING_SZ, 1, 
                                      LARGE_REGION * 16, 0, TIMEOUT_NS, 0, rt, rt, &q1);
    TEST_ASSERT_EQUAL_INT(0, rc_q1);
    TEST_ASSERT_NOT_NULL(q1);
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q1, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_append(a, "hello", 5);
    indexed_appender_close(a);
    indexed_queue_close(q1);

    /* Phase 2: Create a second queue instance with "wrong" default (4KB) */
    IndexedQueue *q2 = NULL;
    int rc_q2 = indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                      MAX_FILE, 0, TIMEOUT_NS, 0, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q2);
    TEST_ASSERT_NOT_NULL(q2);
    
    /* Verify metadata reported by q2 is correct (8MB) */
    TEST_ASSERT_EQUAL_INT32(LARGE_REGION, indexed_queue_region_size(q2));

    /* Create poller from q2 and poll the message */
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q2, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    PollCapture cap = { .directive = NEXT_ADVANCE };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_STRING("hello", cap.data);

    indexed_poller_close(p);
    indexed_queue_close(q2);
}

void test_append_rejects_zero_length(void)
{
    IndexedQueue      *q = NULL;
    int rc_q = indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    int64_t idx = indexed_appender_append(a, "first", 5);
    TEST_ASSERT_EQUAL_INT64(0, idx);

    int64_t bad = indexed_appender_append(a, "", 0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    /* Verify state unchanged — next append gets correct index */
    int64_t next = indexed_appender_append(a, "third", 5);
    TEST_ASSERT_EQUAL_INT64(1, next);

    indexed_appender_close(a);
    indexed_queue_close(q);
}

void test_append_rejects_oversized_message(void)
{
    IndexedQueue      *q = NULL;
    IndexedAppender   *a = NULL;

    /* Create with small region so we can exceed it */
    int rc_q = indexed_queue_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, 1,
        4096LL * 64, 0, TIMEOUT_NS,
        0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    int64_t ok = indexed_appender_append(a, "small", 5);
    TEST_ASSERT_EQUAL_INT64(0, ok);

    /* REGION_SZ = 4096, message of 4096 bytes exceeds region_size - 4 */
    char big[4096];
    memset(big, 'x', sizeof(big));
    int64_t bad = indexed_appender_append(a, big, (int32_t)sizeof(big));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    /* Verify state unchanged — next append succeeds */
    int64_t next = indexed_appender_append(a, "after", 5);
    TEST_ASSERT_EQUAL_INT64(1, next);

    indexed_appender_close(a);
    indexed_queue_close(q);
}

void test_open_normal_as_batch_refused(void)
{
    IndexedQueue *q = NULL;
    int rc_q = indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_append(a, "msg", 3);
    indexed_appender_close(a);
    indexed_queue_close(q);

    /* Attempt to create a batch participant on the same queue */
    IndexedBatchQueue *bq = NULL;
    int rc_bq = indexed_batch_queue_create_default(DIR, QUEUE, rt, rt, &bq);
    TEST_ASSERT_EQUAL_INT(0, rc_bq);
    TEST_ASSERT_NOT_NULL(bq);
    IndexedBatchAppender *ba = NULL;
    int rc_ba = indexed_batch_queue_create_appender(bq, &ba);
    TEST_ASSERT_TRUE(rc_ba != 0);
    TEST_ASSERT(rc_ba != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(ba);

    if (bq) indexed_batch_queue_close(bq);
}

void test_meta_schema_mismatch_refused(void)
{
    IndexedQueue *q = NULL;
    int rc_q = indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_append(a, "msg", 3);
    indexed_appender_close(a);
    indexed_queue_close(q);

    /* Patch schema_version to 99 */
    char path[512];
    snprintf(path, sizeof(path), "/tmp/%s/meta", QUEUE);
    int32_t bad = 99;
    FILE *f = fopen(path, "r+b");
    if (f) {
        fwrite(&bad, sizeof(bad), 1, f);
        fclose(f);
    }

    /* Appender creation should now fail */
    IndexedQueue *q2 = NULL;
    int rc_q2 = indexed_queue_create_default(DIR, QUEUE, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q2);
    TEST_ASSERT_NOT_NULL(q2);
    IndexedAppender *a2 = NULL;
    int rc_a2 = indexed_queue_create_appender(q2, &a2);
    TEST_ASSERT_TRUE(rc_a2 != 0);
    TEST_ASSERT(rc_a2 != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(a2);

    if (q2) indexed_queue_close(q2);
}

void test_meta_fixed8_refused(void)
{
    IndexedQueue *q = NULL;
    int rc_q = indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_append(a, "msg", 3);
    indexed_appender_close(a);
    indexed_queue_close(q);

    /* Patch byte at offset 25 to 1 (QUEUE_TYPE_FIXED8) */
    char path[512];
    snprintf(path, sizeof(path), "/tmp/%s/meta", QUEUE);
    uint8_t qt = QUEUE_TYPE_FIXED8;
    FILE *f = fopen(path, "r+b");
    if (f) {
        fseek(f, QUEUE_META_QUEUE_TYPE_OFFSET, SEEK_SET);
        fwrite(&qt, sizeof(qt), 1, f);
        fclose(f);
    }

    /* Appender creation should now fail */
    IndexedQueue *q2 = NULL;
    int rc_q2 = indexed_queue_create_default(DIR, QUEUE, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q2);
    TEST_ASSERT_NOT_NULL(q2);
    IndexedAppender *a2 = NULL;
    int rc_a2 = indexed_queue_create_appender(q2, &a2);
    TEST_ASSERT_TRUE(rc_a2 != 0);
    TEST_ASSERT_NULL(a2);

    if (q2) indexed_queue_close(q2);
}

/* ── Standalone factory — missing directory ───────────────────────────────── */

static void test_standalone_factory_rejects_missing_directory(void)
{
    IndexedAppender *a = NULL;
    int rc_a = indexed_appender_create_with_id(
        "/nonexistent-dir-for-test", "q",
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 7, 0, &a);
    TEST_ASSERT_TRUE(rc_a != 0);
    TEST_ASSERT_NULL(a);
}

/* ── Corrupt queue — poll detects corrupted payload ───────────────────────── */

static int corrupt_handler_count;
static NextMove corrupt_handler(int64_t index, uint8_t aid,
                                 const void *data, int32_t len, void *ctx)
{
    (void)index; (void)aid; (void)data; (void)len; (void)ctx;
    corrupt_handler_count++;
    return NEXT_ADVANCE;
}

static void test_poll_detects_corrupt_payload(void)
{
    /* Use a unique queue name to avoid conflicts with other tests */
    char qname[64];
    snprintf(qname, sizeof(qname), "%s_corrupt", QUEUE);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    int rc = system(cmd); (void)rc;

    /* Create queue, append one message */
    IndexedQueue *q = NULL;
    int rc_q = indexed_queue_create(
        DIR, qname,
        REGION_SZ, RING_SZ, 1, MAX_FILE,
        0, TIMEOUT_NS, 0, NULL, NULL, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);

    IndexedAppender *a = NULL;
    int rc_a = indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    int64_t idx = indexed_appender_append(a, "hello", 5);
    TEST_ASSERT(idx >= 0);
    indexed_appender_close(a);

    /* Corrupt the payload: find the header at index 0, decode payload offset,
       write zero at that offset (the 4-byte length prefix) */
    IndexedReader *r = NULL;
    int rc_r = indexed_reader_create(DIR, qname, 4, 5000000000ULL, NULL, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    ReadingContext ctx = {0};
    indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT(ctx.status == ENTRY_AVAILABLE);

    /* Corrupt the payload file directly */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, qname, (unsigned)ctx.appender_id);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "failed to open payload file for writing");
    uint32_t zero = 0;
    ssize_t written = pwrite(fd, &zero, sizeof(zero), ctx.msg_offset); (void)written;
    close(fd);
    indexed_reader_close(r);

    /* Now poll — should get POLL_CORRUPT */
    IndexedPoller *p = NULL;
    int rc_p = indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    corrupt_handler_count = 0;
    PollResult pr = indexed_poller_poll(p, corrupt_handler, NULL);
    TEST_ASSERT_EQUAL(POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL(0, corrupt_handler_count);  /* handler was NOT called */
    indexed_poller_close(p);

    indexed_queue_close(q);
    cleanup();
}

static void test_create_with_id_already_in_use(void)
{
    /* Need the queue subdirectory and pool file for the standalone factory */
    char tmp[128];
    snprintf(tmp, sizeof(tmp), "%s/%s_XXXXXX", DIR, QUEUE);
    char *dir = mkdtemp(tmp);
    TEST_ASSERT_NOT_NULL(dir);
    char qdir[256];
    snprintf(qdir, sizeof(qdir), "%s/%s", dir, QUEUE);
    mkdir(qdir, 0755);

    IndexedAppender *a1 = NULL;
    int rc_a1 = indexed_appender_create_with_id(
        dir, QUEUE,
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 7, 0, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);

    IndexedAppender *a2 = NULL;
    int rc_a2 = indexed_appender_create_with_id(
        dir, QUEUE,
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 7, 0, &a2);
    TEST_ASSERT_TRUE(rc_a2 != 0);
    TEST_ASSERT(rc_a2 != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(a2);

    indexed_appender_close(a1);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();

    RUN_TEST(test_append_and_poll_single_message);
    RUN_TEST(test_poll_end_of_stream_when_empty);
    RUN_TEST(test_append_multiple_and_poll_all);

    RUN_TEST(test_reader_read_at_index);
    RUN_TEST(test_reader_end_of_stream_beyond_last);
    RUN_TEST(test_reader_last_index);
    RUN_TEST(test_reader_read_first_and_last);

    RUN_TEST(test_poller_move_to_end);
    RUN_TEST(test_poller_move_to_start);
    RUN_TEST(test_poller_move_to_index);
    RUN_TEST(test_poller_handler_stop);
    RUN_TEST(test_poller_handler_retreat);
    RUN_TEST(test_poller_retreat_at_start_stays);

    RUN_TEST(test_multiple_appenders_interleaved);
    RUN_TEST(test_stale_appender_catch_up);
    RUN_TEST(test_large_message_sequence_crossing_regions);
    RUN_TEST(test_appender_recovers_position_after_close);
    RUN_TEST(test_uncommitted_append_and_commit);
    RUN_TEST(test_uncommitted_iterator_finds_uncommitted);
    RUN_TEST(test_reader_entry_status);
    RUN_TEST(test_poller_exists);
    
    RUN_TEST(test_poller_automatically_discovers_region_size);

    RUN_TEST(test_append_rejects_zero_length);
    RUN_TEST(test_append_rejects_oversized_message);

    RUN_TEST(test_open_normal_as_batch_refused);
    RUN_TEST(test_meta_schema_mismatch_refused);
    RUN_TEST(test_meta_fixed8_refused);

    RUN_TEST(test_standalone_factory_rejects_missing_directory);
    RUN_TEST(test_create_with_id_already_in_use);
    RUN_TEST(test_poll_detects_corrupt_payload);

    return UNITY_END();
}
