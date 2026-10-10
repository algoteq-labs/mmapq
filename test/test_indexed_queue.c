#include "unity.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "runtime.h"
#include "internal/header_codec.h"
#include "internal/header_reader.h"
#include "internal/payload_iterator.h"
#include "mmap_platform.h"
#include "internal/uncommitted_payload_iterator.h"
#include "indexed_batch_queue.h"
#include "internal/queue_meta.h"
#include <stdio.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <stdint.h>

/* ── Test configuration ──────────────────────────────────────────────────── */

static const char    *DIR        = "/tmp";
static const char    *QUEUE      = "mmap_test_iq";
static const int32_t  REGION_SZ  = 4096;
static const int      RING_SZ    = 16;
static const int64_t  MAX_FILE   = 4096LL * 64;   /* small — triggers rolling */
static const uint64_t TIMEOUT_NS = 5000000000ULL;  /* 5 seconds */

static MmapqRuntime       *rt = NULL;
static MmapqIndexedQueue  *q  = NULL;

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
    int rc_rt = mmapq_runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
    int rc_q  = mmapq_indexed_queue_create(DIR, QUEUE,
                                      REGION_SZ, RING_SZ, 1,
                                      MAX_FILE, 0,
                                      TIMEOUT_NS, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
}

void tearDown(void)
{
    if (q) { mmapq_indexed_queue_close(q); q = NULL; }
    cleanup();
}

/* ── Poll handler helpers ────────────────────────────────────────────────── */

typedef struct {
    char     data[1100];   /* large enough for 1000-byte test messages */
    int32_t  length;
    int64_t  index;
    uint8_t  appender_id;
    int      call_count;
    MmapqNextMove directive;
} PollCapture;

static MmapqNextMove capture_handler(
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
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    const char *msg = "hello queue";
    int64_t idx = mmapq_indexed_appender_append(a, msg, (int32_t)strlen(msg));
    TEST_ASSERT_EQUAL_INT64(0, idx);

    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT64(0, cap.index);
    TEST_ASSERT_EQUAL_INT32((int32_t)strlen(msg), cap.length);
    TEST_ASSERT_EQUAL_STRING(msg, cap.data);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poll_end_of_stream_when_empty(void)
{
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r);
    TEST_ASSERT_EQUAL_INT(0, cap.call_count);

    mmapq_indexed_poller_close(p);
}

void test_poller_before_queue_retries(void)
{
    const char *poll_dir  = "/tmp";
    const char *poll_name = "mmap_test_iq_poller_before";
    char cleanup_cmd[512];
    snprintf(cleanup_cmd, sizeof(cleanup_cmd), "rm -rf %s/%s", poll_dir, poll_name);
    { int _rc_ = system(cleanup_cmd); (void)_rc_; }   /* clean stale state from prior runs */

    /* Step 1: create poller before queue exists */
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_poller_create(poll_dir, poll_name, 16, 5000000000ULL, rt, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    MmapqPollResult r1 = mmapq_indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r1);

    /* Step 2: create the queue, append a message */
    MmapqIndexedQueue *q2 = NULL;
    int rc_q = mmapq_indexed_queue_create_default(poll_dir, poll_name, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q2);

    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q2, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    int64_t idx = mmapq_indexed_appender_append(a, "hello", 5);
    TEST_ASSERT_EQUAL_INT64(0, idx);
    mmapq_indexed_appender_close(a);

    /* Step 3: poll should now see the message */
    PollCapture cap2 = { .directive = MMAPQ_NEXT_ADVANCE };
    MmapqPollResult r2 = mmapq_indexed_poller_poll(p, capture_handler, &cap2);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r2);
    TEST_ASSERT_EQUAL_INT(1, cap2.call_count);

    mmapq_indexed_poller_close(p);
    mmapq_indexed_queue_close(q2);
    { int _rc_ = system(cleanup_cmd); (void)_rc_; }
}

void test_append_multiple_and_poll_all(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    const char *msgs[] = { "alpha", "beta", "gamma", "delta" };
    int n = 4;

    for (int i = 0; i < n; i++) {
        int64_t idx = mmapq_indexed_appender_append(a, msgs[i], (int32_t)strlen(msgs[i]));
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    for (int i = 0; i < n; i++) {
        PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
        MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
        TEST_ASSERT_EQUAL_INT64(i, cap.index);
        TEST_ASSERT_EQUAL_STRING(msgs[i], cap.data);
    }

    /* Next poll should be end of stream */
    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM,
        mmapq_indexed_poller_poll(p, capture_handler, &cap));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

/* ── Random access reader ────────────────────────────────────────────────── */

void test_reader_read_at_index(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    mmapq_indexed_appender_append(a, "first",  5);
    mmapq_indexed_appender_append(a, "second", 6);
    mmapq_indexed_appender_append(a, "third",  5);

    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(6, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("second", ctx.data, 6);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_reader_close(r);
}

void test_reader_end_of_stream_beyond_last(void)
{
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    mmapq_indexed_reader_close(r);
}

void test_reader_last_index(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, mmapq_indexed_reader_last_index(r));

    mmapq_indexed_appender_append(a, "msg", 3);
    mmapq_indexed_appender_append(a, "msg", 3);
    mmapq_indexed_appender_append(a, "msg", 3);

    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_reader_last_index(r));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_reader_close(r);
}

void test_reader_read_first_and_last(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    mmapq_indexed_appender_append(a, "first",  5);
    mmapq_indexed_appender_append(a, "middle", 6);
    mmapq_indexed_appender_append(a, "last",   4);

    MmapqReadingContext ctx = {0};

    mmapq_indexed_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_MEMORY("first", ctx.data, 5);

    mmapq_indexed_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(2, ctx.index);
    TEST_ASSERT_EQUAL_MEMORY("last", ctx.data, 4);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_reader_close(r);
}

/* ── Poller cursor movement ──────────────────────────────────────────────── */

void test_poller_move_to_end(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    mmapq_indexed_appender_append(a, "a", 1);
    mmapq_indexed_appender_append(a, "b", 1);
    mmapq_indexed_appender_append(a, "c", 1);

    mmapq_indexed_poller_move_to_end(p);
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_poller_current_index(p));

    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM,
        mmapq_indexed_poller_poll(p, capture_handler, &cap));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poller_move_to_start(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    mmapq_indexed_appender_append(a, "x", 1);
    mmapq_indexed_appender_append(a, "y", 1);

    mmapq_indexed_poller_move_to_end(p);
    mmapq_indexed_poller_move_to_start(p);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poller_move_to_index(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    for (int i = 0; i < 5; i++)
        mmapq_indexed_appender_append(a, "x", 1);

    int ok = mmapq_indexed_poller_move_to_index(p, 3);
    TEST_ASSERT_EQUAL_INT(1, ok);
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_poller_current_index(p));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poller_handler_stop(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    mmapq_indexed_appender_append(a, "stop_me", 7);

    PollCapture cap = { .directive = MMAPQ_NEXT_STOP };
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_STOPPED, r);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));  /* cursor didn't move */

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poller_handler_retreat(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    mmapq_indexed_appender_append(a, "a", 1);
    mmapq_indexed_appender_append(a, "b", 1);

    /* Advance to index 1 */
    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    mmapq_indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_poller_current_index(p));

    /* Retreat */
    cap.directive = MMAPQ_NEXT_RETREAT;
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_RETREATED, r);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poller_retreat_at_start_stays(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    mmapq_indexed_appender_append(a, "x", 1);

    /* Retreat from index 0 — should stop, not go negative */
    PollCapture cap = { .directive = MMAPQ_NEXT_RETREAT };
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_STOPPED, r);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_poller_current_index(p));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

/* ── Multiple appenders ──────────────────────────────────────────────────── */

void test_multiple_appenders_interleaved(void)
{
    MmapqIndexedAppender *a0 = NULL;
    int rc_a0 = mmapq_indexed_queue_create_appender(q, &a0);
    TEST_ASSERT_EQUAL_INT(0, rc_a0);
    TEST_ASSERT_NOT_NULL(a0);
    MmapqIndexedAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Interleave appends from two appenders */
    mmapq_indexed_appender_append(a0, "from_a0_0", 9);
    mmapq_indexed_appender_append(a1, "from_a1_0", 9);
    mmapq_indexed_appender_append(a0, "from_a0_1", 9);
    mmapq_indexed_appender_append(a1, "from_a1_1", 9);

    /* Poll all 4 in order — verify appender IDs alternate */
    uint8_t id0 = mmapq_indexed_appender_id(a0);
    uint8_t id1 = mmapq_indexed_appender_id(a1);

    for (int i = 0; i < 4; i++) {
        PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
        MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);

        if (i == 0 || i == 2)
            TEST_ASSERT_EQUAL_UINT8(id0, cap.appender_id);
        else
            TEST_ASSERT_EQUAL_UINT8(id1, cap.appender_id);
    }

    mmapq_indexed_appender_close(a0);
    mmapq_indexed_appender_close(a1);
    mmapq_indexed_poller_close(p);
}

/* ── Region boundary (padding) ───────────────────────────────────────────── */

void test_large_message_sequence_crossing_regions(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
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
        int64_t idx = mmapq_indexed_appender_append(a, msg, sizeof(msg));
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    /* Poll all messages and verify first byte */
    for (int i = 0; i < count; i++) {
        PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
        MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT_MESSAGE(MMAPQ_POLL_ADVANCED, r, "expected ADVANCED");
        TEST_ASSERT_EQUAL_INT32(1000, cap.length);
        TEST_ASSERT_EQUAL_CHAR((char)('A' + i), cap.data[0]);
    }

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

/* ── Restart recovery ────────────────────────────────────────────────────── */

void test_appender_recovers_position_after_close(void)
{
    /* Phase 1: append some messages and close the appender */
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    uint8_t id = mmapq_indexed_appender_id(a);

    mmapq_indexed_appender_append(a, "msg_0", 5);
    mmapq_indexed_appender_append(a, "msg_1", 5);
    int64_t before_close = mmapq_indexed_appender_current_index(a);
    mmapq_indexed_appender_close(a);

    /* Phase 2: reopen with same ID — should resume at index 2 */
    MmapqIndexedAppender *a2 = NULL;
    int rc_a2 = mmapq_indexed_appender_create_with_id(
        DIR, QUEUE, REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT_NS, 0, rt, id, 0, &a2);
    TEST_ASSERT_EQUAL_INT(0, rc_a2);
    TEST_ASSERT_NOT_NULL(a2);

    TEST_ASSERT_EQUAL_INT64(before_close, mmapq_indexed_appender_current_index(a2));

    /* Phase 3: append more messages — should get index 2, 3 */
    int64_t idx2 = mmapq_indexed_appender_append(a2, "msg_2", 5);
    int64_t idx3 = mmapq_indexed_appender_append(a2, "msg_3", 5);
    TEST_ASSERT_EQUAL_INT64(2, idx2);
    TEST_ASSERT_EQUAL_INT64(3, idx3);

    /* Phase 4: poll all 4 messages */
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    const char *expected[] = { "msg_0", "msg_1", "msg_2", "msg_3" };

    for (int i = 0; i < 4; i++) {
        PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
        MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
        TEST_ASSERT_EQUAL_STRING(expected[i], cap.data);
    }

    mmapq_indexed_appender_close(a2);
    mmapq_indexed_poller_close(p);
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
    MmapqIndexedAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    MmapqIndexedAppender *a2 = NULL;
    int rc_a2 = mmapq_indexed_queue_create_appender(q, &a2);
    TEST_ASSERT_EQUAL_INT(0, rc_a2);
    TEST_ASSERT_NOT_NULL(a2);
    TEST_ASSERT_NOT_NULL(a2);

    int header_files_to_fill = 10;
    int64_t msgs = (header_files_to_fill * MAX_FILE) / HEADER_SIZE;

    const char *payload = "x";
    int32_t     plen    = 1;

    for (int64_t i = 0; i < msgs; i++) {
        int64_t idx = mmapq_indexed_appender_append(a1, payload, plen);
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    int64_t idx = mmapq_indexed_appender_append(a2, payload, plen);
    clock_gettime(CLOCK_MONOTONIC, &ts1);

    TEST_ASSERT_EQUAL_INT64(msgs, idx);

    uint64_t dt_us = (uint64_t)((ts1.tv_sec - ts0.tv_sec) * 1000000LL
                              + (ts1.tv_nsec - ts0.tv_nsec) / 1000LL);
    (void)dt_us;

    mmapq_indexed_appender_close(a1);
    mmapq_indexed_appender_close(a2);
}

/* ── Uncommitted append + commit ─────────────────────────────────────────── */

void test_uncommitted_append_and_commit(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Append uncommitted — not visible to poller yet */
    int64_t offset = mmapq_indexed_appender_append_uncommitted(a, "deferred", 8);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, offset);

    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r);  /* not visible */

    /* Commit — now visible */
    int64_t idx = mmapq_indexed_appender_commit(a, offset);
    TEST_ASSERT_EQUAL_INT64(0, idx);

    r = mmapq_indexed_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT32(8, cap.length);
    TEST_ASSERT_EQUAL_MEMORY("deferred", cap.data, 8);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

/* ── Uncommitted iterator ────────────────────────────────────────────────── */

void test_uncommitted_iterator_finds_uncommitted(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    /* Append one committed, one uncommitted */
    mmapq_indexed_appender_append(a, "committed", 9);
    int64_t uncommitted_off = mmapq_indexed_appender_append_uncommitted(a, "uncommitted", 11);

    /* The uncommitted iterator should find exactly one message */
    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    int found = 0;
    while (mmapqi_uncommitted_iterator_advance(it)) {
        found++;
        Buffer *buf = mmapqi_uncommitted_iterator_current(it);
        TEST_ASSERT_EQUAL_INT32(11, buf->capacity);
        TEST_ASSERT_EQUAL_MEMORY("uncommitted", buf->data, 11);
    }
    TEST_ASSERT_EQUAL_INT(1, found);

    /* Now commit and check again — should find nothing */
    mmapq_indexed_appender_commit(a, uncommitted_off);

    it = mmapq_indexed_appender_uncommitted_iterator(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);
    found = 0;
    while (mmapqi_uncommitted_iterator_advance(it)) found++;
    TEST_ASSERT_EQUAL_INT(0, found);

    mmapq_indexed_appender_close(a);
}

/* ── C-03 regression: out-of-range iterator offsets must not double-free ──── */

/* start_of_stream for a fresh appender is 64, so offset 0 violates the
 * iterator contract (offsets must be >= start_of_stream or NOT_SET).
 * Pre-fix, mmapqi_payload_iterator_init() freed the appender-owned iterator
 * on the out-of-range offset and returned NULL, leaving a->payload_iter
 * dangling: the second call ran init on freed memory and
 * mmapq_indexed_appender_close() freed it again (SIGABRT / ASan
 * double-free). Post-fix the factory rejects out-of-range offsets before
 * touching the appender's iterator. */
static void test_appender_iterator_out_of_range_offset_no_double_free(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "data", 4));

    TEST_ASSERT_NULL(mmapq_indexed_appender_iterator(a, 0, PAYLOAD_ITERATOR_NOT_SET));
    /* The first NULL must not have freed the appender-owned iterator: a
     * second identical call must return NULL without touching freed memory. */
    TEST_ASSERT_NULL(mmapq_indexed_appender_iterator(a, 0, PAYLOAD_ITERATOR_NOT_SET));

    /* The rejection must not be over-broad: a valid call creates the
     * appender-owned iterator and finds the appended message. */
    MmapqPayloadIterator *it =
        mmapq_indexed_appender_iterator(a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);
    TEST_ASSERT_TRUE(mmapqi_payload_iterator_advance(it));
    Buffer *buf = mmapqi_payload_iterator_current(it);
    TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
    TEST_ASSERT_EQUAL_MEMORY("data", buf->data, 4);
    TEST_ASSERT_FALSE(mmapqi_payload_iterator_advance(it));

    mmapq_indexed_appender_close(a);
}

/* ── Reader entry_status ─────────────────────────────────────────────────── */

void test_reader_entry_status(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    mmapq_indexed_appender_append(a, "x", 1);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE,     mmapq_indexed_reader_entry_status(r, 0));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, mmapq_indexed_reader_entry_status(r, 1));
    TEST_ASSERT_TRUE(mmapq_indexed_reader_has_entry(r, 0));
    TEST_ASSERT_FALSE(mmapq_indexed_reader_has_entry(r, 1));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_reader_close(r);
}

/* ── Poller exists ───────────────────────────────────────────────────────── */

void test_poller_exists(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_FALSE(mmapq_indexed_poller_exists(p, 0));
    mmapq_indexed_appender_append(a, "y", 1);
    TEST_ASSERT_TRUE(mmapq_indexed_poller_exists(p, 0));
    TEST_ASSERT_FALSE(mmapq_indexed_poller_exists(p, 1));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_poller_close(p);
}

void test_poller_automatically_discovers_region_size(void)
{
    /* Phase 1: Create a queue with 8MB regions and write a message */
    int32_t LARGE_REGION = 8 * 1024 * 1024;
    MmapqIndexedQueue *q1 = NULL;
    int rc_q1 = mmapq_indexed_queue_create(DIR, QUEUE, LARGE_REGION, RING_SZ, 1, 
                                      LARGE_REGION * 16, 0, TIMEOUT_NS, 0, rt, rt, &q1);
    TEST_ASSERT_EQUAL_INT(0, rc_q1);
    TEST_ASSERT_NOT_NULL(q1);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q1, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "hello", 5);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q1);

    /* Phase 2: Create a second queue instance with "wrong" default (4KB) */
    MmapqIndexedQueue *q2 = NULL;
    int rc_q2 = mmapq_indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                      MAX_FILE, 0, TIMEOUT_NS, 0, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q2);
    TEST_ASSERT_NOT_NULL(q2);
    
    /* Participants discover the persisted geometry despite the factory preferences. */
    MmapqIndexedReader *reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q2, &reader));
    QueueMeta *meta = mmapqi_indexed_reader_queue_meta(reader);
    TEST_ASSERT_NOT_NULL(meta);
    TEST_ASSERT_EQUAL_INT32(LARGE_REGION, queue_meta_region_size(meta));
    TEST_ASSERT_EQUAL_INT64((int64_t)LARGE_REGION * 16, queue_meta_max_file_size(meta));
    mmapq_indexed_reader_close(reader);

    /* Create poller from q2 and poll the message */
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q2, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    MmapqPollResult r = mmapq_indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_STRING("hello", cap.data);

    mmapq_indexed_poller_close(p);
    mmapq_indexed_queue_close(q2);
}

void test_append_rejects_zero_length(void)
{
    MmapqIndexedQueue      *q = NULL;
    int rc_q = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    int64_t idx = mmapq_indexed_appender_append(a, "first", 5);
    TEST_ASSERT_EQUAL_INT64(0, idx);

    int64_t bad = mmapq_indexed_appender_append(a, "", 0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    /* Verify state unchanged — next append gets correct index */
    int64_t next = mmapq_indexed_appender_append(a, "third", 5);
    TEST_ASSERT_EQUAL_INT64(1, next);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

void test_append_rejects_oversized_message(void)
{
    MmapqIndexedQueue      *q = NULL;
    MmapqIndexedAppender   *a = NULL;

    /* Create with small region so we can exceed it */
    int rc_q = mmapq_indexed_queue_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, 1,
        4096LL * 64, 0, TIMEOUT_NS,
        0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    int64_t ok = mmapq_indexed_appender_append(a, "small", 5);
    TEST_ASSERT_EQUAL_INT64(0, ok);

    /* REGION_SZ = 4096, message of 4096 bytes exceeds region_size - 4 */
    char big[4096];
    memset(big, 'x', sizeof(big));
    int64_t bad = mmapq_indexed_appender_append(a, big, (int32_t)sizeof(big));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    /* Verify state unchanged — next append succeeds */
    int64_t next = mmapq_indexed_appender_append(a, "after", 5);
    TEST_ASSERT_EQUAL_INT64(1, next);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

void test_open_normal_as_batch_refused(void)
{
    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "msg", 3);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);

    /* Attempt to create a batch participant on the same queue */
    MmapqIndexedBatchQueue *bq = NULL;
    int rc_bq = mmapq_indexed_batch_queue_create_default(DIR, QUEUE, rt, rt, &bq);
    TEST_ASSERT_EQUAL_INT(0, rc_bq);
    TEST_ASSERT_NOT_NULL(bq);
    MmapqIndexedBatchAppender *ba = NULL;
    int rc_ba = mmapq_indexed_batch_queue_create_appender(bq, &ba);
    TEST_ASSERT_TRUE(rc_ba != 0);
    TEST_ASSERT(rc_ba != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(ba);

    if (bq) mmapq_indexed_batch_queue_close(bq);
}

void test_meta_schema_mismatch_refused(void)
{
    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "msg", 3);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);

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
    MmapqIndexedQueue *q2 = NULL;
    int rc_q2 = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q2);
    TEST_ASSERT_NOT_NULL(q2);
    MmapqIndexedAppender *a2 = NULL;
    int rc_a2 = mmapq_indexed_queue_create_appender(q2, &a2);
    TEST_ASSERT_TRUE(rc_a2 != 0);
    TEST_ASSERT(rc_a2 != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(a2);

    if (q2) mmapq_indexed_queue_close(q2);
}

void test_meta_fixed8_refused(void)
{
    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "msg", 3);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);

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
    MmapqIndexedQueue *q2 = NULL;
    int rc_q2 = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &q2);
    TEST_ASSERT_EQUAL_INT(0, rc_q2);
    TEST_ASSERT_NOT_NULL(q2);
    MmapqIndexedAppender *a2 = NULL;
    int rc_a2 = mmapq_indexed_queue_create_appender(q2, &a2);
    TEST_ASSERT_TRUE(rc_a2 != 0);
    TEST_ASSERT_NULL(a2);

    if (q2) mmapq_indexed_queue_close(q2);
}

/* ── Standalone factory — missing directory ───────────────────────────────── */

static void test_standalone_factory_rejects_missing_directory(void)
{
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_appender_create_with_id(
        "/nonexistent-dir-for-test", "q",
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 7, 0, &a);
    TEST_ASSERT_TRUE(rc_a != 0);
    TEST_ASSERT_NULL(a);
}

/* ── Corrupt queue — poll detects corrupted payload ───────────────────────── */

static int corrupt_handler_count;
static MmapqNextMove corrupt_handler(int64_t index, uint8_t aid,
                                 const void *data, int32_t len, void *ctx)
{
    (void)index; (void)aid; (void)data; (void)len; (void)ctx;
    corrupt_handler_count++;
    return MMAPQ_NEXT_ADVANCE;
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
    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(
        DIR, qname,
        REGION_SZ, RING_SZ, 1, MAX_FILE,
        0, TIMEOUT_NS, 0, NULL, NULL, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);

    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    int64_t idx = mmapq_indexed_appender_append(a, "hello", 5);
    TEST_ASSERT(idx >= 0);
    mmapq_indexed_appender_close(a);

    /* Corrupt the payload: find the header at index 0, decode payload offset,
       write zero at that offset (the 4-byte length prefix) */
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_reader_create(DIR, qname, 4, 5000000000ULL, NULL, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT(ctx.status == MMAPQ_ENTRY_AVAILABLE);

    /* Corrupt the payload file directly */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, qname, (unsigned)ctx.appender_id);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "failed to open payload file for writing");
    uint32_t zero = 0;
    ssize_t written = pwrite(fd, &zero, sizeof(zero), ctx.msg_offset); (void)written;
    close(fd);
    mmapq_indexed_reader_close(r);

    /* Now poll — should get MMAPQ_POLL_CORRUPT */
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    corrupt_handler_count = 0;
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, corrupt_handler, NULL);
    TEST_ASSERT_EQUAL(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL(0, corrupt_handler_count);  /* handler was NOT called */
    mmapq_indexed_poller_close(p);

    mmapq_indexed_queue_close(q);
    cleanup();
}

static void test_reader_detects_corrupt_length(void)
{
    /* Pin for the Phase 16e conversion of indexed_reader.c:163. The length
       check must be a runtime check (returning MMAPQ_ENTRY_ERROR/ECORRUPT),
       not an MMAP_ASSERT that compiles away under NDEBUG. */
    char qname[64];
    snprintf(qname, sizeof(qname), "%s_rlcorrupt", QUEUE);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    int rc = system(cmd); (void)rc;

    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(
        DIR, qname, REGION_SZ, RING_SZ, 1, MAX_FILE, 0, TIMEOUT_NS, 0, NULL, NULL, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);

    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    int64_t idx = mmapq_indexed_appender_append(a, "hello", 5);
    TEST_ASSERT(idx >= 0);
    mmapq_indexed_appender_close(a);

    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_reader_create(DIR, qname, 4, TIMEOUT_NS, NULL, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT(ctx.status == MMAPQ_ENTRY_AVAILABLE);

    /* Corrupt the 4-byte length prefix at msg_offset to 0 (end-of-stream
       sentinel), simulating truncation/corruption on disk. */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, qname, (unsigned)ctx.appender_id);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "failed to open payload file for writing");
    uint32_t zero = 0;
    ssize_t written = pwrite(fd, &zero, sizeof(zero), ctx.msg_offset); (void)written;
    close(fd);

    /* Re-read — must surface the corruption as an error, not assert (which
       would compile away under NDEBUG and hand back a zero-length message). */
    MmapqReadingContext ctx2 = {0};
    mmapq_indexed_reader_read(r, 0, &ctx2);
    TEST_ASSERT(ctx2.status == MMAPQ_ENTRY_ERROR);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));

    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);
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

    MmapqIndexedAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_appender_create_with_id(
        dir, QUEUE,
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 7, 0, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);

    MmapqIndexedAppender *a2 = NULL;
    int rc_a2 = mmapq_indexed_appender_create_with_id(
        dir, QUEUE,
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 7, 0, &a2);
    TEST_ASSERT_TRUE(rc_a2 != 0);
    TEST_ASSERT(rc_a2 != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(a2);

    mmapq_indexed_appender_close(a1);
}

/* ── Appender ID boundary pin ────────────────────────────────────────────── */

static void test_appender_id_255_succeeds(void)
{
    char tmp[128];
    snprintf(tmp, sizeof(tmp), "%s/%s_XXXXXX", DIR, QUEUE);
    char *dir = mkdtemp(tmp);
    TEST_ASSERT_NOT_NULL(dir);
    char qdir[256];
    snprintf(qdir, sizeof(qdir), "%s/%s", dir, QUEUE);
    mkdir(qdir, 0755);

    MmapqIndexedAppender *a = NULL;
    int rc = mmapq_indexed_appender_create_with_id(
        dir, QUEUE,
        4096 * 1024, 4, (4096 * 1024) * 256,
        10000000000, 0, NULL, 255, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    TEST_ASSERT_EQUAL_UINT8(255, mmapq_indexed_appender_id(a));
    mmapq_indexed_appender_close(a);
}

/* ── last_error pins ─────────────────────────────────────────────────────── */

static MmapqNextMove last_error_dummy_handler(int64_t idx, uint8_t aid, const void *data, int32_t len, void *ctx)
{ (void)idx; (void)aid; (void)data; (void)len; (void)ctx; return MMAPQ_NEXT_ADVANCE; }

void test_reader_last_error_reports_eschema(void)
{
    /* Create a batch queue (wrong type for a normal MmapqIndexedReader) */
    cleanup();
    MmapqIndexedBatchQueue *bq = NULL;
    int rc = mmapq_indexed_batch_queue_create(DIR, QUEUE,
              4096, 16, 1, 4096LL * 64, 0, 5000000000ULL, 0, rt, rt, &bq);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(bq);
    /* Write a batch so meta exists with QUEUE_TYPE_BATCH */
    MmapqIndexedBatchAppender *ba = NULL;
    rc = mmapq_indexed_batch_queue_create_appender(bq, &ba);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(ba);
    MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(ba);
    TEST_ASSERT_NOT_NULL(mb);
    mmapq_mutable_batch_append(mb, "x", 1);
    int64_t off = mmapq_mutable_batch_complete(mb);
    mmapq_indexed_batch_appender_commit_batch(ba, off);
    mmapq_indexed_batch_appender_close(ba);
    mmapq_indexed_batch_queue_close(bq);

    /* Open a normal MmapqIndexedReader on the batch queue — should get ESCHEMA */
    MmapqIndexedReader *r = NULL;
    rc = mmapq_indexed_reader_create(DIR, QUEUE, 4, 5000000000ULL, NULL, &r);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL(MMAPQ_ESCHEMA, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);

    /* Cleanup for setUp/tearDown */
    cleanup();
}

void test_last_error_zero_on_success(void)
{
    /* setUp already created the queue and appended nothing. Read of 0
       should be END_OF_STREAM (not error), and last_error should be 0. */
    MmapqIndexedReader *r = NULL;
    int rc = mmapq_indexed_reader_create(DIR, QUEUE, 4, 5000000000ULL, NULL, &r);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL(0, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);

    /* Also verify poller last_error is 0 after a valid end-of-stream poll */
    MmapqIndexedPoller *p = NULL;
    rc = mmapq_indexed_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(p);
    /* Use a real handler so we don't hit the NULL-handler error path */
    MmapqPollResult pr = mmapq_indexed_poller_poll(p, last_error_dummy_handler, NULL);
    TEST_ASSERT_EQUAL(MMAPQ_POLL_END_OF_STREAM, pr);
    TEST_ASSERT_EQUAL(0, mmapq_indexed_poller_last_error(p));
    mmapq_indexed_poller_close(p);
}

/* ── Phase 14a/14b pins ──────────────────────────────────────────────────── */

void test_poller_null_guard(void)
{
    /* NULL poller must return MMAPQ_POLL_ERROR, not dereference before the guard */
    MmapqPollResult r = mmapq_indexed_poller_poll(NULL, capture_handler, NULL);
    TEST_ASSERT_EQUAL(MMAPQ_POLL_ERROR, r);
}

void test_append_rejects_max_int_length(void)
{
    MmapqIndexedQueue    *q = NULL;
    MmapqIndexedAppender *a = NULL;

    int rc_q = mmapq_indexed_queue_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, 1,
        4096LL * 64, 0, TIMEOUT_NS,
        0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    /* INT32_MAX triggers signed-overflow in length + PAYLOAD_LENGTH_SIZE if
       the guard is written as (length + 4 > region_size). Fixed in Phase 14b
       to compare against the complement: (length > region_size - 4).
       This pin exists for UBSan detection: with the old code, UBSan fires
       on signed-integer-overflow; with the fix, it does not. */
    int64_t bad = mmapq_indexed_appender_append(a, "x", INT32_MAX);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

/* ── strerror pins ────────────────────────────────────────────────────────── */

void test_strerror_all_codes(void)
{
    /* Every defined code returns a non-empty string */
    const char *s;
    s = mmapq_strerror(MMAPQ_ETIMEDOUT);   TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_EINVAL);      TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_ECLOSED);     TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_ECORRUPT);    TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_ESCHEMA);     TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_EEXIST);      TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_ENOENT);      TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_EIDCONFLICT); TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_ENOMEM);      TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_EIO);         TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
    s = mmapq_strerror(MMAPQ_ENOTREADY);   TEST_ASSERT_NOT_NULL(s); TEST_ASSERT(strlen(s) > 0);
}

void test_strerror_unknown(void)
{
    const char *s = mmapq_strerror(-99);
    TEST_ASSERT_NOT_NULL(s);
    TEST_ASSERT(strlen(s) > 0);
    TEST_ASSERT_EQUAL_STRING("unknown error", s);
}

/* ── main ────────────────────────────────────────────────────────────────── */

static void test_read_and_poll_reject_payload_beyond_mapping(void)
{
    MmapqIndexedAppender *appender = NULL;
    MmapqIndexedReader *reader = NULL;
    MmapqIndexedPoller *poller = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &appender));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(appender, "data", 4));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &reader));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &poller));
    MmapqReadingContext context = {0};
    mmapq_indexed_reader_read(reader, 0, &context);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, context.status);
    int64_t offset = context.msg_offset;
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, QUEUE, (unsigned)context.appender_id);
    int descriptor = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(descriptor >= 0, "open payload");
    int32_t invalid_lengths[] = {REGION_SZ, INT32_MAX};
    for (size_t index = 0; index < sizeof(invalid_lengths) / sizeof(invalid_lengths[0]); index++) {
        TEST_ASSERT_EQUAL_INT64(sizeof(int32_t), pwrite(descriptor, &invalid_lengths[index], sizeof(int32_t), offset));
        mmapq_indexed_reader_read(reader, 0, &context);
        TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, context.status);
        TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(reader));
        TEST_ASSERT_NULL(context.data);
        PollCapture capture = {.directive = MMAPQ_NEXT_ADVANCE};
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, mmapq_indexed_poller_poll(poller, capture_handler, &capture));
        TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_poller_last_error(poller));
        TEST_ASSERT_EQUAL_INT(0, capture.call_count);
    }
    TEST_ASSERT_EQUAL_INT(0, close(descriptor));
    mmapq_indexed_poller_close(poller);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_appender_close(appender);
}

/* ── C-04 regression: corrupt on-disk length must not hang the reopen walk ── */

static void test_appender_open_rejects_corrupt_length(void)
{
    /*
     * C-04 (first-walk path): corrupt a committed message's length prefix so it
     * overruns the mapped region capacity. On reopen, initialise_append_position's
     * first payload walk reads it. Pre-fix the walk had no bound; the bogus offset
     * fell beyond the current file, and the follow-on walk faulted reading a mapped
     * region past EOF (SIGBUS). Post-fix the bounded walk returns -1 and the
     * appender create fails with MMAPQ_ECORRUPT.
     * Fail-first observed standalone: SIGBUS (exit 135).
     */
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    uint8_t id = mmapq_indexed_appender_id(a);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "data", 4));
    mmapq_indexed_appender_close(a);

    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, QUEUE, (unsigned)id);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "open payload");
    int32_t bogus = REGION_SZ * 8;   /* beyond capacity, jumps past the small file */
    TEST_ASSERT_EQUAL_INT64((int64_t)sizeof(int32_t),
        (int64_t)pwrite(fd, &bogus, sizeof(int32_t), INITIAL_PAYLOAD_POSITION));
    TEST_ASSERT_EQUAL_INT(0, close(fd));

    MmapqIndexedAppender *a2 = NULL;
    int rc = mmapq_indexed_appender_create_with_id(
        DIR, QUEUE, REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT_NS, 0, rt, id, 0, &a2);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a2);
}

static void test_appender_open_rejects_corrupt_uncommitted_tail(void)
{
    /*
     * C-04 (tail-loop path): leave the committed message's length intact and plant
     * a corrupt length just past it — an uncommitted tail. On reopen the first walk
     * past the committed message succeeds and the tail walk reads the corrupt length.
     * Pre-fix the follow-on walk faulted reading a mapped region past EOF (SIGBUS);
     * post-fix the bounded walk / next_len < 0 guard returns MMAPQ_ECORRUPT.
     * Fail-first observed standalone: SIGBUS (exit 135).
     */
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    uint8_t id = mmapq_indexed_appender_id(a);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "data", 4));
    mmapq_indexed_appender_close(a);

    /* Position just past message 0: INITIAL_PAYLOAD_POSITION + 4 (length) + 4 (payload) */
    int64_t tail = INITIAL_PAYLOAD_POSITION + PAYLOAD_LENGTH_SIZE + 4;
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, QUEUE, (unsigned)id);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "open payload");
    int32_t bogus = REGION_SZ * 8;
    TEST_ASSERT_EQUAL_INT64((int64_t)sizeof(int32_t),
        (int64_t)pwrite(fd, &bogus, sizeof(int32_t), tail));
    TEST_ASSERT_EQUAL_INT(0, close(fd));

    MmapqIndexedAppender *a2 = NULL;
    int rc = mmapq_indexed_appender_create_with_id(
        DIR, QUEUE, REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT_NS, 0, rt, id, 0, &a2);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a2);
}

static void test_poller_move_to_index_far_ahead_keeps_accessor_usable(void)
{
    MmapqIndexedAppender *a = NULL;
    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));

    for (int i = 0; i < 3; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "xyz", 3));

    /* Map region 0 and populate the accessor fast-path cache. */
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_index(p, 2));

    /* header_position(1<<40) hashes to region 0's ring slot: the slot remap
     * drops region 0's mapping and then fails (beyond file). A failed wrap
     * must invalidate the accessor fast-path cache; otherwise the next
     * in-window access is served a pointer into the unmapped region. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_move_to_index(p, 1LL << 40));

    /* In-window probe: must re-wrap through the region layer, not read the
     * stale fast-path cache. */
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_index(p, 3));
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_poller_current_index(p));

    /* The accessor must still be usable: poll all three messages back. */
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_index(p, 0));
    PollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_poller_poll(p, capture_handler, &cap));
        TEST_ASSERT_EQUAL_INT64(i, cap.index);
        TEST_ASSERT_EQUAL_INT32(3, cap.length);
        TEST_ASSERT_EQUAL_STRING("xyz", cap.data);
    }
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, mmapq_indexed_poller_poll(p, capture_handler, &cap));

    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(a);
}

int main(void)
{
    UNITY_BEGIN();

    RUN_TEST(test_read_and_poll_reject_payload_beyond_mapping);
    RUN_TEST(test_appender_open_rejects_corrupt_length);
    RUN_TEST(test_appender_open_rejects_corrupt_uncommitted_tail);
    RUN_TEST(test_append_and_poll_single_message);
    RUN_TEST(test_poll_end_of_stream_when_empty);
    RUN_TEST(test_poller_before_queue_retries);
    RUN_TEST(test_append_multiple_and_poll_all);

    RUN_TEST(test_reader_read_at_index);
    RUN_TEST(test_reader_end_of_stream_beyond_last);
    RUN_TEST(test_reader_last_index);
    RUN_TEST(test_reader_read_first_and_last);

    RUN_TEST(test_poller_move_to_end);
    RUN_TEST(test_poller_move_to_start);
    RUN_TEST(test_poller_move_to_index);
    RUN_TEST(test_poller_move_to_index_far_ahead_keeps_accessor_usable);
    RUN_TEST(test_poller_handler_stop);
    RUN_TEST(test_poller_handler_retreat);
    RUN_TEST(test_poller_retreat_at_start_stays);

    RUN_TEST(test_multiple_appenders_interleaved);
    RUN_TEST(test_stale_appender_catch_up);
    RUN_TEST(test_large_message_sequence_crossing_regions);
    RUN_TEST(test_appender_recovers_position_after_close);
    RUN_TEST(test_uncommitted_append_and_commit);
    RUN_TEST(test_uncommitted_iterator_finds_uncommitted);
    RUN_TEST(test_appender_iterator_out_of_range_offset_no_double_free);
    RUN_TEST(test_reader_entry_status);
    RUN_TEST(test_poller_exists);
    
    RUN_TEST(test_poller_automatically_discovers_region_size);

    RUN_TEST(test_append_rejects_zero_length);
    RUN_TEST(test_append_rejects_oversized_message);
    RUN_TEST(test_append_rejects_max_int_length);
    RUN_TEST(test_poller_null_guard);

    RUN_TEST(test_open_normal_as_batch_refused);
    RUN_TEST(test_meta_schema_mismatch_refused);
    RUN_TEST(test_meta_fixed8_refused);

    RUN_TEST(test_standalone_factory_rejects_missing_directory);
    RUN_TEST(test_create_with_id_already_in_use);
    RUN_TEST(test_appender_id_255_succeeds);
    RUN_TEST(test_poll_detects_corrupt_payload);
    RUN_TEST(test_reader_detects_corrupt_length);

    RUN_TEST(test_reader_last_error_reports_eschema);
    RUN_TEST(test_last_error_zero_on_success);

    RUN_TEST(test_strerror_all_codes);
    RUN_TEST(test_strerror_unknown);

    return UNITY_END();
}
