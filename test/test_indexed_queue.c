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
#include "internal/test_hooks.h"
#include <stdio.h>
#include <time.h>

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <stdint.h>

/* ── Test configuration ──────────────────────────────────────────────────── */

static const char    *DIR        = "/tmp";
static const char    *QUEUE      = "mmap_test_iq";
/* CAND-38: 32768 is the smallest legal region (one 32 KB header block). */
static const int32_t  REGION_SZ  = 32768;
static const int      RING_SZ    = 16;
static const int64_t  MAX_FILE   = 32768LL * 8;   /* small — triggers rolling */
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
     * Region size = 32768 bytes. Each message = 1000 bytes (+4 prefix).
     * ~32 messages per region. 140 messages = 140 KB crosses 4+ regions
     * (140 x 1004 = 140560 > 4 x 32768 = 131072) and stays within
     * MAX_FILE (8 regions).
     */
    char msg[1000];
    memset(msg, 'X', sizeof(msg));

    int count = 140;
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
    /* D8: advance is three-way (1 message / 0 exhausted / negative MMAPQ_E*
     * propagated error). A negative is truthy in C — `while (advance(...))`
     * loops forever on a repeating error; compare against 1 explicitly. */
    while (mmapqi_uncommitted_iterator_advance(it) == 1) {
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
    while (mmapqi_uncommitted_iterator_advance(it) == 1) found++;
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
    TEST_ASSERT_EQUAL_INT(1, mmapqi_payload_iterator_advance(it));
    Buffer *buf = mmapqi_payload_iterator_current(it);
    TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
    TEST_ASSERT_EQUAL_MEMORY("data", buf->data, 4);
    TEST_ASSERT_EQUAL_INT(0, mmapqi_payload_iterator_advance(it));

    mmapq_indexed_appender_close(a);
}

/* ── D8: walk-layer timeout/corruption distinction ─────────────────────────
 * The payload walk used to collapse every failure — transient mapping
 * timeout AND on-disk corruption — into -1, and each layer above collapsed
 * further (iterator: error -> end-of-stream; scan/init/join: error ->
 * ECORRUPT). Retry (timeout) and recreate (corruption) are opposite
 * remedies, so these pins drive both codes through every fixed layer.
 * Test access: a stub RegionAccessor delegating to the real one except at
 * one chosen position (probe idiom, audit/claude-D8-pre/d8_probe.c); the
 * join-layer pins use the site-selecting appender-walk hook instead
 * (audit/D8-spec.md §4 pin 5). */

typedef struct {
    RegionAccessor  base;
    RegionAccessor *real;
    int64_t         fail_pos;
    WrapResult      fail_with;
    int             hits;
} D8StubAccessor;

static WrapResult d8_stub_wrap(RegionAccessor *self, int64_t position, Buffer *buf)
{
    D8StubAccessor *s = (D8StubAccessor *)self;
    if (position == s->fail_pos) { s->hits++; return s->fail_with; }
    return s->real->wrap(s->real, position, buf);
}

static WrapResult d8_stub_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buf)
{
    D8StubAccessor *s = (D8StubAccessor *)self;
    if (position == s->fail_pos) { s->hits++; return s->fail_with; }
    return s->real->wrap_no_premap(s->real, position, buf);
}

static void d8_stub_close(RegionAccessor *self) { (void)self;  /* not owned */ }

static void d8_stub_init(D8StubAccessor *s, RegionAccessor *real,
                         int64_t fail_pos, WrapResult fail_with)
{
    memset(s, 0, sizeof(*s));
    s->base.wrap           = d8_stub_wrap;
    s->base.wrap_no_premap = d8_stub_wrap_no_premap;
    s->base.close          = d8_stub_close;
    s->base.region_size    = real->region_size;
    s->real                = real;
    s->fail_pos            = fail_pos;
    s->fail_with           = fail_with;
}

/* Pin 2 (F2): a mapping TIMEOUT on the scan's header read must fail init
 * with MMAPQ_ETIMEDOUT, not truncate the scan. Pre-fix the plain header
 * read collapses the timeout to HEADER_NULL: the scan stops early, init
 * returns 0, and committed messages past the timeout are misreported as
 * uncommitted (probes P3/P4). */
void test_uncommitted_scan_header_timeout_propagates(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    for (int i = 0; i < 4; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "c", 1));

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);   /* clean init before the stub */

    D8StubAccessor s;
    d8_stub_init(&s, it->header_accessor, header_position(1), WRAP_MAP_TIMED_OUT);
    it->header_accessor = &s.base;

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
        mmapqi_uncommitted_iterator_init(it, PAYLOAD_ITERATOR_NOT_SET,
                                         PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_EQUAL_INT(1, s.hits);
    /* The public factory refuses on the same timeout (init failure -> NULL). */
    TEST_ASSERT_NULL(mmapq_indexed_appender_uncommitted_iterator(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET));

    it->header_accessor = s.real;   /* restore before close */
    mmapq_indexed_appender_close(a);
}

/* Pin 3 (probe P5 shape): a mapping TIMEOUT walking a committed header's
 * payload during the scan must propagate out of init as MMAPQ_ETIMEDOUT.
 * Pre-fix the walk's -1 collapsed to scan failure -> init MMAPQ_ECORRUPT. */
void test_uncommitted_scan_payload_timeout_propagates(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    D8StubAccessor s;
    d8_stub_init(&s, it->inner->payload_accessor, INITIAL_PAYLOAD_POSITION,
                 WRAP_MAP_TIMED_OUT);
    it->inner->payload_accessor = &s.base;

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
        mmapqi_uncommitted_iterator_init(it, PAYLOAD_ITERATOR_NOT_SET,
                                         PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_EQUAL_INT(1, s.hits);

    it->inner->payload_accessor = s.real;   /* restore before close */
    mmapq_indexed_appender_close(a);
}

/* Pin 4 (§1.2): the committed payload iterator is three-way — a mapping
 * TIMEOUT mid-stream propagates as MMAPQ_ETIMEDOUT instead of collapsing
 * to end-of-stream (pre-fix: advance == 0). The success path is guarded on
 * both sides: 1 yields the message, 0 only at end of stream, and the
 * iterator remains usable after a propagated error (timeout is retryable). */
void test_payload_iterator_advance_timeout_propagates(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    /* append() returns the index; the payload positions are 64 and 70. */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "d0", 2));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, "d1", 2));
    const int64_t p1 = INITIAL_PAYLOAD_POSITION + 6;

    MmapqPayloadIterator *it =
        mmapq_indexed_appender_iterator(a, PAYLOAD_ITERATOR_NOT_SET,
                                        PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    TEST_ASSERT_EQUAL_INT(1, mmapqi_payload_iterator_advance(it));   /* d0 */
    Buffer *buf = mmapqi_payload_iterator_current(it);
    TEST_ASSERT_EQUAL_INT32(2, buf->capacity);
    TEST_ASSERT_EQUAL_MEMORY("d0", buf->data, 2);

    D8StubAccessor s;
    d8_stub_init(&s, it->payload_accessor, p1, WRAP_MAP_TIMED_OUT);
    it->payload_accessor = &s.base;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_payload_iterator_advance(it));
    TEST_ASSERT_EQUAL_INT(1, s.hits);

    /* Timeout is retryable: with the stub removed the same iterator advances. */
    it->payload_accessor = s.real;
    TEST_ASSERT_EQUAL_INT(1, mmapqi_payload_iterator_advance(it));   /* d1 */
    buf = mmapqi_payload_iterator_current(it);
    TEST_ASSERT_EQUAL_MEMORY("d1", buf->data, 2);
    TEST_ASSERT_EQUAL_INT(0, mmapqi_payload_iterator_advance(it));   /* end of stream */

    mmapq_indexed_appender_close(a);
}

/* Pin 6a (the :122 gap branch, probe S2 real-gap geometry): c0 committed,
 * u1 uncommitted, c2 committed — the gap-resync advance walks u1. Pre-fix
 * the swallow was harmless in outcome (init rc 0, 2 correct ranges);
 * post-fix the timeout propagates. Behaviour change, stated: corrupt
 * framing inside a gap now fails init with ECORRUPT; before, it was a
 * silent gap and later a silent end of stream. */
void test_uncommitted_scan_gap_branch_timeout_real_gap(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));
    int64_t u1 = mmapq_indexed_appender_append_uncommitted(a, "u1", 2);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION + 6, u1);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, "c2", 2));

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    D8StubAccessor s;
    d8_stub_init(&s, it->inner->payload_accessor, u1, WRAP_MAP_TIMED_OUT);
    it->inner->payload_accessor = &s.base;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
        mmapqi_uncommitted_iterator_init(it, PAYLOAD_ITERATOR_NOT_SET,
                                         PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_EQUAL_INT(1, s.hits);

    it->inner->payload_accessor = s.real;
    mmapq_indexed_appender_close(a);
}

/* Pin 6b (the :122 gap branch, probe S2 contiguous geometry): c0, c1
 * committed and contiguous — the resync advance walks c1 itself. Pre-fix:
 * the gap branch's re-init advance hit the stub again and the scan failed,
 * init MMAPQ_ECORRUPT (hits=2). Post-fix the first failure propagates as
 * MMAPQ_ETIMEDOUT (hits=1). */
void test_uncommitted_scan_gap_branch_timeout_contiguous(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    /* append() returns the index; c1's payload position is 70. */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, "c1", 2));
    const int64_t p1 = INITIAL_PAYLOAD_POSITION + 6;

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    D8StubAccessor s;
    d8_stub_init(&s, it->inner->payload_accessor, p1, WRAP_MAP_TIMED_OUT);
    it->inner->payload_accessor = &s.base;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
        mmapqi_uncommitted_iterator_init(it, PAYLOAD_ITERATOR_NOT_SET,
                                         PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_EQUAL_INT(1, s.hits);

    it->inner->payload_accessor = s.real;
    mmapq_indexed_appender_close(a);
}

/* Pin 6c (the gap branch's RESYNC advance — the third inner-advance site;
 * review catch, probe audit/claude-D8/claude-D8-review.zip resync.c):
 * c0 committed, u1 uncommitted, c2 committed. The :122 advance walks u1
 * (offset mismatch -> gap branch), the branch re-inits at c2, and the
 * resync advance fires the stub there. Post-fix init propagates
 * MMAPQ_ETIMEDOUT; pre-fix (and under mutant M9) the timeout collapses to
 * MMAPQ_ECORRUPT — the exact collapse D8 exists to remove. Pins 6a/6b
 * cover only the :122 condition advance. */
void test_uncommitted_scan_gap_resync_timeout_propagates(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));
    int64_t u1 = mmapq_indexed_appender_append_uncommitted(a, "u1", 2);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION + 6, u1);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, "c2", 2));
    const int64_t c2_pos = INITIAL_PAYLOAD_POSITION + 12;   /* 64 + 6 + 6 */

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);   /* clean init before the stub */

    D8StubAccessor s;
    d8_stub_init(&s, it->inner->payload_accessor, c2_pos, WRAP_MAP_TIMED_OUT);
    it->inner->payload_accessor = &s.base;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
        mmapqi_uncommitted_iterator_init(it, PAYLOAD_ITERATOR_NOT_SET,
                                         PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_EQUAL_INT(1, s.hits);

    it->inner->payload_accessor = s.real;   /* restore before close */
    mmapq_indexed_appender_close(a);
}

/* Pin 7 (the OUTER advance, probe S1 geometry): the stub is swapped in only
 * AFTER a clean init — a stub at a gap's first message would fail init
 * instead, because the scan walks the first message of every gap. Post-fix
 * the outer advance propagates MMAPQ_ETIMEDOUT; pre-fix it returned 0
 * (timeout collapsed to end-of-stream). */
void test_uncommitted_advance_timeout_propagates(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));
    int64_t u1 = mmapq_indexed_appender_append_uncommitted(a, "u1", 2);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION + 6, u1);

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);   /* init runs clean — no stub yet */

    D8StubAccessor s;
    d8_stub_init(&s, it->inner->payload_accessor, u1, WRAP_MAP_TIMED_OUT);
    it->inner->payload_accessor = &s.base;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
                          mmapqi_uncommitted_iterator_advance(it));
    TEST_ASSERT_EQUAL_INT(1, s.hits);

    /* Retryable: with the stub removed the same iterator yields the tail. */
    it->inner->payload_accessor = s.real;
    TEST_ASSERT_EQUAL_INT(1, mmapqi_uncommitted_iterator_advance(it));
    Buffer *buf = mmapqi_uncommitted_iterator_current(it);
    TEST_ASSERT_EQUAL_INT32(2, buf->capacity);
    TEST_ASSERT_EQUAL_MEMORY("u1", buf->data, 2);
    TEST_ASSERT_EQUAL_INT(0, mmapqi_uncommitted_iterator_advance(it));

    mmapq_indexed_appender_close(a);
}

/* Pin 8 (S3, owner-ruled; probe d8_spec32k.c migrated to the post-CAND-38
 * 32 KB region floor): the uncommitted scan must start at the queue's start
 * index, not header 0. After a legitimate header reclaim + rejoin, starting
 * at 0 hits the reclaimed header file, stops, and misreports committed
 * messages as uncommitted — re-observed pre-fix on dc58b23 at this exact
 * geometry: 4,101 yielded (4,100 committed + the genuine tail). Post-fix:
 * exactly 1. */
void test_uncommitted_iterator_scan_starts_at_queue_start_index(void)
{
    const char *qn = "mmap_test_iq_d8_s3";
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qn);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
    char qdir[256];
    snprintf(qdir, sizeof(qdir), "%s/%s", DIR, qn);
    mkdir(qdir, 0775);

    /* max_file_size 32768 -> hif = 4096 headers per header file: archiving
     * to index 4200 covers header_0 (indices 0..4095) completely. */
    MmapqIndexedQueue *cq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(DIR, qn, REGION_SZ,
        RING_SZ, 1, 32768LL, 0, TIMEOUT_NS, 1, rt, rt, &cq));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(cq, 3, &a));
    for (int i = 0; i < 8300; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "cccc", 4));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION + 8300 * 8,
        mmapq_indexed_appender_append_uncommitted(a, "uuuu", 4));

    MmapqIndexedArchiver *ar = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_archiver(cq, &ar));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 4200));
    char *paths[256] = {0};
    int n = mmapq_indexed_archiver_archivable_files(ar, paths, 256);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    if (n > 0)
        TEST_ASSERT_EQUAL_INT(n, mmapq_indexed_archiver_reclaim_files(ar, paths, n));
    for (int i = 0; i < n; i++) free(paths[i]);
    mmapq_indexed_archiver_close(ar);
    /* Non-vacuity: the covered header file is really gone. */
    char h0[320];
    snprintf(h0, sizeof(h0), "%s/header_0", qdir);
    TEST_ASSERT_NOT_EQUAL(0, access(h0, F_OK));
    mmapq_indexed_appender_close(a);

    /* Rejoin with the same appender id, then iterate: only the genuine
     * uncommitted tail may be yielded. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(cq, 3, &a));
    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);
    int found = 0;
    while (mmapqi_uncommitted_iterator_advance(it) == 1) {
        Buffer *buf = mmapqi_uncommitted_iterator_current(it);
        TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
        TEST_ASSERT_EQUAL_MEMORY("uuuu", buf->data, 4);
        found++;
    }
    TEST_ASSERT_EQUAL_INT(1, found);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(cq);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
}

/* Pin 5 (B3 — per join site, hook-selected). Each arms MMAPQ_ETIMEDOUT at
 * one walk site of initialise_append_position and rejoins. Pre-fix the
 * site's flatten returns MMAPQ_ECORRUPT; post-fix the timeout propagates.
 * Each pin asserts the hook FIRED — a vacuous geometry fails the liveness
 * assertion ([A1]). Geometries per the executed probe record
 * (audit/claude-D8-v2/RESULTS-v2.txt): a truly empty queue and a
 * never-written own lane are vacuous (the pre-wrap treats the missing
 * payload file as end of stream before the walk runs), so site 0 requires
 * this appender's own uncommitted payload on disk. */

/* J0a: site 0 (walk_payload_to_end), empty-headers path that REACHES the
 * walk — no committed headers, own uncommitted payload present. */
void test_join_walk_to_end_timeout_no_committed_headers(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(q, 3, &a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION,
        mmapq_indexed_appender_append_uncommitted(a, "u0", 2));
    mmapq_indexed_appender_close(a);

    mmapq_test_hook_set_appender_walk_result(0, MMAPQ_ETIMEDOUT);
    MmapqIndexedAppender *rej = NULL;
    int rc = mmapq_indexed_queue_create_appender_with_id(q, 3, &rej);
    int fired = mmapq_test_hook_appender_walk_fired();
    mmapq_test_hook_set_appender_walk_result(-1, 0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    TEST_ASSERT_EQUAL_INT(1, fired);
    TEST_ASSERT_NULL(rej);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* J0b: site 0 (walk_payload_to_end), no-own-live-entry path — another
 * appender has committed; this appender's own lane holds ONLY uncommitted
 * data. */
void test_join_walk_to_end_timeout_no_own_live_entry(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(q, 3, &a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION,
        mmapq_indexed_appender_append_uncommitted(a, "u0", 2));
    mmapq_indexed_appender_close(a);
    MmapqIndexedAppender *o = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(q, 9, &o));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(o, "x", 1));
    mmapq_indexed_appender_close(o);

    mmapq_test_hook_set_appender_walk_result(0, MMAPQ_ETIMEDOUT);
    MmapqIndexedAppender *rej = NULL;
    int rc = mmapq_indexed_queue_create_appender_with_id(q, 3, &rej);
    int fired = mmapq_test_hook_appender_walk_fired();
    mmapq_test_hook_set_appender_walk_result(-1, 0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    TEST_ASSERT_EQUAL_INT(1, fired);
    TEST_ASSERT_NULL(rej);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* J1: site 1 (the committed-entry walk) — own committed entry present. */
void test_join_committed_walk_timeout_propagates(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(q, 3, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));
    mmapq_indexed_appender_close(a);

    mmapq_test_hook_set_appender_walk_result(1, MMAPQ_ETIMEDOUT);
    MmapqIndexedAppender *rej = NULL;
    int rc = mmapq_indexed_queue_create_appender_with_id(q, 3, &rej);
    int fired = mmapq_test_hook_appender_walk_fired();
    mmapq_test_hook_set_appender_walk_result(-1, 0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    TEST_ASSERT_EQUAL_INT(1, fired);
    TEST_ASSERT_NULL(rej);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* J2: site 2 (the tail walk) — no special geometry needed: site 2 fires on
 * any rejoin with an own committed entry, even with no uncommitted tail. */
void test_join_tail_walk_timeout_propagates(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(q, 3, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "c0", 2));
    mmapq_indexed_appender_close(a);

    mmapq_test_hook_set_appender_walk_result(2, MMAPQ_ETIMEDOUT);
    MmapqIndexedAppender *rej = NULL;
    int rc = mmapq_indexed_queue_create_appender_with_id(q, 3, &rej);
    int fired = mmapq_test_hook_appender_walk_fired();
    mmapq_test_hook_set_appender_walk_result(-1, 0);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    TEST_ASSERT_EQUAL_INT(1, fired);
    TEST_ASSERT_NULL(rej);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
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

    /* Phase 2: Create a second queue instance with "wrong" default (32KB) */
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
        MAX_FILE, 0, TIMEOUT_NS,
        0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    int64_t ok = mmapq_indexed_appender_append(a, "small", 5);
    TEST_ASSERT_EQUAL_INT64(0, ok);

    /* Message of REGION_SZ bytes exceeds region_size - 4 */
    char big[REGION_SZ];
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
              32768, 16, 1, 32768LL * 8, 0, 5000000000ULL, 0, rt, rt, &bq);
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
        MAX_FILE, 0, TIMEOUT_NS,
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
    /* Beyond the region capacity but landing INSIDE the (zero-filled) file 0,
     * so the unbounded follow-on walk keeps reading garbage rather than
     * hitting a clean WRAP_FILE_NOT_FOUND. (CAND-38 migration: REGION_SZ*8
     * == MAX_FILE landed past file 0 at the 32 KB region geometry, masking
     * the missing bound — vacuous. R9 revert re-run proved this value
     * discriminates: SIGBUS (135) unfixed, ECORRUPT fixed.) */
    int32_t bogus = REGION_SZ * 4;
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
    int32_t bogus = REGION_SZ * 4;   /* see the first-walk pin: inside file 0 */
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

/* ── C-08 regression: concurrent appenders on small-region geometry ────────
 *
 * In-suite pin for C-08 (stale RegionAccessor fast-path cache after a failed
 * remap, fixed in A1). Until D9 the only coverage was the cross-process
 * script audit/stress/run_c08_concurrent.sh (5/5 SIGSEGV pre-fix at 20000
 * messages, 0/5 at 8000). Geometry and payload pattern replicate that repro
 * (audit/interop/c_interop.c race_size/race_fill/race_handler): 64 KiB
 * regions, 4-slot ring, 256 KiB max file, 25000 variable-size messages per
 * lane. Participants are created inside their lane thread per
 * indexed_queue.h:34-36 (concurrent creation on a shared queue is allowed;
 * each appender stays on its own thread). The sync variant passes NULL
 * runtimes, matching the original repro exactly.
 *
 * Measured under mutation (A1's invalidation reverted), this geometry: sync
 * variant SIGSEGV 5/5, async variant 0/5 — with a runtime serializing
 * mapping, async wraps essentially never fail, so the async variant is
 * concurrency coverage, not the C-08 pin. Two lanes measured weaker (sync
 * 3/5); lane-skew variants measured counterproductive (0/5: the pauses let
 * one lane finish while the other slept, removing wrap contention). The pin
 * depends on sustained wrap contention between lanes — do not reduce
 * C08_COUNT or C08_LANES and do not add sleeps. Below ~20000 messages per
 * lane the lagging appender never falls MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH
 * (100) headers behind, so the failed-remap path is not exercised. */

#define C08_REGION   65536
#define C08_RING     4
#define C08_MAXFILE  262144
#define C08_LANES    4
#define C08_COUNT    25000
/* 10 s, matching the repro: mapping waits under TSan can exceed the 5 s
 * suite default. */
#define C08_TIMEOUT_NS 10000000000ULL

static const char *QUEUE_C08 = "mmap_test_iq_c08";

static void cleanup_c08(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE_C08);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
}

static int32_t c08_size(int w, int64_t seq)
{
    return 16 + (int32_t)(((seq * 13 + w * 7) % 300));
}

static void c08_fill(int w, int64_t seq, unsigned char *buf, int32_t size)
{
    uint64_t s = (uint64_t)seq;
    uint32_t wid = (uint32_t)w;
    uint32_t sz = (uint32_t)size;
    memcpy(buf, &s, 8);
    memcpy(buf + 8, &wid, 4);
    memcpy(buf + 12, &sz, 4);
    for (int32_t j = 16; j < size; j++) {
        buf[j] = (unsigned char)((seq * 131 + (int64_t)w * 17 + (int64_t)j * 29) & 0xff);
    }
}

typedef struct {
    MmapqIndexedQueue *queue;
    int      writer;
    int      ok;          /* 1 when all C08_COUNT appends succeeded */
    int64_t  failed_seq;  /* -1 when no append failed */
    int64_t  append_rc;
} C08Lane;

/* Worker: no Unity assertions here (Unity longjmp is not thread-safe);
 * the lane records its outcome and the main thread asserts. */
static void *c08_append_lane(void *arg)
{
    C08Lane *lane = (C08Lane *)arg;
    lane->ok = 0;
    lane->failed_seq = -1;
    lane->append_rc = 0;
    MmapqIndexedAppender *a = NULL;
    int rc = mmapq_indexed_queue_create_appender_with_id(lane->queue, (uint8_t)lane->writer, &a);
    if (rc != 0 || a == NULL) { lane->append_rc = rc; return NULL; }
    unsigned char buf[320];
    for (int64_t seq = 0; seq < C08_COUNT; seq++) {
        int32_t size = c08_size(lane->writer, seq);
        c08_fill(lane->writer, seq, buf, size);
        int64_t rc_a = mmapq_indexed_appender_append(a, buf, size);
        if (rc_a < 0) { lane->append_rc = rc_a; lane->failed_seq = seq; break; }
    }
    mmapq_indexed_appender_close(a);
    lane->ok = (lane->failed_seq < 0);
    return NULL;
}

typedef struct {
    unsigned char *seen;   /* C08_LANES * C08_COUNT bytes */
    int64_t        seen_total;
    int            bad;         /* 0 ok, else first-failure reason code */
    uint8_t        bad_writer;  /* diagnostics for the first failure — a */
    int64_t        bad_seq;     /* probabilistic test may not give a second */
    int32_t        bad_length;  /* chance to see why */
} C08Verify;

/* reason codes: 1 appender_id range, 2 payload writer != appender_id,
 * 3 seq out of range, 4 size mismatch, 5 content mismatch, 6 duplicate */
static MmapqNextMove c08_verify_fail(C08Verify *v, int reason, uint8_t writer,
                                     int64_t seq, int32_t length)
{
    if (!v->bad) {
        v->bad = reason;
        v->bad_writer = writer;
        v->bad_seq = seq;
        v->bad_length = length;
    }
    return MMAPQ_NEXT_STOP;
}

static MmapqNextMove c08_verify_handler(int64_t index, uint8_t appender_id,
                                        const void *data, int32_t length, void *ctx)
{
    (void)index;
    C08Verify *v = (C08Verify *)ctx;
    if (appender_id >= C08_LANES)
        return c08_verify_fail(v, 1, appender_id, -1, length);
    const unsigned char *b = (const unsigned char *)data;
    int64_t seq;
    uint32_t wid, sz;
    memcpy(&seq, b, 8);
    memcpy(&wid, b + 8, 4);
    memcpy(&sz, b + 12, 4);
    if ((int)wid != (int)appender_id)
        return c08_verify_fail(v, 2, appender_id, -1, length);
    if (seq < 0 || seq >= C08_COUNT)
        return c08_verify_fail(v, 3, appender_id, seq, length);
    int32_t esize = c08_size((int)wid, seq);
    if (length != esize || (int32_t)sz != esize)
        return c08_verify_fail(v, 4, appender_id, seq, length);
    unsigned char expect[320];
    c08_fill((int)wid, seq, expect, esize);
    if (memcmp(data, expect, (size_t)esize) != 0)
        return c08_verify_fail(v, 5, appender_id, seq, length);
    int64_t slot = (int64_t)wid * C08_COUNT + seq;
    if (v->seen[slot])
        return c08_verify_fail(v, 6, appender_id, seq, length);
    v->seen[slot] = 1;
    v->seen_total++;
    return MMAPQ_NEXT_ADVANCE;
}

static void c08_run_concurrent(MmapqRuntime *write_rt, MmapqRuntime *read_rt)
{
    cleanup_c08();
    MmapqIndexedQueue *cq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(DIR, QUEUE_C08,
        C08_REGION, C08_RING, 1, C08_MAXFILE, 0, C08_TIMEOUT_NS, 0,
        write_rt, read_rt, &cq));
    TEST_ASSERT_NOT_NULL(cq);

    C08Lane lanes[C08_LANES];
    pthread_t threads[C08_LANES];
    for (int w = 0; w < C08_LANES; w++) {
        lanes[w].queue = cq;
        lanes[w].writer = w;
        /* If a later pthread_create fails, Unity's assert longjmps while
         * earlier lanes are still running — accepted: the test is already
         * a fatal failure in that case. */
        TEST_ASSERT_EQUAL_INT(0, pthread_create(&threads[w], NULL, c08_append_lane, &lanes[w]));
    }
    for (int w = 0; w < C08_LANES; w++) {
        TEST_ASSERT_EQUAL_INT(0, pthread_join(threads[w], NULL));
    }
    for (int w = 0; w < C08_LANES; w++) {
        char msg[96];
        snprintf(msg, sizeof(msg), "lane %d: append failed rc=%lld at seq=%lld",
                 w, (long long)lanes[w].append_rc, (long long)lanes[w].failed_seq);
        TEST_ASSERT_TRUE_MESSAGE(lanes[w].ok, msg);
    }

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(cq, &p));
    TEST_ASSERT_NOT_NULL(p);
    C08Verify v;
    v.seen = calloc(C08_LANES * C08_COUNT, 1);
    TEST_ASSERT_NOT_NULL(v.seen);
    v.seen_total = 0;
    v.bad = 0;
    v.bad_writer = 0;
    v.bad_seq = -1;
    v.bad_length = 0;
    /* CAND-17: a mapping timeout on the poller's header read is reported as
     * MMAPQ_POLL_END_OF_STREAM, indistinguishable from "no message yet".
     * Under TSan one slow mapping would otherwise end this loop early and
     * look like lost messages. Retry END_OF_STREAM until a bounded deadline;
     * do not remove the retry while CAND-17 is open. */
    struct timespec ts;
    TEST_ASSERT_EQUAL_INT(0, clock_gettime(CLOCK_MONOTONIC, &ts));
    int64_t deadline_ns = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec
                          + 2 * (int64_t)C08_TIMEOUT_NS;
    while (v.seen_total < (int64_t)(C08_LANES * C08_COUNT)) {
        MmapqPollResult r = mmapq_indexed_poller_poll(p, c08_verify_handler, &v);
        if (r == MMAPQ_POLL_ADVANCED) continue;
        if (r == MMAPQ_POLL_END_OF_STREAM) {
            TEST_ASSERT_EQUAL_INT(0, clock_gettime(CLOCK_MONOTONIC, &ts));
            int64_t now_ns = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
            if (now_ns < deadline_ns) continue;
        }
        break;
    }
    if (v.bad) {
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "verify failed: reason=%d writer=%u seq=%lld length=%d",
                 v.bad, (unsigned)v.bad_writer,
                 (long long)v.bad_seq, (int)v.bad_length);
        TEST_FAIL_MESSAGE(msg);
    }
    TEST_ASSERT_EQUAL_INT64((int64_t)(C08_LANES * C08_COUNT), v.seen_total);
    free(v.seen);
    mmapq_indexed_poller_close(p);
    mmapq_indexed_queue_close(cq);
    cleanup_c08();
}

void test_concurrent_appenders_small_region_commit(void)
{
    /* region_mapper returns a clone of the process-lifetime singleton; the
     * caller owns one reference and releases it with destroy (refcount--,
     * runtime.h:243-244,304-305) — never close, which would shut the shared
     * runtime down mid-suite. setUp's per-test reference is likewise never
     * released (filed as D11; not this task). */
    MmapqRuntime *rt2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt2));
    TEST_ASSERT_NOT_NULL(rt2);
    c08_run_concurrent(rt2, rt2);
    mmapq_runtime_destroy(rt2);
}

void test_concurrent_appenders_small_region_commit_sync(void)
{
    c08_run_concurrent(NULL, NULL);
}

/* ── D3 (CAND-16): absurd indices alias already-written slots ────────────── */

/* D3_MAX_INDEX literal must match MMAPQI_MAX_INDEX in header_codec.h. */
#define D3_MAX_INDEX   (((int64_t)1 << 56) - 1)
#define D3_ALIAS_INDEX ((int64_t)1 << 61)   /* position wraps to 0: index 0's slot */

static void d3_append_two(MmapqIndexedAppender **out)
{
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, out));
    TEST_ASSERT_NOT_NULL(*out);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(*out, "d3-msg-0", 8));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(*out, "d3-msg-1", 8));
}

void test_poller_rejects_absurd_index(void)
{
    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
    TEST_ASSERT_NOT_NULL(p);

    /* Unpatched: all three return 1 — 2^61 aliases index 0's slot and
     * 2^61+1 aliases index 1's. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_move_to_index(p, D3_ALIAS_INDEX));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_exists(p, D3_ALIAS_INDEX));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_move_to_index(p, D3_ALIAS_INDEX + 1));

    /* Rejection must not have moved the poller: the next poll yields index 0. */
    PollCapture c = { .directive = MMAPQ_NEXT_ADVANCE };
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_poller_poll(p, capture_handler, &c));
    TEST_ASSERT_EQUAL_INT64(0, c.index);
    TEST_ASSERT_EQUAL_INT(8, c.length);
    TEST_ASSERT_EQUAL_STRING("d3-msg-0", c.data);

    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(a);
}

void test_reader_rejects_absurd_index(void)
{
    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);

    /* Unpatched: AVAILABLE with index 0's payload ('d3-msg-0'). */
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, D3_ALIAS_INDEX, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_reader_last_error(r));

    /* Unpatched: AVAILABLE. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR,
        mmapq_indexed_reader_entry_status(r, D3_ALIAS_INDEX));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_reader_last_error(r));

    /* has_entry inherits entry_status's rejection. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_has_entry(r, D3_ALIAS_INDEX));

    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
}

void test_reader_absurd_index_not_collapse_to_timeout(void)
{
    /* Anti-collapse pin: indices in [2^60, 2^61) wrap to negative positions,
     * which the ring accessor reports as WRAP_MAP_TIMED_OUT — a retry-later
     * code for a caller error. Unpatched: ENTRY_ERROR with ETIMEDOUT. */
    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);

    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, ((int64_t)1 << 60) + 5, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_reader_last_error(r));

    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
}

void test_reader_index_boundary(void)
{
    /* The boundary is observable only through the reader (at pollers and
     * archivers MAX+1 already fails on unpatched code). Unpatched, at this
     * geometry (MAX_FILE = 2^18) read(MAX+1) returns AVAILABLE: position
     * 2^59 / 2^18 = 2^41 truncates to 0 in the rolled mapper's int
     * file_index (file_mapper_rolled.c:160, filed as CAND-28) and aliases
     * header_0. */
    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);

    /* entry_status boundary first, on the fresh reader (last_error still 0):
     * entry_status uses its own inline > MAX check (negative indices must
     * keep falling through to ARCHIVED), so the shared-helper pins do not
     * cover it — owner review found this boundary survived bound mutations
     * to MAX+4096 and to 2^60. Unpatched, entry_status(MAX+1) is AVAILABLE
     * via the same truncation alias. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM,
        mmapq_indexed_reader_entry_status(r, D3_MAX_INDEX));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR,
        mmapq_indexed_reader_entry_status(r, D3_MAX_INDEX + 1));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_reader_last_error(r));

    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, D3_MAX_INDEX, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));

    mmapq_indexed_reader_read(r, D3_MAX_INDEX + 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_reader_last_error(r));

    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
}

void test_queue_create_start_index_boundary(void)
{
    char qdir[256];

    MmapqIndexedQueue *q2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, "mmap_test_iq_d3max", REGION_SZ, RING_SZ, 1, MAX_FILE,
        D3_MAX_INDEX, TIMEOUT_NS, 0, rt, rt, &q2));
    TEST_ASSERT_NOT_NULL(q2);
    mmapq_indexed_queue_close(q2);
    snprintf(qdir, sizeof(qdir), "rm -rf %s/%s", DIR, "mmap_test_iq_d3max");
    if (system(qdir) != 0) { /* best-effort */ }

    MmapqIndexedQueue *q3 = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_queue_create(
        DIR, "mmap_test_iq_d3over", REGION_SZ, RING_SZ, 1, MAX_FILE,
        D3_MAX_INDEX + 1, TIMEOUT_NS, 0, rt, rt, &q3));
    TEST_ASSERT_NULL(q3);
    snprintf(qdir, sizeof(qdir), "rm -rf %s/%s", DIR, "mmap_test_iq_d3over");
    if (system(qdir) != 0) { /* best-effort */ }
}

/* On-disk variant of the aliasing (owner-observed: start_index = 2^61
 * reopens rc=0, read_first returns index 0's data labelled 2^61). Post-fix
 * meta_validate refuses at the lazy open. */
void test_reader_refuses_queue_with_absurd_disk_start_index(void)
{
    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);
    mmapq_indexed_appender_close(a);

    char path[512];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "open meta");
    int64_t bad = D3_ALIAS_INDEX;
    TEST_ASSERT_EQUAL_INT64((int64_t)sizeof(bad),
        (int64_t)pwrite(fd, &bad, sizeof(bad), QUEUE_META_START_INDEX_OFFSET));
    TEST_ASSERT_EQUAL_INT(0, close(fd));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT_NS, NULL, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);
}

/* Negative on-disk start_index. Pre-fix failure mode recorded: in the debug
 * config the follow-on read aborts inside header_position's
 * MMAP_ASSERT(index >= 0) — a process abort, not a clean assertion failure
 * (under NDEBUG it returns END_OF_STREAM). The pin asserts the refusal at
 * the lazy open, before any read reaches header_position. */
void test_reader_refuses_queue_with_negative_disk_start_index(void)
{
    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);
    mmapq_indexed_appender_close(a);

    char path[512];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "open meta");
    int64_t bad = -4096;
    TEST_ASSERT_EQUAL_INT64((int64_t)sizeof(bad),
        (int64_t)pwrite(fd, &bad, sizeof(bad), QUEUE_META_START_INDEX_OFFSET));
    TEST_ASSERT_EQUAL_INT(0, close(fd));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT_NS, NULL, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);
}

/* Search-clamp pin: at start_index = MAX the exponential probe must not
 * read beyond MAX (such reads aliased via the rolled mapper's int file_index
 * truncation, CAND-28 — now fixed) and move_to_end must land at MAX + 1
 * exactly. Unpatched (observed via probe): move_to_end parks at
 * 2^57 - 2^51 — a phantom "last index" found in aliased header_0 slots.
 *
 * Geometry note (owner review correction): at 64 KiB regions / 4 MiB files,
 * position(MAX) = 2^59 - 8 gives file_index 2^37 - 1, which the rolled
 * mapper's int truncation used to turn into -1 — pre-CAND-28, index MAX
 * actually lived in a 4 MiB file named header_-1 (write and read truncated
 * identically). Since CAND-28 widened file_index to int64_t, this test is
 * its regression pin: MAX now really lives at offset 2^59 - 8 in a sparse
 * header_137438953471. */
void test_move_to_end_at_max_index(void)
{
    char qdir[256];
    snprintf(qdir, sizeof(qdir), "rm -rf %s/%s", DIR, "mmap_test_iq_d3me");
    if (system(qdir) != 0) { /* best-effort */ }

    MmapqIndexedQueue *q2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, "mmap_test_iq_d3me", 65536, 16, 1, 65536LL * 64,
        D3_MAX_INDEX, TIMEOUT_NS, 0, rt, rt, &q2));
    TEST_ASSERT_NOT_NULL(q2);

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q2, &a));
    TEST_ASSERT_NOT_NULL(a);
    TEST_ASSERT_EQUAL_INT64(D3_MAX_INDEX,
        mmapq_indexed_appender_append(a, "d3-at-max", 9));

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q2, &p));
    TEST_ASSERT_NOT_NULL(p);
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_end(p));
    TEST_ASSERT_EQUAL_INT64(D3_MAX_INDEX + 1, mmapq_indexed_poller_current_index(p));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q2, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, D3_MAX_INDEX, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(D3_MAX_INDEX, ctx.index);
    TEST_ASSERT_EQUAL_INT(9, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("d3-at-max", ctx.data, 9);

    mmapq_indexed_reader_close(r);
    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q2);
    if (system(qdir) != 0) { /* best-effort */ }
}

/* CAND-28 pin: index 2^47 is legal (<= MAX) and never written. Header
 * position 2^50 / MAX_FILE 2^18 = file_index exactly 2^32, which the rolled
 * mapper's int file_index truncated to 0 — aliasing header_0 and serving
 * index 0/1's payloads for a never-written index. Pre-fix (owner-observed):
 * AVAILABLE "d3-msg-0" / AVAILABLE "d3-msg-1" / exists == 1 /
 * move_to_index == 1. */
void test_read_no_alias_at_file_index_2pow32(void)
{
    const int64_t c28 = (int64_t)1 << 47;

    MmapqIndexedAppender *a = NULL;
    d3_append_two(&a);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
    TEST_ASSERT_NOT_NULL(p);

    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, c28, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));

    mmapq_indexed_reader_read(r, c28 + 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_last_error(r));

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_exists(p, c28));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_move_to_index(p, c28 + 1));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM,
        mmapq_indexed_reader_entry_status(r, c28));

    mmapq_indexed_poller_close(p);
    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
}

/* ── CAND-32 pins: on-disk appender_meta.start_offset validation ───────────
 *
 * appender_meta_0's start_offset (int64 at file offset 0) was mapped without
 * validation; a corrupt value flowed from the join walk straight into
 * current_payload_position and then into header_encode (debug: SIGABRT at
 * header_codec.h:113; release: the position masked into the 56-bit field —
 * 2^56 and 2^57 both mask to 0 = HEADER_NULL, so append reported committed
 * at index 0 while reads saw END_OF_STREAM forever).
 *
 * The two checks have DIFFERENT upper bounds by design:
 *   C1 (open-time, appender_meta.c): start_offset is the END of an archived
 *      message — valid range [INITIAL_PAYLOAD_POSITION, 2^56], 2^56
 *      inclusive (a message ending exactly at the region boundary 2^56 is
 *      legitimate; P8 pins this).
 *   C2 (append-time, both append commit paths): encoded positions are
 *      STARTS — valid range [INITIAL_PAYLOAD_POSITION, 2^56−1].
 *
 * Geometry per the owner probes, migrated by CAND-38: region 32768 (the
 * smallest legal region — was 4096), ring 16, max_file_size 2^20,
 * archivable 1.
 */

#define C32_REGION_SIZE    32768
#define C32_RING_SIZE      16
#define C32_MAX_FILE_SIZE  ((int64_t)1 << 20)
#define C32_POS_LIMIT      ((int64_t)1 << 56)   /* 2^56 */

static void c32_cleanup(const char *qname)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    if (system(cmd) != 0) { /* best-effort cleanup */ }
}

/* Create the pin queue (archivable) and appender id 0 so appender_meta_0
 * exists on disk, then close both. */
static void c32_create_queue(const char *qname)
{
    c32_cleanup(qname);
    MmapqIndexedQueue *cq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, qname, C32_REGION_SIZE, C32_RING_SIZE, 1,
        C32_MAX_FILE_SIZE, 0, TIMEOUT_NS, 1, rt, rt, &cq));
    TEST_ASSERT_NOT_NULL(cq);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_queue_create_appender_with_id(cq, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(cq);
}

/* Overwrite the on-disk start_offset (int64 at offset 0 of appender_meta_0). */
static void c32_plant_start_offset(const char *qname, int64_t value)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/appender_meta_0", DIR, qname);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "open appender_meta_0");
    TEST_ASSERT_EQUAL_INT64((int64_t)sizeof(value),
        (int64_t)pwrite(fd, &value, sizeof(value), 0));
    TEST_ASSERT_EQUAL_INT(0, close(fd));
}

/* Reopen the pin queue and rejoin appender id 0; returns the appender
 * create rc (*out_a stays NULL on failure). */
static int c32_rejoin(const char *qname, MmapqIndexedQueue **out_q,
                      MmapqIndexedAppender **out_a)
{
    *out_q = NULL;
    *out_a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, qname, C32_REGION_SIZE, C32_RING_SIZE, 1,
        C32_MAX_FILE_SIZE, 0, TIMEOUT_NS, 1, rt, rt, out_q));
    TEST_ASSERT_NOT_NULL(*out_q);
    return mmapq_indexed_queue_create_appender_with_id(*out_q, 0, out_a);
}

/* Index 0 must have no committed entry (the refused append wrote nothing
 * readable). */
static void c32_assert_index0_empty(MmapqIndexedQueue *cq)
{
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(cq, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);
    mmapq_indexed_reader_close(r);
}

void test_c32_p1_start_offset_2pow57_rejoin_refused(void)
{
    const char *qn = "mmap_test_iq_c32_p1";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, (int64_t)1 << 57);

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    int rc = c32_rejoin(qn, &cq, &a);
    if (rc == 0) {
        /* Pre-CAND-32 defect chain: the corrupt start_offset was accepted
         * and the append encodes it — debug builds abort in header_encode
         * (header_codec.h:113); release builds mask 2^57 into HEADER_NULL
         * and report a commit nothing can read. */
        int64_t idx = mmapq_indexed_appender_append(a, "HELLO", 5);
        char msg[96];
        snprintf(msg, sizeof(msg),
                 "start_offset 2^57 accepted: rejoin rc=0, append=%lld",
                 (long long)idx);
        mmapq_indexed_appender_close(a);
        mmapq_indexed_queue_close(cq);
        c32_cleanup(qn);
        TEST_FAIL_MESSAGE(msg);
    }
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a);

    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_p2_start_offset_2pow56_append_refused(void)
{
    const char *qn = "mmap_test_iq_c32_p2";
    c32_create_queue(qn);
    /* 2^56 is a legitimate END position (region boundary) — C1 accepts. */
    c32_plant_start_offset(qn, C32_POS_LIMIT);

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* ...but 2^56 is an unencodable START (56-bit field): C2 refuses. */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT,
        mmapq_indexed_appender_append(a, "HELLO", 5));
    c32_assert_index0_empty(cq);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_p3_start_offset_negative_rejoin_refused(void)
{
    const char *qn = "mmap_test_iq_c32_p3";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, -1000);

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    int rc = c32_rejoin(qn, &cq, &a);
    /* Pre-fix this was -1 from the failed join walk — a timeout-flavoured
     * code for on-disk corruption. C1 makes it ECORRUPT at open. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a);

    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_p4_start_offset_below_initial_rejoin_refused(void)
{
    const char *qn = "mmap_test_iq_c32_p4";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, INITIAL_PAYLOAD_POSITION - 1);   /* 63 */

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    int rc = c32_rejoin(qn, &cq, &a);
    if (rc == 0) {
        /* Pre-CAND-32 record path (planner prediction, verified at
         * fail-first): the join walk reads the zeroed reserved prefix as
         * length 0, 63 encodes fine, and the append writes into the
         * reserved prefix region — silent corruption, no error. */
        int64_t idx = mmapq_indexed_appender_append(a, "HELLO", 5);
        char msg[96];
        snprintf(msg, sizeof(msg),
                 "start_offset 63 accepted: rejoin rc=0, append=%lld",
                 (long long)idx);
        mmapq_indexed_appender_close(a);
        mmapq_indexed_queue_close(cq);
        c32_cleanup(qn);
        TEST_FAIL_MESSAGE(msg);
    }
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a);

    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_p5_start_offset_roll_to_limit_append_refused(void)
{
    const char *qn = "mmap_test_iq_c32_p5";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, C32_POS_LIMIT - 2);

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* 2 bytes of region capacity remain at 2^56−2 (2^56 is a multiple of
     * the region size): write_padding rolls the position to exactly 2^56 —
     * an unencodable START. C2 refuses. */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT,
        mmapq_indexed_appender_append(a, "HELLO", 5));
    c32_assert_index0_empty(cq);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_p6_start_offset_near_limit_append_works(void)
{
    /* POSITIVE CONTROL: a legitimate near-limit start_offset must still
     * append and read back — guards over-rejection by C1/C2. */
    const char *qn = "mmap_test_iq_c32_p6";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, C32_POS_LIMIT - C32_MAX_FILE_SIZE);

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "HELLO", 5));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(cq, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(5, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("HELLO", ctx.data, 5);
    mmapq_indexed_reader_close(r);

    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_p7_commit_unencodable_offset_refused(void)
{
    /* Caller-input path: commit(a, 2^57) is above this appender's last
     * written start — a caller error, MMAPQ_EINVAL (CAND-32 v3 C3 ruling),
     * never a debug abort (header_codec.h:113) or a release masked-to-NULL
     * "commit". No corruption planted — uses the setUp queue. */
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_NOT_NULL(a);

    int64_t off = mmapq_indexed_appender_append_uncommitted(a, "HELLO", 5);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_appender_commit(a, (int64_t)1 << 57));

    /* The refused commit must not have disturbed the appender: the real
     * payload commits as index 0 and reads back. */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(5, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("HELLO", ctx.data, 5);
    mmapq_indexed_reader_close(r);

    mmapq_indexed_appender_close(a);
}

void test_c32_p9_commit_above_last_written_refused(void)
{
    /* C3: an IN-DOMAIN offset above the appender's last written start was
     * never written by this appender — caller error, MMAPQ_EINVAL. Pre-C3
     * (owner probe, re-observed at fail-first): commit(5000) returned 0,
     * having CASed index 0's header at position 5000; index 0 then read
     * ENTRY_ERROR forever. Uses the setUp queue. */
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_NOT_NULL(a);

    int64_t off = mmapq_indexed_appender_append_uncommitted(a, "small", 5);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, mmapq_indexed_appender_commit(a, 5000));

    /* The refused commit published nothing; the real payload still commits
     * as index 0 and reads back. */
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM, ctx.status);

    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off));
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(5, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("small", ctx.data, 5);
    mmapq_indexed_reader_close(r);

    mmapq_indexed_appender_close(a);
}

void test_c32_p8_region_boundary_end_offset_legitimate(void)
{
    /* c32_limit owner-probe scenario: a message ending exactly at 2^56
     * (region boundary) archives to start_offset == 2^56. That value must
     * survive rejoin and a fresh archiver — C1's upper bound is 2^56
     * INCLUSIVE; a 2^56−1 bound (mutation M3d) ECORRUPTs this legitimate
     * queue. */
    const char *qn = "mmap_test_iq_c32_p8";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, C32_POS_LIMIT - C32_REGION_SIZE);

    MmapqIndexedQueue *cq = NULL;
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* 2^56−32768 start + 4-byte prefix + 32764 bytes ends exactly at 2^56. */
    char msg[C32_REGION_SIZE - PAYLOAD_LENGTH_SIZE];
    memset(msg, 'L', sizeof(msg));
    TEST_ASSERT_EQUAL_INT64(0,
        mmapq_indexed_appender_append(a, msg, (int32_t)sizeof(msg)));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(cq, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(msg), ctx.length);
    TEST_ASSERT_EQUAL_MEMORY(msg, ctx.data, sizeof(msg));
    mmapq_indexed_reader_close(r);

    MmapqIndexedArchiver *ar = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_archiver(cq, &ar));
    TEST_ASSERT_NOT_NULL(ar);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 1));
    int64_t so = -1;
    /* start_offset() returns 1 on success, 0 if the appender ID is unused */
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_start_offset(ar, 0, &so));
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT, so);
    mmapq_indexed_archiver_close(ar);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(cq);

    /* Rejoin with start_offset == 2^56 on disk: C1 must accept. */
    MmapqIndexedAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a2));
    TEST_ASSERT_NOT_NULL(a2);
    mmapq_indexed_appender_close(a2);

    /* A fresh archiver opens the meta through the same validated path. */
    MmapqIndexedArchiver *ar2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_archiver(cq, &ar2));
    TEST_ASSERT_NOT_NULL(ar2);
    so = -1;
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_start_offset(ar2, 0, &so));
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT, so);
    mmapq_indexed_archiver_close(ar2);
    mmapq_indexed_queue_close(cq);
    c32_cleanup(qn);
}

/* ── CAND-38 pin P1: the geometry rule at create ───────────────────────────
 *
 * Header-file reclaim assumes each header file holds a contiguous index
 * range; that holds iff max_file_size is a multiple of the 32 KB transposed
 * header block, which the rule guarantees via region_size >= 32 KB (a power
 * of two dividing max_file_size). Pre-rule geometries (region < 32 KB) lose
 * live committed headers on reclaim — reviewer probe
 * audit/claude-CAND-38/hdrloss.c: 992 of the first 2112 live entries
 * unreadable at 8 KB files. Refused at create with EINVAL. */

void test_c38_create_refuses_region_below_header_block(void)
{
    MmapqIndexedQueue *q2 = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_queue_create(
        DIR, "mmap_test_iq_c38_small", 4096, 16, 1, 8192, 0,
        TIMEOUT_NS, 0, rt, rt, &q2));
    TEST_ASSERT_NULL(q2);
    c32_cleanup("mmap_test_iq_c38_small");

    /* Region below the block with a BLOCK-ALIGNED file: only the region
     * rule can refuse this one (mutation M1 discriminates here). */
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_queue_create(
        DIR, "mmap_test_iq_c38_small2", 4096, 16, 1, 32768, 0,
        TIMEOUT_NS, 0, rt, rt, &q2));
    TEST_ASSERT_NULL(q2);
    c32_cleanup("mmap_test_iq_c38_small2");
}

void test_c38_create_accepts_non_pow2_file_multiple(void)
{
    /* Positive control: the smallest legal region and a NON-power-of-two
     * multiple of it (3 x 32 KB) — legal: file % region == 0 and the file
     * is a multiple of the header block. */
    MmapqIndexedQueue *q2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, "mmap_test_iq_c38_ok", 32768, 16, 1, 32768LL * 3, 0,
        TIMEOUT_NS, 0, rt, rt, &q2));
    TEST_ASSERT_NOT_NULL(q2);
    mmapq_indexed_queue_close(q2);
    c32_cleanup("mmap_test_iq_c38_ok");
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
    RUN_TEST(test_concurrent_appenders_small_region_commit);
    RUN_TEST(test_concurrent_appenders_small_region_commit_sync);
    RUN_TEST(test_large_message_sequence_crossing_regions);
    RUN_TEST(test_appender_recovers_position_after_close);
    RUN_TEST(test_uncommitted_append_and_commit);
    RUN_TEST(test_uncommitted_iterator_finds_uncommitted);
    RUN_TEST(test_appender_iterator_out_of_range_offset_no_double_free);
    RUN_TEST(test_uncommitted_scan_header_timeout_propagates);
    RUN_TEST(test_uncommitted_scan_payload_timeout_propagates);
    RUN_TEST(test_payload_iterator_advance_timeout_propagates);
    RUN_TEST(test_uncommitted_scan_gap_branch_timeout_real_gap);
    RUN_TEST(test_uncommitted_scan_gap_branch_timeout_contiguous);
    RUN_TEST(test_uncommitted_scan_gap_resync_timeout_propagates);
    RUN_TEST(test_uncommitted_advance_timeout_propagates);
    RUN_TEST(test_uncommitted_iterator_scan_starts_at_queue_start_index);
    RUN_TEST(test_join_walk_to_end_timeout_no_committed_headers);
    RUN_TEST(test_join_walk_to_end_timeout_no_own_live_entry);
    RUN_TEST(test_join_committed_walk_timeout_propagates);
    RUN_TEST(test_join_tail_walk_timeout_propagates);
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

    RUN_TEST(test_poller_rejects_absurd_index);
    RUN_TEST(test_reader_rejects_absurd_index);
    RUN_TEST(test_reader_absurd_index_not_collapse_to_timeout);
    RUN_TEST(test_reader_index_boundary);
    RUN_TEST(test_queue_create_start_index_boundary);
    RUN_TEST(test_reader_refuses_queue_with_absurd_disk_start_index);
    RUN_TEST(test_reader_refuses_queue_with_negative_disk_start_index);
    RUN_TEST(test_move_to_end_at_max_index);
    RUN_TEST(test_read_no_alias_at_file_index_2pow32);

    RUN_TEST(test_c32_p1_start_offset_2pow57_rejoin_refused);
    RUN_TEST(test_c32_p2_start_offset_2pow56_append_refused);
    RUN_TEST(test_c32_p3_start_offset_negative_rejoin_refused);
    RUN_TEST(test_c32_p4_start_offset_below_initial_rejoin_refused);
    RUN_TEST(test_c32_p5_start_offset_roll_to_limit_append_refused);
    RUN_TEST(test_c32_p6_start_offset_near_limit_append_works);
    RUN_TEST(test_c32_p7_commit_unencodable_offset_refused);
    RUN_TEST(test_c32_p8_region_boundary_end_offset_legitimate);
    RUN_TEST(test_c32_p9_commit_above_last_written_refused);

    RUN_TEST(test_c38_create_refuses_region_below_header_block);
    RUN_TEST(test_c38_create_accepts_non_pow2_file_multiple);

    return UNITY_END();
}
