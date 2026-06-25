#include "unity.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "runtime.h"
#include "header_codec.h"
#include "header_reader.h"

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

/* ── Test configuration ──────────────────────────────────────────────────── */

static const char    *DIR        = "/tmp";
static const char    *QUEUE      = "mmap_test_iq";
static const int32_t  REGION_SZ  = 4096;
static const int      RING_SZ    = 4;
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
    rt = runtime_region_mapper();
    q  = indexed_queue_create(DIR, QUEUE,
                               REGION_SZ, RING_SZ, 1,
                               MAX_FILE, 0,
                               TIMEOUT_NS, rt, rt);
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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedPoller *p = indexed_queue_create_poller(q);

    PollCapture cap = { .directive = NEXT_ADVANCE };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, r);
    TEST_ASSERT_EQUAL_INT(0, cap.call_count);

    indexed_poller_close(p);
}

void test_append_multiple_and_poll_all(void)
{
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedReader   *r = indexed_queue_create_reader(q);

    indexed_appender_append(a, "first",  5);
    indexed_appender_append(a, "second", 6);
    indexed_appender_append(a, "third",  5);

    ReadingContext ctx;
    indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(6, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("second", ctx.data, 6);

    indexed_appender_close(a);
    indexed_reader_close(r);
}

void test_reader_end_of_stream_beyond_last(void)
{
    IndexedReader *r = indexed_queue_create_reader(q);
    ReadingContext ctx;
    indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_END_OF_STREAM, ctx.status);
    indexed_reader_close(r);
}

void test_reader_last_index(void)
{
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedReader   *r = indexed_queue_create_reader(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedReader   *r = indexed_queue_create_reader(q);

    indexed_appender_append(a, "first",  5);
    indexed_appender_append(a, "middle", 6);
    indexed_appender_append(a, "last",   4);

    ReadingContext ctx;

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a0 = indexed_queue_create_appender(q);
    IndexedAppender *a1 = indexed_queue_create_appender(q);
    IndexedPoller   *p  = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    uint8_t id = indexed_appender_id(a);

    indexed_appender_append(a, "msg_0", 5);
    indexed_appender_append(a, "msg_1", 5);
    int64_t before_close = indexed_appender_current_index(a);
    indexed_appender_close(a);

    /* Phase 2: reopen with same ID — should resume at index 2 */
    IndexedAppender *a2 = indexed_appender_create_with_id(
        DIR, QUEUE, REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT_NS, rt, id);

    TEST_ASSERT_EQUAL_INT64(before_close, indexed_appender_current_index(a2));

    /* Phase 3: append more messages — should get index 2, 3 */
    int64_t idx2 = indexed_appender_append(a2, "msg_2", 5);
    int64_t idx3 = indexed_appender_append(a2, "msg_3", 5);
    TEST_ASSERT_EQUAL_INT64(2, idx2);
    TEST_ASSERT_EQUAL_INT64(3, idx3);

    /* Phase 4: poll all 4 messages */
    IndexedPoller *p = indexed_queue_create_poller(q);
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

/* ── Uncommitted append + commit ─────────────────────────────────────────── */

void test_uncommitted_append_and_commit(void)
{
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);

    /* Append one committed, one uncommitted */
    indexed_appender_append(a, "committed", 9);
    int64_t uncommitted_off = indexed_appender_append_uncommitted(a, "uncommitted", 11);

    /* The uncommitted iterator should find exactly one message */
    UncommittedPayloadIterator *it =
        indexed_appender_uncommitted_iterator(a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);

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
    found = 0;
    while (uncommitted_iterator_advance(it)) found++;
    TEST_ASSERT_EQUAL_INT(0, found);

    indexed_appender_close(a);
}

/* ── Reader entry_status ─────────────────────────────────────────────────── */

void test_reader_entry_status(void)
{
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedReader   *r = indexed_queue_create_reader(q);

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
    IndexedAppender *a = indexed_queue_create_appender(q);
    IndexedPoller   *p = indexed_queue_create_poller(q);

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
    IndexedQueue *q1 = indexed_queue_create(DIR, QUEUE, LARGE_REGION, RING_SZ, 1, 
                                             LARGE_REGION * 16, 0, TIMEOUT_NS, rt, rt);
    IndexedAppender *a = indexed_queue_create_appender(q1);
    indexed_appender_append(a, "hello", 5);
    indexed_appender_close(a);
    indexed_queue_close(q1);

    /* Phase 2: Create a second queue instance with "wrong" default (4KB) */
    IndexedQueue *q2 = indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                             MAX_FILE, 0, TIMEOUT_NS, rt, rt);
    
    /* Verify metadata reported by q2 is correct (8MB) */
    TEST_ASSERT_EQUAL_INT32(LARGE_REGION, indexed_queue_region_size(q2));

    /* Create poller from q2 and poll the message */
    IndexedPoller *p = indexed_queue_create_poller(q2);
    PollCapture cap = { .directive = NEXT_ADVANCE };
    PollResult r = indexed_poller_poll(p, capture_handler, &cap);

    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_STRING("hello", cap.data);

    indexed_poller_close(p);
    indexed_queue_close(q2);
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
    RUN_TEST(test_large_message_sequence_crossing_regions);
    RUN_TEST(test_appender_recovers_position_after_close);
    RUN_TEST(test_uncommitted_append_and_commit);
    RUN_TEST(test_uncommitted_iterator_finds_uncommitted);
    RUN_TEST(test_reader_entry_status);
    RUN_TEST(test_poller_exists);
    
    RUN_TEST(test_poller_automatically_discovers_region_size);

    return UNITY_END();
}
