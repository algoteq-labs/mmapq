#include "unity.h"
#include "mmap_platform.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_reader.h"
#include "runtime.h"
#include "internal/header_codec.h"
#include "internal/header_reader.h"
#include "indexed_queue.h"
#include "internal/queue_meta.h"
#include "internal/batch_payload_iterator_internal.h"
#include <stdio.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

static const char    *DIR       = "/tmp";
static const char    *QUEUE     = "mmap_test_ibq";
static const int32_t  REGION_SZ = 4096 * 16;
static const int      RING_SZ   = 4;
static const int64_t  MAX_FILE  = (int64_t)4096 * 16 * 64;
static const uint64_t TIMEOUT   = 5000000000ULL;

static MmapqRuntime           *rt = NULL;
static MmapqIndexedBatchQueue *q  = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

void setUp(void)
{
    cleanup();
    int rc_rt = mmapq_runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
    int rc_q  = mmapq_indexed_batch_queue_create(DIR, QUEUE,
              REGION_SZ, RING_SZ, 1,
              MAX_FILE, 0, TIMEOUT,
              0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
}

void tearDown(void)
{
    if (q) { mmapq_indexed_batch_queue_close(q); q = NULL; }
    cleanup();
}

/* ── Capture helpers ─────────────────────────────────────────────────────── */

typedef struct {
    int64_t captured_batch_index;
    int32_t captured_message_count;
    char    captured_first_msg[256];
    int     call_count;
    MmapqNextMove directive;
} BatchPollCapture;

static MmapqNextMove capture_handler(
    int64_t              batch_index,
    MmapqBatchReadingContext *ctx,
    void                *user_ctx)
{
    BatchPollCapture *c = (BatchPollCapture *)user_ctx;
    c->captured_batch_index = batch_index;
    c->captured_message_count = ctx->message_count;
    c->call_count++;

    if (mmapq_batch_iterator_advance(ctx->iterator)) {
        int32_t len = batch_iterator_current_length(ctx->iterator);
        if (len > 0 && len < (int32_t)sizeof(c->captured_first_msg) - 1) {
            memcpy(c->captured_first_msg, batch_iterator_current_data(ctx->iterator), (size_t)len);
            c->captured_first_msg[len] = '\0';
        }
    }

    return c->directive;
}

static MmapqNextMove advance_all_handler(
    int64_t              batch_index,
    MmapqBatchReadingContext *ctx,
    void                *user_ctx)
{
    (void)batch_index;
    int *count = (int *)user_ctx;
    *count = 0;
    while (mmapq_batch_iterator_advance(ctx->iterator)) {
        (*count)++;
    }
    return MMAPQ_NEXT_ADVANCE;
}

/* ── Poller tests ────────────────────────────────────────────────────────── */

void test_batch_poll_end_of_stream(void)
{
    MmapqIndexedBatchPoller *p = NULL;
    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    BatchPollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, pr);

    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poll_single_batch(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "msg0", 4);
    mmapq_mutable_batch_append(b, "msg1", 4);
    mmapq_mutable_batch_append(b, "msg2", 4);
    int64_t off = mmapq_mutable_batch_complete(b);
    mmapq_indexed_batch_appender_commit_batch(a, off);

    BatchPollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_INT(3, cap.captured_message_count);
    TEST_ASSERT_EQUAL_STRING("msg0", cap.captured_first_msg);
    TEST_ASSERT_EQUAL_INT(1, cap.call_count);

    pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, pr);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poll_multiple_batches(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "b0m0", 4);
    mmapq_mutable_batch_append(b, "b0m1", 4);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "b1m0", 4);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };

    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_INT(2, cap.captured_message_count);
    TEST_ASSERT_EQUAL_STRING("b0m0", cap.captured_first_msg);

    pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(1, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_INT(1, cap.captured_message_count);
    TEST_ASSERT_EQUAL_STRING("b1m0", cap.captured_first_msg);

    pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, pr);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poll_advance_all_messages(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "m0", 2);
    mmapq_mutable_batch_append(b, "m1", 2);
    mmapq_mutable_batch_append(b, "m2", 2);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    int msg_count = 0;
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, advance_all_handler, &msg_count);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(3, msg_count);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poll_stop(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "msg", 3);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = MMAPQ_NEXT_STOP };
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_STOPPED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poll_retreat(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "batch0", 6);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "batch1", 6);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);

    /* Retreat from batch 1 — handler sees batch 1, cursor goes back to 0 */
    cap.directive = MMAPQ_NEXT_RETREAT;
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_RETREATED, pr);
    TEST_ASSERT_EQUAL_INT(1, cap.captured_batch_index);

    /* Now poll batch 0 again */
    cap.directive = MMAPQ_NEXT_ADVANCE;
    pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poll_retreat_at_start(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "msg", 3);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = MMAPQ_NEXT_RETREAT };
    BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_STOPPED, pr);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poller_exists(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_FALSE(mmapq_indexed_batch_poller_exists(p, 0));

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "x", 1);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    TEST_ASSERT_TRUE(mmapq_indexed_batch_poller_exists(p, 0));
    TEST_ASSERT_FALSE(mmapq_indexed_batch_poller_exists(p, 1));

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

void test_batch_poller_move_to_last_existing(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_FALSE(mmapq_indexed_batch_poller_move_to_last_existing_batch(p));

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "first", 5);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "second", 6);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    TEST_ASSERT_TRUE(mmapq_indexed_batch_poller_move_to_last_existing_batch(p));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_batch_poller_current_batch_index(p));

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_poller_close(p);
}

/* ── Reader tests (unchanged) ────────────────────────────────────────────── */

void test_reader_read_by_index(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchReader *r = NULL;

    int rc_r = mmapq_indexed_batch_queue_create_reader(q, &r);

    TEST_ASSERT_EQUAL_INT(0, rc_r);

    TEST_ASSERT_NOT_NULL(r);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "first0", 6);
    mmapq_mutable_batch_append(b, "second0", 7);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "first1", 6);
    mmapq_mutable_batch_append(b, "second1", 7);
    mmapq_mutable_batch_append(b, "third1", 6);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    MmapqBatchReadingContext ctx = {0};

    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_INT(2, ctx.message_count);

    TEST_ASSERT_TRUE(mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_INT(6, batch_iterator_current_length(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("first0", batch_iterator_current_data(ctx.iterator), 6);

    TEST_ASSERT_TRUE(mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("second0", batch_iterator_current_data(ctx.iterator), 7);

    TEST_ASSERT_FALSE(mmapq_batch_iterator_advance(ctx.iterator));

    mmapq_indexed_batch_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(3, ctx.message_count);

    TEST_ASSERT_TRUE(mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("first1", batch_iterator_current_data(ctx.iterator), 6);
    TEST_ASSERT_TRUE(mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("second1", batch_iterator_current_data(ctx.iterator), 7);
    TEST_ASSERT_TRUE(mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("third1", batch_iterator_current_data(ctx.iterator), 6);
    TEST_ASSERT_FALSE(mmapq_batch_iterator_advance(ctx.iterator));

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_reader_close(r);
}

void test_reader_read_first_and_last(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchReader *r = NULL;

    int rc_r = mmapq_indexed_batch_queue_create_reader(q, &r);

    TEST_ASSERT_EQUAL_INT(0, rc_r);

    TEST_ASSERT_NOT_NULL(r);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "only", 4);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "last_a", 6);
    mmapq_mutable_batch_append(b, "last_b", 6);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    MmapqBatchReadingContext ctx = {0};

    mmapq_indexed_batch_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);

    mmapq_indexed_batch_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(1, ctx.index);
    TEST_ASSERT_EQUAL_INT(2, ctx.message_count);

    TEST_ASSERT_TRUE(mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("last_a", batch_iterator_current_data(ctx.iterator), 6);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_reader_close(r);
}

void test_reader_end_of_stream_when_empty(void)
{
    MmapqIndexedBatchReader *r = NULL;
    int rc_r = mmapq_indexed_batch_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, mmapq_indexed_batch_reader_last_index(r));

    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, ctx.index);

    mmapq_indexed_batch_reader_close(r);
}

void test_reader_has_entry(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqIndexedBatchReader *r = NULL;

    int rc_r = mmapq_indexed_batch_queue_create_reader(q, &r);

    TEST_ASSERT_EQUAL_INT(0, rc_r);

    TEST_ASSERT_NOT_NULL(r);

    TEST_ASSERT_FALSE(mmapq_indexed_batch_reader_has_entry(r, 0));

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "x", 1);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

    TEST_ASSERT_TRUE(mmapq_indexed_batch_reader_has_entry(r, 0));
    TEST_ASSERT_FALSE(mmapq_indexed_batch_reader_has_entry(r, 1));

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_reader_close(r);
}

void test_batch_append_rejects_zero_length(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);

    int32_t ok = mmapq_mutable_batch_append(b, "valid", 5);
    TEST_ASSERT_EQUAL_INT(0, ok);

    int32_t bad = mmapq_mutable_batch_append(b, "", 0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    int32_t next = mmapq_mutable_batch_append(b, "also-ok", 7);
    TEST_ASSERT_EQUAL_INT(1, next);

    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);
}

void test_batch_append_rejects_oversized_message(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);

    int32_t ok = mmapq_mutable_batch_append(b, "small", 5);
    TEST_ASSERT_EQUAL_INT(0, ok);

    /* REGION_SZ = 65536, message of 65536 bytes exceeds region_size - 4 */
    char big[65536];
    memset(big, 'x', sizeof(big));
    int32_t bad = mmapq_mutable_batch_append(b, big, (int32_t)sizeof(big));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    /* Batch is partially populated — must discard, not commit */
    mmapq_indexed_batch_appender_discard_batch(a, b->batch_offset);
    mmapq_indexed_batch_appender_close(a);
}

void test_open_batch_as_normal_refused(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "msg", 3);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);

    /* Attempt to create a normal participant on the same queue */
    MmapqIndexedQueue *nq = NULL;
    int rc_nq = mmapq_indexed_queue_create_default(DIR, QUEUE, rt, rt, &nq);
    TEST_ASSERT_EQUAL_INT(0, rc_nq);
    TEST_ASSERT_NOT_NULL(nq);

    MmapqIndexedAppender *na = NULL;
    int rc_na = mmapq_indexed_queue_create_appender(nq, &na);
    TEST_ASSERT_TRUE(rc_na != 0);
    TEST_ASSERT(rc_na != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(na);

    if (nq) mmapq_indexed_queue_close(nq);
}

void test_batch_roundtrip_type_stamped(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "x", 1);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);

    /* Verify byte at offset 25 == 2 (QUEUE_TYPE_BATCH) */
    char path[512];
    snprintf(path, sizeof(path), "/tmp/%s/meta", QUEUE);
    uint8_t qt = 0xFF;
    FILE *f = fopen(path, "rb");
    TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQUAL_INT(0, fseek(f, QUEUE_META_QUEUE_TYPE_OFFSET, SEEK_SET));
    TEST_ASSERT_EQUAL_INT(1, (int)fread(&qt, sizeof(qt), 1, f));
    fclose(f);
    TEST_ASSERT_EQUAL_INT(QUEUE_TYPE_BATCH, qt);
}

/* ── Batch corrupt payload ────────────────────────────────────────────────── */

static int batch_corrupt_handler_count;
static MmapqNextMove batch_corrupt_handler(int64_t bi, MmapqBatchReadingContext *ctx, void *user)
{
    (void)bi; (void)ctx; (void)user;
    batch_corrupt_handler_count++;
    return MMAPQ_NEXT_ADVANCE;
}

static void test_batch_poll_detects_corrupt_payload(void)
{
    char qname[64];
    snprintf(qname, sizeof(qname), "%s_corrupt", QUEUE);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    int rc = system(cmd); (void)rc;

    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(
        DIR, qname, REGION_SZ, RING_SZ, 1, MAX_FILE, 0, TIMEOUT, 0, NULL, NULL, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);

    MmapqIndexedBatchAppender *a = NULL;

    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);

    TEST_ASSERT_EQUAL_INT(0, rc_a);

    TEST_ASSERT_NOT_NULL(a);

    TEST_ASSERT_NOT_NULL(a);
    MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(mb);
    int32_t r = mmapq_mutable_batch_append(mb, "hello", 5);
    TEST_ASSERT(r >= 0);
    int64_t off = mmapq_mutable_batch_complete(mb);
    TEST_ASSERT(off >= 0);
    int64_t idx = mmapq_indexed_batch_appender_commit_batch(a, off);
    TEST_ASSERT(idx >= 0);
    mmapq_indexed_batch_appender_close(a);

    /* Find the payload file for appender 0 */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_0_0", DIR, qname);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "failed to open batch payload file");
    /* Zero the 4-byte message count at offset 0 (first batch starts at 0) */
    uint32_t zero = 0;
    ssize_t written = pwrite(fd, &zero, sizeof(zero), off); (void)written;
    close(fd);

    MmapqIndexedBatchPoller *p = NULL;

    int rc_p = mmapq_indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_NOT_NULL(p);
    batch_corrupt_handler_count = 0;
    MmapqPollResult pr = mmapq_indexed_batch_poller_poll(p, batch_corrupt_handler, NULL);
    TEST_ASSERT_EQUAL(MMAPQ_POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL(0, batch_corrupt_handler_count);
    mmapq_indexed_batch_poller_close(p);

    mmapq_indexed_batch_queue_close(q);
    cleanup();
}

/* ── Batch recovery truncation (A6) ─────────────────────────────────────────
 * A corrupt message length inside the last committed batch truncates the
 * recovery scan (skip_to_end_of_stream). Recovery must refuse: accepting a
 * partial scan places the append position inside committed data (short
 * truncation — overwrite) or past end of stream in unreachable space (long
 * truncation). Mirrors the last_error refusal in indexed_batch_archiver.c
 * and the ECORRUPT refusal in single-message indexed_appender.c. */

static int64_t rec_msg_offsets[8];
static int     rec_n_msgs;

static MmapqNextMove rec_collect_handler(int64_t bi, MmapqBatchReadingContext *ctx, void *user)
{
    (void)bi; (void)user;
    const void *d;
    int32_t l;
    rec_n_msgs = 0;
    while (mmapq_batch_iterator_next(ctx->iterator, &d, &l) == 1 && rec_n_msgs < 8)
        rec_msg_offsets[rec_n_msgs++] = ctx->iterator->current_msg_offset;
    return MMAPQ_NEXT_ADVANCE;
}

/* Appends two committed batches of three 13-byte messages; the second batch
 * starts at offset 119 (start-of-stream 64 + 4-byte count + 3*(4+13)). */
static void rec_append_two_batches(MmapqIndexedBatchAppender *a)
{
    int64_t off = -1;
    for (int64_t b = 0; b < 2; b++) {
        MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(a);
        TEST_ASSERT_NOT_NULL(mb);
        for (int i = 0; i < 3; i++) {
            char buf[32];
            snprintf(buf, sizeof(buf), "b%lld-m%d-padding", (long long)b, i);
            TEST_ASSERT_EQUAL_INT(i, mmapq_mutable_batch_append(mb, buf, (int32_t)strlen(buf)));
        }
        off = mmapq_mutable_batch_complete(mb);
        TEST_ASSERT_EQUAL_INT64(64 + 55 * b, off);
        TEST_ASSERT_EQUAL_INT64(b, mmapq_indexed_batch_appender_commit_batch(a, off));
    }
}

/* Corrupts the length prefix of message 1 of the last committed batch with
 * `bogus`, then asserts that recovery (a fresh appender) refuses the
 * truncated scan with MMAPQ_ECORRUPT and produces no appender. */
static void rec_assert_recovery_refused(const char *suffix, int32_t bogus)
{
    char qname[64];
    snprintf(qname, sizeof(qname), "%s_%s", QUEUE, suffix);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    int rc = system(cmd); (void)rc;

    MmapqIndexedBatchQueue *tq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, qname, REGION_SZ, RING_SZ, 1, MAX_FILE, 0, TIMEOUT, 0, rt, rt, &tq));
    TEST_ASSERT_NOT_NULL(tq);

    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(tq, &a));
    TEST_ASSERT_NOT_NULL(a);
    rec_append_two_batches(a);

    /* Learn the length-prefix offset of message 1 of the last committed batch. */
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(tq, &p));
    TEST_ASSERT_NOT_NULL(p);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(p, rec_collect_handler, NULL));
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(p, rec_collect_handler, NULL));
    mmapq_indexed_batch_poller_close(p);
    TEST_ASSERT_EQUAL_INT(3, rec_n_msgs);
    const int64_t inject_off = rec_msg_offsets[1];
    TEST_ASSERT_EQUAL_INT64(140, inject_off);

    mmapq_indexed_batch_appender_close(a);

    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_0_0", DIR, qname);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "failed to open batch payload file");
    TEST_ASSERT_EQUAL_INT(sizeof(bogus), pwrite(fd, &bogus, sizeof(bogus), inject_off));
    close(fd);

    MmapqIndexedBatchAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_batch_queue_create_appender(tq, &a2));
    TEST_ASSERT_NULL(a2);

    mmapq_indexed_batch_queue_close(tq);
}

static void test_batch_recovery_refuses_short_truncated_scan(void)
{
    /* Negative non-padding length: the scan stops 34 bytes inside the last
     * committed batch; accepting it would overwrite committed data. */
    rec_assert_recovery_refused("rec_short", (int32_t)0x80000010);
}

static void test_batch_recovery_refuses_long_truncated_scan(void)
{
    /* Large positive length: the scan walks 3417 bytes past end of stream;
     * accepting it would strand everything appended after recovery. */
    rec_assert_recovery_refused("rec_long", (int32_t)0x7FFFFFF0);
}

/* Completes one more batch after the two-batch fixture; returns its offset. */
static int64_t rec_complete_third_batch(MmapqIndexedBatchAppender *a)
{
    MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(mb);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(mb, "AFTER-RECOVERY", 14));
    int64_t off = mmapq_mutable_batch_complete(mb);
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(0, off);
    return off;
}

static void test_batch_recovery_clean_tree_unaffected(void)
{
    /* The control offset comes from a clean run inside the test — a
     * never-closed appender's third batch — not a hard-coded constant, so a
     * framing change voids neither side of the comparison. */
    char qname[64];
    char cmd[512];

    snprintf(qname, sizeof(qname), "%s_rec_clean_ctrl", QUEUE);
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    int rc = system(cmd); (void)rc;
    MmapqIndexedBatchQueue *tq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, qname, REGION_SZ, RING_SZ, 1, MAX_FILE, 0, TIMEOUT, 0, rt, rt, &tq));
    TEST_ASSERT_NOT_NULL(tq);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(tq, &a));
    TEST_ASSERT_NOT_NULL(a);
    rec_append_two_batches(a);
    const int64_t control_off = rec_complete_third_batch(a);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(tq);

    snprintf(qname, sizeof(qname), "%s_rec_clean_rec", QUEUE);
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    rc = system(cmd); (void)rc;
    tq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, qname, REGION_SZ, RING_SZ, 1, MAX_FILE, 0, TIMEOUT, 0, rt, rt, &tq));
    TEST_ASSERT_NOT_NULL(tq);
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(tq, &a));
    TEST_ASSERT_NOT_NULL(a);
    rec_append_two_batches(a);
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(tq, &a2));
    TEST_ASSERT_NOT_NULL(a2);
    const int64_t recovered_off = rec_complete_third_batch(a2);
    mmapq_indexed_batch_appender_close(a2);
    mmapq_indexed_batch_queue_close(tq);

    TEST_ASSERT_EQUAL_INT64(control_off, recovered_off);
}

/* ── Stale appender catch-up ─────────────────────────────────────────────── */

static void test_batch_stale_appender_catch_up(void)
{
    /*
     * Regression: batch appender variant of the stale-appender livelock fix.
     * Fill many batches with one appender, then append with a second (stale)
     * appender and verify the returned index is correct.
     */
    MmapqIndexedBatchAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_batch_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);

    MmapqIndexedBatchAppender *a2 = NULL;
    int rc_a2 = mmapq_indexed_batch_queue_create_appender(q, &a2);
    TEST_ASSERT_EQUAL_INT(0, rc_a2);
    TEST_ASSERT_NOT_NULL(a2);

    TEST_ASSERT_NOT_NULL(a1);
    TEST_ASSERT_NOT_NULL(a2);

    int64_t batch_count = 50000;
    const char *payload = "x";
    int32_t     plen    = 1;

    for (int64_t i = 0; i < batch_count; i++) {
        int64_t idx = mmapq_indexed_batch_appender_append_and_commit(a1, payload, plen);
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    int64_t idx = mmapq_indexed_batch_appender_append_and_commit(a2, payload, plen);
    clock_gettime(CLOCK_MONOTONIC, &ts1);

    TEST_ASSERT_EQUAL_INT64(batch_count, idx);

    uint64_t dt_us = (uint64_t)((ts1.tv_sec - ts0.tv_sec) * 1000000LL
                              + (ts1.tv_nsec - ts0.tv_nsec) / 1000LL);
    (void)dt_us;

    mmapq_indexed_batch_appender_close(a1);
    mmapq_indexed_batch_appender_close(a2);
}

static void test_batch_appender_id_255_succeeds(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    int rc = mmapq_indexed_batch_queue_create_appender_with_id(q, 255, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    TEST_ASSERT_EQUAL_UINT8(255, mmapq_indexed_batch_appender_id(a));
    mmapq_indexed_batch_appender_close(a);
}

static MmapqNextMove capture_appender_id(int64_t index, MmapqBatchReadingContext *ctx, void *user)
{
    (void)index;
    *(uint8_t *)user = ctx->appender_id;
    return MMAPQ_NEXT_ADVANCE;
}

static void test_batch_poller_reports_appender_id(void)
{
    MmapqIndexedBatchAppender *appender = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(q, 173, &appender));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_append_and_commit(appender, "message", 7));
    MmapqIndexedBatchPoller *poller = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(q, &poller));
    uint8_t appender_id = 0;
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(poller, capture_appender_id, &appender_id));
    mmapq_indexed_batch_poller_close(poller);
    mmapq_indexed_batch_appender_close(appender);
    TEST_ASSERT_EQUAL_UINT8(173, appender_id);
}

static void test_batch_reader_rejects_zero_message_count(void)
{
    MmapqIndexedBatchAppender *appender = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(q, 0, &appender));
    MmapqMutableBatch *batch = mmapq_indexed_batch_appender_start_batch(appender);
    TEST_ASSERT_NOT_NULL(batch);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(batch, "message", 7));
    int64_t offset = mmapq_mutable_batch_complete(batch);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(appender, offset));
    mmapq_indexed_batch_appender_close(appender);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_0_0", DIR, QUEUE);
    int descriptor = open(path, O_WRONLY);
    TEST_ASSERT_TRUE(descriptor >= 0);
    int32_t count = 0;
    TEST_ASSERT_EQUAL_INT(sizeof(count), pwrite(descriptor, &count, sizeof(count), offset));
    close(descriptor);
    MmapqIndexedBatchReader *reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &reader));
    MmapqBatchReadingContext context = {0};
    mmapq_indexed_batch_reader_read(reader, 0, &context);
    int error = mmapq_indexed_batch_reader_last_error(reader);
    mmapq_indexed_batch_reader_close(reader);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR, context.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, error);
}

static WrapResult timeout_wrap(RegionAccessor *accessor, int64_t position, Buffer *buffer)
{
    (void)accessor;
    (void)position;
    (void)buffer;
    return WRAP_MAP_TIMED_OUT;
}

static void test_batch_message_count_propagates_mapping_failure(void)
{
    int32_t stale_count = 1;
    RegionAccessor accessor = {.wrap = timeout_wrap};
    MmapqBatchPayloadIterator iterator = {
        .payload_acc = &accessor,
        .payload_buf = {.data = (uint8_t *)&stale_count, .capacity = sizeof(stale_count)},
    };
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapq_batch_iterator_read_message_count(&iterator, 0));
}

static void test_batch_iterator_rejects_invalid_lengths(void)
{
    const int32_t lengths[] = {0, INT32_MAX, INT32_MIN};
    for (size_t index = 0; index < sizeof(lengths) / sizeof(lengths[0]); index++) {
        int32_t payload[] = {1, lengths[index], 0, 0};
        RegionAccessor accessor = {
            .active_address = (uint8_t *)payload,
            .active_base = 0,
            .fast_path_end = sizeof(payload),
            .mapped_capacity = sizeof(payload),
        };
        MmapqBatchPayloadIterator iterator = {.payload_acc = &accessor};
        mmapq_batch_iterator_init(&iterator, 0, 1, 0);
        TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_advance(&iterator));
        TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_has_current_ffi(&iterator));
    }
}

static void test_batch_iterator_stops_on_mapping_failure(void)
{
    int32_t stale_payload[] = {3, 0};
    RegionAccessor accessor = {.wrap = timeout_wrap};
    MmapqBatchPayloadIterator iterator = {
        .payload_acc = &accessor,
        .payload_buf = {.data = (uint8_t *)stale_payload, .capacity = sizeof(stale_payload)},
    };
    mmapq_batch_iterator_init(&iterator, 0, 1, 0);
    TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_advance(&iterator));
    TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_has_current_ffi(&iterator));
}

static void test_checked_batch_step(void)
{
    int32_t framed[] = {1, 4, 123};
    RegionAccessor accessor = {.wrap = timeout_wrap};
    MmapqBatchPayloadIterator iterator = {
        .payload_acc = &accessor,
    };
    const void *data = framed;
    int32_t length = 99;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_batch_iterator_next(NULL, &data, &length));
    TEST_ASSERT_NULL(data);
    TEST_ASSERT_EQUAL_INT(0, length);
    accessor.active_address = (uint8_t *)framed;
    accessor.active_base = 0;
    accessor.fast_path_end = sizeof(framed);
    accessor.mapped_capacity = sizeof(framed);
    mmapq_batch_iterator_init(&iterator, 0, 1, 0);
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(&iterator, &data, &length));
    TEST_ASSERT_EQUAL_PTR(&framed[2], data);
    TEST_ASSERT_EQUAL_INT(4, length);
    TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_next(&iterator, &data, &length));
    TEST_ASSERT_NULL(data);
    TEST_ASSERT_EQUAL_INT(0, length);
    framed[1] = 0;
    mmapq_batch_iterator_init(&iterator, 0, 1, 0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_batch_iterator_next(&iterator, &data, &length));
    framed[1] = 4;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_batch_iterator_next(&iterator, &data, &length));
    TEST_ASSERT_NULL(data);
    TEST_ASSERT_EQUAL_INT(0, length);
    accessor.fast_path_end = 0;
    mmapq_batch_iterator_init(&iterator, 0, 1, 0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapq_batch_iterator_next(&iterator, &data, &length));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapq_batch_iterator_next(&iterator, &data, &length));
    TEST_ASSERT_NULL(data);
    TEST_ASSERT_EQUAL_INT(0, length);
}

static void test_checked_batch_step_skips_short_and_long_padding(void)
{
    unsigned char payload[65536];
    memset(payload, 0x5a, sizeof(payload));
    const int32_t remaining[] = {1, 2, 3, 4, 100};
    MmapqIndexedBatchReader *reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &reader));
    for (size_t index = 0; index < sizeof(remaining) / sizeof(remaining[0]); index++) {
        MmapqIndexedBatchAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(q, (uint8_t)index, &appender));
        MmapqMutableBatch *batch = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(appender, &batch));
        int32_t first_length = REGION_SZ - 64 - 4 - 4 - remaining[index];
        TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(batch, payload, first_length));
        TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(batch, payload, 256));
        int64_t offset = mmapq_mutable_batch_complete(batch);
        TEST_ASSERT_EQUAL_INT64((int64_t)index, mmapq_indexed_batch_appender_commit_batch(appender, offset));
        MmapqBatchReadingContext context = {0};
        mmapq_indexed_batch_reader_read(reader, (int64_t)index, &context);
        TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, context.status);
        const void *data = NULL;
        int32_t length = 0;
        TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(context.iterator, &data, &length));
        TEST_ASSERT_EQUAL_INT(first_length, length);
        TEST_ASSERT_EQUAL_MEMORY(payload, data, (size_t)length);
        TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(context.iterator, &data, &length));
        TEST_ASSERT_EQUAL_INT(256, length);
        TEST_ASSERT_EQUAL_MEMORY(payload, data, (size_t)length);
        TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_next(context.iterator, &data, &length));
        mmapq_indexed_batch_appender_close(appender);
    }
    mmapq_indexed_batch_reader_close(reader);
}

/* ── D3 (CAND-16): absurd batch indices alias already-written slots ──────── */

/* D3_MAX_INDEX literal must match MMAPQI_MAX_INDEX in header_codec.h. */
#define D3_MAX_INDEX   (((int64_t)1 << 56) - 1)
#define D3_ALIAS_INDEX ((int64_t)1 << 61)   /* position wraps to 0: batch 0's slot */

static void d3_commit_one_batch(MmapqIndexedBatchAppender **out)
{
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, out));
    TEST_ASSERT_NOT_NULL(*out);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(*out);
    mmapq_mutable_batch_append(b, "d3-batch0", 9);
    TEST_ASSERT_EQUAL_INT64(0,
        mmapq_indexed_batch_appender_commit_batch(*out, mmapq_mutable_batch_complete(b)));
}

void test_batch_poller_rejects_absurd_index(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    d3_commit_one_batch(&a);

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(q, &p));
    TEST_ASSERT_NOT_NULL(p);

    /* Unpatched: all three return 1 — 2^61 aliases batch 0's slot and
     * 2^61+1 aliases batch 1's. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_move_to_index(p, D3_ALIAS_INDEX));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_exists(p, D3_ALIAS_INDEX));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_move_to_index(p, D3_ALIAS_INDEX + 1));

    /* Rejection must not have moved the poller: the next poll yields batch 0. */
    BatchPollCapture cap = { .directive = MMAPQ_NEXT_ADVANCE };
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(p, capture_handler, &cap));
    TEST_ASSERT_EQUAL_INT64(0, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_STRING("d3-batch0", cap.captured_first_msg);

    mmapq_indexed_batch_poller_close(p);
    mmapq_indexed_batch_appender_close(a);
}

void test_batch_reader_rejects_absurd_index(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    d3_commit_one_batch(&a);

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);

    /* entry_status boundary first, on the fresh reader (last_error still 0):
     * entry_status uses its own inline > MAX check (negative indices must
     * keep falling through to ARCHIVED), so the shared-helper pins do not
     * cover it — owner review found this boundary survived a bound mutation
     * to MAX+4096. Unpatched, entry_status(MAX+1) is AVAILABLE via the
     * rolled-mapper truncation alias (CAND-28). */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_END_OF_STREAM,
        mmapq_indexed_batch_reader_entry_status(r, D3_MAX_INDEX));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR,
        mmapq_indexed_batch_reader_entry_status(r, D3_MAX_INDEX + 1));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_reader_last_error(r));

    /* Unpatched: AVAILABLE with batch 0's payload. */
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, D3_ALIAS_INDEX, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_reader_last_error(r));

    /* Unpatched: AVAILABLE. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR,
        mmapq_indexed_batch_reader_entry_status(r, D3_ALIAS_INDEX));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_reader_last_error(r));

    /* has_entry inherits entry_status's rejection. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_has_entry(r, D3_ALIAS_INDEX));

    /* Anti-collapse pin, batch surface: indices in [2^60, 2^61) wrap to
     * negative positions reported as WRAP_MAP_TIMED_OUT. Unpatched:
     * BATCH_ENTRY_ERROR with ETIMEDOUT. */
    mmapq_indexed_batch_reader_read(r, ((int64_t)1 << 60) + 5, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_reader_last_error(r));

    /* Boundary, observable through the reader. Unpatched, at this geometry
     * read(MAX+1) aliases header_0 via the rolled mapper's int file_index
     * truncation (CAND-28) and returns AVAILABLE. */
    mmapq_indexed_batch_reader_read(r, D3_MAX_INDEX, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));

    mmapq_indexed_batch_reader_read(r, D3_MAX_INDEX + 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_reader_last_error(r));

    mmapq_indexed_batch_reader_close(r);
    mmapq_indexed_batch_appender_close(a);
}

void test_batch_queue_create_start_index_boundary(void)
{
    char qdir[256];

    MmapqIndexedBatchQueue *q2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, "mmap_test_ibq_d3max", REGION_SZ, RING_SZ, 1, MAX_FILE,
        D3_MAX_INDEX, TIMEOUT, 0, rt, rt, &q2));
    TEST_ASSERT_NOT_NULL(q2);
    mmapq_indexed_batch_queue_close(q2);
    snprintf(qdir, sizeof(qdir), "rm -rf %s/%s", DIR, "mmap_test_ibq_d3max");
    if (system(qdir) != 0) { /* best-effort */ }

    MmapqIndexedBatchQueue *q3 = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_queue_create(
        DIR, "mmap_test_ibq_d3over", REGION_SZ, RING_SZ, 1, MAX_FILE,
        D3_MAX_INDEX + 1, TIMEOUT, 0, rt, rt, &q3));
    TEST_ASSERT_NULL(q3);
    snprintf(qdir, sizeof(qdir), "rm -rf %s/%s", DIR, "mmap_test_ibq_d3over");
    if (system(qdir) != 0) { /* best-effort */ }
}

/* CAND-28 pin, batch surface: index 2^51 is legal (<= MAX) and never
 * written. Header position 2^54 / MAX_FILE 2^22 = file_index exactly 2^32,
 * which the rolled mapper's int file_index truncated to 0 — aliasing
 * header_0 and serving batch 0's payload for a never-written index.
 * Pre-fix: AVAILABLE / exists == 1 / move_to_index == 1. */
void test_batch_read_no_alias_at_file_index_2pow32(void)
{
    const int64_t c28 = (int64_t)1 << 51;

    MmapqIndexedBatchAppender *a = NULL;
    d3_commit_one_batch(&a);

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(q, &p));
    TEST_ASSERT_NOT_NULL(p);

    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, c28, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_last_error(r));

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_exists(p, c28));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_move_to_index(p, c28 + 1));

    mmapq_indexed_batch_poller_close(p);
    mmapq_indexed_batch_reader_close(r);
    mmapq_indexed_batch_appender_close(a);
}

/* ── CAND-32 pins, batch surface: on-disk appender_meta.start_offset ───────
 *
 * Batch mirrors of the test_indexed_queue.c CAND-32 pins (see that file's
 * section header for the defect mechanism and the C1/C2 bounds). The batch
 * append-time check (C2) sits at the top of commit_batch. The v3 caller-
 * offset check (C3) is mirrored by the P7/P9 batch pins; P8 mirrors the
 * c32_limit scenario with a batch whose end lands exactly on 2^56.
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
    int rc = system(cmd); (void)rc;
}

/* Create the pin queue (archivable) and appender id 0 so appender_meta_0
 * exists on disk, then close both. */
static void c32_create_queue(const char *qname)
{
    c32_cleanup(qname);
    MmapqIndexedBatchQueue *cq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, qname, C32_REGION_SIZE, C32_RING_SIZE, 1,
        C32_MAX_FILE_SIZE, 0, TIMEOUT, 1, rt, rt, &cq));
    TEST_ASSERT_NOT_NULL(cq);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_queue_create_appender_with_id(cq, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(cq);
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
static int c32_rejoin(const char *qname, MmapqIndexedBatchQueue **out_q,
                      MmapqIndexedBatchAppender **out_a)
{
    *out_q = NULL;
    *out_a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, qname, C32_REGION_SIZE, C32_RING_SIZE, 1,
        C32_MAX_FILE_SIZE, 0, TIMEOUT, 1, rt, rt, out_q));
    TEST_ASSERT_NOT_NULL(*out_q);
    return mmapq_indexed_batch_queue_create_appender_with_id(*out_q, 0, out_a);
}

/* One-message batch "HELLO": start, append, complete; returns the batch
 * offset for commit_batch. */
static int64_t c32_complete_hello_batch(MmapqIndexedBatchAppender *a)
{
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "HELLO", 5));
    return mmapq_mutable_batch_complete(b);
}

/* Batch index 0 must have no committed entry. */
static void c32_assert_batch0_empty(MmapqIndexedBatchQueue *cq)
{
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(cq, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_END_OF_STREAM, ctx.status);
    mmapq_indexed_batch_reader_close(r);
}

void test_c32_batch_p1_start_offset_2pow57_rejoin_refused(void)
{
    const char *qn = "mmap_test_ibq_c32_p1";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, (int64_t)1 << 57);

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    int rc = c32_rejoin(qn, &cq, &a);
    if (rc == 0) {
        /* Pre-CAND-32 defect chain (see the plain P1 pin): debug aborts in
         * header_encode (header_codec.h:113) from commit_batch; release
         * masks 2^57 into HEADER_NULL and reports a commit nothing reads. */
        int64_t off = c32_complete_hello_batch(a);
        int64_t idx = mmapq_indexed_batch_appender_commit_batch(a, off);
        char msg[96];
        snprintf(msg, sizeof(msg),
                 "start_offset 2^57 accepted: rejoin rc=0, commit_batch=%lld",
                 (long long)idx);
        mmapq_indexed_batch_appender_close(a);
        mmapq_indexed_batch_queue_close(cq);
        c32_cleanup(qn);
        TEST_FAIL_MESSAGE(msg);
    }
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a);

    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p2_start_offset_2pow56_commit_refused(void)
{
    const char *qn = "mmap_test_ibq_c32_p2";
    c32_create_queue(qn);
    /* 2^56 is a legitimate END position (region boundary) — C1 accepts. */
    c32_plant_start_offset(qn, C32_POS_LIMIT);

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* ...but 2^56 is an unencodable START (56-bit field): C2 refuses. */
    int64_t off = c32_complete_hello_batch(a);
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT, off);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT,
        mmapq_indexed_batch_appender_commit_batch(a, off));
    c32_assert_batch0_empty(cq);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p3_start_offset_negative_rejoin_refused(void)
{
    const char *qn = "mmap_test_ibq_c32_p3";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, -1000);

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    int rc = c32_rejoin(qn, &cq, &a);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a);

    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p4_start_offset_below_initial_rejoin_refused(void)
{
    const char *qn = "mmap_test_ibq_c32_p4";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, INITIAL_PAYLOAD_POSITION - 1);   /* 63 */

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    int rc = c32_rejoin(qn, &cq, &a);
    if (rc == 0) {
        /* Pre-CAND-32 record path: 63 encodes fine — the batch commits over
         * the reserved prefix region, silent corruption, no error. */
        int64_t off = c32_complete_hello_batch(a);
        int64_t idx = mmapq_indexed_batch_appender_commit_batch(a, off);
        char msg[96];
        snprintf(msg, sizeof(msg),
                 "start_offset 63 accepted: rejoin rc=0, commit_batch=%lld",
                 (long long)idx);
        mmapq_indexed_batch_appender_close(a);
        mmapq_indexed_batch_queue_close(cq);
        c32_cleanup(qn);
        TEST_FAIL_MESSAGE(msg);
    }
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(a);

    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p5_start_offset_roll_to_limit_commit_refused(void)
{
    const char *qn = "mmap_test_ibq_c32_p5";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, C32_POS_LIMIT - 2);

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* 2 bytes of region capacity remain at 2^56−2 — too small for the
     * 4-byte batch count, so start_batch rolls the position to exactly
     * 2^56: an unencodable START. C2 refuses at commit_batch. */
    int64_t off = c32_complete_hello_batch(a);
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT, off);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT,
        mmapq_indexed_batch_appender_commit_batch(a, off));
    c32_assert_batch0_empty(cq);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p6_start_offset_near_limit_append_works(void)
{
    /* POSITIVE CONTROL: a legitimate near-limit start_offset must still
     * commit and read back — guards over-rejection by C1/C2. */
    const char *qn = "mmap_test_ibq_c32_p6";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, C32_POS_LIMIT - C32_MAX_FILE_SIZE);

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    int64_t off = c32_complete_hello_batch(a);
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT - C32_MAX_FILE_SIZE, off);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(a, off));

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(cq, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
    const void *data = NULL;
    int32_t length = 0;
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(ctx.iterator, &data, &length));
    TEST_ASSERT_EQUAL_INT32(5, length);
    TEST_ASSERT_EQUAL_MEMORY("HELLO", data, 5);
    mmapq_indexed_batch_reader_close(r);

    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p8_region_boundary_end_offset_legitimate(void)
{
    /* Batch mirror of the c32_limit scenario: batch [count=1][len=32760]
     * [32760 bytes] at 2^56−32768 ends exactly at 2^56 and archives to
     * start_offset == 2^56. C1's upper bound is 2^56 INCLUSIVE; a 2^56−1
     * bound (mutation M3d) ECORRUPTs this legitimate queue. */
    const char *qn = "mmap_test_ibq_c32_p8";
    c32_create_queue(qn);
    c32_plant_start_offset(qn, C32_POS_LIMIT - C32_REGION_SIZE);

    MmapqIndexedBatchQueue *cq = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a));
    TEST_ASSERT_NOT_NULL(a);

    char msg[C32_REGION_SIZE - MMAPQ_BATCH_MSG_COUNT_SIZE - PAYLOAD_LENGTH_SIZE];
    memset(msg, 'L', sizeof(msg));
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_mutable_batch_append(b, msg, (int32_t)sizeof(msg)));
    int64_t off = mmapq_mutable_batch_complete(b);
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT - C32_REGION_SIZE, off);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(a, off));

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(cq, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
    const void *data = NULL;
    int32_t length = 0;
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(ctx.iterator, &data, &length));
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(msg), length);
    TEST_ASSERT_EQUAL_MEMORY(msg, data, sizeof(msg));
    mmapq_indexed_batch_reader_close(r);

    MmapqIndexedBatchArchiver *ar = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_archiver(cq, &ar));
    TEST_ASSERT_NOT_NULL(ar);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(ar, 1));
    int64_t so = -1;
    /* start_offset() returns 1 on success, 0 if the appender ID is unused */
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_start_offset(ar, 0, &so));
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT, so);
    mmapq_indexed_batch_archiver_close(ar);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(cq);

    /* Rejoin with start_offset == 2^56 on disk: C1 must accept. */
    MmapqIndexedBatchAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, c32_rejoin(qn, &cq, &a2));
    TEST_ASSERT_NOT_NULL(a2);
    mmapq_indexed_batch_appender_close(a2);

    /* A fresh archiver opens the meta through the same validated path. */
    MmapqIndexedBatchArchiver *ar2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_archiver(cq, &ar2));
    TEST_ASSERT_NOT_NULL(ar2);
    so = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_start_offset(ar2, 0, &so));
    TEST_ASSERT_EQUAL_INT64(C32_POS_LIMIT, so);
    mmapq_indexed_batch_archiver_close(ar2);
    mmapq_indexed_batch_queue_close(cq);
    c32_cleanup(qn);
}

void test_c32_batch_p7_commit_batch_2pow57_refused(void)
{
    /* Batch C3: commit_batch(a, 2^57) is above this appender's last written
     * start — a caller error, MMAPQ_EINVAL (CAND-32 v3), refused before the
     * encode (never a debug abort at header_codec.h:113). Uses the setUp
     * queue. */
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    TEST_ASSERT_NOT_NULL(a);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "small", 5));
    int64_t off = mmapq_mutable_batch_complete(b);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(a, (int64_t)1 << 57));
    c32_assert_batch0_empty(q);

    /* The real batch still commits as index 0 and reads back. */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(a, off));
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
    const void *data = NULL;
    int32_t length = 0;
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(ctx.iterator, &data, &length));
    TEST_ASSERT_EQUAL_INT32(5, length);
    TEST_ASSERT_EQUAL_MEMORY("small", data, 5);
    mmapq_indexed_batch_reader_close(r);

    mmapq_indexed_batch_appender_close(a);
}

void test_c32_batch_p9_commit_batch_above_last_written_refused(void)
{
    /* Batch C3: an IN-DOMAIN offset above the last completed batch's start
     * was never written by this appender — caller error, MMAPQ_EINVAL.
     * Pre-C3 (re-observed at fail-first): commit_batch(5000) returned 0,
     * having CASed index 0's header at position 5000; index 0 then read
     * BATCH_ENTRY_ERROR forever. Uses the setUp queue. */
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    TEST_ASSERT_NOT_NULL(a);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "small", 5));
    int64_t off = mmapq_mutable_batch_complete(b);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(a, 5000));
    c32_assert_batch0_empty(q);

    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(a, off));
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
    const void *data = NULL;
    int32_t length = 0;
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(ctx.iterator, &data, &length));
    TEST_ASSERT_EQUAL_INT32(5, length);
    TEST_ASSERT_EQUAL_MEMORY("small", data, 5);
    mmapq_indexed_batch_reader_close(r);

    mmapq_indexed_batch_appender_close(a);
}

/* ── CAND-38 pin P1 (batch twin): the geometry rule at create ──────────────
 * Same rule as the plain queue (see test_indexed_queue.c): region_size <
 * 32 KB lets max_file_size be a non-multiple of the transposed header
 * block, and header-file reclaim then deletes live committed headers
 * (probe audit/claude-CAND-38/hdrloss_batch.c: 992 of the first 2112 live
 * batches unreadable at 8 KB files). Refused at create with EINVAL. */

void test_c38_batch_create_refuses_region_below_header_block(void)
{
    MmapqIndexedBatchQueue *bq = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_queue_create(
        DIR, "mmap_test_ibq_c38_small", 4096, 16, 1, 8192, 0,
        TIMEOUT, 0, rt, rt, &bq));
    TEST_ASSERT_NULL(bq);
    c32_cleanup("mmap_test_ibq_c38_small");

    /* Region below the block with a BLOCK-ALIGNED file: only the region
     * rule can refuse this one (mutation M1 discriminates here). */
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_batch_queue_create(
        DIR, "mmap_test_ibq_c38_small2", 4096, 16, 1, 32768, 0,
        TIMEOUT, 0, rt, rt, &bq));
    TEST_ASSERT_NULL(bq);
    c32_cleanup("mmap_test_ibq_c38_small2");
}

void test_c38_batch_create_accepts_non_pow2_file_multiple(void)
{
    /* Positive control: 3 x 32 KB files on the smallest legal region. */
    MmapqIndexedBatchQueue *bq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        DIR, "mmap_test_ibq_c38_ok", 32768, 16, 1, 32768LL * 3, 0,
        TIMEOUT, 0, rt, rt, &bq));
    TEST_ASSERT_NOT_NULL(bq);
    mmapq_indexed_batch_queue_close(bq);
    c32_cleanup("mmap_test_ibq_c38_ok");
}

/* ── CAND-23 pins ── */
void test_batch_iterator_init_null_returns_null(void)
{
    TEST_ASSERT_NULL(mmapq_batch_iterator_init(NULL, 0, 1, 0));
}

void test_batch_iterator_huge_count_walks_to_ecorrupt(void)
{
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "first0", 6);
    mmapq_mutable_batch_append(b, "second0", 7);
    mmapq_mutable_batch_append(b, "third0", 6);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b)));
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &r));
    MmapqBatchReadingContext ctx;
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    mmapq_batch_iterator_init(ctx.iterator, ctx.iterator->batch_offset, 2147483632, 0);
    int ok = 0, iters = 0, ended = 0;
    for (; iters < 64; iters++) { if (!mmapq_batch_iterator_advance(ctx.iterator)) { ended = 1; break; } ok++; }
    TEST_ASSERT_EQUAL_INT(1, ended);
    TEST_ASSERT_EQUAL_INT(3, ok);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, ctx.iterator->last_error);
    mmapq_indexed_batch_reader_close(r);
    mmapq_indexed_batch_appender_close(a);
}

static MmapqNextMove span_handler(int64_t bi, MmapqBatchReadingContext *ctx, void *u)
{
    (void)bi; int *bad = (int *)u; int k = 0;
    while (mmapq_batch_iterator_advance(ctx->iterator)) {
        if (batch_iterator_current_length(ctx->iterator) != 1 ||
            *(const uint8_t *)batch_iterator_current_data(ctx->iterator) != (uint8_t)(k & 0xFF)) (*bad)++;
        k++;
    }
    if (k != 6553 || ctx->iterator->last_error != 0) (*bad)++;
    return MMAPQ_NEXT_ADVANCE;
}

void test_batch_spanning_regions_commits_polls_recovers(void)
{
    const char *SQ = "mmap_test_ibq_span";
    char cmd[512]; snprintf(cmd, sizeof cmd, "rm -rf %s/%s", DIR, SQ); int rc0 = system(cmd); (void)rc0;
    MmapqIndexedBatchQueue *sq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(DIR, SQ, 32768, RING_SZ, 1, (int64_t)32768 * 64, 0, TIMEOUT, 0, rt, rt, &sq));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(sq, &a));
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    for (int i = 0; i < 6553; i++) { uint8_t x = (uint8_t)(i & 0xFF); TEST_ASSERT_EQUAL_INT(i, mmapq_mutable_batch_append(b, &x, 1)); }
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b)));
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(sq, &p));
    int bad = 0;
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(p, span_handler, &bad));
    TEST_ASSERT_EQUAL_INT(0, bad);
    mmapq_indexed_batch_poller_close(p);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(sq); sq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(DIR, SQ, 32768, RING_SZ, 1, (int64_t)32768 * 64, 0, TIMEOUT, 0, rt, rt, &sq));
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(sq, &r));
    MmapqBatchReadingContext ctx;
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    bad = 0; span_handler(0, &ctx, &bad);
    TEST_ASSERT_EQUAL_INT(0, bad);
    /* appender recovery on reopen walks the batch via probe_batch + init(-1) + exhaust */
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(sq, &a));
    b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "z", 1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b)));
    mmapq_indexed_batch_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    bad = 0; span_handler(0, &ctx, &bad);
    TEST_ASSERT_EQUAL_INT(0, bad);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_reader_close(r);
    mmapq_indexed_batch_queue_close(sq);
    rc0 = system(cmd); (void)rc0;
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_checked_batch_step);
    RUN_TEST(test_checked_batch_step_skips_short_and_long_padding);
    RUN_TEST(test_batch_iterator_rejects_invalid_lengths);
    RUN_TEST(test_batch_iterator_stops_on_mapping_failure);
    RUN_TEST(test_batch_poller_reports_appender_id);
    RUN_TEST(test_batch_reader_rejects_zero_message_count);
    RUN_TEST(test_batch_message_count_propagates_mapping_failure);
    RUN_TEST(test_batch_append_rejects_zero_length);
    RUN_TEST(test_batch_append_rejects_oversized_message);
    RUN_TEST(test_batch_poll_end_of_stream);
    RUN_TEST(test_batch_poll_single_batch);
    RUN_TEST(test_batch_poll_multiple_batches);
    RUN_TEST(test_batch_poll_advance_all_messages);
    RUN_TEST(test_batch_poll_stop);
    RUN_TEST(test_batch_poll_retreat);
    RUN_TEST(test_batch_poll_retreat_at_start);
    RUN_TEST(test_batch_poller_exists);
    RUN_TEST(test_batch_poller_move_to_last_existing);
    RUN_TEST(test_reader_read_by_index);
    RUN_TEST(test_reader_read_first_and_last);
    RUN_TEST(test_reader_end_of_stream_when_empty);
    RUN_TEST(test_reader_has_entry);
    RUN_TEST(test_open_batch_as_normal_refused);
    RUN_TEST(test_batch_roundtrip_type_stamped);
    RUN_TEST(test_batch_poll_detects_corrupt_payload);
    RUN_TEST(test_batch_recovery_refuses_short_truncated_scan);
    RUN_TEST(test_batch_recovery_refuses_long_truncated_scan);
    RUN_TEST(test_batch_recovery_clean_tree_unaffected);
    RUN_TEST(test_batch_stale_appender_catch_up);
    RUN_TEST(test_batch_appender_id_255_succeeds);
    RUN_TEST(test_batch_poller_rejects_absurd_index);
    RUN_TEST(test_batch_reader_rejects_absurd_index);
    RUN_TEST(test_batch_queue_create_start_index_boundary);
    RUN_TEST(test_batch_read_no_alias_at_file_index_2pow32);
    RUN_TEST(test_c32_batch_p1_start_offset_2pow57_rejoin_refused);
    RUN_TEST(test_c32_batch_p2_start_offset_2pow56_commit_refused);
    RUN_TEST(test_c32_batch_p3_start_offset_negative_rejoin_refused);
    RUN_TEST(test_c32_batch_p4_start_offset_below_initial_rejoin_refused);
    RUN_TEST(test_c32_batch_p5_start_offset_roll_to_limit_commit_refused);
    RUN_TEST(test_c32_batch_p6_start_offset_near_limit_append_works);
    RUN_TEST(test_c32_batch_p8_region_boundary_end_offset_legitimate);
    RUN_TEST(test_c32_batch_p7_commit_batch_2pow57_refused);
    RUN_TEST(test_c32_batch_p9_commit_batch_above_last_written_refused);
    RUN_TEST(test_c38_batch_create_refuses_region_below_header_block);
    RUN_TEST(test_c38_batch_create_accepts_non_pow2_file_multiple);
    RUN_TEST(test_batch_iterator_init_null_returns_null);
    RUN_TEST(test_batch_iterator_huge_count_walks_to_ecorrupt);
    RUN_TEST(test_batch_spanning_regions_commits_polls_recovers);
    return UNITY_END();
}
