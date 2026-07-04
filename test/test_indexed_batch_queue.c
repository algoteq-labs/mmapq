#include "unity.h"
#include "mmap_platform.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_reader.h"
#include "runtime.h"
#include "header_codec.h"
#include "header_reader.h"
#include "indexed_queue.h"
#include "queue_meta.h"
#include <stdio.h>

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

static Runtime           *rt = NULL;
static IndexedBatchQueue *q  = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    (void)system(cmd);
}

void setUp(void)
{
    cleanup();
    rt = runtime_region_mapper();
    q  = indexed_batch_queue_create(DIR, QUEUE,
             REGION_SZ, RING_SZ, 1,
             MAX_FILE, 0, TIMEOUT,
             0, rt, rt);
}

void tearDown(void)
{
    if (q) { indexed_batch_queue_close(q); q = NULL; }
    cleanup();
}

/* ── Capture helpers ─────────────────────────────────────────────────────── */

typedef struct {
    int64_t captured_batch_index;
    int32_t captured_message_count;
    char    captured_first_msg[256];
    int     call_count;
    NextMove directive;
} BatchPollCapture;

static NextMove capture_handler(
    int64_t              batch_index,
    BatchReadingContext *ctx,
    void                *user_ctx)
{
    BatchPollCapture *c = (BatchPollCapture *)user_ctx;
    c->captured_batch_index = batch_index;
    c->captured_message_count = ctx->message_count;
    c->call_count++;

    if (batch_iterator_advance(&ctx->batch_iterator)) {
        int32_t len = batch_iterator_current_length(&ctx->batch_iterator);
        if (len > 0 && len < (int32_t)sizeof(c->captured_first_msg) - 1) {
            memcpy(c->captured_first_msg, batch_iterator_current_data(&ctx->batch_iterator), (size_t)len);
            c->captured_first_msg[len] = '\0';
        }
    }

    return c->directive;
}

static NextMove advance_all_handler(
    int64_t              batch_index,
    BatchReadingContext *ctx,
    void                *user_ctx)
{
    (void)batch_index;
    int *count = (int *)user_ctx;
    *count = 0;
    while (batch_iterator_advance(&ctx->batch_iterator)) {
        (*count)++;
    }
    return NEXT_ADVANCE;
}

/* ── Poller tests ────────────────────────────────────────────────────────── */

void test_batch_poll_end_of_stream(void)
{
    IndexedBatchPoller *p = indexed_batch_queue_create_poller(q);

    BatchPollCapture cap = { .directive = NEXT_ADVANCE };
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, pr);

    indexed_batch_poller_close(p);
}

void test_batch_poll_single_batch(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg0", 4);
    mutable_batch_append(b, "msg1", 4);
    mutable_batch_append(b, "msg2", 4);
    int64_t off = mutable_batch_complete(b);
    indexed_batch_appender_commit_batch(a, off);

    BatchPollCapture cap = { .directive = NEXT_ADVANCE };
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_INT(3, cap.captured_message_count);
    TEST_ASSERT_EQUAL_STRING("msg0", cap.captured_first_msg);
    TEST_ASSERT_EQUAL_INT(1, cap.call_count);

    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, pr);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poll_multiple_batches(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "b0m0", 4);
    mutable_batch_append(b, "b0m1", 4);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "b1m0", 4);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = NEXT_ADVANCE };

    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_INT(2, cap.captured_message_count);
    TEST_ASSERT_EQUAL_STRING("b0m0", cap.captured_first_msg);

    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(1, cap.captured_batch_index);
    TEST_ASSERT_EQUAL_INT(1, cap.captured_message_count);
    TEST_ASSERT_EQUAL_STRING("b1m0", cap.captured_first_msg);

    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, pr);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poll_advance_all_messages(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "m0", 2);
    mutable_batch_append(b, "m1", 2);
    mutable_batch_append(b, "m2", 2);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    int msg_count = 0;
    BatchPollResult pr = indexed_batch_poller_poll(p, advance_all_handler, &msg_count);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(3, msg_count);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poll_stop(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg", 3);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = NEXT_STOP };
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_STOPPED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poll_retreat(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "batch0", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "batch1", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = NEXT_ADVANCE };
    indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);

    /* Retreat from batch 1 — handler sees batch 1, cursor goes back to 0 */
    cap.directive = NEXT_RETREAT;
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_RETREATED, pr);
    TEST_ASSERT_EQUAL_INT(1, cap.captured_batch_index);

    /* Now poll batch 0 again */
    cap.directive = NEXT_ADVANCE;
    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.captured_batch_index);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poll_retreat_at_start(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg", 3);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = NEXT_RETREAT };
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_STOPPED, pr);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poller_exists(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    TEST_ASSERT_FALSE(indexed_batch_poller_exists(p, 0));

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "x", 1);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    TEST_ASSERT_TRUE(indexed_batch_poller_exists(p, 0));
    TEST_ASSERT_FALSE(indexed_batch_poller_exists(p, 1));

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_poller_move_to_last_existing(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    TEST_ASSERT_FALSE(indexed_batch_poller_move_to_last_existing_batch(p));

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "first", 5);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "second", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    TEST_ASSERT_TRUE(indexed_batch_poller_move_to_last_existing_batch(p));
    TEST_ASSERT_EQUAL_INT64(1, indexed_batch_poller_current_batch_index(p));

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

/* ── Reader tests (unchanged) ────────────────────────────────────────────── */

void test_reader_read_by_index(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchReader   *r = indexed_batch_queue_create_reader(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "first0", 6);
    mutable_batch_append(b, "second0", 7);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "first1", 6);
    mutable_batch_append(b, "second1", 7);
    mutable_batch_append(b, "third1", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchReadingContext ctx;

    indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_INT(2, ctx.message_count);

    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_INT(6, batch_iterator_current_length(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("first0", batch_iterator_current_data(&ctx.batch_iterator), 6);

    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("second0", batch_iterator_current_data(&ctx.batch_iterator), 7);

    TEST_ASSERT_FALSE(batch_iterator_advance(&ctx.batch_iterator));

    indexed_batch_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(3, ctx.message_count);

    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("first1", batch_iterator_current_data(&ctx.batch_iterator), 6);
    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("second1", batch_iterator_current_data(&ctx.batch_iterator), 7);
    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("third1", batch_iterator_current_data(&ctx.batch_iterator), 6);
    TEST_ASSERT_FALSE(batch_iterator_advance(&ctx.batch_iterator));

    indexed_batch_appender_close(a);
    indexed_batch_reader_close(r);
}

void test_reader_read_first_and_last(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchReader   *r = indexed_batch_queue_create_reader(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "only", 4);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "last_a", 6);
    mutable_batch_append(b, "last_b", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchReadingContext ctx;

    indexed_batch_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);

    indexed_batch_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(1, ctx.index);
    TEST_ASSERT_EQUAL_INT(2, ctx.message_count);

    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("last_a", batch_iterator_current_data(&ctx.batch_iterator), 6);

    indexed_batch_appender_close(a);
    indexed_batch_reader_close(r);
}

void test_reader_end_of_stream_when_empty(void)
{
    IndexedBatchReader *r = indexed_batch_queue_create_reader(q);

    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, indexed_batch_reader_last_index(r));

    BatchReadingContext ctx;
    indexed_batch_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, ctx.index);

    indexed_batch_reader_close(r);
}

void test_reader_has_entry(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchReader   *r = indexed_batch_queue_create_reader(q);

    TEST_ASSERT_FALSE(indexed_batch_reader_has_entry(r, 0));

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "x", 1);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    TEST_ASSERT_TRUE(indexed_batch_reader_has_entry(r, 0));
    TEST_ASSERT_FALSE(indexed_batch_reader_has_entry(r, 1));

    indexed_batch_appender_close(a);
    indexed_batch_reader_close(r);
}

void test_batch_append_rejects_zero_length(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    MutableBatch *b = indexed_batch_appender_start_batch(a);

    int32_t ok = mutable_batch_append(b, "valid", 5);
    TEST_ASSERT_EQUAL_INT(0, ok);

    int32_t bad = mutable_batch_append(b, "", 0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    int32_t next = mutable_batch_append(b, "also-ok", 7);
    TEST_ASSERT_EQUAL_INT(1, next);

    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);
}

void test_open_batch_as_normal_refused(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg", 3);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

    /* Attempt to create a normal participant on the same queue */
    IndexedQueue *nq = indexed_queue_create_default(DIR, QUEUE, rt, rt);
    IndexedAppender *na = indexed_queue_create_appender(nq);
    TEST_ASSERT_NULL(na);

    if (nq) indexed_queue_close(nq);
}

void test_batch_roundtrip_type_stamped(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "x", 1);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

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

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_batch_append_rejects_zero_length);
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
    return UNITY_END();
}
