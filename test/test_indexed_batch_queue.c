#include "unity.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_reader.h"
#include "runtime.h"
#include "header_codec.h"
#include "header_reader.h"

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
             rt, rt);
}

void tearDown(void)
{
    if (q) { indexed_batch_queue_close(q); q = NULL; }
    cleanup();
}

/* ── Capture helpers ─────────────────────────────────────────────────────── */

typedef struct {
    char    data[256];
    int32_t length;
    int32_t message_index;
    int32_t message_count;
    int     call_count;
    MessageHandlerDirective directive;
} BatchPollCapture;

static MessageHandlerDirective capture_handler(
    int64_t batch_index, int32_t message_index, int32_t message_count,
    const void *data, int32_t length, void *ctx)
{
    (void)batch_index;
    BatchPollCapture *c = (BatchPollCapture *)ctx;
    c->message_index = message_index;
    c->message_count = message_count;
    c->length        = length;
    c->call_count++;
    if (length > 0 && length < (int32_t)sizeof(c->data) - 1) {
        memcpy(c->data, data, (size_t)length);
        c->data[length] = '\0';
    }
    return c->directive;
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_batch_append_and_poll_three_messages(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg0", 4);
    mutable_batch_append(b, "msg1", 4);
    mutable_batch_append(b, "msg2", 4);
    int64_t off = mutable_batch_complete(b);
    indexed_batch_appender_commit_batch(a, off);

    BatchPollCapture cap = { .directive = HANDLER_ADVANCE };

    /* msg0 */
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_ADVANCED_MESSAGE, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.message_index);
    TEST_ASSERT_EQUAL_INT(3, cap.message_count);
    TEST_ASSERT_EQUAL_STRING("msg0", cap.data);

    /* msg1 */
    cap.call_count = 0;
    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_ADVANCED_MESSAGE, pr);
    TEST_ASSERT_EQUAL_INT(1, cap.message_index);
    TEST_ASSERT_EQUAL_STRING("msg1", cap.data);

    /* msg2 — last in batch, returns ADVANCED_BATCH */
    cap.call_count = 0;
    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_ADVANCED_BATCH, pr);
    TEST_ASSERT_EQUAL_INT(2, cap.message_index);
    TEST_ASSERT_EQUAL_STRING("msg2", cap.data);

    /* end of stream */
    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_END_OF_STREAM, pr);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_batch_multiple_batches_polled_in_order(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchPoller   *p = indexed_batch_queue_create_poller(q);

    /* batch 0: 2 messages */
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "b0m0", 4);
    mutable_batch_append(b, "b0m1", 4);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    /* batch 1: 1 message */
    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "b1m0", 4);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchPollCapture cap = { .directive = HANDLER_ADVANCE };

    /* batch 0, msg 0 */
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_ADVANCED_MESSAGE, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.message_index);
    TEST_ASSERT_EQUAL_INT(2, cap.message_count);
    TEST_ASSERT_EQUAL_STRING("b0m0", cap.data);

    /* batch 0, msg 1 — last, advances batch */
    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_ADVANCED_BATCH, pr);
    TEST_ASSERT_EQUAL_STRING("b0m1", cap.data);

    /* batch 1, msg 0 — only message, advances batch */
    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_ADVANCED_BATCH, pr);
    TEST_ASSERT_EQUAL_INT(0, cap.message_index);
    TEST_ASSERT_EQUAL_INT(1, cap.message_count);
    TEST_ASSERT_EQUAL_STRING("b1m0", cap.data);

    pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_BATCH_END_OF_STREAM, pr);

    indexed_batch_appender_close(a);
    indexed_batch_poller_close(p);
}

void test_reader_read_by_index(void)
{
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    IndexedBatchReader   *r = indexed_batch_queue_create_reader(q);

    /* batch 0 */
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "first0", 6);
    mutable_batch_append(b, "second0", 7);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    /* batch 1 */
    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "first1", 6);
    mutable_batch_append(b, "second1", 7);
    mutable_batch_append(b, "third1", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchReadingContext ctx;

    /* read batch 0 */
    indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_INT(2, ctx.message_count);

    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_INT(6, batch_iterator_current_length(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("first0", batch_iterator_current_data(&ctx.batch_iterator), 6);

    TEST_ASSERT_TRUE(batch_iterator_advance(&ctx.batch_iterator));
    TEST_ASSERT_EQUAL_MEMORY("second0", batch_iterator_current_data(&ctx.batch_iterator), 7);

    TEST_ASSERT_FALSE(batch_iterator_advance(&ctx.batch_iterator));

    /* read batch 1 */
    indexed_batch_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
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

    /* batch 0: 1 message */
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "only", 4);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    /* batch 1: 2 messages */
    b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "last_a", 6);
    mutable_batch_append(b, "last_b", 6);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

    BatchReadingContext ctx;

    indexed_batch_reader_read_first(r, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT64(0, ctx.index);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);

    indexed_batch_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(ENTRY_AVAILABLE, ctx.status);
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
    TEST_ASSERT_EQUAL_INT(ENTRY_END_OF_STREAM, ctx.status);
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

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_batch_append_and_poll_three_messages);
    RUN_TEST(test_batch_multiple_batches_polled_in_order);
    RUN_TEST(test_reader_read_by_index);
    RUN_TEST(test_reader_read_first_and_last);
    RUN_TEST(test_reader_end_of_stream_when_empty);
    RUN_TEST(test_reader_has_entry);
    return UNITY_END();
}
