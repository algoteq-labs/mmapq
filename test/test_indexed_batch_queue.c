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

static Runtime           *rt = NULL;
static IndexedBatchQueue *q  = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

void setUp(void)
{
    cleanup();
    int rc_rt = runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
    int rc_q  = indexed_batch_queue_create(DIR, QUEUE,
              REGION_SZ, RING_SZ, 1,
              MAX_FILE, 0, TIMEOUT,
              0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
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

    if (batch_iterator_advance(ctx->iterator)) {
        int32_t len = batch_iterator_current_length(ctx->iterator);
        if (len > 0 && len < (int32_t)sizeof(c->captured_first_msg) - 1) {
            memcpy(c->captured_first_msg, batch_iterator_current_data(ctx->iterator), (size_t)len);
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
    while (batch_iterator_advance(ctx->iterator)) {
        (*count)++;
    }
    return NEXT_ADVANCE;
}

/* ── Poller tests ────────────────────────────────────────────────────────── */

void test_batch_poll_end_of_stream(void)
{
    IndexedBatchPoller *p = NULL;
    int rc_p = indexed_batch_queue_create_poller(q, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    BatchPollCapture cap = { .directive = NEXT_ADVANCE };
    BatchPollResult pr = indexed_batch_poller_poll(p, capture_handler, &cap);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, pr);

    indexed_batch_poller_close(p);
}

void test_batch_poll_single_batch(void)
{
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchReader *r = NULL;

    int rc_r = indexed_batch_queue_create_reader(q, &r);

    TEST_ASSERT_EQUAL_INT(0, rc_r);

    TEST_ASSERT_NOT_NULL(r);

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

    TEST_ASSERT_TRUE(batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_INT(6, batch_iterator_current_length(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("first0", batch_iterator_current_data(ctx.iterator), 6);

    TEST_ASSERT_TRUE(batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("second0", batch_iterator_current_data(ctx.iterator), 7);

    TEST_ASSERT_FALSE(batch_iterator_advance(ctx.iterator));

    indexed_batch_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(3, ctx.message_count);

    TEST_ASSERT_TRUE(batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("first1", batch_iterator_current_data(ctx.iterator), 6);
    TEST_ASSERT_TRUE(batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("second1", batch_iterator_current_data(ctx.iterator), 7);
    TEST_ASSERT_TRUE(batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("third1", batch_iterator_current_data(ctx.iterator), 6);
    TEST_ASSERT_FALSE(batch_iterator_advance(ctx.iterator));

    indexed_batch_appender_close(a);
    indexed_batch_reader_close(r);
}

void test_reader_read_first_and_last(void)
{
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchReader *r = NULL;

    int rc_r = indexed_batch_queue_create_reader(q, &r);

    TEST_ASSERT_EQUAL_INT(0, rc_r);

    TEST_ASSERT_NOT_NULL(r);

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

    TEST_ASSERT_TRUE(batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("last_a", batch_iterator_current_data(ctx.iterator), 6);

    indexed_batch_appender_close(a);
    indexed_batch_reader_close(r);
}

void test_reader_end_of_stream_when_empty(void)
{
    IndexedBatchReader *r = NULL;
    int rc_r = indexed_batch_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);

    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, indexed_batch_reader_last_index(r));

    BatchReadingContext ctx;
    indexed_batch_reader_read_last(r, &ctx);
    TEST_ASSERT_EQUAL_INT(BATCH_ENTRY_END_OF_STREAM, ctx.status);
    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, ctx.index);

    indexed_batch_reader_close(r);
}

void test_reader_has_entry(void)
{
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    IndexedBatchReader *r = NULL;

    int rc_r = indexed_batch_queue_create_reader(q, &r);

    TEST_ASSERT_EQUAL_INT(0, rc_r);

    TEST_ASSERT_NOT_NULL(r);

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
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

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

void test_batch_append_rejects_oversized_message(void)
{
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);

    int32_t ok = mutable_batch_append(b, "small", 5);
    TEST_ASSERT_EQUAL_INT(0, ok);

    /* REGION_SZ = 65536, message of 65536 bytes exceeds region_size - 4 */
    char big[65536];
    memset(big, 'x', sizeof(big));
    int32_t bad = mutable_batch_append(b, big, (int32_t)sizeof(big));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, bad);

    /* Batch is partially populated — must discard, not commit */
    indexed_batch_appender_discard_batch(a, b->batch_offset);
    indexed_batch_appender_close(a);
}

void test_open_batch_as_normal_refused(void)
{
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg", 3);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

    /* Attempt to create a normal participant on the same queue */
    IndexedQueue *nq = NULL;
    int rc_nq = indexed_queue_create_default(DIR, QUEUE, rt, rt, &nq);
    TEST_ASSERT_EQUAL_INT(0, rc_nq);
    TEST_ASSERT_NOT_NULL(nq);

    IndexedAppender *na = NULL;
    int rc_na = indexed_queue_create_appender(nq, &na);
    TEST_ASSERT_TRUE(rc_na != 0);
    TEST_ASSERT(rc_na != MMAPQ_ETIMEDOUT);
    TEST_ASSERT_NULL(na);

    if (nq) indexed_queue_close(nq);
}

void test_batch_roundtrip_type_stamped(void)
{
    IndexedBatchAppender *a = NULL;
    int rc_a = indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);

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

/* ── Batch corrupt payload ────────────────────────────────────────────────── */

static int batch_corrupt_handler_count;
static NextMove batch_corrupt_handler(int64_t bi, BatchReadingContext *ctx, void *user)
{
    (void)bi; (void)ctx; (void)user;
    batch_corrupt_handler_count++;
    return NEXT_ADVANCE;
}

static void test_batch_poll_detects_corrupt_payload(void)
{
    char qname[64];
    snprintf(qname, sizeof(qname), "%s_corrupt", QUEUE);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, qname);
    int rc = system(cmd); (void)rc;

    IndexedBatchQueue *q = NULL;
    int rc_q = indexed_batch_queue_create(
        DIR, qname, REGION_SZ, RING_SZ, 1, MAX_FILE, 0, TIMEOUT, 0, NULL, NULL, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);

    IndexedBatchAppender *a = NULL;

    int rc_a = indexed_batch_queue_create_appender(q, &a);

    TEST_ASSERT_EQUAL_INT(0, rc_a);

    TEST_ASSERT_NOT_NULL(a);

    TEST_ASSERT_NOT_NULL(a);
    MutableBatch *mb = indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(mb);
    int32_t r = mutable_batch_append(mb, "hello", 5);
    TEST_ASSERT(r >= 0);
    int64_t off = mutable_batch_complete(mb);
    TEST_ASSERT(off >= 0);
    int64_t idx = indexed_batch_appender_commit_batch(a, off);
    TEST_ASSERT(idx >= 0);
    indexed_batch_appender_close(a);

    /* Find the payload file for appender 0 */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_0_0", DIR, qname);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_MESSAGE(fd >= 0, "failed to open batch payload file");
    /* Zero the 4-byte message count at offset 0 (first batch starts at 0) */
    uint32_t zero = 0;
    ssize_t written = pwrite(fd, &zero, sizeof(zero), off); (void)written;
    close(fd);

    IndexedBatchPoller *p = NULL;

    int rc_p = indexed_batch_queue_create_poller(q, &p);

    TEST_ASSERT_EQUAL_INT(0, rc_p);

    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_NOT_NULL(p);
    batch_corrupt_handler_count = 0;
    PollResult pr = indexed_batch_poller_poll(p, batch_corrupt_handler, NULL);
    TEST_ASSERT_EQUAL(POLL_CORRUPT, pr);
    TEST_ASSERT_EQUAL(0, batch_corrupt_handler_count);
    indexed_batch_poller_close(p);

    indexed_batch_queue_close(q);
    cleanup();
}

/* ── Stale appender catch-up ─────────────────────────────────────────────── */

static void test_batch_stale_appender_catch_up(void)
{
    /*
     * Regression: batch appender variant of the stale-appender livelock fix.
     * Fill many batches with one appender, then append with a second (stale)
     * appender and verify the returned index is correct.
     */
    IndexedBatchAppender *a1 = NULL;
    int rc_a1 = indexed_batch_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);

    IndexedBatchAppender *a2 = NULL;
    int rc_a2 = indexed_batch_queue_create_appender(q, &a2);
    TEST_ASSERT_EQUAL_INT(0, rc_a2);
    TEST_ASSERT_NOT_NULL(a2);

    TEST_ASSERT_NOT_NULL(a1);
    TEST_ASSERT_NOT_NULL(a2);

    int64_t batch_count = 50000;
    const char *payload = "x";
    int32_t     plen    = 1;

    for (int64_t i = 0; i < batch_count; i++) {
        int64_t idx = indexed_batch_appender_append_and_commit(a1, payload, plen);
        TEST_ASSERT_EQUAL_INT64(i, idx);
    }

    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    int64_t idx = indexed_batch_appender_append_and_commit(a2, payload, plen);
    clock_gettime(CLOCK_MONOTONIC, &ts1);

    TEST_ASSERT_EQUAL_INT64(batch_count, idx);

    uint64_t dt_us = (uint64_t)((ts1.tv_sec - ts0.tv_sec) * 1000000LL
                              + (ts1.tv_nsec - ts0.tv_nsec) / 1000LL);
    (void)dt_us;

    indexed_batch_appender_close(a1);
    indexed_batch_appender_close(a2);
}

int main(void)
{
    UNITY_BEGIN();
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
    RUN_TEST(test_batch_stale_appender_catch_up);
    return UNITY_END();
}
