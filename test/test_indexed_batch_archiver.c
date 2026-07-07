#include "unity.h"
#include "indexed_batch_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "queue_meta.h"
#include "runtime.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

static const char    *DIR      = "/tmp";
static const char    *QUEUE    = "mmap_test_batch_arch";
static const int32_t  REGION_SZ = 4096 * 16;
static const int64_t  MAX_FILE  = (int64_t)4096 * 16 * 64;
static const int      RING_SZ   = 4;
static const uint64_t TIMEOUT   = 5000000000ULL;

static Runtime *rt = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

static IndexedBatchAppender *create_archivable_batch_appender(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);

    QueueMeta *m = queue_meta_open_rw(DIR, QUEUE, 0,
        REGION_SZ, MAX_FILE, /* archivable= */ 1, QUEUE_TYPE_BATCH);
    TEST_ASSERT_NOT_NULL(m);
    queue_meta_close(m);

    return indexed_batch_appender_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT, 0, rt);
}

static NextMove advance_handler(int64_t bi, BatchReadingContext *ctx, void *user_ctx)
{
    (void)bi; (void)ctx; (void)user_ctx;
    return NEXT_ADVANCE;
}

void setUp(void)
{
    cleanup();
    rt = runtime_region_mapper();
}

void tearDown(void)
{
    cleanup();
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_batch_archiver_start_index_initially_zero(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "x", 1);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    TEST_ASSERT_EQUAL_INT64(0, indexed_batch_archiver_start_index(arch));
    TEST_ASSERT_EQUAL_INT(1, indexed_batch_archiver_archivable(arch));
    indexed_batch_archiver_close(arch);
}

void test_batch_archiver_archive_forward_updates_start_index(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    for (int i = 0; i < 5; i++) {
        MutableBatch *b = indexed_batch_appender_start_batch(a);
        char msg[16];
        snprintf(msg, sizeof(msg), "batch%d", i);
        mutable_batch_append(b, msg, (int32_t)strlen(msg));
        indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    }
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    indexed_batch_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, indexed_batch_archiver_start_index(arch));
    indexed_batch_archiver_close(arch);
}

void test_batch_archiver_poller_returns_archived_below_start(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    for (int i = 0; i < 5; i++) {
        MutableBatch *b = indexed_batch_appender_start_batch(a);
        mutable_batch_append(b, "x", 1);
        indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    }
    indexed_batch_appender_close(a);

    /* Create poller BEFORE archiving — cursor starts at 0 */
    IndexedBatchPoller *p = indexed_batch_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt);

    /* Verify batch 0 is readable before archive */
    BatchPollResult r = indexed_batch_poller_poll(p, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);

    /* Reset to 0 */
    indexed_batch_poller_move_to_index(p, 0);

    /* Archive batches 0..2 */
    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    indexed_batch_archiver_archive_up_to_index(arch, 3);
    indexed_batch_archiver_close(arch);

    /* Cursor at 0, start_index now 3 — returns POLL_ARCHIVED */
    r = indexed_batch_poller_poll(p, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(POLL_ARCHIVED, r);

    indexed_batch_poller_close(p);

    /* New poller starts at start_index=3 automatically */
    IndexedBatchPoller *p2 = indexed_batch_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt);
    r = indexed_batch_poller_poll(p2, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r); /* batch 3 */

    r = indexed_batch_poller_poll(p2, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r); /* batch 4 */

    r = indexed_batch_poller_poll(p2, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, r);

    indexed_batch_poller_close(p2);
}

void test_batch_archiver_start_offset_advances_after_archive(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    uint8_t appender_id = indexed_batch_appender_id(a);
    for (int i = 0; i < 3; i++) {
        MutableBatch *b = indexed_batch_appender_start_batch(a);
        mutable_batch_append(b, "msg", 3);
        indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    }
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    int64_t before = 0;
    int ok = indexed_batch_archiver_start_offset(arch, appender_id, &before);
    TEST_ASSERT_EQUAL_INT(1, ok);

    indexed_batch_archiver_archive_up_to_index(arch, 2);

    int64_t after = 0;
    ok = indexed_batch_archiver_start_offset(arch, appender_id, &after);
    TEST_ASSERT_EQUAL_INT(1, ok);
    TEST_ASSERT_TRUE(after > before);
    TEST_ASSERT_EQUAL_INT64(2, indexed_batch_archiver_start_index(arch));
    indexed_batch_archiver_close(arch);
}

void test_batch_archiver_archivable_files_none_at_start_zero(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "x", 1);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    char *paths[64];
    int n = indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    indexed_batch_archiver_close(arch);
}

void test_batch_archiver_backward_unarchive(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    for (int i = 0; i < 4; i++) {
        MutableBatch *b = indexed_batch_appender_start_batch(a);
        mutable_batch_append(b, "x", 1);
        indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    }
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    indexed_batch_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, indexed_batch_archiver_start_index(arch));

    indexed_batch_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT64(1, indexed_batch_archiver_start_index(arch));
    indexed_batch_archiver_close(arch);
}

void test_batch_archiver_noop_when_index_equals_start(void)
{
    IndexedBatchAppender *a = create_archivable_batch_appender();
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "x", 1);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    indexed_batch_archiver_archive_up_to_index(arch, 0); /* no-op */
    TEST_ASSERT_EQUAL_INT64(0, indexed_batch_archiver_start_index(arch));
    indexed_batch_archiver_close(arch);
}

void test_batch_archive_up_to_index_rejects_non_archivable(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);

    IndexedBatchAppender *a = indexed_batch_appender_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT, 0, rt);
    TEST_ASSERT_NOT_NULL(a);
    MutableBatch *b = indexed_batch_appender_start_batch(a);
    mutable_batch_append(b, "msg", 3);
    indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));
    indexed_batch_appender_close(a);

    IndexedBatchArchiver *arch = indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt);
    TEST_ASSERT_NOT_NULL(arch);
    int rc = indexed_batch_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc);
    indexed_batch_archiver_close(arch);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_batch_archiver_start_index_initially_zero);
    RUN_TEST(test_batch_archiver_archive_forward_updates_start_index);
    RUN_TEST(test_batch_archiver_poller_returns_archived_below_start);
    RUN_TEST(test_batch_archiver_start_offset_advances_after_archive);
    RUN_TEST(test_batch_archiver_archivable_files_none_at_start_zero);
    RUN_TEST(test_batch_archiver_backward_unarchive);
    RUN_TEST(test_batch_archiver_noop_when_index_equals_start);
    RUN_TEST(test_batch_archive_up_to_index_rejects_non_archivable);
    return UNITY_END();
}
