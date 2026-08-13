#include "unity.h"
#include "indexed_batch_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "internal/queue_meta.h"
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

static MmapqRuntime *rt = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

static MmapqIndexedBatchAppender *create_archivable_batch_appender(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);

    QueueMeta *m = NULL;
    int qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        REGION_SZ, MAX_FILE, /* archivable= */ 1, QUEUE_TYPE_BATCH, &m);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedBatchAppender *a = NULL;
    int rc = mmapq_indexed_batch_appender_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    if (rc != 0) return NULL;
    return a;
}

static MmapqNextMove advance_handler(int64_t bi, MmapqBatchReadingContext *ctx, void *user_ctx)
{
    (void)bi; (void)ctx; (void)user_ctx;
    return MMAPQ_NEXT_ADVANCE;
}

void setUp(void)
{
    cleanup();
    int rc_rt = mmapq_runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
}

void tearDown(void)
{
    cleanup();
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_batch_archiver_start_index_initially_zero(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "x", 1);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_archiver_start_index(arch));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_batch_archiver_archivable(arch));
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archiver_archive_forward_updates_start_index(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    for (int i = 0; i < 5; i++) {
        MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
        char msg[32];
        snprintf(msg, sizeof(msg), "batch%d", i);
        mmapq_mutable_batch_append(b, msg, (int32_t)strlen(msg));
        mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    }
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_batch_archiver_start_index(arch));
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archiver_poller_returns_archived_below_start(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    for (int i = 0; i < 5; i++) {
        MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
        mmapq_mutable_batch_append(b, "x", 1);
        mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    }
    mmapq_indexed_batch_appender_close(a);

    /* Create poller BEFORE archiving — cursor starts at 0 */
    MmapqIndexedBatchPoller *p = NULL;
    int rc_p = mmapq_indexed_batch_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Verify batch 0 is readable before archive */
    BatchPollResult r = mmapq_indexed_batch_poller_poll(p, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);

    /* Reset to 0 */
    mmapq_indexed_batch_poller_move_to_index(p, 0);

    /* Archive batches 0..2 */
    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3);
    mmapq_indexed_batch_archiver_close(arch);

    /* Cursor at 0, start_index now 3 — returns MMAPQ_POLL_ARCHIVED */
    r = mmapq_indexed_batch_poller_poll(p, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, r);

    mmapq_indexed_batch_poller_close(p);

    /* New poller starts at start_index=3 automatically */
    MmapqIndexedBatchPoller *p2 = NULL;
    int rc_p2 = mmapq_indexed_batch_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &p2);
    TEST_ASSERT_EQUAL_INT(0, rc_p2);
    TEST_ASSERT_NOT_NULL(p2);
    r = mmapq_indexed_batch_poller_poll(p2, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r); /* batch 3 */

    r = mmapq_indexed_batch_poller_poll(p2, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r); /* batch 4 */

    r = mmapq_indexed_batch_poller_poll(p2, advance_handler, NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r);

    mmapq_indexed_batch_poller_close(p2);
}

void test_batch_archiver_start_offset_advances_after_archive(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    uint8_t appender_id = mmapq_indexed_batch_appender_id(a);
    for (int i = 0; i < 3; i++) {
        MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
        mmapq_mutable_batch_append(b, "msg", 3);
        mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    }
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    int64_t before = 0;
    int ok = mmapq_indexed_batch_archiver_start_offset(arch, appender_id, &before);
    TEST_ASSERT_EQUAL_INT(1, ok);

    mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2);

    int64_t after = 0;
    ok = mmapq_indexed_batch_archiver_start_offset(arch, appender_id, &after);
    TEST_ASSERT_EQUAL_INT(1, ok);
    TEST_ASSERT_TRUE(after > before);
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_batch_archiver_start_index(arch));
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archiver_archivable_files_none_at_start_zero(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "x", 1);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    char *paths[64];
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archiver_backward_unarchive(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    for (int i = 0; i < 4; i++) {
        MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
        mmapq_mutable_batch_append(b, "x", 1);
        mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    }
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_batch_archiver_start_index(arch));

    mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_batch_archiver_start_index(arch));
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archiver_noop_when_index_equals_start(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "x", 1);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_batch_archiver_archive_up_to_index(arch, 0); /* no-op */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_archiver_start_index(arch));
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archive_up_to_index_rejects_non_archivable(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);

    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_appender_create(DIR, QUEUE,
        REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    mmapq_mutable_batch_append(b, "msg", 3);
    mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc_ba = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_NOT_NULL(arch);
    int rc = mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc);
    mmapq_indexed_batch_archiver_close(arch);
}

void test_batch_archive_forward_empty_queue(void)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    int rc = mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(arch);
    int rc2 = mmapq_indexed_batch_archiver_archive_up_to_index(arch, 5);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc2);
    mmapq_indexed_batch_archiver_close(arch);
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
    RUN_TEST(test_batch_archive_forward_empty_queue);
    return UNITY_END();
}
