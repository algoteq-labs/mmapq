/*
 * START-INDEX-REOPEN-01 pins (0.2.1).
 *
 * (1) The start_index argument of mmapq_indexed_queue_create and the batch
 *     twin is "used ONLY if creating new" (indexed_queue.h). The appender
 *     join used the ARGUMENT as its header-search floor and, when the queue
 *     looked empty from there, as the next index. Reopening an existing
 *     queue with an argument above its last entry therefore committed past
 *     a run of empty header slots: the append returned success, pollers
 *     stopped at the first empty slot and last_index did not see it. The
 *     join now uses the stored start_index.
 * (2) The join walk's first wrap (walk_payload_to_end; skip_to_end_of_stream
 *     in the batch twin) reported a payload file it cannot read as
 *     MMAPQ_ECORRUPT; every other read site has reported MMAPQ_EIO since
 *     0.2.1's read-side change (doc/reference/error-codes.md). The file is
 *     made unreadable with a symlink loop (ELOOP), which bites for root too.
 *
 * Guards (pass before and after): a new queue starts at the caller's
 * start_index; a reopen below an archived start_index appends at the tail.
 */
#include "unity.h"
#include "mmapq.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SR_REGION   ((int32_t)32768)
#define SR_FILE     ((int64_t)32768 * 8)
#define SR_TIMEOUT  ((uint64_t)2000000000)
#define SR_FIRST    3              /* entries written by the first incarnation */
#define SR_REOPEN   ((int64_t)10)  /* start_index argument of the reopen */
#define SR_MAX_SEEN 64
#define SR_LANE_START ((int64_t)64) /* first payload offset of a new lane */

static char base_dir[64];

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_sr_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
}

void tearDown(void)
{
    char cmd[256];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", base_dir);
    if (system(cmd) != 0) { /* best effort */ }
}

/* ── helpers ─────────────────────────────────────────────────────────────── */

static int64_t seen[SR_MAX_SEEN];
static int     n_seen;

static MmapqNextMove record_single(int64_t index, uint8_t appender_id,
                                   const void *data, int32_t length, void *ctx)
{
    (void)appender_id; (void)data; (void)length; (void)ctx;
    if (n_seen < SR_MAX_SEEN) seen[n_seen] = index;
    n_seen++;
    return MMAPQ_NEXT_ADVANCE;
}

static MmapqNextMove record_batch(int64_t batch_index, MmapqBatchReadingContext *ctx,
                                  void *user)
{
    (void)ctx; (void)user;
    if (n_seen < SR_MAX_SEEN) seen[n_seen] = batch_index;
    n_seen++;
    return MMAPQ_NEXT_ADVANCE;
}

static MmapqIndexedQueue *open_single(int64_t start_index, int archivable)
{
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        base_dir, "q", SR_REGION, mmapq_min_ring_size(SR_REGION), 0, SR_FILE,
        start_index, SR_TIMEOUT, archivable, NULL, NULL, &q));
    return q;
}

static MmapqIndexedBatchQueue *open_batch(int64_t start_index)
{
    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        base_dir, "q", SR_REGION, mmapq_min_ring_size(SR_REGION), 0, SR_FILE,
        start_index, SR_TIMEOUT, 0, NULL, NULL, &q));
    return q;
}

/* Polls until the poll does not advance; returns the final outcome. */
static int drain_single(MmapqIndexedQueue *q)
{
    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
    n_seen = 0;
    int r;
    while ((r = mmapq_indexed_poller_poll(p, record_single, NULL)) == MMAPQ_POLL_ADVANCED)
        if (n_seen > SR_MAX_SEEN) break;
    mmapq_indexed_poller_close(p);
    return r;
}

static int drain_batch(MmapqIndexedBatchQueue *q)
{
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(q, &p));
    n_seen = 0;
    int r;
    while ((r = mmapq_indexed_batch_poller_poll(p, record_batch, NULL)) == MMAPQ_POLL_ADVANCED)
        if (n_seen > SR_MAX_SEEN) break;
    mmapq_indexed_batch_poller_close(p);
    return r;
}

static int64_t last_index_single(MmapqIndexedQueue *q)
{
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    int64_t li = mmapq_indexed_reader_last_index(r);
    mmapq_indexed_reader_close(r);
    return li;
}

static int64_t last_index_batch(MmapqIndexedBatchQueue *q)
{
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(q, &r));
    int64_t li = mmapq_indexed_batch_reader_last_index(r);
    mmapq_indexed_batch_reader_close(r);
    return li;
}

static void assert_seen_from(int64_t first, int count)
{
    TEST_ASSERT_EQUAL_INT(count, n_seen);
    for (int i = 0; i < count; i++)
        TEST_ASSERT_EQUAL_INT64(first + i, seen[i]);
}

/* ── (1) reopen with a larger start_index: single lane ──────────────────── */

static void test_reopen_with_higher_start_index_appends_at_tail(void)
{
    MmapqIndexedQueue *q = open_single(0, 0);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    for (int64_t i = 0; i < SR_FIRST; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "msg", 3));
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);

    q = open_single(SR_REOPEN, 0);
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(SR_FIRST, mmapq_indexed_appender_append(a, "msg", 3));
    mmapq_indexed_appender_close(a);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, drain_single(q));
    assert_seen_from(0, SR_FIRST + 1);
    TEST_ASSERT_EQUAL_INT64(SR_FIRST, last_index_single(q));
    mmapq_indexed_queue_close(q);
}

static void test_reopen_empty_queue_with_higher_start_index_starts_at_stored(void)
{
    MmapqIndexedQueue *q = open_single(0, 0);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));  /* writes the meta */
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);

    q = open_single(SR_REOPEN, 0);
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "msg", 3));
    mmapq_indexed_appender_close(a);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, drain_single(q));
    assert_seen_from(0, 1);
    TEST_ASSERT_EQUAL_INT64(0, last_index_single(q));
    mmapq_indexed_queue_close(q);
}

/* ── (1) batch lane ─────────────────────────────────────────────────────── */

static void test_batch_reopen_with_higher_start_index_appends_at_tail(void)
{
    MmapqIndexedBatchQueue *q = open_batch(0);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    for (int64_t i = 0; i < SR_FIRST; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_batch_appender_append_and_commit(a, "msg", 3));
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);

    q = open_batch(SR_REOPEN);
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(SR_FIRST, mmapq_indexed_batch_appender_append_and_commit(a, "msg", 3));
    mmapq_indexed_batch_appender_close(a);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, drain_batch(q));
    assert_seen_from(0, SR_FIRST + 1);
    TEST_ASSERT_EQUAL_INT64(SR_FIRST, last_index_batch(q));
    mmapq_indexed_batch_queue_close(q);
}

static void test_batch_reopen_empty_queue_with_higher_start_index_starts_at_stored(void)
{
    MmapqIndexedBatchQueue *q = open_batch(0);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);

    q = open_batch(SR_REOPEN);
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_append_and_commit(a, "msg", 3));
    mmapq_indexed_batch_appender_close(a);

    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, drain_batch(q));
    assert_seen_from(0, 1);
    TEST_ASSERT_EQUAL_INT64(0, last_index_batch(q));
    mmapq_indexed_batch_queue_close(q);
}

/* ── guards: unchanged directions ───────────────────────────────────────── */

static void test_new_queue_starts_at_caller_start_index(void)
{
    MmapqIndexedQueue *q = open_single(SR_REOPEN, 0);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(SR_REOPEN, mmapq_indexed_appender_append(a, "msg", 3));
    mmapq_indexed_appender_close(a);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, drain_single(q));
    assert_seen_from(SR_REOPEN, 1);
    mmapq_indexed_queue_close(q);
}

static void test_reopen_below_archived_start_index_appends_at_tail(void)
{
    MmapqIndexedQueue *q = open_single(0, 1);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    for (int64_t i = 0; i < 20; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "msg", 3));
    mmapq_indexed_appender_close(a);
    MmapqIndexedArchiver *ar = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_archiver(q, &ar));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, SR_REOPEN));
    TEST_ASSERT_EQUAL_INT64(SR_REOPEN, mmapq_indexed_archiver_start_index(ar));
    mmapq_indexed_archiver_close(ar);
    mmapq_indexed_queue_close(q);

    q = open_single(0, 1);
    a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_EQUAL_INT64(20, mmapq_indexed_appender_append(a, "msg", 3));
    mmapq_indexed_appender_close(a);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, drain_single(q));
    assert_seen_from(SR_REOPEN, 21 - (int)SR_REOPEN);
    mmapq_indexed_queue_close(q);
}

/* ── (2) unreadable payload file at the join's first wrap ───────────────── */

static void make_unreadable(unsigned lane)
{
    char path[256];
    snprintf(path, sizeof path, "%s/q/payload_%u_0", base_dir, lane);
    TEST_ASSERT_EQUAL_INT(0, unlink(path));
    TEST_ASSERT_EQUAL_INT(0, symlink(path, path));   /* stat/open: ELOOP */
}

static void test_join_over_unreadable_payload_file_is_eio(void)
{
    MmapqIndexedQueue *q = open_single(0, 0);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(q, 7, &a));
    /* an uncommitted record only: the lane has no header, so the rejoin
     * walks from the join anchor (walk_payload_to_end's first wrap) */
    TEST_ASSERT_EQUAL_INT64(SR_LANE_START, mmapq_indexed_appender_append_uncommitted(a, "msg", 3));
    mmapq_indexed_appender_close(a);
    make_unreadable(7);

    a = NULL;
    int rc = mmapq_indexed_queue_create_appender_with_id(q, 7, &a);
    if (a != NULL) mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, rc);
}

static void test_batch_join_over_unreadable_payload_file_is_eio(void)
{
    MmapqIndexedBatchQueue *q = open_batch(0);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(q, 7, &a));
    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "msg", 3));
    TEST_ASSERT_EQUAL_INT64(SR_LANE_START, mmapq_mutable_batch_complete_uncommitted(b));
    mmapq_indexed_batch_appender_close(a);
    make_unreadable(7);

    a = NULL;
    int rc = mmapq_indexed_batch_queue_create_appender_with_id(q, 7, &a);
    if (a != NULL) mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, rc);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_reopen_with_higher_start_index_appends_at_tail);
    RUN_TEST(test_reopen_empty_queue_with_higher_start_index_starts_at_stored);
    RUN_TEST(test_batch_reopen_with_higher_start_index_appends_at_tail);
    RUN_TEST(test_batch_reopen_empty_queue_with_higher_start_index_starts_at_stored);
    RUN_TEST(test_new_queue_starts_at_caller_start_index);
    RUN_TEST(test_reopen_below_archived_start_index_appends_at_tail);
    RUN_TEST(test_join_over_unreadable_payload_file_is_eio);
    RUN_TEST(test_batch_join_over_unreadable_payload_file_is_eio);
    return UNITY_END();
}
