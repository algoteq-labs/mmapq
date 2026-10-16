#include "unity.h"
#include "indexed_batch_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/queue_meta.h"
#include "internal/header_codec.h"
#include "internal/test_hooks.h"
#include "runtime.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>

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

static void check_batch_archive_rejects_partial_scan(int undo)
{
    MmapqIndexedBatchAppender *a = create_archivable_batch_appender();
    TEST_ASSERT_NOT_NULL(a);
    uint8_t aid = mmapq_indexed_batch_appender_id(a);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, "first", 5));
    TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(b, "second", 6));
    int64_t batch_offset = mmapq_mutable_batch_complete(b);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, batch_offset);
    TEST_ASSERT_EQUAL_INT64(0,
        mmapq_indexed_batch_appender_commit_batch(a, batch_offset));
    TEST_ASSERT_EQUAL_INT64(1,
        mmapq_indexed_batch_appender_append_and_commit(a, "tail", 4));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    if (undo) {
        /* Establish a valid archive boundary before corrupting the batch
         * that the undo operation must validate. */
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    }
    int64_t before_index = mmapq_indexed_batch_archiver_start_index(arch);
    TEST_ASSERT_EQUAL_INT64(undo ? 1 : 0, before_index);
    int64_t before_offset;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_start_offset(arch, aid, &before_offset));
    mmapq_indexed_batch_archiver_close(arch);

    /* Keep the count and first message valid. The iterator must make real
     * progress, then fail on the second message instead of reaching the end. */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/payload_%u_0", DIR, QUEUE, (unsigned)aid);
    FILE *f = fopen(path, "r+b");
    TEST_ASSERT_NOT_NULL(f);
    int64_t length_offset = batch_offset + MMAPQ_BATCH_MSG_COUNT_SIZE
                          + PAYLOAD_LENGTH_SIZE + 5;
    TEST_ASSERT_EQUAL_INT(0, fseek(f, (long)length_offset, SEEK_SET));
    int32_t corrupt_length = 1073741824;
    TEST_ASSERT_EQUAL_INT64(sizeof(corrupt_length),
        fwrite(&corrupt_length, 1, sizeof(corrupt_length), f));
    TEST_ASSERT_EQUAL_INT(0, fclose(f));

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    int rc = mmapq_indexed_batch_archiver_archive_up_to_index(arch, undo ? 0 : 1);
    int64_t after_index = mmapq_indexed_batch_archiver_start_index(arch);
    int64_t after_offset;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_start_offset(arch, aid, &after_offset));
    mmapq_indexed_batch_archiver_close(arch);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_EQUAL_INT64(before_index, after_index);
    TEST_ASSERT_EQUAL_INT64(before_offset, after_offset);
}

void test_batch_archiver_forward_rejects_partial_scan(void)
{
    check_batch_archive_rejects_partial_scan(0);
}

void test_batch_archiver_undo_rejects_partial_scan(void)
{
    check_batch_archive_rejects_partial_scan(1);
}

/* ── CAND-30 pins (batch twins: P3/P4/P6/P12 equivalents) ────────────────── */
/* Geometry: 4 KiB regions, 4 KiB files, archivable. */

static MmapqIndexedBatchAppender *tiny_archivable_batch_appender(uint8_t id)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_BATCH, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create_with_id(
        DIR, QUEUE, 4096, 4, 4096, TIMEOUT, 0, rt, id, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedBatchArchiver *tiny_batch_archiver(void)
{
    MmapqIndexedBatchArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, QUEUE, 4, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    return arch;
}

static int64_t batch_arch_start_offset(MmapqIndexedBatchArchiver *arch, uint8_t aid)
{
    int64_t off = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_start_offset(arch, aid, &off));
    return off;
}

static int64_t commit_one_msg_batch(MmapqIndexedBatchAppender *a,
                                    const void *data, int32_t len)
{
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, data, len));
    return mmapq_indexed_batch_appender_commit_batch(
        a, mmapq_mutable_batch_complete(b));
}

/* P10/P3: undo lowers the boundary to the lowest restored batch position and
 * never raises it. (In-order batches: min == first — this is the guard twin;
 * the discriminating pin is the plain queue's P3.) */
void test_batch_archiver_undo_never_raises_start_offset(void)
{
    char msg[800]; memset(msg, 'b', sizeof(msg));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a, msg, (int32_t)sizeof(msg)));
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a, msg, (int32_t)sizeof(msg)));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2));
    int64_t after_forward = batch_arch_start_offset(arch, 0);
    TEST_ASSERT_GREATER_THAN_INT64(64, after_forward);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 0));
    int64_t after_undo = batch_arch_start_offset(arch, 0);
    TEST_ASSERT_LESS_OR_EQUAL_INT64(after_forward, after_undo);
    TEST_ASSERT_LESS_OR_EQUAL_INT64(64, after_undo);
    mmapq_indexed_batch_archiver_close(arch);
}

/* P10/P4: joining batch appender with no live entry must walk forward from
 * start_offset, not write at it (bytes there may be a dead batch). a0
 * abandons a completed-but-uncommitted batch; a1 commits idx 0. */
void test_batch_appender_rejoin_over_abandoned_batch(void)
{
    char payload_a[3000]; memset(payload_a, 'A', sizeof(payload_a));

    MmapqIndexedBatchAppender *a0 = tiny_archivable_batch_appender(0);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a0);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, payload_a, (int32_t)sizeof(payload_a)));
    int64_t off_a = mmapq_mutable_batch_complete(b);
    TEST_ASSERT_GREATER_THAN_INT64(0, off_a);
    /* abandoned — never committed */
    mmapq_indexed_batch_appender_close(a0);

    MmapqIndexedBatchAppender *a1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a1, "B-live", 6));
    mmapq_indexed_batch_appender_close(a1);

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    mmapq_indexed_batch_archiver_close(arch);

    MmapqIndexedBatchAppender *r1 = tiny_archivable_batch_appender(0);
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(r1, "Zshort", 6));
    TEST_ASSERT_EQUAL_INT64(2, commit_one_msg_batch(r1, "W", 1));
    mmapq_indexed_batch_appender_close(r1);

    /* Second rejoin must recover cleanly */
    MmapqIndexedBatchAppender *r2 = tiny_archivable_batch_appender(0);
    mmapq_indexed_batch_appender_close(r2);

    /* Batches 1 and 2 read back intact */
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_poller_create(DIR, QUEUE, 4, TIMEOUT, rt, &p));
    MmapqBatchReadingContext ctx = {0};
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED,
        mmapq_indexed_batch_poller_poll(p, advance_handler, NULL));   /* idx 0 */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED,
        mmapq_indexed_batch_poller_poll(p, advance_handler, NULL));   /* idx 1 */
    (void)ctx;
    mmapq_indexed_batch_poller_close(p);
}

/* P10/P6 (guard): in-order batches — the header-based boundary equals the old
 * computation exactly. The 11th batch's offset IS the end of batch idx 9. */
void test_batch_archiver_in_order_boundary_unchanged(void)
{
    char msg[800]; memset(msg, 'm', sizeof(msg));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    int64_t off_10 = -1;
    for (int i = 0; i < 11; i++) {
        MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
        TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, msg, (int32_t)sizeof(msg)));
        int64_t off = mmapq_mutable_batch_complete(b);
        if (i == 10) { off_10 = off; continue; }
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_batch_appender_commit_batch(a, off));
    }
    mmapq_indexed_batch_appender_close(a);
    TEST_ASSERT_GREATER_THAN_INT64(0, off_10);

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 10));
    TEST_ASSERT_EQUAL_INT64(off_10, batch_arch_start_offset(arch, 0));
    mmapq_indexed_batch_archiver_close(arch);
}

/* P10/P12: the batch join's backward header scan must be bounded at
 * start_index — an archived header may point into a deleted payload file.
 * a0's single batch spans into file 1 (two 3000 B messages), so archiving it
 * lists payload_0_0; the file is deleted; a1 stays live. Pre-fix: the scan
 * finds the archived batch header, the walk stops early at the deleted file,
 * and the rejoined appender writes into file 0 (below the boundary). */
void test_batch_appender_rejoin_after_payload_file_deleted(void)
{
    char big[3000]; memset(big, 'Z', sizeof(big));

    MmapqIndexedBatchAppender *a0 = tiny_archivable_batch_appender(0);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a0);
    /* append returns the message's index within the batch */
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(b, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_commit_batch(
        a0, mmapq_mutable_batch_complete(b)));
    mmapq_indexed_batch_appender_close(a0);

    MmapqIndexedBatchAppender *a1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a1, "live", 4));
    mmapq_indexed_batch_appender_close(a1);

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(4096, batch_arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    int found = 0;
    for (int i = 0; i < n; i++) {
        if (strstr(paths[i], "payload_0_0")) found = 1;
        unlink(paths[i]);
        free(paths[i]);
    }
    TEST_ASSERT_TRUE(found);
    mmapq_indexed_batch_archiver_close(arch);

    /* Rejoin a0: its only batch is archived and its first payload file is
     * gone. The bounded scan finds no live entry; the join skips from
     * start_offset to the end of the stream. */
    MmapqIndexedBatchAppender *r1 = tiny_archivable_batch_appender(0);
    TEST_ASSERT_EQUAL_INT64(2, commit_one_msg_batch(r1, "back", 4));
    mmapq_indexed_batch_appender_close(r1);

    /* The new batch must land at or past the boundary (file 1), not in the
     * reclaimed file 0 — verify via the boundary after archiving it */
    MmapqIndexedBatchArchiver *arch2 = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch2, 3));
    TEST_ASSERT_GREATER_THAN_INT64(4096, batch_arch_start_offset(arch2, 0));
    mmapq_indexed_batch_archiver_close(arch2);
}

/* ── CAND-36 pins (batch twins of P1–P6): reclaim_files under archive.lock SH ── */
/* Batch geometry: a one-message batch is a 4-byte count field followed by
 * the message under the appender's padding rule at region == file == 4096.
 * batch_record_end(64, 4000) = 4072; batch_record_end(4072, 100) = 4200
 * (rolls to file 1) — mirrors the single-message 4068/4200 (B2). */

static int64_t batch_record_end(int64_t pos, int32_t len)
{
    int64_t p = pos + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int64_t cap = 4096 - (p % 4096);
    if (cap < (int64_t)(PAYLOAD_LENGTH_SIZE + len)) p += cap;
    return p + PAYLOAD_LENGTH_SIZE + len;
}

static int batch_queue_file_present(const char *name)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/%s", DIR, QUEUE, name);
    return access(path, F_OK) == 0;
}

static char *batch_mark_of(char **paths, int n, const char *suffix)
{
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], suffix)) return strdup(paths[i]);
    return NULL;
}

static int batch_list_contains(char **paths, int n, const char *suffix)
{
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], suffix)) return 1;
    return 0;
}

static void batch_free_paths(char **paths, int n)
{
    for (int i = 0; i < n; i++) free(paths[i]);
}

/* Single-lane: batch A one 4000-byte msg (idx 0, @64, ends 4072), batch B
 * one 100-byte msg (idx 1, rolls, ends 4200). archive(2) -> start_offset
 * 4200 -> payload_0_0 listed. */
static void batch_single_lane_geometry(void)
{
    char big[4000]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a, sm, (int32_t)sizeof(sm)));
    mmapq_indexed_batch_appender_close(a);
    TEST_ASSERT_EQUAL_INT64(4072, batch_record_end(64, 4000));
    TEST_ASSERT_EQUAL_INT64(4200, batch_record_end(4072, 100));
}

/* B2 batch geometry: two appenders, a0:4000, a1:4000, a0:100, a1:100
 * (indices 0-3). archive(4) -> 4200/4200, payload_0_0 and payload_1_0
 * listed. */
static void batch_two_lane_geometry(void)
{
    char big[4000]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));
    MmapqIndexedBatchAppender *a0 = tiny_archivable_batch_appender(0);
    MmapqIndexedBatchAppender *a1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a0, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a1, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(2, commit_one_msg_batch(a0, sm, (int32_t)sizeof(sm)));
    TEST_ASSERT_EQUAL_INT64(3, commit_one_msg_batch(a1, sm, (int32_t)sizeof(sm)));
    mmapq_indexed_batch_appender_close(a0);
    mmapq_indexed_batch_appender_close(a1);
    TEST_ASSERT_EQUAL_INT64(4072, batch_record_end(64, 4000));
    TEST_ASSERT_EQUAL_INT64(4200, batch_record_end(4072, 100));
}

static int b36_wait(_Atomic int *flag)
{
    for (int ms = 0; ms < 5000; ms++) {
        if (atomic_load_explicit(flag, memory_order_acquire)) return 1;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
    }
    return 0;
}

typedef struct {
    MmapqIndexedBatchArchiver *arch;
    char                     **marks;
    int                       n_marks;
    _Atomic int               done;
    int                       rc;
} B36Reclaim;

static void *b36_reclaim_main(void *arg)
{
    B36Reclaim *p = arg;
    p->rc = mmapq_indexed_batch_archiver_reclaim_files(p->arch, p->marks,
                                                       p->n_marks);
    atomic_store_explicit(&p->done, 1, memory_order_release);
    return NULL;
}

/* B1 (twin of P1) */
void test_batch_reclaim_deletes_only_marked_still_archivable(void)
{
    batch_two_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 4));
    TEST_ASSERT_EQUAL_INT64(4200, batch_arch_start_offset(arch, 0));
    TEST_ASSERT_EQUAL_INT64(4200, batch_arch_start_offset(arch, 1));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(2, n);
    char *marks[1] = { batch_mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    batch_free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_reclaim_files(arch, marks, 1));

    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));
    TEST_ASSERT_TRUE(batch_queue_file_present("payload_1_0"));
    memset(paths, 0, sizeof(paths));
    n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(batch_list_contains(paths, n, "payload_1_0"));
    batch_free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_reclaim_files(arch, marks, 1));

    TEST_ASSERT_TRUE(batch_queue_file_present("payload_0_1"));
    TEST_ASSERT_TRUE(batch_queue_file_present("payload_1_1"));

    free(marks[0]);
    mmapq_indexed_batch_archiver_close(arch);
}

/* B2 (twin of P2) */
void test_batch_reclaim_relist_excludes_undo_restored_files(void)
{
    batch_single_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2));
    TEST_ASSERT_EQUAL_INT64(4200, batch_arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { batch_mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    batch_free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 0));

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_reclaim_files(arch, marks, 1));
    TEST_ASSERT_EQUAL_INT(0, access(marks[0], F_OK));

    free(marks[0]);
    mmapq_indexed_batch_archiver_close(arch);
}

/* B4 (twin of P4) */
void test_batch_reclaim_lock_is_shared(void)
{
    batch_single_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { batch_mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    batch_free_paths(paths, n);

    char lockpath[512];
    snprintf(lockpath, sizeof(lockpath), "%s/%s/archive.lock", DIR, QUEUE);
    int fd = open(lockpath, O_RDWR);
    TEST_ASSERT_GREATER_OR_EQUAL(0, fd);
    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_SH));

    B36Reclaim p = { .arch = arch, .marks = marks, .n_marks = 1,
                     .done = 0, .rc = -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, b36_reclaim_main, &p));

    if (!b36_wait(&p.done)) {
        /* unblock the EX-blocked thread BEFORE joining, or the suite hangs */
        flock(fd, LOCK_UN);
        close(fd);
        pthread_join(t, NULL);
        free(marks[0]);
        mmapq_indexed_batch_archiver_close(arch);
        TEST_FAIL_MESSAGE("reclaim blocked behind a shared hold (lock taken EX?)");
    }
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_UN));
    close(fd);

    TEST_ASSERT_EQUAL_INT(1, p.rc);
    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));

    free(marks[0]);
    mmapq_indexed_batch_archiver_close(arch);
}

/* B6 (twin of P6 — B1 the blocking finding) */
void test_batch_reclaim_relists_beyond_mark_capacity(void)
{
    batch_two_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 4));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(2, n);
    char *marks[1] = { batch_mark_of(paths, n, "payload_1_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    batch_free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_reclaim_files(arch, marks, 1));
    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));
    TEST_ASSERT_TRUE(batch_queue_file_present("payload_0_0"));

    memset(paths, 0, sizeof(paths));
    n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(batch_list_contains(paths, n, "payload_0_0"));
    batch_free_paths(paths, n);

    free(marks[0]);
    mmapq_indexed_batch_archiver_close(arch);
}

#ifdef MMAPQ_TEST_HOOKS
static _Atomic int b36_hook_fired, b36_hook_timed_out, b36_hook_release;

static void b36_hold_hook(void)
{
    /* Bounded: a hook routing regression must fail the pin via the
     * fired/timed_out assertions, never hang the suite. */
    atomic_store_explicit(&b36_hook_fired, 1, memory_order_release);
    for (int spins = 0; spins < 5000000; spins++) {
        if (atomic_load_explicit(&b36_hook_release, memory_order_acquire)) return;
        sched_yield();
    }
    atomic_store_explicit(&b36_hook_timed_out, 1, memory_order_release);
}

typedef struct {
    MmapqIndexedBatchArchiver *arch;
    int64_t                   index;
    _Atomic int               done;
    int                       rc;
} B36Undo;

static void *b36_undo_main(void *arg)
{
    B36Undo *p = arg;
    p->rc = mmapq_indexed_batch_archiver_archive_up_to_index(p->arch, p->index);
    atomic_store_explicit(&p->done, 1, memory_order_release);
    return NULL;
}
#endif

/* B3 (twin of P3) */
void test_batch_reclaim_unlink_excludes_concurrent_undo(void)
{
#ifdef MMAPQ_TEST_HOOKS
    batch_single_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { batch_mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    batch_free_paths(paths, n);

    /* Second archiver instance for the undo — archivers are
     * thread-confined. Capture into locals, assert after
     * release+join+disarm. */
    MmapqIndexedBatchArchiver *arch2 = tiny_batch_archiver();

    atomic_store(&b36_hook_fired, 0);
    atomic_store(&b36_hook_timed_out, 0);
    atomic_store(&b36_hook_release, 0);
    mmapq_test_hook_set_batch_reclaim_pre_unlink(b36_hold_hook);

    B36Reclaim r = { .arch = arch, .marks = marks, .n_marks = 1,
                     .done = 0, .rc = -999 };
    pthread_t t1;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t1, NULL, b36_reclaim_main, &r));
    if (!b36_wait(&b36_hook_fired)) {
        atomic_store(&b36_hook_release, 1);
        pthread_join(t1, NULL);
        mmapq_test_hook_set_batch_reclaim_pre_unlink(NULL);
        free(marks[0]);
        mmapq_indexed_batch_archiver_close(arch);
        mmapq_indexed_batch_archiver_close(arch2);
        TEST_FAIL_MESSAGE("batch reclaim_pre_unlink never fired (hook routing regression?)");
    }

    B36Undo u = { .arch = arch2, .index = 0, .done = 0, .rc = -999 };
    pthread_t t2;
    int create2_rc = pthread_create(&t2, NULL, b36_undo_main, &u);

    /* captured while threads are alive; asserted after release+join+disarm
     * (F1). Short bounded window for the negative check. */
    int undo_done_during_hold = -1;
    if (create2_rc == 0) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 200000000 };
        nanosleep(&ts, NULL);
        undo_done_during_hold = atomic_load_explicit(&u.done, memory_order_acquire);
    }

    atomic_store_explicit(&b36_hook_release, 1, memory_order_release);
    int join1_rc = pthread_join(t1, NULL);
    int join2_rc = 0;
    if (create2_rc == 0) {
        b36_wait(&u.done);
        join2_rc = pthread_join(t2, NULL);
    }
    mmapq_test_hook_set_batch_reclaim_pre_unlink(NULL);

    TEST_ASSERT_EQUAL_INT(0, create2_rc);
    TEST_ASSERT_EQUAL_INT(0, join1_rc);
    TEST_ASSERT_EQUAL_INT(0, join2_rc);
    TEST_ASSERT_FALSE_MESSAGE(undo_done_during_hold,
        "undo completed while the reclaim held archive.lock SH");
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&b36_hook_fired),
        "batch reclaim_pre_unlink never fired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&b36_hook_timed_out),
        "batch reclaim_pre_unlink spin expired");
    TEST_ASSERT_EQUAL_INT(1, r.rc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, u.rc);
    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));

    free(marks[0]);
    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_archiver_close(arch2);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* B5 (twin of P5) */
void test_batch_reclaim_tolerates_concurrent_unlink(void)
{
#ifdef MMAPQ_TEST_HOOKS
    batch_single_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { batch_mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    batch_free_paths(paths, n);

    atomic_store(&b36_hook_fired, 0);
    atomic_store(&b36_hook_timed_out, 0);
    atomic_store(&b36_hook_release, 0);
    mmapq_test_hook_set_batch_reclaim_pre_unlink(b36_hold_hook);

    B36Reclaim r = { .arch = arch, .marks = marks, .n_marks = 1,
                     .done = 0, .rc = -999 };
    pthread_t t1;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t1, NULL, b36_reclaim_main, &r));
    if (!b36_wait(&b36_hook_fired)) {
        atomic_store(&b36_hook_release, 1);
        pthread_join(t1, NULL);
        mmapq_test_hook_set_batch_reclaim_pre_unlink(NULL);
        free(marks[0]);
        mmapq_indexed_batch_archiver_close(arch);
        TEST_FAIL_MESSAGE("batch reclaim_pre_unlink never fired (hook routing regression?)");
    }

    /* captured, asserted after join+disarm (F1) */
    int unl_rc = unlink(marks[0]);

    atomic_store_explicit(&b36_hook_release, 1, memory_order_release);
    int join_rc = pthread_join(t1, NULL);
    mmapq_test_hook_set_batch_reclaim_pre_unlink(NULL);

    TEST_ASSERT_EQUAL_INT(0, unl_rc);
    TEST_ASSERT_EQUAL_INT(0, join_rc);

    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&b36_hook_fired),
        "batch reclaim_pre_unlink never fired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&b36_hook_timed_out),
        "batch reclaim_pre_unlink spin expired");
    TEST_ASSERT_EQUAL_INT(0, r.rc);   /* ENOENT tolerated, not counted */

    free(marks[0]);
    mmapq_indexed_batch_archiver_close(arch);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* Non-vacuity read-back for the CAND-35 batch pins: capture the first
 * message of the batch at want_index. */
typedef struct {
    int64_t want_index;
    int     seen;
    int32_t len;
    char    first;
} C35BatchCapture;

static MmapqNextMove c35_batch_capture_handler(
    int64_t batch_index, MmapqBatchReadingContext *ctx, void *user_ctx)
{
    C35BatchCapture *c = user_ctx;
    if (batch_index != c->want_index) return MMAPQ_NEXT_ADVANCE;
    c->seen = 1;
    if (mmapq_batch_iterator_advance(ctx->iterator)) {
        c->len = batch_iterator_current_length(ctx->iterator);
        c->first = *(const char *)batch_iterator_current_data(ctx->iterator);
    }
    return MMAPQ_NEXT_ADVANCE;
}

/* ── CAND-35 pins (batch twins): the listing caps each lane's boundary at ───
 * the lowest live payload visible in the headers. Batch commits never touch
 * start_offset, so the stranded state (live batch below the published
 * boundary) needs no crash: start A, start B, commit B, archive, then
 * commit A. (This construction is CAND-33-batch-illegal-in-future; CAND-33
 * must CONVERT these pins, never delete them — see the CAND-33 entry.)
 * Probed geometry (v2 spec): batch A one 4000-byte msg @64 ends 4072; batch
 * B one 100-byte msg STARTS at 4072, its message rolls into file 1, ends
 * 4200; archive(1) with B committed -> boundary 4200. */

/* B1: a stale commit stranding the boundary above a live batch — the
 * capped listing must not list the live batch's payload file.
 * Fail-first: pre-fix lists payload_0_0. */
void test_c35_batch_listing_cap_covers_stranded_batch(void)
{
    char big[4000]; memset(big, 'A', sizeof(big));
    char sm[100];   memset(sm, 'B', sizeof(sm));

    TEST_ASSERT_EQUAL_INT64(4072, batch_record_end(64, 4000));
    TEST_ASSERT_EQUAL_INT64(4200, batch_record_end(4072, 100));

    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    /* Batch A: written and completed, NOT committed */
    MmapqMutableBatch *ba = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(ba);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_mutable_batch_append(ba, big, (int32_t)sizeof(big)));
    int64_t off_a = mmapq_mutable_batch_complete(ba);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    /* Batch B committed first -> index 0 */
    TEST_ASSERT_EQUAL_INT64(0,
        commit_one_msg_batch(a, sm, (int32_t)sizeof(sm)));

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(4200, batch_arch_start_offset(arch, 0));

    /* The stale commit lands BELOW the published boundary (index 1) */
    TEST_ASSERT_EQUAL_INT64(1,
        mmapq_indexed_batch_appender_commit_batch(a, off_a));
    mmapq_indexed_batch_appender_close(a);

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    TEST_ASSERT_FALSE(batch_list_contains(paths, n, "payload_0_0"));
    batch_free_paths(paths, n);
    mmapq_indexed_batch_archiver_close(arch);

    /* Non-vacuity: the stranded batch really is live and readable */
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_poller_create(DIR, QUEUE, 4, TIMEOUT, rt, &p));
    C35BatchCapture cap = { .want_index = 1, .seen = 0, .len = -1 };
    /* The poller starts at start_index (1): one poll serves batch 1 */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED,
        mmapq_indexed_batch_poller_poll(p, c35_batch_capture_handler, &cap));
    mmapq_indexed_batch_poller_close(p);
    TEST_ASSERT_TRUE_MESSAGE(cap.seen, "stranded batch index 1 not readable");
    TEST_ASSERT_EQUAL_INT32((int32_t)sizeof(big), cap.len);
    TEST_ASSERT_EQUAL_INT('A', cap.first);
}

/* B2 (twin of P2): a mapping timeout inside the batch listing's
 * live-header scan must propagate as MMAPQ_ETIMEDOUT — the checked read
 * is load-bearing. */
#ifdef MMAPQ_TEST_HOOKS
static _Atomic int c35_batch_scan_fired;

static int c35_batch_listing_inject(void)
{
    atomic_store_explicit(&c35_batch_scan_fired, 1, memory_order_release);
    return 1;   /* inject WRAP_MAP_TIMED_OUT at the wrap-result level */
}
#endif

void test_c35_batch_listing_scan_timeout_propagates(void)
{
#ifdef MMAPQ_TEST_HOOKS
    /* batch A idx 0 @64 (ends 4072), batch B idx 1 @4072 (rolls, ends 4200) */
    batch_single_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(4072, batch_arch_start_offset(arch, 0));

    /* Control: the scan runs (B at 4072 floors the lane; cap
     * min(4072, 4072)) and the honest listing stays empty */
    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    batch_free_paths(paths, n);

    atomic_store(&c35_batch_scan_fired, 0);
    mmapq_test_hook_set_batch_archiver_listing_scan(c35_batch_listing_inject);
    n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    mmapq_test_hook_set_batch_archiver_listing_scan(NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, n);
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c35_batch_scan_fired),
        "batch listing scan hook never fired");
    mmapq_indexed_batch_archiver_close(arch);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* B3 (twin of P3): the cap is PER LANE — lane 1's honestly archivable
 * file stays listed while the stranded lane 0's file drops out.
 * Fail-first: pre-fix lists BOTH payload_0_0 and payload_1_0. */
void test_c35_batch_listing_cap_is_per_lane(void)
{
    char big[4000]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));

    TEST_ASSERT_EQUAL_INT64(4072, batch_record_end(64, 4000));
    TEST_ASSERT_EQUAL_INT64(4200, batch_record_end(4072, 100));

    /* Lane 1: two committed batches (indices 0, 1) — both archived below */
    MmapqIndexedBatchAppender *b0 = tiny_archivable_batch_appender(0);
    MmapqIndexedBatchAppender *b1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(0,
        commit_one_msg_batch(b1, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1,
        commit_one_msg_batch(b1, sm, (int32_t)sizeof(sm)));

    /* Lane 0: A0 completed uncommitted @64; B0 committed -> index 2 */
    MmapqMutableBatch *ba = mmapq_indexed_batch_appender_start_batch(b0);
    TEST_ASSERT_NOT_NULL(ba);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_mutable_batch_append(ba, big, (int32_t)sizeof(big)));
    int64_t off_a = mmapq_mutable_batch_complete(ba);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    TEST_ASSERT_EQUAL_INT64(2,
        commit_one_msg_batch(b0, sm, (int32_t)sizeof(sm)));

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(4200, batch_arch_start_offset(arch, 0));
    TEST_ASSERT_EQUAL_INT64(4200, batch_arch_start_offset(arch, 1));

    /* Lane 0's stale commit strands the boundary above live A0 (index 3) */
    TEST_ASSERT_EQUAL_INT64(3,
        mmapq_indexed_batch_appender_commit_batch(b0, off_a));
    mmapq_indexed_batch_appender_close(b0);
    mmapq_indexed_batch_appender_close(b1);

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(batch_list_contains(paths, n, "payload_1_0"));
    TEST_ASSERT_FALSE(batch_list_contains(paths, n, "payload_0_0"));
    batch_free_paths(paths, n);
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
    RUN_TEST(test_batch_archiver_forward_rejects_partial_scan);
    RUN_TEST(test_batch_archiver_undo_rejects_partial_scan);

    /* CAND-30 pins (batch) */
    RUN_TEST(test_batch_archiver_undo_never_raises_start_offset);
    RUN_TEST(test_batch_appender_rejoin_over_abandoned_batch);
    RUN_TEST(test_batch_archiver_in_order_boundary_unchanged);
    RUN_TEST(test_batch_appender_rejoin_after_payload_file_deleted);

    /* CAND-36 pins (batch) */
    RUN_TEST(test_batch_reclaim_deletes_only_marked_still_archivable);
    RUN_TEST(test_batch_reclaim_relist_excludes_undo_restored_files);
    RUN_TEST(test_batch_reclaim_unlink_excludes_concurrent_undo);
    RUN_TEST(test_batch_reclaim_lock_is_shared);
    RUN_TEST(test_batch_reclaim_tolerates_concurrent_unlink);
    RUN_TEST(test_batch_reclaim_relists_beyond_mark_capacity);

    /* CAND-35 pins (batch) */
    RUN_TEST(test_c35_batch_listing_cap_covers_stranded_batch);
    RUN_TEST(test_c35_batch_listing_scan_timeout_propagates);
    RUN_TEST(test_c35_batch_listing_cap_is_per_lane);
    return UNITY_END();
}
