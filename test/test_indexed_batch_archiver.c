#include "unity.h"
#include "indexed_batch_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "internal/queue_meta.h"
#include "internal/header_codec.h"
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
    return UNITY_END();
}
