#include "unity.h"
#include "indexed_batch_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_reader.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/appender_meta.h"
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

/* Tiny archivable geometry for the CAND-30/35/36 batch pins: 32 KiB
 * regions, 32 KiB files — one 32 KB header block per file, the smallest
 * legal geometry since CAND-38 (the pre-rule probes used 4 KiB). A big
 * one-message batch fills file 0 leaving 24 bytes, so the next batch's
 * message rolls to file 1. Exact layout: batch A @64 ends TINYB_A_END,
 * batch B's message rolls to @TINYB_B_START, ends TINYB_B_END. */
#define TINY_REGION   32768
#define TINY_FILE     32768
#define TINY_BIG_LEN  (TINY_FILE - INITIAL_PAYLOAD_POSITION - MMAPQ_BATCH_MSG_COUNT_SIZE - PAYLOAD_LENGTH_SIZE - 24)
#define TINYB_A_END   ((int64_t)TINY_FILE - 24)                          /* 32744 */
#define TINYB_B_START ((int64_t)TINY_FILE)                               /* 32768 */
#define TINYB_B_END   ((int64_t)TINY_FILE + PAYLOAD_LENGTH_SIZE + 100)   /* 32872 */

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
    mmapq_mutable_batch_complete(b);
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
        mmapq_mutable_batch_complete(b);
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
        mmapq_mutable_batch_complete(b);
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
        mmapq_mutable_batch_complete(b);
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
    mmapq_mutable_batch_complete(b);
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
        mmapq_mutable_batch_complete(b);
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
    mmapq_mutable_batch_complete(b);
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
    mmapq_mutable_batch_complete(b);
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
    int64_t batch_offset = mmapq_mutable_batch_complete_uncommitted(b);
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
/* Geometry: 32 KiB regions, 32 KiB files, archivable (smallest legal since
 * CAND-38; the pre-rule probes used 4 KiB). */

static MmapqIndexedBatchAppender *tiny_archivable_batch_appender(uint8_t id)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_BATCH, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create_with_id(
        DIR, QUEUE, TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, id, 0, &a));
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
    return mmapq_mutable_batch_complete(b);   /* CAND-49: complete publishes */
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
    int64_t off_a = mmapq_mutable_batch_complete_uncommitted(b);
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
    /* 4000 B x 11 batches = 44 KB > 32 KiB file: rolling stays exercised
     * (CAND-38 migration: was 800 B at 4 KiB files). */
    char msg[4000]; memset(msg, 'm', sizeof(msg));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    int64_t off_10 = -1;
    for (int i = 0; i < 11; i++) {
        MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
        TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, msg, (int32_t)sizeof(msg)));
        if (i == 10) { off_10 = mmapq_mutable_batch_complete_uncommitted(b); continue; }
        TEST_ASSERT_EQUAL_INT64(i, mmapq_mutable_batch_complete(b));
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
 * a0's single batch spans into file 1 (two 20000 B messages: 40 KB > 32 KiB;
 * CAND-38 migration: was two 3000 B at 4 KiB), so archiving it lists
 * payload_0_0; the file is deleted; a1 stays live. Pre-fix: the scan
 * finds the archived batch header, the walk stops early at the deleted file,
 * and the rejoined appender writes into file 0 (below the boundary). */
void test_batch_appender_rejoin_after_payload_file_deleted(void)
{
    char big[20000]; memset(big, 'Z', sizeof(big));

    MmapqIndexedBatchAppender *a0 = tiny_archivable_batch_appender(0);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a0);
    /* append returns the message's index within the batch */
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(b, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_mutable_batch_complete(b));
    mmapq_indexed_batch_appender_close(a0);

    MmapqIndexedBatchAppender *a1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a1, "live", 4));
    mmapq_indexed_batch_appender_close(a1);

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(TINY_FILE, batch_arch_start_offset(arch, 0));

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

    /* Boundary reach, asserted directly (CAND-38 migration): the rejoined
     * batch must NOT land in the reclaimed file 0. Pre-fix the unbounded
     * scan found the archived header, the walk stopped at the deleted
     * file, and "back" landed at 64 — below the boundary (scratch revert:
     * batch_offset 64 vs 52772 fixed). The boundary assertion below cannot
     * see this — the forward publish never lowers — so it is pinned here
     * on the batch's own offset. */
    {
        MmapqIndexedBatchQueue *vq = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
            DIR, QUEUE, TINY_REGION, 4, 1, TINY_FILE, 0, TIMEOUT, 1, rt, rt, &vq));
        MmapqIndexedBatchReader *vr = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(vq, &vr));
        MmapqBatchReadingContext vctx = {0};
        mmapq_indexed_batch_reader_read(vr, 2, &vctx);
        TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, vctx.status);
        TEST_ASSERT_GREATER_OR_EQUAL_INT64(TINY_FILE, vctx.batch_offset);
        mmapq_indexed_batch_reader_close(vr);
        mmapq_indexed_batch_queue_close(vq);
    }

    /* The new batch must land at or past the boundary (file 1), not in the
     * reclaimed file 0 — verify via the boundary after archiving it */
    MmapqIndexedBatchArchiver *arch2 = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch2, 3));
    TEST_ASSERT_GREATER_THAN_INT64(TINY_FILE, batch_arch_start_offset(arch2, 0));
    mmapq_indexed_batch_archiver_close(arch2);
}

/* ── CAND-36 pins (batch twins of P1–P6): reclaim_files under archive.lock SH ── */
/* Batch geometry: a one-message batch is a 4-byte count field followed by
 * the message under the appender's padding rule at region == file ==
 * TINY_FILE. batch_record_end(64, TINY_BIG_LEN) = TINYB_A_END;
 * batch_record_end(TINYB_A_END, 100) = TINYB_B_END (rolls to file 1) —
 * mirrors the single-message TINY_A_END/TINY_B_END (B2). */

static int64_t batch_record_end(int64_t pos, int32_t len)
{
    int64_t p = pos + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int64_t cap = TINY_FILE - (p % TINY_FILE);
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

/* Single-lane: batch A one TINY_BIG_LEN-byte msg (idx 0, @64, ends
 * TINYB_A_END), batch B one 100-byte msg (idx 1, rolls, ends TINYB_B_END).
 * archive(2) -> start_offset TINYB_B_END -> payload_0_0 listed. */
static void batch_single_lane_geometry(void)
{
    char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a, sm, (int32_t)sizeof(sm)));
    mmapq_indexed_batch_appender_close(a);
    TEST_ASSERT_EQUAL_INT64(TINYB_A_END, batch_record_end(64, TINY_BIG_LEN));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_record_end(TINYB_A_END, 100));
}

/* B2 batch geometry: two appenders, a0:big, a1:big, a0:100, a1:100
 * (indices 0-3). archive(4) -> TINYB_B_END/TINYB_B_END, payload_0_0 and
 * payload_1_0 listed. */
static void batch_two_lane_geometry(void)
{
    char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));
    MmapqIndexedBatchAppender *a0 = tiny_archivable_batch_appender(0);
    MmapqIndexedBatchAppender *a1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a0, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, commit_one_msg_batch(a1, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(2, commit_one_msg_batch(a0, sm, (int32_t)sizeof(sm)));
    TEST_ASSERT_EQUAL_INT64(3, commit_one_msg_batch(a1, sm, (int32_t)sizeof(sm)));
    mmapq_indexed_batch_appender_close(a0);
    mmapq_indexed_batch_appender_close(a1);
    TEST_ASSERT_EQUAL_INT64(TINYB_A_END, batch_record_end(64, TINY_BIG_LEN));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_record_end(TINYB_A_END, 100));
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
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_arch_start_offset(arch, 0));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_arch_start_offset(arch, 1));

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
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_arch_start_offset(arch, 0));

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
 * start_offset. Converted by CAND-49 (§7.6): the stale commit below the
 * boundary is illegal under §3.5 (non-member EINVAL), so the stranded state
 * (live batch below the boundary) is built by committing every batch
 * legally BEFORE any boundary exists, then raising the lane's start_offset
 * through the internal appender_meta API (appender_meta_start_offset_cas)
 * to reproduce the crash-stranded boundary. Probed geometry (v2 spec,
 * migrated by CAND-38): batch A one TINY_BIG_LEN-byte msg @64 ends
 * TINYB_A_END; batch B one 100-byte msg STARTS at TINYB_A_END, its message
 * rolls into file 1, ends TINYB_B_END; the raised boundary is TINYB_B_END. */

/* B1: a boundary stranded above a live batch — the capped listing must not
 * list the live batch's payload file. Fail-first: pre-fix lists payload_0_0. */
void test_c35_batch_listing_cap_covers_stranded_batch(void)
{
    char big[TINY_BIG_LEN]; memset(big, 'A', sizeof(big));
    char sm[100];   memset(sm, 'B', sizeof(sm));

    TEST_ASSERT_EQUAL_INT64(TINYB_A_END, batch_record_end(64, TINY_BIG_LEN));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_record_end(TINYB_A_END, 100));

    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);
    /* Batch A committed -> index 0; batch B committed -> index 1. Both legal,
     * before any boundary exists. */
    MmapqMutableBatch *ba = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(ba);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_mutable_batch_append(ba, big, (int32_t)sizeof(big)));
    int64_t off_a = mmapq_mutable_batch_complete_uncommitted(ba);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    TEST_ASSERT_EQUAL_INT64(0,
        mmapq_indexed_batch_appender_commit_batch(a, off_a));
    TEST_ASSERT_EQUAL_INT64(1,
        commit_one_msg_batch(a, sm, (int32_t)sizeof(sm)));

    /* The crash-stranded boundary: TINYB_B_END, with live A below it. */
    AppenderMeta *am = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_rw(DIR, QUEUE, 0, &am));
    TEST_ASSERT_EQUAL_INT(1,
        appender_meta_start_offset_cas(am, INITIAL_PAYLOAD_POSITION, TINYB_B_END));
    mmapqi_appender_meta_close(am);

    /* The commit-EINVAL step (mutant M9): stage C, stage D (so C is not the
     * tail), raise the boundary past C — commit_batch(C) is overtaken and
     * final. */
    MmapqMutableBatch *bc = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(bc, sm, (int32_t)sizeof(sm)));
    int64_t off_c = mmapq_mutable_batch_complete_uncommitted(bc);
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, off_c);
    MmapqMutableBatch *bd = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(bd, sm, (int32_t)sizeof(sm)));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END + MMAPQ_BATCH_MSG_COUNT_SIZE
        + PAYLOAD_LENGTH_SIZE + 100,
        mmapq_mutable_batch_complete_uncommitted(bd));
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_rw(DIR, QUEUE, 0, &am));
    TEST_ASSERT_EQUAL_INT(1,
        appender_meta_start_offset_cas(am, TINYB_B_END,
            TINYB_B_END + MMAPQ_BATCH_MSG_COUNT_SIZE
                + PAYLOAD_LENGTH_SIZE + 100));
    mmapqi_appender_meta_close(am);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(a, off_c));
    mmapq_indexed_batch_appender_close(a);

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
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
    C35BatchCapture cap = { .want_index = 0, .seen = 0, .len = -1 };
    /* The poller starts at start_index (0): one poll serves batch 0 */
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED,
        mmapq_indexed_batch_poller_poll(p, c35_batch_capture_handler, &cap));
    mmapq_indexed_batch_poller_close(p);
    TEST_ASSERT_TRUE_MESSAGE(cap.seen, "stranded batch index 0 not readable");
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
    /* batch A idx 0 @64 (ends TINYB_A_END), batch B idx 1 @TINYB_A_END
     * (rolls, ends TINYB_B_END) */
    batch_single_lane_geometry();

    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(TINYB_A_END, batch_arch_start_offset(arch, 0));

    /* Control: the scan runs (B at TINYB_A_END floors the lane; cap
     * min(TINYB_A_END, TINYB_A_END)) and the honest listing stays empty */
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
    char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));

    TEST_ASSERT_EQUAL_INT64(TINYB_A_END, batch_record_end(64, TINY_BIG_LEN));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_record_end(TINYB_A_END, 100));

    /* Lane 1: two committed batches (indices 0, 1); lane 0: A0 @64
     * (index 2), B0 (index 3) — all committed legally before any boundary
     * exists (converted by CAND-49 §7.6). */
    MmapqIndexedBatchAppender *b0 = tiny_archivable_batch_appender(0);
    MmapqIndexedBatchAppender *b1 = tiny_archivable_batch_appender(1);
    TEST_ASSERT_EQUAL_INT64(0,
        commit_one_msg_batch(b1, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1,
        commit_one_msg_batch(b1, sm, (int32_t)sizeof(sm)));
    MmapqMutableBatch *ba = mmapq_indexed_batch_appender_start_batch(b0);
    TEST_ASSERT_NOT_NULL(ba);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_mutable_batch_append(ba, big, (int32_t)sizeof(big)));
    int64_t off_a = mmapq_mutable_batch_complete_uncommitted(ba);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    TEST_ASSERT_EQUAL_INT64(2,
        mmapq_indexed_batch_appender_commit_batch(b0, off_a));
    TEST_ASSERT_EQUAL_INT64(3,
        commit_one_msg_batch(b0, sm, (int32_t)sizeof(sm)));

    /* Archive lane 1's entries (idx 0, 1): lane 1's boundary rises
     * honestly; lane 0's is untouched. */
    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 2));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, batch_arch_start_offset(arch, 1));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, batch_arch_start_offset(arch, 0));

    /* The crash-stranded boundary on lane 0: TINYB_B_END with live A0
     * below it (internal appender_meta API, per the §7.6 conversion). */
    AppenderMeta *am = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_rw(DIR, QUEUE, 0, &am));
    TEST_ASSERT_EQUAL_INT(1,
        appender_meta_start_offset_cas(am, INITIAL_PAYLOAD_POSITION, TINYB_B_END));
    mmapqi_appender_meta_close(am);

    /* The commit-EINVAL step (mutant M9): stage C0, stage D0 (so C0 is not
     * the tail), raise the boundary past C0 — commit_batch(C0) is
     * overtaken and final. */
    MmapqMutableBatch *bc = mmapq_indexed_batch_appender_start_batch(b0);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(bc, sm, (int32_t)sizeof(sm)));
    int64_t off_c = mmapq_mutable_batch_complete_uncommitted(bc);
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END, off_c);
    MmapqMutableBatch *bd = mmapq_indexed_batch_appender_start_batch(b0);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(bd, sm, (int32_t)sizeof(sm)));
    TEST_ASSERT_EQUAL_INT64(TINYB_B_END + MMAPQ_BATCH_MSG_COUNT_SIZE
        + PAYLOAD_LENGTH_SIZE + 100,
        mmapq_mutable_batch_complete_uncommitted(bd));
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_rw(DIR, QUEUE, 0, &am));
    TEST_ASSERT_EQUAL_INT(1,
        appender_meta_start_offset_cas(am, TINYB_B_END,
            TINYB_B_END + MMAPQ_BATCH_MSG_COUNT_SIZE
                + PAYLOAD_LENGTH_SIZE + 100));
    mmapqi_appender_meta_close(am);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(b0, off_c));
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

/* ── CAND-38 pin P2 (batch twin): positive reclaim guard at aligned ────────
 * geometry — see test_indexed_archiver.c for the full comment. Mirrors the
 * probe hdrloss_batch.c's aligned rows (0 unreadable); the misaligned rows
 * (992/2112 live batches lost at 8 KB files) are refused at create
 * post-rule (P1). */
static void c38_batch_reclaim_loses_nothing(int64_t max_file)
{
    const int32_t region = 32768;
    const int64_t hif    = max_file / HEADER_SIZE;  /* headers per file */
    const int64_t N      = 2 * hif + 100;
    const int64_t upto   = hif + 50;                /* past one header file */

    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        region, max_file, /* archivable= */ 1, QUEUE_TYPE_BATCH, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create_with_id(
        DIR, QUEUE, region, 16, max_file, TIMEOUT, 0, rt, 0, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    for (int64_t i = 0; i < N; i++)
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_batch_appender_append_and_commit(a, "cccc", 4));

    MmapqIndexedBatchArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, QUEUE, 4, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, upto));

    char *paths[64] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(1, n);
    int hdr = 0;
    for (int i = 0; i < n; i++) if (strstr(paths[i], "/header_")) hdr++;
    TEST_ASSERT_GREATER_OR_EQUAL(1, hdr);   /* a header file really listed */
    TEST_ASSERT_EQUAL_INT(n,
        mmapq_indexed_batch_archiver_reclaim_files(arch, paths, n));
    batch_free_paths(paths, n);
    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);

    /* Fresh reader over the whole live range: nothing lost. */
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_reader_create(DIR, QUEUE, 4, TIMEOUT, rt, &r));
    TEST_ASSERT_NOT_NULL(r);
    for (int64_t i = upto; i < N; i++)
        TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE,
            mmapq_indexed_batch_reader_entry_status(r, i));
    mmapq_indexed_batch_reader_close(r);

    /* Rejoin: the next append is index N — no committed index reused. */
    MmapqIndexedBatchAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create_with_id(
        DIR, QUEUE, region, 16, max_file, TIMEOUT, 0, rt, 0, 0, &a2));
    TEST_ASSERT_NOT_NULL(a2);
    TEST_ASSERT_EQUAL_INT64(N,
        mmapq_indexed_batch_appender_append_and_commit(a2, "next", 4));
    mmapq_indexed_batch_appender_close(a2);
}

void test_c38_batch_reclaim_aligned_32k_file_loses_nothing(void)
{
    c38_batch_reclaim_loses_nothing(32768);
}

void test_c38_batch_reclaim_aligned_96k_file_loses_nothing(void)
{
    c38_batch_reclaim_loses_nothing(98304);
}

/* ── CAND-51b pins (F-REC-1: the commit_batch re-check) ──────────────────── */

#ifdef MMAPQ_TEST_HOOKS
static _Atomic int c51b_fired, c51b_release;
static void c51b_hold_hook(void)
{
    atomic_store_explicit(&c51b_fired, 1, memory_order_release);
    for (long s = 0; s < 50000000; s++) {   /* bounded — never hang the suite */
        if (atomic_load_explicit(&c51b_release, memory_order_acquire)) return;
        sched_yield();
    }
}
typedef struct { MmapqIndexedBatchAppender *a; int64_t off; int64_t rc; } C51bCommit;
static void *c51b_commit_main(void *p)
{
    C51bCommit *j = p;
    j->rc = mmapq_indexed_batch_appender_commit_batch(j->a, j->off);
    return NULL;
}
static void c51b_park_commit(MmapqIndexedBatchAppender *a, int64_t off,
                             C51bCommit *j, pthread_t *t)
{
    atomic_store(&c51b_fired, 0); atomic_store(&c51b_release, 0);
    mmapq_test_hook_set_batch_commit_pre_publish(c51b_hold_hook);
    j->a = a; j->off = off; j->rc = -999;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(t, NULL, c51b_commit_main, j));
    int waited = 0;
    while (!atomic_load_explicit(&c51b_fired, memory_order_acquire)) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
        if (++waited > 5000) {
            atomic_store_explicit(&c51b_release, 1, memory_order_release);
            pthread_join(*t, NULL);
            mmapq_test_hook_set_batch_commit_pre_publish(NULL);
            TEST_FAIL_MESSAGE("commit_batch hook never fired (routing regression?)");
        }
    }
}
static void c51b_release_commit(pthread_t t)
{
    atomic_store_explicit(&c51b_release, 1, memory_order_release);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    mmapq_test_hook_set_batch_commit_pre_publish(NULL);
}
#endif

/* F-REC-1 fail-first pin (the arm-B interleaving of the CAND-51b probe):
 * commit_batch parked between the §3.5 overtaken pre-check and the publish
 * CAS while a full archive runs; the archiver publishes 496 and its live
 * pass sees nothing; the commit then lands the header live at slot 3.
 * Post-fix the fence + re-check + min lower start_offset to 64.
 * Pre-fix: start_offset stays 496 (the §4.5 carve-out — executed: probe arm
 * B, audit/claude-CAND-51b/probe-output.txt).
 * Exact layout (100-byte messages): B1 staged @64 (ends 172); B2 @172 →
 * idx 0 (ends 280); B3 @280 → idx 1 (ends 388); B4 @388 → idx 2 (ends 496).
 * Mutation: remove the min call → this pin fails (start_offset 496). */
void test_c51b_batch_commit_batch_recheck_lowers_boundary(void)
{
#ifdef MMAPQ_TEST_HOOKS
    char sm[100]; memset(sm, 'B', sizeof(sm));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, sm, (int32_t)sizeof(sm)));
    TEST_ASSERT_EQUAL_INT64(64, mmapq_mutable_batch_complete_uncommitted(b));
    for (int i = 0; i < 3; i++)
        TEST_ASSERT_EQUAL_INT64(i, commit_one_msg_batch(a, sm, (int32_t)sizeof(sm)));

    C51bCommit j; pthread_t t;
    c51b_park_commit(a, 64, &j, &t);              /* parked pre-publish */
    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(496, batch_arch_start_offset(arch, 0));
    c51b_release_commit(t);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);             /* the CAS landed live */
    TEST_ASSERT_EQUAL_INT64(64, batch_arch_start_offset(arch, 0));

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
#endif
}

/* Guard (F-REC-1 tail exemption): commit_batch of the CURRENT batch's
 * offset skips the overtaken pre-check — the lane tail can never be
 * overtaken in real operation (the §3.4 fast-path proof's batch form), so
 * the boundary here is raised artificially to make the branch observable.
 * The commit succeeds; the re-check min then lowers the boundary to the
 * just-published offset. */
void test_c51b_batch_commit_batch_tail_exemption(void)
{
    char sm[100]; memset(sm, 'C', sizeof(sm));
    MmapqIndexedBatchAppender *a = tiny_archivable_batch_appender(0);

    TEST_ASSERT_EQUAL_INT64(0, commit_one_msg_batch(a, sm, (int32_t)sizeof(sm)));
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, sm, (int32_t)sizeof(sm)));
    int64_t off = mmapq_mutable_batch_complete_uncommitted(b);
    TEST_ASSERT_EQUAL_INT64(172, off);

    /* Artificial boundary above the staged tail (meta surgery, as in the
     * CAND-35 stranded-batch pin). */
    AppenderMeta *am = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_rw(DIR, QUEUE, 0, &am));
    TEST_ASSERT_EQUAL_INT(1,
        appender_meta_start_offset_cas(am, INITIAL_PAYLOAD_POSITION, 1000));
    mmapqi_appender_meta_close(am);

    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_batch_appender_commit_batch(a, off));
    MmapqIndexedBatchArchiver *arch = tiny_batch_archiver();
    TEST_ASSERT_EQUAL_INT64(172, batch_arch_start_offset(arch, 0));
    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
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

    /* CAND-38 pins (batch) */
    RUN_TEST(test_c38_batch_reclaim_aligned_32k_file_loses_nothing);
    RUN_TEST(test_c38_batch_reclaim_aligned_96k_file_loses_nothing);

    /* CAND-51b pins (batch) */
    RUN_TEST(test_c51b_batch_commit_batch_recheck_lowers_boundary);
    RUN_TEST(test_c51b_batch_commit_batch_tail_exemption);
    return UNITY_END();
}
