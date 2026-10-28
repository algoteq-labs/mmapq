/* CAND-54 fail-first pins (spec §10; audit/CAND-54-implementation-handoff.md).
 *
 * Ten pins are written against the post-fix behaviour and were observed
 * FAILING against the pre-fix tree before any fix landed (stage-1
 * transcripts: audit trail, 2026-10-25 — arm C, L(i) semantics,
 * mismatched-flag reopen, join clears a stale intent, the §6(c)
 * refusal, both queue types where applicable). The FOUR hole pins are
 * different: they cannot fail-first against the pre-fix tree because
 * their hook points (the lister's listing_pre_intent midpoint, the
 * batch commit_post_cas / commit_min_pre) did not exist there, and
 * without the intra-lister interleaving the intent simply blocks the
 * deletion — the hole is unreachable (the reviewer's stage-1 finding).
 * Their acceptance evidence is the post-fix mutation matrix: L(ii)
 * removed → the hole-1 pins fail; L(i) removed → the hole-2 pins fail
 * (each mutant's failing assertion is in the stage-2 report).
 * Geometry: 32 KiB
 * regions/files (one 32 KB header block per file, the CAND-38 minimum).
 * Layouts measured against the tree (layout probe, 2026-10-25):
 *
 *   single-message: X (100 B) staged @64 ends 168; Z (100 B) staged @168
 *     ends 272; B2/B3/B4 (16000 B) committed at indices 0,1,2 — B2@272,
 *     B3@16276, B4 rolls to file 1 @32768 and ends 48772. An archive
 *     covering index 2 publishes the boundary 48772 (file 0 wholly below).
 *   batch: X (1x100 B) staged @64 ends 172; Z @172 ends 280; B2/B3/B4
 *     (1x16000 B) committed at 0,1,2; the boundary is likewise 48772.
 *
 * "Crash" in these pins is close + reopen: the state that matters (the
 * min-lowered start_offset, the archive line) is persisted in the meta
 * mmap, and nothing is in flight at the close point — a kill -9 would
 * leave identical bytes. The join-clears pin simulates the killed writer's
 * stale intent by poking byte 24 (the intent slot, padding today — the
 * appender_meta open path does not validate padding) directly.
 */
#include "unity.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_archiver.h"
#include "indexed_reader.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_archiver.h"
#include "indexed_batch_reader.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/uncommitted_payload_iterator.h"
#include "internal/uncommitted_batch_iterator.h"
#include "internal/queue_meta.h"
#include "internal/appender_meta.h"
#include "internal/header_codec.h"
#include "internal/region_accessor.h"
#include "internal/test_hooks.h"
#include "internal/test_reach.h"
#include "runtime.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>

static const char    *DIR     = "/tmp";
static const int32_t  REGION  = 32768;
static const int64_t  MAXFILE = 32768;
static const int      RING    = 4;
static const uint64_t TIMEOUT = 5000000000ULL;

#define SMALL_LEN   100
#define BIG_LEN     16000
#define X_OFF       64                       /* staged X, both types      */
#define SZ_OFF      168                      /* staged Z, single-message  */
#define ZB_OFF      172                      /* staged Z, batch           */
#define BOUND       48772                    /* published boundary        */

static MmapqRuntime *rt = NULL;
static char msg_small[SMALL_LEN];
static char msg_big[BIG_LEN];

/* ── queue/dir helpers ── */

static void rm_queue(const char *name)
{
    char cmd[512];
    snprintf(cmd, sizeof cmd, "rm -rf %s/%s", DIR, name);
    int rc = system(cmd); (void)rc;
}

static void mk_queue_dir(const char *name)
{
    char d[512];
    snprintf(d, sizeof d, "%s/%s", DIR, name);
    mkdir(d, 0775);
}

static void mk_meta(const char *name, int64_t start_index, int archivable,
                    int queue_type)
{
    mk_queue_dir(name);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, name, start_index,
        REGION, MAXFILE, archivable, queue_type, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
}

static void payload0_path(const char *name, char *out, size_t n)
{
    snprintf(out, n, "%s/%s/payload_0_0", DIR, name);
}

static int path_exists(const char *path)
{
    return access(path, F_OK) == 0;
}

/* ── appender/archiver construction ── */

static MmapqIndexedAppender *mk_appender(const char *name, int archivable,
                                         int64_t start_index)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, name,
        REGION, RING, MAXFILE, TIMEOUT, archivable, rt, start_index, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedBatchAppender *mk_batch_appender(const char *name,
                                                    int archivable,
                                                    int64_t start_index)
{
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create(DIR, name,
        REGION, RING, MAXFILE, TIMEOUT, archivable, rt, start_index, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedArchiver *mk_archiver(const char *name)
{
    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, name, RING, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    return arch;
}

static MmapqIndexedBatchArchiver *mk_batch_archiver(const char *name)
{
    MmapqIndexedBatchArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, name, RING, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    return arch;
}

#ifdef MMAPQ_TEST_HOOKS
static int64_t arch_so(MmapqIndexedArchiver *arch)
{
    int64_t off = -1;
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_start_offset(arch, 0, &off));
    return off;
}

static int64_t batch_arch_so(MmapqIndexedBatchArchiver *arch)
{
    int64_t off = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_archiver_start_offset(arch, 0, &off));
    return off;
}
#endif

/* ── staging helpers (exact layouts per the header comment) ── */

static void stage_three_big_single(MmapqIndexedAppender *a)
{
    for (int i = 0; i < 3; i++)
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_appender_append(a, msg_big, sizeof msg_big));
}

static int64_t stage_batch(MmapqIndexedBatchAppender *a, const void *msg,
                           int32_t len)
{
    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(b, msg, len));
    return mmapq_mutable_batch_complete_uncommitted(b);
}

#ifdef MMAPQ_TEST_HOOKS
static void stage_three_big_batch(MmapqIndexedBatchAppender *a)
{
    for (int i = 0; i < 3; i++)
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_batch_appender_append_and_commit(a, msg_big,
                                                           sizeof msg_big));
}
#endif

/* ── park machinery (dg discipline: atomics, bounded spin, never hang) ── */

#define PARK_SLOTS 4
static _Atomic int park_fired[PARK_SLOTS];
static _Atomic int park_release[PARK_SLOTS];

static void park_reset(void)
{
    for (int i = 0; i < PARK_SLOTS; i++) {
        atomic_store(&park_fired[i], 0);
        atomic_store(&park_release[i], 0);
    }
}

#ifdef MMAPQ_TEST_HOOKS

static void park_hook_0(void) { atomic_store(&park_fired[0], 1);
    for (long s = 0; s < 50000000 && !atomic_load(&park_release[0]); s++) sched_yield(); }
static void park_hook_1(void) { atomic_store(&park_fired[1], 1);
    for (long s = 0; s < 50000000 && !atomic_load(&park_release[1]); s++) sched_yield(); }
static void park_hook_2(void) { atomic_store(&park_fired[2], 1);
    for (long s = 0; s < 50000000 && !atomic_load(&park_release[2]); s++) sched_yield(); }

static void wait_parked(int slot)
{
    for (long w = 0; !atomic_load(&park_fired[slot]); w++) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
        if (w > 5000)
            TEST_FAIL_MESSAGE("CAND-54 pin: hook never fired (vacuous geometry?)");
    }
}

static void release_park(int slot) { atomic_store(&park_release[slot], 1); }

/* ── commit/reclaim threads ── */

typedef struct { MmapqIndexedAppender *a; int64_t off; int64_t rc; } CommitJob;
static void *commit_main(void *p)
{
    CommitJob *j = p;
    j->rc = mmapq_indexed_appender_commit(j->a, j->off);
    return NULL;
}

typedef struct { MmapqIndexedBatchAppender *a; int64_t off; int64_t rc; } BatchCommitJob;
static void *batch_commit_main(void *p)
{
    BatchCommitJob *j = p;
    j->rc = mmapq_indexed_batch_appender_commit_batch(j->a, j->off);
    return NULL;
}

typedef struct { MmapqIndexedArchiver *arch; char **paths; int n; int rc; } ReclaimJob;
static void *reclaim_main(void *p)
{
    ReclaimJob *j = p;
    j->rc = mmapq_indexed_archiver_reclaim_files(j->arch, j->paths, j->n);
    return NULL;
}

typedef struct { MmapqIndexedBatchArchiver *arch; char **paths; int n; int rc; } BatchReclaimJob;
static void *batch_reclaim_main(void *p)
{
    BatchReclaimJob *j = p;
    j->rc = mmapq_indexed_batch_archiver_reclaim_files(j->arch, j->paths, j->n);
    return NULL;
}
#endif /* MMAPQ_TEST_HOOKS (park machinery + job threads) */

/* ── listing helper ── */

static int list_has_payload0(MmapqIndexedArchiver *arch, char **paths, int cap,
                             int *out_n)
{
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, cap);
    int found = 0;
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], "payload_0_0")) found = 1;
    if (out_n) *out_n = n;
    return found;
}

#ifdef MMAPQ_TEST_HOOKS
static int batch_list_has_payload0(MmapqIndexedBatchArchiver *arch,
                                   char **paths, int cap, int *out_n)
{
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, cap);
    int found = 0;
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], "payload_0_0")) found = 1;
    if (out_n) *out_n = n;
    return found;
}
#endif

static void free_paths(char **paths, int n)
{
    for (int i = 0; i < n; i++) free(paths[i]);
}

/* ── meta field access (dg pattern) ── */

static int64_t meta_field(const char *name, int32_t field_offset)
{
    if (field_offset == APPENDER_META_INTENT_OFFSET) {
        /* no accessor (padding today) — read the raw bytes */
        int64_t v = -1;
        char path[512];
        snprintf(path, sizeof path, "%s/%s/appender_meta_0", DIR, name);
        int fd = open(path, O_RDONLY);
        TEST_ASSERT_GREATER_THAN_INT(0, fd);
        TEST_ASSERT_EQUAL_INT(8, (int)pread(fd, &v, 8, APPENDER_META_INTENT_OFFSET));
        close(fd);
        return v;
    }
    AppenderMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_ro(DIR, name, 0, &m));
    int64_t v;
    if (field_offset == APPENDER_META_ARCHIVE_OFFSET_OFFSET)
        v = appender_meta_archive_offset(m);
    else
        v = appender_meta_start_offset(m);
    mmapqi_appender_meta_close(m);
    return v;
}

static void poke_intent(const char *name, int64_t val)
{
    char path[512];
    snprintf(path, sizeof path, "%s/%s/appender_meta_0", DIR, name);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_GREATER_THAN_INT(0, fd);
    TEST_ASSERT_EQUAL_INT(8, (int)pwrite(fd, &val, 8, APPENDER_META_INTENT_OFFSET));
    close(fd);
}

/* ── readers ── */

#ifdef MMAPQ_TEST_HOOKS
/* Read index `idx` on the single-message queue; assert it is X's staged
 * payload (100 x 'A'). Used to detect recreation-vs-survival: a recreated
 * file 0 holds different bytes at the offsets a live header names. */
static void assert_single_reads_X(const char *name, int64_t idx)
{
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, name, RING, TIMEOUT, NULL, &r));
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, idx, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(SMALL_LEN, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY(msg_small, ctx.data, SMALL_LEN);
    mmapq_indexed_reader_close(r);
}

static void assert_batch_reads_X(const char *name, int64_t idx)
{
    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_reader_create(DIR, name, RING, TIMEOUT, rt, &r));
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, idx, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_INT32(SMALL_LEN,
        batch_iterator_current_length(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY(msg_small,
        batch_iterator_current_data(ctx.iterator), SMALL_LEN);
    mmapq_indexed_batch_reader_close(r);
}
#endif /* MMAPQ_TEST_HOOKS (readers) */

void setUp(void)
{
    memset(msg_small, 'A', sizeof msg_small);
    memset(msg_big, 'B', sizeof msg_big);
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt));
    TEST_ASSERT_NOT_NULL(rt);
    park_reset();
}

void tearDown(void) { }

/* ════════════════════════════════════════════════════════════════════════
 * Arm C (51b probe promoted; handoff "Pins"): a commit parked between the
 * pre-check and the CAS across mark + re-list + unlink. Post-fix the intent
 * blocks the listing (file survives, record delivered). Pre-fix failure:
 * payload_0_0 is listed, unlinked, and the read of the live record fails.
 * Hooks: commit_pre (indexed_appender.c:742 region) /
 * batch_commit_pre_publish (indexed_batch_appender.c:605).
 * ════════════════════════════════════════════════════════════════════════ */
#ifdef MMAPQ_TEST_HOOKS

void test_c54_arm_c_single(void)
{
    rm_queue("c54p_ac_s");
    mk_meta("c54p_ac_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_ac_s", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    stage_three_big_single(a);

    MmapqIndexedArchiver *arch = mk_archiver("c54p_ac_s");

    CommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_commit_pre(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(BOUND, arch_so(arch));

    /* Post-fix: X's intent is set while it is parked, so the listing's
     * inflight term caps the lane at 64 and payload_0_0 is NOT listed. */
    char *paths[16] = {0};
    int n = -1;
    int marked = list_has_payload0(arch, paths, 16, &n);
    free_paths(paths, n);

    /* Arm-C timing preserved: the commit is released from inside the
     * reclaim's SH hold (reclaim_pre_unlink), as in the 51b probe. The mark
     * is explicit (not the listing's result): with n_marked == 0 the reclaim
     * returns before its pre-unlink hook (indexed_archiver.c:627 vs :666),
     * and post-fix the listing above is empty — the re-list must run under
     * the pending intent and refuse the mark. */
    char p0[512]; payload0_path("c54p_ac_s", p0, sizeof p0);
    char *marks[1] = { p0 };
    ReclaimJob rj = { arch, marks, 1, -999 };
    pthread_t rt_;
    mmapq_test_hook_set_reclaim_pre_unlink(park_hook_1);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&rt_, NULL, reclaim_main, &rj));
    wait_parked(1);
    release_park(0);              /* commit resumes: CAS, fence, re-check, min */
    release_park(1);
    pthread_join(t, NULL);
    pthread_join(rt_, NULL);
    mmapq_test_hook_set_commit_pre(NULL);
    mmapq_test_hook_set_reclaim_pre_unlink(NULL);

    TEST_ASSERT_FALSE(marked);
    TEST_ASSERT_EQUAL_INT(0, n);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT(0, rj.rc);   /* the re-list refused the mark */
    TEST_ASSERT_TRUE(path_exists(p0));
    assert_single_reads_X("c54p_ac_s", 3);

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
}

void test_c54_arm_c_batch(void)
{
    rm_queue("c54p_ac_b");
    mk_meta("c54p_ac_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_ac_b", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF, stage_batch(a, msg_small, sizeof msg_small));
    stage_three_big_batch(a);

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver("c54p_ac_b");

    BatchCommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_batch_commit_pre_publish(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(BOUND, batch_arch_so(arch));

    char *paths[16] = {0};
    int n = -1;
    int marked = batch_list_has_payload0(arch, paths, 16, &n);
    free_paths(paths, n);

    /* Explicit mark, as the single-message arm: n_marked == 0 would return
     * before the pre-unlink hook (indexed_batch_archiver.c:618 vs :657). */
    char p0[512]; payload0_path("c54p_ac_b", p0, sizeof p0);
    char *marks[1] = { p0 };
    BatchReclaimJob rj = { arch, marks, 1, -999 };
    pthread_t rt_;
    mmapq_test_hook_set_batch_reclaim_pre_unlink(park_hook_1);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&rt_, NULL, batch_reclaim_main, &rj));
    wait_parked(1);
    release_park(0);
    release_park(1);
    pthread_join(t, NULL);
    pthread_join(rt_, NULL);
    mmapq_test_hook_set_batch_commit_pre_publish(NULL);
    mmapq_test_hook_set_batch_reclaim_pre_unlink(NULL);

    TEST_ASSERT_FALSE(marked);
    TEST_ASSERT_EQUAL_INT(0, n);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT(0, rj.rc);
    TEST_ASSERT_TRUE(path_exists(p0));
    assert_batch_reads_X("c54p_ac_b", 3);

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
}

/* ════════════════════════════════════════════════════════════════════════
 * Holes 1 and 2 (spec §7.1/§7.2; handoff pins, both queue types) — the
 * REAL interleavings, driven through the CAND-54 hooks.
 *
 * Hole 1: commit min below the archived region -> the reclaim's re-list
 * reads start_offset BEFORE the min and the intent AFTER the clear (the
 * listing_pre_intent hook parks the lister in between) -> the wholly
 * archived file 0 is legitimately deleted -> crash -> the join must
 * anchor at the archive LINE (L(ii)) and never recreate the file.
 *
 * Hole 2: the same interleaving, then a second-generation commit into
 * the deleted file must get EINVAL (L(i): the pre-check floors at
 * max(start_offset, line)).
 *
 * These four pins CANNOT fail-first against the pre-fix tree: the
 * listing_pre_intent hook point and the batch commit_post_cas /
 * commit_min_pre hooks did not exist there (without the intra-lister
 * interleaving the intent simply blocks the deletion and the hole is
 * unreachable — the reviewer's stage-1 finding). Their acceptance
 * evidence is the mutation matrix (stage 2 report): L(ii) removed ->
 * hole-1 pins fail; L(i) removed -> hole-2 pins fail.
 * ════════════════════════════════════════════════════════════════════════ */

void test_c54_hole1_single(void)
{
    rm_queue("c54p_h1_s");
    mk_meta("c54p_h1_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_h1_s", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    stage_three_big_single(a);

    MmapqIndexedArchiver *arch = mk_archiver("c54p_h1_s");

    /* X commits at index 3, parked between the CAS and the re-check */
    CommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_commit_post_cas(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    /* archive covers X: so = line = 48772, si = 4 — file 0 wholly below */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 4));
    TEST_ASSERT_EQUAL_INT64(BOUND, arch_so(arch));
    TEST_ASSERT_EQUAL_INT64(BOUND, meta_field("c54p_h1_s", APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    /* let the commit run to the re-check, park just before the min */
    mmapq_test_hook_set_commit_post_cas(NULL);
    mmapq_test_hook_set_commit_min_pre(park_hook_1);
    release_park(0);
    wait_parked(1);

    /* the reclaim's re-list loads start_offset = 48772 (pre-min) and
     * parks before the intent load */
    char p0[512]; payload0_path("c54p_h1_s", p0, sizeof p0);
    char p1[512]; snprintf(p1, sizeof p1, "%s/c54p_h1_s/payload_0_1", DIR);
    /* Two marks, one a never-listed dummy: the reclaim's completeness
     * loop re-lists when the listing exactly fills the mark capacity
     * (indexed_archiver.c:632-639), and a second re-list would read the
     * post-min start_offset. n_marked=2 makes the interleaved re-list
     * final (1 < 2). Mirrors a real retry's marking. */
    char *marks[2] = { p0, p1 };
    ReclaimJob rj = { arch, marks, 2, -999 };
    pthread_t rt_;
    mmapq_test_hook_set_archiver_listing_pre_intent(park_hook_2);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&rt_, NULL, reclaim_main, &rj));
    wait_parked(2);

    /* the min lowers start_offset below the line and the commit clears
     * its intent — THEN the lister reads the cleared intent */
    release_park(1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_min_pre(NULL);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_h1_s", APPENDER_META_START_OFFSET_OFFSET));

    release_park(2);
    pthread_join(rt_, NULL);
    mmapq_test_hook_set_archiver_listing_pre_intent(NULL);
    /* the deletion is legitimate (spec §7.1 step 4): every entry in
     * file 0 is archived, no intent pending, nothing live */
    TEST_ASSERT_EQUAL_INT(1, rj.rc);
    TEST_ASSERT_FALSE(path_exists(p0));

    /* crash + join: clean close/reopen — the meta mmap persists the
     * min-lowered start_offset; nothing is in flight at close */
    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
    MmapqIndexedAppender *a2 = mk_appender("c54p_h1_s", 1, 0);

    /* L(ii): the no-live-entry join anchors at the archive line, so the
     * next write lands at the boundary — never in the reclaimed file. */
    int64_t off = mmapq_indexed_appender_append_uncommitted(a2, msg_small,
                                                            sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(BOUND, off);
    TEST_ASSERT_FALSE(path_exists(p0));

    mmapq_indexed_appender_close(a2);
}

void test_c54_hole1_batch(void)
{
    rm_queue("c54p_h1_b");
    mk_meta("c54p_h1_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_h1_b", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF, stage_batch(a, msg_small, sizeof msg_small));
    stage_three_big_batch(a);

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver("c54p_h1_b");

    /* X's batch commits at index 3, parked right after the publish CAS
     * (the CAND-54 batch hook — the pre-publish park cannot reach the
     * hole: post-fix the parked commit's own intent would block the
     * deletion) */
    BatchCommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_batch_commit_post_cas(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 4));
    TEST_ASSERT_EQUAL_INT64(BOUND, batch_arch_so(arch));
    TEST_ASSERT_EQUAL_INT64(BOUND, meta_field("c54p_h1_b", APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    mmapq_test_hook_set_batch_commit_post_cas(NULL);
    mmapq_test_hook_set_batch_commit_min_pre(park_hook_1);
    release_park(0);
    wait_parked(1);

    char p0[512]; payload0_path("c54p_h1_b", p0, sizeof p0);
    char p1[512]; snprintf(p1, sizeof p1, "%s/c54p_h1_b/payload_0_1", DIR);
    /* dummy second mark: keeps the reclaim's completeness loop from
     * re-listing (see test_c54_hole1_single) */
    char *marks[2] = { p0, p1 };
    BatchReclaimJob rj = { arch, marks, 2, -999 };
    pthread_t rt_;
    mmapq_test_hook_set_batch_archiver_listing_pre_intent(park_hook_2);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&rt_, NULL, batch_reclaim_main, &rj));
    wait_parked(2);

    release_park(1);              /* min -> 64, intent cleared */
    pthread_join(t, NULL);
    mmapq_test_hook_set_batch_commit_min_pre(NULL);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_h1_b", APPENDER_META_START_OFFSET_OFFSET));

    release_park(2);
    pthread_join(rt_, NULL);
    mmapq_test_hook_set_batch_archiver_listing_pre_intent(NULL);
    TEST_ASSERT_EQUAL_INT(1, rj.rc);   /* legitimate deletion (§7.1 step 4) */
    TEST_ASSERT_FALSE(path_exists(p0));

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
    MmapqIndexedBatchAppender *a2 = mk_batch_appender("c54p_h1_b", 1, 0);

    /* L(ii): no live entry (X's batch at index 3 < si = 4) — the join
     * anchors at the archive line and the next batch lands at 48772. */
    int64_t off = stage_batch(a2, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(BOUND, off);
    TEST_ASSERT_FALSE(path_exists(p0));

    mmapq_indexed_batch_appender_close(a2);
}

/* Hole 2: same interleaving as hole 1; then generation 2's commit into
 * the deleted file. L(i): EINVAL against max(start_offset, line). */

void test_c54_hole2_single(void)
{
    rm_queue("c54p_h2_s");
    mk_meta("c54p_h2_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_h2_s", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(SZ_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    stage_three_big_single(a);

    MmapqIndexedArchiver *arch = mk_archiver("c54p_h2_s");

    CommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_commit_post_cas(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 4));
    TEST_ASSERT_EQUAL_INT64(BOUND, arch_so(arch));

    mmapq_test_hook_set_commit_post_cas(NULL);
    mmapq_test_hook_set_commit_min_pre(park_hook_1);
    release_park(0);
    wait_parked(1);

    char p0[512]; payload0_path("c54p_h2_s", p0, sizeof p0);
    char p1[512]; snprintf(p1, sizeof p1, "%s/c54p_h2_s/payload_0_1", DIR);
    /* dummy second mark: keeps the reclaim's completeness loop from
     * re-listing (see test_c54_hole1_single) */
    char *marks[2] = { p0, p1 };
    ReclaimJob rj = { arch, marks, 2, -999 };
    pthread_t rt_;
    mmapq_test_hook_set_archiver_listing_pre_intent(park_hook_2);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&rt_, NULL, reclaim_main, &rj));
    wait_parked(2);

    /* the min lowers start_offset below the line BEFORE generation 2's
     * pre-check; the lister reads the cleared intent and deletes */
    release_park(1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_min_pre(NULL);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_h2_s", APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(BOUND, meta_field("c54p_h2_s", APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    release_park(2);
    pthread_join(rt_, NULL);
    mmapq_test_hook_set_archiver_listing_pre_intent(NULL);
    TEST_ASSERT_EQUAL_INT(1, rj.rc);
    TEST_ASSERT_FALSE(path_exists(p0));

    /* generation 2: Z @168 is between start_offset (64) and the line
     * (48772) — L(i): EINVAL. Without L(i) it commits into the deleted
     * file. */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_appender_commit(a, SZ_OFF));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
}

void test_c54_hole2_batch(void)
{
    rm_queue("c54p_h2_b");
    mk_meta("c54p_h2_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_h2_b", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF, stage_batch(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(ZB_OFF, stage_batch(a, msg_small, sizeof msg_small));
    stage_three_big_batch(a);

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver("c54p_h2_b");

    BatchCommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_batch_commit_post_cas(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 4));
    TEST_ASSERT_EQUAL_INT64(BOUND, batch_arch_so(arch));

    mmapq_test_hook_set_batch_commit_post_cas(NULL);
    mmapq_test_hook_set_batch_commit_min_pre(park_hook_1);
    release_park(0);
    wait_parked(1);

    char p0[512]; payload0_path("c54p_h2_b", p0, sizeof p0);
    char p1[512]; snprintf(p1, sizeof p1, "%s/c54p_h2_b/payload_0_1", DIR);
    /* dummy second mark: keeps the reclaim's completeness loop from
     * re-listing (see test_c54_hole1_single) */
    char *marks[2] = { p0, p1 };
    BatchReclaimJob rj = { arch, marks, 2, -999 };
    pthread_t rt_;
    mmapq_test_hook_set_batch_archiver_listing_pre_intent(park_hook_2);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&rt_, NULL, batch_reclaim_main, &rj));
    wait_parked(2);

    release_park(1);              /* min -> 64, intent cleared */
    pthread_join(t, NULL);
    mmapq_test_hook_set_batch_commit_min_pre(NULL);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_h2_b", APPENDER_META_START_OFFSET_OFFSET));

    release_park(2);
    pthread_join(rt_, NULL);
    mmapq_test_hook_set_batch_archiver_listing_pre_intent(NULL);
    TEST_ASSERT_EQUAL_INT(1, rj.rc);
    TEST_ASSERT_FALSE(path_exists(p0));

    /* generation 2: Z @172 is between start_offset (64) and the line —
     * L(i): EINVAL. Without L(i) it commits into the deleted file. */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(a, ZB_OFF));

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
}

/* ════════════════════════════════════════════════════════════════════════
 * L(i) semantics (ruling 1; handoff pin, both queue types): an uncommitted
 * payload between start_offset and the archive line is uncommittable
 * (EINVAL). The commit min (single: parked post-CAS across the archive;
 * batch: parked pre-publish) leaves start_offset = 64 < line = 48772, then
 * Z is committed. Pre-fix failure: the commit returns an index.
 * ════════════════════════════════════════════════════════════════════════ */

void test_c54_l1_semantics_single(void)
{
    rm_queue("c54p_l1_s");
    mk_meta("c54p_l1_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_l1_s", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(SZ_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    stage_three_big_single(a);

    MmapqIndexedArchiver *arch = mk_archiver("c54p_l1_s");

    CommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_commit_post_cas(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 4));
    release_park(0);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_post_cas(NULL);

    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    /* the geometry precondition: start_offset below the line */
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_l1_s", APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(BOUND, meta_field("c54p_l1_s", APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    /* Z @168: 64 <= 168 < 48772 — committable today, EINVAL under L(i) */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_appender_commit(a, SZ_OFF));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
}

void test_c54_l1_semantics_batch(void)
{
    rm_queue("c54p_l1_b");
    mk_meta("c54p_l1_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_l1_b", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF, stage_batch(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(ZB_OFF, stage_batch(a, msg_small, sizeof msg_small));
    stage_three_big_batch(a);

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver("c54p_l1_b");

    BatchCommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_batch_commit_pre_publish(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3));
    release_park(0);
    pthread_join(t, NULL);
    mmapq_test_hook_set_batch_commit_pre_publish(NULL);

    TEST_ASSERT_EQUAL_INT64(3, j.rc);
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_l1_b", APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(BOUND, meta_field("c54p_l1_b", APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(a, ZB_OFF));

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
}

/* ════════════════════════════════════════════════════════════════════════
 * Mismatched-flag reopen (handoff pin): the queue is archivable=1 on disk;
 * the appender is opened with archivable=0. The committer must branch on
 * the on-disk flag (cached at open from queue_meta), never the argument —
 * so arm C on this handle still blocks the listing. Fail-first against a
 * variant that caches the argument; pre-fix failure: no intent exists at
 * all, so payload_0_0 is listed and unlinked.
 * ════════════════════════════════════════════════════════════════════════ */

void test_c54_mismatched_flag_single(void)
{
    rm_queue("c54p_mm_s");
    mk_meta("c54p_mm_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_mm_s", 0, 0);   /* arg=0, disk=1 */
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    stage_three_big_single(a);

    MmapqIndexedArchiver *arch = mk_archiver("c54p_mm_s");

    CommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_commit_pre(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(BOUND, arch_so(arch));

    char *paths[16] = {0};
    int n = -1;
    int marked = list_has_payload0(arch, paths, 16, &n);
    free_paths(paths, n);

    release_park(0);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_pre(NULL);

    TEST_ASSERT_FALSE(marked);
    TEST_ASSERT_EQUAL_INT(0, n);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);

    char p0[512]; payload0_path("c54p_mm_s", p0, sizeof p0);
    TEST_ASSERT_TRUE(path_exists(p0));
    assert_single_reads_X("c54p_mm_s", 3);

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
}

void test_c54_mismatched_flag_batch(void)
{
    rm_queue("c54p_mm_b");
    mk_meta("c54p_mm_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_mm_b", 0, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF, stage_batch(a, msg_small, sizeof msg_small));
    stage_three_big_batch(a);

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver("c54p_mm_b");

    BatchCommitJob j = { a, X_OFF, -999 };
    pthread_t t;
    mmapq_test_hook_set_batch_commit_pre_publish(park_hook_0);
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_commit_main, &j));
    wait_parked(0);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(BOUND, batch_arch_so(arch));

    char *paths[16] = {0};
    int n = -1;
    int marked = batch_list_has_payload0(arch, paths, 16, &n);
    free_paths(paths, n);

    release_park(0);
    pthread_join(t, NULL);
    mmapq_test_hook_set_batch_commit_pre_publish(NULL);

    TEST_ASSERT_FALSE(marked);
    TEST_ASSERT_EQUAL_INT(0, n);
    TEST_ASSERT_EQUAL_INT64(3, j.rc);

    char p0[512]; payload0_path("c54p_mm_b", p0, sizeof p0);
    TEST_ASSERT_TRUE(path_exists(p0));
    assert_batch_reads_X("c54p_mm_b", 3);

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
}

/* ════════════════════════════════════════════════════════════════════════
 * Join clears a stale intent (ruling 2; handoff pin): a writer killed after
 * the intent store leaves the slot set; the join clears it, so the lane is
 * reclaimable again. The kill is simulated: the writer stages X and closes
 * (clean close writes nothing), the stale intent is poked into byte 24 (the
 * intent slot — padding today, never validated on open), then the appender
 * rejoins. Pre-fix failure: the slot still reads 64 after the join (nothing
 * clears it). The reclaim leg asserts the lane really is reclaimable; it
 * passes pre-fix (nothing reads intents) and is the liveness guard
 * post-fix.
 * ════════════════════════════════════════════════════════════════════════ */
#endif /* MMAPQ_TEST_HOOKS */

void test_c54_join_clears_intent_single(void)
{
    rm_queue("c54p_jc_s");
    mk_meta("c54p_jc_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_jc_s", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    mmapq_indexed_appender_close(a);

    poke_intent("c54p_jc_s", X_OFF);      /* killed after the intent store */
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_jc_s", APPENDER_META_INTENT_OFFSET));

    a = mk_appender("c54p_jc_s", 1, 0);   /* the join clears the stale intent */
    TEST_ASSERT_EQUAL_INT64(0, meta_field("c54p_jc_s", APPENDER_META_INTENT_OFFSET));

    /* the lane is reclaimable again */
    stage_three_big_single(a);
    MmapqIndexedArchiver *arch = mk_archiver("c54p_jc_s");
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 3));
    char *paths[16] = {0};
    int n = -1;
    TEST_ASSERT_TRUE(list_has_payload0(arch, paths, 16, &n));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_reclaim_files(arch, paths, n));
    free_paths(paths, n);
    char p0[512]; payload0_path("c54p_jc_s", p0, sizeof p0);
    TEST_ASSERT_FALSE(path_exists(p0));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
}

void test_c54_join_clears_intent_batch(void)
{
    rm_queue("c54p_jc_b");
    mk_meta("c54p_jc_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_jc_b", 1, 0);
    mmapq_indexed_batch_appender_close(a);

    poke_intent("c54p_jc_b", X_OFF);      /* a dead lane's stale intent */
    TEST_ASSERT_EQUAL_INT64(X_OFF, meta_field("c54p_jc_b", APPENDER_META_INTENT_OFFSET));

    a = mk_batch_appender("c54p_jc_b", 1, 0);
    TEST_ASSERT_EQUAL_INT64(0, meta_field("c54p_jc_b", APPENDER_META_INTENT_OFFSET));

    mmapq_indexed_batch_appender_close(a);
}

/* ════════════════════════════════════════════════════════════════════════
 * §6(c) refusal (handoff pin): archivable_files / reclaim_files refuse a
 * non-archivable queue explicitly. Today the refusal does not exist and the
 * header listing leans on the file-exists check when the creation
 * start_index > 0 (handoff, "§6(c) refusals"): the only thing keeping a
 * wholly-covered header file off the listing is that no code path creates
 * one. This pin manufactures that latent state with the PUBLIC creation
 * parameter (start_index = 8192, non-archivable) plus a planted header_0
 * (a byte copy of the real header_2 — headers below start_index are never
 * read), then asserts the listing and the reclaim refuse. Pre-fix failure:
 * header_0 is listed and unlinked on a non-archivable queue.
 * ════════════════════════════════════════════════════════════════════════ */

static void plant_header0(const char *name)
{
    char src[512], dst[512];
    snprintf(src, sizeof src, "%s/%s/header_2", DIR, name);
    snprintf(dst, sizeof dst, "%s/%s/header_0", DIR, name);
    char cmd[1100];
    snprintf(cmd, sizeof cmd, "cp %s %s", src, dst);
    TEST_ASSERT_EQUAL_INT(0, system(cmd));
}

static void refusal_single(void)
{
    rm_queue("c54p_rf_s");
    mk_meta("c54p_rf_s", 8192, 0, QUEUE_TYPE_NORMAL);   /* NOT archivable */
    MmapqIndexedAppender *a = mk_appender("c54p_rf_s", 0, 8192);
    TEST_ASSERT_EQUAL_INT64(8192,
        mmapq_indexed_appender_append(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(8193,
        mmapq_indexed_appender_append(a, msg_small, sizeof msg_small));
    mmapq_indexed_appender_close(a);
    plant_header0("c54p_rf_s");

    MmapqIndexedArchiver *arch = mk_archiver("c54p_rf_s");
    char *paths[16] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 16);
    TEST_ASSERT_EQUAL_INT(0, n);                 /* refusal: no listing */
    char h0[512];
    snprintf(h0, sizeof h0, "%s/c54p_rf_s/header_0", DIR);
    char *marks[1] = { h0 };
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_reclaim_files(arch, marks, 1));
    TEST_ASSERT_TRUE(path_exists(h0));           /* refusal: no unlink */
    free_paths(paths, n);

    mmapq_indexed_archiver_close(arch);
}

static void refusal_batch(void)
{
    rm_queue("c54p_rf_b");
    mk_meta("c54p_rf_b", 8192, 0, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_rf_b", 0, 8192);
    TEST_ASSERT_EQUAL_INT64(8192,
        mmapq_indexed_batch_appender_append_and_commit(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(8193,
        mmapq_indexed_batch_appender_append_and_commit(a, msg_small, sizeof msg_small));
    mmapq_indexed_batch_appender_close(a);
    plant_header0("c54p_rf_b");

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver("c54p_rf_b");
    char *paths[16] = {0};
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 16);
    TEST_ASSERT_EQUAL_INT(0, n);
    char h0[512];
    snprintf(h0, sizeof h0, "%s/c54p_rf_b/header_0", DIR);
    char *marks[1] = { h0 };
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_reclaim_files(arch, marks, 1));
    TEST_ASSERT_TRUE(path_exists(h0));
    free_paths(paths, n);

    mmapq_indexed_batch_archiver_close(arch);
}

void test_c54_refusal_single_nonarchivable(void) { refusal_single(); }
void test_c54_refusal_batch_nonarchivable(void)  { refusal_batch(); }

/* ════════════════════════════════════════════════════════════════════════
 * Publish failure clears the intent (ruling 2026-10-25, both queue types):
 * every failure return in commit_header / batch_publish precedes a
 * successful CAS, so nothing was published and the intent is cleared; a
 * retry stores it again. The header-wrap fault is the wrap_fault_oneshot
 * idiom armed at the commit's target header slot, reached via
 * mmapqi_appender_header_acc / mmapqi_batch_appender_header_acc
 * (internal/test_reach.h — CAND-58 removed the iterator alias
 * it->header_accessor == a->header_acc when the walker factories moved to
 * the walk accessor). No MMAPQ_TEST_HOOKS dependency —
 * runs in both build configs. Fail-first evidence: the mutant "no clear
 * on publish failure" fails both pins at the slot assert (stage-2
 * report, mutant M7).
 * ════════════════════════════════════════════════════════════════════════ */

/* the wrap_fault_oneshot.h idiom (D8), accessor-generic: the batch twin's
 * bq_* set is typed for its own arming helper, so this file carries its
 * own set rather than including the batch-typed header */
static WrapResult (*c54_oneshot_real_wrap)(RegionAccessor *, int64_t, Buffer *);
static int64_t c54_oneshot_pos;
static int     c54_oneshot_hits;
static WrapResult c54_oneshot_wrap(RegionAccessor *self, int64_t position, Buffer *buf)
{
    if (position == c54_oneshot_pos) {
        c54_oneshot_hits++;
        self->wrap = c54_oneshot_real_wrap;   /* one-shot: restore in place */
        return WRAP_MAP_TIMED_OUT;
    }
    return c54_oneshot_real_wrap(self, position, buf);
}

void test_c54_publish_failure_clears_intent_single(void)
{
    rm_queue("c54p_pf_s");
    mk_meta("c54p_pf_s", 0, 1, QUEUE_TYPE_NORMAL);
    MmapqIndexedAppender *a = mk_appender("c54p_pf_s", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF,
        mmapq_indexed_appender_append_uncommitted(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(0,
        mmapq_indexed_appender_append(a, msg_big, sizeof msg_big));

    /* CAND-58 removed the iterator alias (it->header_accessor ==
     * a->header_acc) this reach relied on — the commit path's header
     * accessor comes from internal/test_reach.h now. */
    RegionAccessor *acc = mmapqi_appender_header_acc(a);
    TEST_ASSERT_NOT_NULL(acc);
    c54_oneshot_real_wrap = acc->wrap;
    c54_oneshot_pos       = header_position(1);   /* the commit's CAS slot */
    c54_oneshot_hits      = 0;
    acc->wrap = c54_oneshot_wrap;
    region_accessor_invalidate_fast_path(acc);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT,
        mmapq_indexed_appender_commit(a, X_OFF));
    TEST_ASSERT_EQUAL_INT(1, c54_oneshot_hits);
    /* nothing was published — the intent slot reads 0 */
    TEST_ASSERT_EQUAL_INT64(0, meta_field("c54p_pf_s", APPENDER_META_INTENT_OFFSET));
    /* member retained, lane usable: the retry stores the intent again and
     * publishes at index 1 */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, X_OFF));
    TEST_ASSERT_EQUAL_INT64(0, meta_field("c54p_pf_s", APPENDER_META_INTENT_OFFSET));

    mmapq_indexed_appender_close(a);
}

void test_c54_publish_failure_clears_intent_batch(void)
{
    rm_queue("c54p_pf_b");
    mk_meta("c54p_pf_b", 0, 1, QUEUE_TYPE_BATCH);
    MmapqIndexedBatchAppender *a = mk_batch_appender("c54p_pf_b", 1, 0);
    TEST_ASSERT_EQUAL_INT64(X_OFF, stage_batch(a, msg_small, sizeof msg_small));
    TEST_ASSERT_EQUAL_INT64(0,
        mmapq_indexed_batch_appender_append_and_commit(a, msg_big, sizeof msg_big));

    /* the commit path's header accessor via internal/test_reach.h
     * (CAND-58 removed the iterator alias) */
    RegionAccessor *acc = mmapqi_batch_appender_header_acc(a);
    TEST_ASSERT_NOT_NULL(acc);
    c54_oneshot_real_wrap = acc->wrap;
    c54_oneshot_pos       = header_position(1);
    c54_oneshot_hits      = 0;
    acc->wrap = c54_oneshot_wrap;
    region_accessor_invalidate_fast_path(acc);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT,
        mmapq_indexed_batch_appender_commit_batch(a, X_OFF));
    TEST_ASSERT_EQUAL_INT(1, c54_oneshot_hits);
    TEST_ASSERT_EQUAL_INT64(0, meta_field("c54p_pf_b", APPENDER_META_INTENT_OFFSET));
    /* member retained: the retry publishes at index 1 */
    TEST_ASSERT_EQUAL_INT64(1,
        mmapq_indexed_batch_appender_commit_batch(a, X_OFF));
    TEST_ASSERT_EQUAL_INT64(0, meta_field("c54p_pf_b", APPENDER_META_INTENT_OFFSET));

    mmapq_indexed_batch_appender_close(a);
}

/* ── registration ── */

int main(void)
{
    UNITY_BEGIN();
#ifdef MMAPQ_TEST_HOOKS
    RUN_TEST(test_c54_arm_c_single);
    RUN_TEST(test_c54_arm_c_batch);
    RUN_TEST(test_c54_hole1_single);
    RUN_TEST(test_c54_hole1_batch);
    RUN_TEST(test_c54_hole2_single);
    RUN_TEST(test_c54_hole2_batch);
    RUN_TEST(test_c54_l1_semantics_single);
    RUN_TEST(test_c54_l1_semantics_batch);
    RUN_TEST(test_c54_mismatched_flag_single);
    RUN_TEST(test_c54_mismatched_flag_batch);
#endif
    RUN_TEST(test_c54_join_clears_intent_single);
    RUN_TEST(test_c54_join_clears_intent_batch);
    RUN_TEST(test_c54_refusal_single_nonarchivable);
    RUN_TEST(test_c54_refusal_batch_nonarchivable);
    RUN_TEST(test_c54_publish_failure_clears_intent_single);
    RUN_TEST(test_c54_publish_failure_clears_intent_batch);
    return UNITY_END();
}
