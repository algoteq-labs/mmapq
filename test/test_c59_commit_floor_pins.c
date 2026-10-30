/* CAND-59 pins (audit/CAND-59-spec.md v1.1 §6): the commit path's header
 * wrap below a raised start_index.
 *
 * Geometry mirrors the card's repro (REGION=MAXFILE=32768 → 4096 header
 * slots per header file, RING=4, archivable=1): lane B pre-fills PRE_FILL
 * (4050) commits, the stale lane A/C joins (stale slot 4050, header_0),
 * stages two records, parks its commit of the first (commit_pre /
 * batch_commit_pre_publish — the slow path); B fills to FULL_FILL (8300);
 * archive + reclaim; release.
 *
 * Pin cells (spec §6):
 *   P1 warm (card variant i, regression): archive to 4097 — SAFE both eras.
 *   P2 scan (variant ii): archive to 8193 — post-fix the entry clamp jumps
 *       A to si. (The async cell is the card's masked cell: green pre-fix
 *       via the join premap's warm real data — recorded as such.)
 *   P3 map service of a reclaimed file (variant iii, made deterministic):
 *       a test-thread wrap of the reclaimed file below si must NOT
 *       re-create it, and the commit must not ride the zeros. (The card's
 *       join-time parked premap is racy by construction: the parked mapper
 *       thread can stall the same join's payload eager wrap — this cell
 *       drives the same map service from the test thread instead.)
 *   P4 batch twin of P2.
 *   P5 two-stale-lanes (items 1+2+4): a hand-planted stray zero header_1
 *       (the residual-window artifact) — lane C must not commit below si
 *       into it. Sync: parked post-clamp (mtlh_clamped) while si moves.
 *       Async: C's cold map of the stray is serviced (parked) only after
 *       si moved and the stray was planted — the adoption-time floor check
 *       is the only guard. The eviction sweep makes the file-1 mapping
 *       cold and evicts its cached fd so the map goes to the filesystem.
 *   P6 stray-zero-file (item 5, library half): the stray is listed by
 *       archivable_files, reclaim deletes it, the planted archived copy is
 *       byte-unchanged (no library path writes it).
 *   P7 frontier creation regression (the new explicit create path).
 *   P8 the no-progress detector (v1.1): forced floor rejections surface
 *       MMAPQ_ECORRUPT + the no-progress stderr line, never a timeout.
 *
 * Fail-first (spec §6): P2-sync/P3/P4-sync/P5/P8 observed red on the
 * hooks-only scratch checkout (5488a16 + the hook hunks, no fix); P1,
 * P2-async, P4-async (masked cells), P6, P7 green both eras (regression /
 * documentation pins — recorded with that reason). Every async cell uses
 * its own runtime so a failing cell cannot pollute a later one.
 */

#include "indexed_appender.h"
#include "indexed_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_archiver.h"
#include "internal/queue_meta.h"
#include "internal/appender_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/region_accessor.h"
#include "internal/test_hooks.h"
#include "internal/test_reach.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "unity.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>

#ifdef MMAPQ_TEST_HOOKS

static const char    *DIR     = "/tmp";
static const int32_t  REGION  = 32768;
static const int64_t  MAXFILE = 32768;
static const int      RING    = 4;
static const uint64_t TIMEOUT = 5000000000ULL;

#define SMALL_LEN   100
#define PRE_FILL    4050     /* B's commits before the stale lane joins */
#define FULL_FILL   8300     /* B's total commits (> 2 header files) */
#define H1_BYTES    32768    /* one header file */

static char msg[SMALL_LEN];

/* ── queue/dir helpers ── */

static void rm_queue(const char *name)
{
    char cmd[512];
    snprintf(cmd, sizeof cmd, "rm -rf %s/%s", DIR, name);
    int rc = system(cmd); (void)rc;
    char d[512];
    snprintf(d, sizeof d, "%s/%s", DIR, name);
    TEST_ASSERT_EQUAL_INT(0, mkdir(d, 0775));
}

static void qpath(const char *name, const char *file, char *out, size_t n)
{
    snprintf(out, n, "%s/%s/%s", DIR, name, file);
}

static int file_exists(const char *name, const char *file)
{
    char p[512]; qpath(name, file, p, sizeof p);
    return access(p, F_OK) == 0;
}

static int file_all_zero(const char *name, const char *file)
{
    char p[512]; qpath(name, file, p, sizeof p);
    FILE *f = fopen(p, "rb");
    if (f == NULL) return 0;
    unsigned char b[4096];
    size_t n;
    int zero = 1;
    while ((n = fread(b, 1, sizeof b, f)) > 0) {
        for (size_t k = 0; k < n; k++)
            if (b[k] != 0) { zero = 0; break; }
        if (!zero) break;
    }
    fclose(f);
    return zero;
}

/* Plant the residual-window artifact: a zero-filled header file below si
 * (post-fix code cannot create one — the pin plants it, standing for lane
 * A's residual-window re-creation). */
static void plant_stray_zero_header(const char *name, const char *file)
{
    char p[512]; qpath(name, file, p, sizeof p);
    FILE *f = fopen(p, "wb");
    TEST_ASSERT_NOT_NULL(f);
    static unsigned char z[4096];
    for (long off = 0; off < H1_BYTES; off += (long)sizeof z)
        TEST_ASSERT_EQUAL_INT((int)sizeof z, (int)fwrite(z, 1, sizeof z, f));
    fclose(f);
}

/* Plant the archived copy (the app-side archive directory; the library
 * never writes there). */
static void plant_archive_copy(const char *name, const char *base)
{
    char d[512]; qpath(name, "archive", d, sizeof d);
    int mrc = mkdir(d, 0775);
    TEST_ASSERT_TRUE(mrc == 0 || errno == EEXIST);
    char p[1100];
    snprintf(p, sizeof p, "%s/%s", d, base);
    FILE *f = fopen(p, "wb");
    TEST_ASSERT_NOT_NULL(f);
    unsigned char b[4096];
    memset(b, 0x5A, sizeof b);
    for (long off = 0; off < H1_BYTES; off += (long)sizeof b)
        TEST_ASSERT_EQUAL_INT((int)sizeof b, (int)fwrite(b, 1, sizeof b, f));
    fclose(f);
}

static uint64_t archived_copy_crc(const char *name, const char *base)
{
    char d[512]; qpath(name, "archive", d, sizeof d);
    char p[1100];
    snprintf(p, sizeof p, "%s/%s", d, base);
    FILE *f = fopen(p, "rb");
    if (f == NULL) return 0;
    unsigned char b[4096];
    size_t n;
    uint64_t h = 1469598103934665603ULL;
    while ((n = fread(b, 1, sizeof b, f)) > 0)
        for (size_t k = 0; k < n; k++) { h ^= b[k]; h *= 1099511628211ULL; }
    fclose(f);
    return h;
}

static int64_t meta_si(const char *name, uint8_t qtype)
{
    QueueMeta *qm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_ro(DIR, name, qtype, &qm));
    int64_t v = queue_meta_start_index(qm);
    mmapqi_queue_meta_close(qm);
    return v;
}

/* read the header at idx via a fresh RO accessor; 1 present, 0 NULL */
static int read_header_at(const char *name, int64_t idx, uint8_t *lane_out)
{
    QueueRegionAccessor *ro = NULL;
    if (mmapqi_queue_region_accessor_create_ro(DIR, name, REGION, RING, 0,
                                               MAXFILE, TIMEOUT, NULL, &ro) != 0)
        return -1;
    RegionAccessor *hacc = queue_region_accessor_header(ro);
    Buffer buf = {0};
    uint64_t h = HEADER_NULL;
    int hrc = mmapqi_header_read_checked(hacc, &buf, idx, &h);
    mmapqi_queue_region_accessor_close(ro);
    if (hrc != 0) return -1;
    if (h == HEADER_NULL) return 0;
    *lane_out = header_appender_id(h);
    return 1;
}

/* does the next archivable_files listing contain base? */
static int listing_contains(const char *name, int batch, const char *base)
{
    char *paths[64];
    int n;
    if (batch) {
        MmapqIndexedBatchArchiver *ba = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_archiver_create(DIR, name, RING, NULL, &ba));
        n = mmapq_indexed_batch_archiver_archivable_files(ba, paths, 64);
        mmapq_indexed_batch_archiver_close(ba);
    } else {
        MmapqIndexedArchiver *a = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_archiver_create(DIR, name, RING, NULL, &a));
        n = mmapq_indexed_archiver_archivable_files(a, paths, 64);
        mmapq_indexed_archiver_close(a);
    }
    TEST_ASSERT_TRUE(n >= 0);
    int found = 0;
    for (int i = 0; i < n; i++) {
        const char *b = strrchr(paths[i], '/');
        b = b ? b + 1 : paths[i];
        if (strcmp(b, base) == 0) found = 1;
        free(paths[i]);
    }
    return found;
}

/* ── park machinery (slots: 0 = commit, 1 = map service, 2 = clamped) ── */

#define PARK_SLOTS 3
static _Atomic int park_fired[PARK_SLOTS], park_release[PARK_SLOTS];

static void park_reset(void)
{
    for (int i = 0; i < PARK_SLOTS; i++) {
        atomic_store(&park_fired[i], 0);
        atomic_store(&park_release[i], 0);
    }
}

static void park_slot(int slot)
{
    atomic_store(&park_fired[slot], 1);
    for (long s = 0; s < 200000000L && !atomic_load(&park_release[slot]); s++)
        sched_yield();
}

static void wait_parked(int slot)
{
    for (long w = 0; !atomic_load(&park_fired[slot]); w++) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
        TEST_ASSERT_TRUE_MESSAGE(w < 10000, "park never fired (vacuous geometry)");
    }
}

/* ── hook callbacks ── */

static int mtlh_count[256];            /* failed-CAS count per lane */
static void on_mtlh(uint8_t lane) { mtlh_count[lane]++; }

static int arm_commit_park = 0;
static void on_commit_pre(void) { if (arm_commit_park) park_slot(0); }
static void on_batch_pre_publish(void) { if (arm_commit_park) park_slot(0); }

static int64_t clamped_idx = -1;
static void on_clamped(uint8_t lane, int64_t idx)
{
    (void)lane;
    clamped_idx = idx;
    if (arm_commit_park) park_slot(2);
}

/* park the RW header map service whose requested position is in
 * [svc_park_lo, svc_park_hi) */
static int64_t svc_park_lo = -1, svc_park_hi = -1;
static void on_map_process(const char *prefix, int is_rw, int64_t requested_position)
{
    if (svc_park_lo >= 0 && strstr(prefix, "header") != NULL && is_rw &&
        requested_position >= svc_park_lo && requested_position < svc_park_hi)
        park_slot(1);
}

/* ── commit threads ── */

typedef struct { MmapqIndexedAppender *a; int64_t off; int64_t rc; } CommitJob;
static void *commit_main(void *p)
{
    CommitJob *j = p;
    j->rc = mmapq_indexed_appender_commit(j->a, j->off);
    return NULL;
}

typedef struct { MmapqIndexedBatchAppender *a; int64_t off; int64_t rc; } BCommitJob;
static void *bcommit_main(void *p)
{
    BCommitJob *j = p;
    j->rc = mmapq_indexed_batch_appender_commit_batch(j->a, j->off);
    return NULL;
}

/* shared scenario top: B pre-fills, the stale lane joins and stages two
 * records, its commit of the first parks at commit_pre */
static MmapqIndexedAppender *stale_lane_setup(const char *name, MmapqRuntime *rt_lane,
                                              MmapqRuntime *rt_fill,
                                              MmapqIndexedAppender **b_out,
                                              int64_t *x1_out, uint8_t *lane_out)
{
    MmapqIndexedAppender *b = NULL, *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt_fill, 0, &b));
    for (int i = 0; i < PRE_FILL; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(b, msg, SMALL_LEN));

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt_lane, 0, &a));
    *lane_out = mmapq_indexed_appender_id(a);

    int64_t x1 = mmapq_indexed_appender_append_uncommitted(a, msg, SMALL_LEN);
    TEST_ASSERT_TRUE(x1 > 0);
    /* the second record makes the commit of the first take the SLOW path
     * (not the flag-tracked tail), where commit_pre fires */
    TEST_ASSERT_TRUE(mmapq_indexed_appender_append_uncommitted(a, msg, SMALL_LEN) > x1);

    *b_out = b;
    *x1_out = x1;
    return a;
}

static void fill_archive_reclaim(const char *name, MmapqIndexedAppender *b,
                                 MmapqIndexedArchiver *arch, int64_t archive_to)
{
    /* B always fills to FULL_FILL first (every variant); only the archive
     * point differs */
    for (int i = PRE_FILL; i < FULL_FILL; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(b, msg, SMALL_LEN));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, archive_to));
    TEST_ASSERT_EQUAL_INT64(archive_to, meta_si(name, QUEUE_TYPE_NORMAL));
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_TRUE(n > 0);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
}

/* ── P1/P2: the single-message warm/scan cells ── */

static void p12_single(int use_async, const char *name, int64_t archive_to)
{
    rm_queue(name);
    MmapqRuntime *rt = NULL;
    if (use_async) TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt));
    int nf0 = mmapq_test_hook_c59_header_wrap_not_found_count();

    MmapqIndexedAppender *b = NULL;
    int64_t x1;
    uint8_t a_lane;
    MmapqIndexedAppender *a = stale_lane_setup(name, rt, rt, &b, &x1, &a_lane);
    mtlh_count[a_lane] = 0;

    arm_commit_park = 1;
    mmapq_test_hook_set_commit_pre(on_commit_pre);
    mmapq_test_hook_set_mtlh_entry(on_mtlh);
    CommitJob j = { a, x1, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_create(DIR, name, RING, rt, &arch));
    fill_archive_reclaim(name, b, arch, archive_to);

    TEST_ASSERT_FALSE(file_exists(name, "header_0"));
    if (archive_to > 4097) TEST_ASSERT_FALSE(file_exists(name, "header_1"));

    atomic_store(&park_release[0], 1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_pre(NULL);
    mmapq_test_hook_set_mtlh_entry(NULL);

    /* the commit jumps to the frontier: exactly FULL_FILL, ≥ si */
    TEST_ASSERT_EQUAL_INT64(FULL_FILL, j.rc);
    TEST_ASSERT_TRUE(j.rc >= archive_to);
    TEST_ASSERT_EQUAL_INT(1, mtlh_count[a_lane]);          /* exactly 1 failed CAS */
    /* the clamp jumps before any below-si probe: the NOT_FOUND handler
     * never engaged */
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_test_hook_c59_header_wrap_not_found_count() - nf0);
    /* nothing re-created, nothing re-listed */
    TEST_ASSERT_FALSE(file_exists(name, "header_0"));
    if (archive_to > 4097) {
        TEST_ASSERT_FALSE(file_exists(name, "header_1"));
        TEST_ASSERT_FALSE(listing_contains(name, 0, "header_1"));
    }
    TEST_ASSERT_FALSE(listing_contains(name, 0, "header_0"));
    /* A's entry is really there, at the frontier, A's own lane */
    uint8_t lane = 0xFF;
    TEST_ASSERT_EQUAL_INT(1, read_header_at(name, FULL_FILL, &lane));
    TEST_ASSERT_EQUAL_UINT8(a_lane, lane);

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_appender_close(b);
    if (rt != NULL) { mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
}

/* ── P3: a map service of a reclaimed file must not re-create it ──
 *
 * Both runtimes; the map is driven from the test thread through the stale
 * lane's own RW header accessor (test_reach), with the commit still
 * parked — the card's variant (iii) essence without the racy join-time
 * premap park. The sweep makes the file-1 mapping cold and evicts its
 * cached fd, so the map goes to the filesystem. */

static const int64_t sweep_pos[] = { 65536, 98304, 163840, 196608, 229376 };

static void evict_file1(const char *name, MmapqIndexedAppender *a)
{
    (void)name;
    RegionAccessor *acc = mmapqi_appender_header_acc(a);
    TEST_ASSERT_NOT_NULL(acc);
    Buffer buf = {0};
    /* regions 2,3,5,6,7: slot 1 is remapped by region 5 (region 0 stays
     * warm in slot 0); the rolled cache (cap ring+2 = 6) evicts file 1's
     * fd once 7 files are touched. Results ignored — post-fix these are
     * NOT_FOUND (the files do not exist yet), pre-fix they create legal
     * above-frontier zero files. */
    for (unsigned i = 0; i < sizeof sweep_pos / sizeof sweep_pos[0]; i++)
        (void)region_accessor_wrap_no_premap(acc, sweep_pos[i], &buf);
}

static void p3_premap(int use_async, const char *name)
{
    rm_queue(name);
    MmapqRuntime *rtc = NULL;
    if (use_async) TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rtc));

    MmapqIndexedAppender *b = NULL;
    int64_t x1;
    uint8_t c_lane;
    MmapqIndexedAppender *c = stale_lane_setup(name, rtc, NULL, &b, &x1, &c_lane);
    mtlh_count[c_lane] = 0;

    arm_commit_park = 1;
    mmapq_test_hook_set_commit_pre(on_commit_pre);
    mmapq_test_hook_set_mtlh_entry(on_mtlh);
    CommitJob j = { c, x1, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    evict_file1(name, c);   /* C's commit thread is parked: no accessor race */

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_create(DIR, name, RING, NULL, &arch));
    fill_archive_reclaim(name, b, arch, 8193);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));

    /* the map service, before any commit activity: must NOT re-create */
    RegionAccessor *acc = mmapqi_appender_header_acc(c);
    Buffer buf = {0};
    (void)region_accessor_wrap_no_premap(acc, header_position(4096), &buf);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));

    atomic_store(&park_release[0], 1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_pre(NULL);
    mmapq_test_hook_set_mtlh_entry(NULL);

    TEST_ASSERT_EQUAL_INT64(FULL_FILL, j.rc);
    TEST_ASSERT_EQUAL_INT(1, mtlh_count[c_lane]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));
    TEST_ASSERT_FALSE(listing_contains(name, 0, "header_1"));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(c);
    mmapq_indexed_appender_close(b);
    if (rtc != NULL) { mmapq_runtime_close(rtc); mmapq_runtime_destroy(rtc); }
}

/* ── P4: the batch twin of P2 ── */

static void p4_batch(int use_async, const char *name)
{
    rm_queue(name);
    MmapqRuntime *rt = NULL;
    if (use_async) TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt));
    int nf0 = mmapq_test_hook_c59_header_wrap_not_found_count();

    MmapqIndexedBatchAppender *b = NULL, *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt, 0, &b));
    for (int i = 0; i < PRE_FILL; i++)
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_batch_appender_append_and_commit(b, msg, SMALL_LEN));

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt, 0, &a));
    uint8_t a_lane = mmapq_indexed_batch_appender_id(a);
    mtlh_count[a_lane] = 0;

    MmapqMutableBatch *h = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &h));
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(h, msg, SMALL_LEN));
    int64_t xoff = mmapq_mutable_batch_complete_uncommitted(h);
    TEST_ASSERT_TRUE(xoff > 0);

    arm_commit_park = 1;
    mmapq_test_hook_set_batch_commit_pre_publish(on_batch_pre_publish);
    mmapq_test_hook_set_mtlh_entry_batch(on_mtlh);
    BCommitJob j = { a, xoff, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, bcommit_main, &j));
    wait_parked(0);

    for (int i = PRE_FILL; i < FULL_FILL; i++)
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_batch_appender_append_and_commit(b, msg, SMALL_LEN));

    MmapqIndexedBatchArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, name, RING, rt, &arch));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 8193));
    TEST_ASSERT_EQUAL_INT64(8193, meta_si(name, QUEUE_TYPE_BATCH));
    char *paths[64];
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_TRUE(n > 0);
    TEST_ASSERT_TRUE(mmapq_indexed_batch_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));

    atomic_store(&park_release[0], 1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_batch_commit_pre_publish(NULL);
    mmapq_test_hook_set_mtlh_entry_batch(NULL);

    TEST_ASSERT_EQUAL_INT64(FULL_FILL, j.rc);
    TEST_ASSERT_EQUAL_INT(1, mtlh_count[a_lane]);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_test_hook_c59_header_wrap_not_found_count() - nf0);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));
    TEST_ASSERT_FALSE(listing_contains(name, 1, "header_1"));

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_appender_close(b);
    if (rt != NULL) { mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
}

/* ── P5: two stale lanes — C opens the stray zero file A re-created ── */

static void p5_sync(void)
{
    const char *name = "c59p5s";
    rm_queue(name);

    MmapqIndexedAppender *b = NULL;
    int64_t x1;
    uint8_t c_lane;
    MmapqIndexedAppender *c = stale_lane_setup(name, NULL, NULL, &b, &x1, &c_lane);
    mtlh_count[c_lane] = 0;

    arm_commit_park = 1;
    mmapq_test_hook_set_commit_pre(on_commit_pre);
    mmapq_test_hook_set_mtlh_entry(on_mtlh);
    mmapq_test_hook_set_mtlh_clamped(on_clamped);
    CommitJob j = { c, x1, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    /* si1 = 4097: header_0 reclaimed; header_1 live */
    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_create(DIR, name, RING, NULL, &arch));
    fill_archive_reclaim(name, b, arch, 4097);
    TEST_ASSERT_FALSE(file_exists(name, "header_0"));
    TEST_ASSERT_TRUE(file_exists(name, "header_1"));

    /* release the commit: C's CAS on warm full header_0 data fails, mtlh
     * clamps to si1=4097 and parks there (the clamped hook) */
    atomic_store(&park_release[0], 1);
    wait_parked(2);

    /* si2 = 8193 behind the clamped lane; reclaim header_1; plant the
     * residual-window stray (post-fix code cannot create it) */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 8193));
    TEST_ASSERT_EQUAL_INT64(8193, meta_si(name, QUEUE_TYPE_NORMAL));
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_TRUE(n > 0);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));
    plant_stray_zero_header(name, "header_1");
    TEST_ASSERT_TRUE(file_all_zero(name, "header_1"));

    atomic_store(&park_release[2], 1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_pre(NULL);
    mmapq_test_hook_set_mtlh_entry(NULL);
    mmapq_test_hook_set_mtlh_clamped(NULL);

    /* C must not commit below si2: the item-2 floor check rejects the
     * stray's zeros and C jumps to the frontier. */
    TEST_ASSERT_EQUAL_INT64(FULL_FILL, j.rc);
    TEST_ASSERT_EQUAL_INT(1, mtlh_count[c_lane]);
    /* the stray was never committed into: still all zero */
    TEST_ASSERT_TRUE(file_exists(name, "header_1"));
    TEST_ASSERT_TRUE(file_all_zero(name, "header_1"));

    /* ...and reclaim deletes it (item 5's tail) */
    n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    int found = 0;
    for (int i = 0; i < n; i++) {
        const char *base = strrchr(paths[i], '/');
        if (strcmp(base ? base + 1 : paths[i], "header_1") == 0) found = 1;
    }
    TEST_ASSERT_TRUE(found);   /* the re-list vector stays visible (loud) */
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(c);
    mmapq_indexed_appender_close(b);
}

static void p5_async(void)
{
    const char *name = "c59p5a";
    rm_queue(name);
    MmapqRuntime *rtc = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rtc));

    MmapqIndexedAppender *b = NULL;
    int64_t x1;
    uint8_t c_lane;
    MmapqIndexedAppender *c = stale_lane_setup(name, rtc, NULL, &b, &x1, &c_lane);
    mtlh_count[c_lane] = 0;

    arm_commit_park = 1;
    mmapq_test_hook_set_commit_pre(on_commit_pre);
    mmapq_test_hook_set_mtlh_entry(on_mtlh);
    CommitJob j = { c, x1, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, commit_main, &j));
    wait_parked(0);

    evict_file1(name, c);   /* C's commit thread is parked: no accessor race */

    /* si1 = 4097: header_0 reclaimed; header_1 live */
    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_create(DIR, name, RING, NULL, &arch));
    fill_archive_reclaim(name, b, arch, 4097);

    /* release the commit: C's CAS fails on warm full header_0 data, mtlh
     * clamps to si1=4097 (post-fix) and wraps file 1 — cold after the
     * sweep — and the map SERVICE parks */
    svc_park_lo = 32768; svc_park_hi = 65536;
    mmapq_test_hook_set_async_map_process_pre(on_map_process);
    atomic_store(&park_release[0], 1);
    wait_parked(1);
    mmapq_test_hook_set_async_map_process_pre(NULL);

    /* si2 = 8193 while the service is parked; reclaim header_1; plant the
     * stray; then release the service — it maps the stray (the file
     * EXISTS) and C adopts it: the adoption-time floor check (items 2+4)
     * is the only guard. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 8193));
    TEST_ASSERT_EQUAL_INT64(8193, meta_si(name, QUEUE_TYPE_NORMAL));
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_TRUE(n > 0);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));
    plant_stray_zero_header(name, "header_1");

    atomic_store(&park_release[1], 1);
    pthread_join(t, NULL);
    mmapq_test_hook_set_commit_pre(NULL);
    mmapq_test_hook_set_mtlh_entry(NULL);

    TEST_ASSERT_EQUAL_INT64(FULL_FILL, j.rc);
    TEST_ASSERT_EQUAL_INT(1, mtlh_count[c_lane]);
    TEST_ASSERT_TRUE(file_exists(name, "header_1"));
    TEST_ASSERT_TRUE(file_all_zero(name, "header_1"));   /* never committed into */

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(c);
    mmapq_indexed_appender_close(b);
    mmapq_runtime_close(rtc);
    mmapq_runtime_destroy(rtc);
}

/* ── P6: the stray zero file (item 5, library half) ── */

static void p6_stray(void)
{
    const char *name = "c59p6";
    rm_queue(name);

    MmapqIndexedAppender *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, NULL, 0, &b));
    for (int i = 0; i < FULL_FILL; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(b, msg, SMALL_LEN));

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_create(DIR, name, RING, NULL, &arch));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 8193));
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_TRUE(n > 0);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));

    /* the residual-window stray + the previously archived copy */
    plant_stray_zero_header(name, "header_1");
    plant_archive_copy(name, "header_1");
    uint64_t crc_before = archived_copy_crc(name, "header_1");
    TEST_ASSERT_TRUE(crc_before != 0);

    /* the re-list vector stays visible: the stray IS listed... */
    TEST_ASSERT_TRUE(listing_contains(name, 0, "header_1"));
    /* ...no library path writes the archived copy... */
    TEST_ASSERT_EQUAL_UINT64(crc_before, archived_copy_crc(name, "header_1"));

    /* ...and reclaim deletes the stray from the live dir */
    n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_TRUE(n > 0);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_reclaim_files(arch, paths, n) > 0);
    for (int i = 0; i < n; i++) free(paths[i]);
    TEST_ASSERT_FALSE(file_exists(name, "header_1"));
    TEST_ASSERT_EQUAL_UINT64(crc_before, archived_copy_crc(name, "header_1"));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(b);
}

/* ── P7: frontier creation still works (regression) ── */

static void p7_frontier(int use_async, const char *name)
{
    rm_queue(name);
    MmapqRuntime *rt = NULL;
    if (use_async) TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt));

    MmapqIndexedAppender *a = NULL, *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt, 0, &a));
    /* crosses header files 0 → 1 → 2: every frontier file is created via
     * the explicit path (post-fix) — exact indices throughout */
    for (int i = 0; i < FULL_FILL; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, msg, SMALL_LEN));
    TEST_ASSERT_TRUE(file_exists(name, "header_0"));
    TEST_ASSERT_TRUE(file_exists(name, "header_1"));
    TEST_ASSERT_TRUE(file_exists(name, "header_2"));

    /* a second lane joins and appends past the frontier */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt, 0, &b));
    TEST_ASSERT_EQUAL_INT64(FULL_FILL,
        mmapq_indexed_appender_append(b, msg, SMALL_LEN));
    TEST_ASSERT_EQUAL_INT64(FULL_FILL + 1,
        mmapq_indexed_appender_append(a, msg, SMALL_LEN));

    mmapq_indexed_appender_close(a);
    mmapq_indexed_appender_close(b);
    if (rt != NULL) { mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
}

/* ── P8: the no-progress detector surfaces ECORRUPT (v1.1) ── */

static void p8_no_progress(int use_async, const char *name)
{
    rm_queue(name);
    MmapqRuntime *rt = NULL;
    if (use_async) TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt));

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(
        DIR, name, REGION, RING, MAXFILE, TIMEOUT, 1, rt, 0, &a));
    /* fill exactly one header file; all warm, no floor-check site hits */
    for (int i = 0; i < 4096; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, msg, SMALL_LEN));

    int nf0 = mmapq_test_hook_c59_header_wrap_not_found_count();

    /* force two floor rejections at the boundary-crossing commit: the
     * handler creates/EEXISTs header_1 but the re-wrap is rejected again
     * with a stagnant floor → no-progress → ECORRUPT. */
    mmapq_test_hook_set_c59_force_floor_reject(2);

    char cap[512]; qpath(name, "stderr.cap", cap, sizeof cap);
    fflush(stderr);
    int saved = dup(STDERR_FILENO);
    int capfd = open(cap, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT_TRUE(capfd >= 0);
    TEST_ASSERT_TRUE(dup2(capfd, STDERR_FILENO) >= 0);
    int64_t rc = mmapq_indexed_appender_append(a, msg, SMALL_LEN);
    fflush(stderr);
    TEST_ASSERT_TRUE(dup2(saved, STDERR_FILENO) >= 0);
    close(saved);
    close(capfd);
    mmapq_test_hook_set_c59_force_floor_reject(0);

    /* a bug, not a timeout: exactly ECORRUPT, never ETIMEDOUT/-1 */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT, rc);
    /* the handler engaged exactly once (the initial genuine NOT_FOUND) */
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_test_hook_c59_header_wrap_not_found_count() - nf0);

    FILE *f = fopen(cap, "rb");
    TEST_ASSERT_NOT_NULL(f);
    char buf[8192];
    size_t got = fread(buf, 1, sizeof buf - 1, f);
    buf[got] = 0;
    fclose(f);
    TEST_ASSERT_NOT_NULL(strstr(buf, "no progress"));
    TEST_ASSERT_NULL(strstr(buf, "1024-retry cap"));

    mmapq_indexed_appender_close(a);
    if (rt != NULL) { mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
}

/* ── runtime-variant wrappers ── */

static void test_c59_p1_warm_sync(void)  { park_reset(); p12_single(0, "c59p1s", 4097); }
static void test_c59_p1_warm_async(void) { park_reset(); p12_single(1, "c59p1a", 4097); }
static void test_c59_p2_scan_sync(void)  { park_reset(); p12_single(0, "c59p2s", 8193); }
static void test_c59_p2_scan_async(void) { park_reset(); p12_single(1, "c59p2a", 8193); }
static void test_c59_p3_premap_sync(void)  { park_reset(); p3_premap(0, "c59p3s"); }
static void test_c59_p3_premap_async(void) { park_reset(); p3_premap(1, "c59p3a"); }
static void test_c59_p4_batch_sync(void) { park_reset(); p4_batch(0, "c59p4s"); }
static void test_c59_p4_batch_async(void){ park_reset(); p4_batch(1, "c59p4a"); }
static void test_c59_p5_two_stale_sync(void)  { park_reset(); p5_sync(); }
static void test_c59_p5_two_stale_async(void) { park_reset(); p5_async(); }
static void test_c59_p6_stray_zero_file(void) { park_reset(); p6_stray(); }
static void test_c59_p7_frontier_sync(void)   { park_reset(); p7_frontier(0, "c59p7s"); }
static void test_c59_p7_frontier_async(void)  { park_reset(); p7_frontier(1, "c59p7a"); }
static void test_c59_p8_no_progress_sync(void)  { park_reset(); p8_no_progress(0, "c59p8s"); }
static void test_c59_p8_no_progress_async(void) { park_reset(); p8_no_progress(1, "c59p8a"); }

#endif /* MMAPQ_TEST_HOOKS */

void setUp(void)
{
#ifdef MMAPQ_TEST_HOOKS
    memset(msg, 'x', SMALL_LEN);
#endif
}

void tearDown(void) { }

int main(void)
{
    UNITY_BEGIN();
#ifdef MMAPQ_TEST_HOOKS
    RUN_TEST(test_c59_p1_warm_sync);
    RUN_TEST(test_c59_p1_warm_async);
    RUN_TEST(test_c59_p2_scan_sync);
    RUN_TEST(test_c59_p2_scan_async);
    RUN_TEST(test_c59_p3_premap_sync);
    RUN_TEST(test_c59_p3_premap_async);
    RUN_TEST(test_c59_p4_batch_sync);
    RUN_TEST(test_c59_p4_batch_async);
    RUN_TEST(test_c59_p5_two_stale_sync);
    RUN_TEST(test_c59_p5_two_stale_async);
    RUN_TEST(test_c59_p6_stray_zero_file);
    RUN_TEST(test_c59_p7_frontier_sync);
    RUN_TEST(test_c59_p7_frontier_async);
    RUN_TEST(test_c59_p8_no_progress_sync);
    RUN_TEST(test_c59_p8_no_progress_async);
#endif
    return UNITY_END();
}
