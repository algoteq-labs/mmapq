/* CAND-58 walk pins (audit/CAND-58-spec.md v2.2 §6) — fail-first pins for
 * the read-only size-checked walk accessor + the shared classification rule.
 *
 * Written against the post-fix behaviour and observed FAILING (by name)
 * against the pre-fix scratch checkout before the fix landed (the fail-first
 * transcripts are cited in the implementation report). Pin shapes:
 *
 *   P1a  the ruling's scenario (the executed round-2 shape): the factory's
 *        span walk parked; archive round 2 + reclaim past the park; release.
 *        Post-fix: the clamp skips the below-line span on the first pass
 *        (no hook firing below the line), the at-line span's advance misses,
 *        classifies overtaken, the factory restarts (retry >= 1) and
 *        succeeds; payload_0_0 stays absent.
 *   P1b  the §4.3 hand-deletion shape: the round-1 L(iv) state
 *        (si=5, so=64, line=81540), the span walk parked, payload_0_0
 *        hand-deleted during the park (the library's reclaim refuses it
 *        there — so=64 protects it). Post-fix: clamp skip, 0 restarts,
 *        rc=0; then a legitimate reclaim of file 1 after archive to 6.
 *        Mutation (iv) (no clamp) reds here via the no-progress detector.
 *   P2   cold above-line probe parked between the span init and its advance;
 *        archive+reclaim past it; the factory retries internally and
 *        succeeds (the executed midwalk shape).
 *   P3   hand-deletion at a known-written position >= the line: ECORRUPT,
 *        no re-creation.
 *   P4   the join's step-3 walk parked at a below-line committed record
 *        (resequenced X, the L(iv) state); archive past X + reclaim during
 *        the park; the join recomputes JoinBase >= the classifying line and
 *        the append position equals the true tail.
 *   P4b  P4's geometry; the step-3 walk parked inside [so, line) and
 *        payload_0_1 hand-deleted (strictly between X's file and the line —
 *        never library-reclaimable there). Post-fix: overtaken against the
 *        line, restart, append position == the true tail. Mutation (ii)
 *        (classify against start_offset) reds here (EOS at the miss → wrong
 *        tail).
 *   P5   the use phase overtaken mid-drain (the executed usephase shape):
 *        the drain parked before probing an uncommitted gap in file 2;
 *        archive+reclaim; the drain prunes and resumes — completes, yields
 *        exactly the over-the-line tail.
 *   P6   the factory's header scan parked at index 0 while start_index
 *        passes a whole header file and reclaim deletes it: the scan
 *        restarts from the new start_index; the drain yields exactly the
 *        uncommitted tail; plus the separation assertion (the factory's
 *        header accessor is not the commit path's).
 *
 * Batch twins of P1a/P1b/P2/P5 (the batch paths mirror the sites).
 * Every pin runs against both runtimes (sync: runtime NULL; async: the
 * region-mapper runtime). Geometry per the spec/handoff: REGION=MAXFILE=
 * 32768 (one region per payload file), RING=4, archivable=1, lane 0,
 * SMALL=100 (extent 104; batch 108), BIG=16000 (extent 16004; batch 16008).
 *
 * All pins need the MMAPQ_TEST_HOOKS park machinery — the whole suite is
 * hook-gated (the release-flags loop compiles it out, as the CAND-54 pins'
 * hook sections). "Crash" is close + reopen (the meta mmap persists; the
 * CAND-54 pins' crash equivalence).
 */
#include "unity.h"
#include "indexed_appender.h"
#include "indexed_archiver.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_archiver.h"
#include "batch_payload_iterator.h"
#include "internal/uncommitted_payload_iterator.h"
#include "internal/uncommitted_batch_iterator.h"
#include "internal/queue_meta.h"
#include "internal/appender_meta.h"
#include "internal/header_codec.h"
#include "internal/region_accessor.h"
#include "internal/test_hooks.h"
#include "internal/test_reach.h"
#include "runtime.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>

#ifdef MMAPQ_TEST_HOOKS

static const char    *DIR     = "/tmp";
static const int32_t  REGION  = 32768;
static const int64_t  MAXFILE = 32768;
static const int      RING    = 4;
static const uint64_t TIMEOUT = 5000000000ULL;

#define SMALL_LEN   100
#define BIG_LEN     16000

static MmapqRuntime *rt = NULL;   /* the async runtime; sync pins pass NULL */
static char msg_small[SMALL_LEN];
static char msg_big[BIG_LEN];

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

/* ── meta state (RO opens; the repro harness's dump_state pattern) ── */

static int64_t meta_si(const char *name, int queue_type)
{
    QueueMeta *qm = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_queue_meta_open_ro(DIR, name, queue_type, &qm));
    int64_t v = queue_meta_start_index(qm);
    mmapqi_queue_meta_close(qm);
    return v;
}

static int64_t meta_so(const char *name)
{
    AppenderMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_ro(DIR, name, 0, &m));
    int64_t v = appender_meta_start_offset(m);
    mmapqi_appender_meta_close(m);
    return v;
}

static int64_t meta_line(const char *name)
{
    AppenderMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_ro(DIR, name, 0, &m));
    int64_t v = appender_meta_archive_offset(m);
    mmapqi_appender_meta_close(m);
    return v;
}

/* ── file state ── */

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

static void hand_delete(const char *name, const char *file)
{
    char p[512]; qpath(name, file, p, sizeof p);
    TEST_ASSERT_EQUAL_INT(0, unlink(p));   /* test-side surgery: the demo's
                                            * hand-delete mode */
}

/* ── park machinery (the CAND-54 pins' discipline: atomics, bounded spin) ── */

static _Atomic int park_fired, park_release;
static int64_t     parked_off;

#define MAX_FIRES 32
static int64_t fire_off[MAX_FIRES];
static int     n_fires;

static void park_reset(void)
{
    atomic_store(&park_fired, 0);
    atomic_store(&park_release, 0);
    parked_off = -1;
    n_fires = 0;
}

static void park_here(void)
{
    atomic_store(&park_fired, 1);
    for (long s = 0; s < 50000000 && !atomic_load(&park_release); s++)
        sched_yield();
}

static void wait_parked(void)
{
    for (long w = 0; !atomic_load(&park_fired); w++) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
        if (w > 5000)
            TEST_FAIL_MESSAGE("CAND-58 pin: hook never fired (vacuous geometry?)");
    }
}

static void release_park(void) { atomic_store(&park_release, 1); }

/* park modes for the span hook */
#define PARK_FIRST       0   /* park on the first firing */
#define PARK_AT_OFFSET   1   /* park when the firing offset == park_target */

static int     park_mode = PARK_FIRST;
static int64_t park_target = -1;

static int park_wanted(int64_t off)
{
    if (park_mode == PARK_AT_OFFSET) return off == park_target;
    return 1;
}

/* every span firing must be at/above the line (the clamp evidence: the
 * below-line span was never even measured) */
static void assert_no_fires_below(int64_t line)
{
    for (int i = 0; i < n_fires; i++) {
        if (fire_off[i] < line) {
            printf("clamp violation: span firing at %ld (< line %ld)\n",
                   (long)fire_off[i], (long)line);
            TEST_FAIL_MESSAGE("CAND-58: a span below the line was walked");
        }
    }
}

/* ── the hook callbacks (test side) ── */

static void span_hook(int64_t off, int64_t ss, int64_t line)
{
    (void)ss; (void)line;
    if (n_fires < MAX_FIRES) fire_off[n_fires++] = off;
    if (park_wanted(off) && !atomic_load(&park_fired)) {
        parked_off = off;
        park_here();
    }
}

static void hdr_hook(int64_t idx)
{
    if (n_fires < MAX_FIRES) fire_off[n_fires++] = idx;
    if (park_wanted(idx) && !atomic_load(&park_fired)) {
        parked_off = idx;
        park_here();
    }
}

static void use_hook(int64_t probing)
{
    if (n_fires < MAX_FIRES) fire_off[n_fires++] = probing;
    if (park_wanted(probing) && !atomic_load(&park_fired)) {
        parked_off = probing;
        park_here();
    }
}

static void join_hook(int64_t probing)
{
    if (n_fires < MAX_FIRES) fire_off[n_fires++] = probing;
    if (park_wanted(probing) && !atomic_load(&park_fired)) {
        parked_off = probing;
        park_here();
    }
}

/* ── threads ── */

typedef struct {
    MmapqIndexedAppender *a;
    MmapqUncommittedPayloadIterator *it;
    int rc;
} FactoryJob;

static void *factory_main(void *p)
{
    FactoryJob *j = p;
    j->rc = mmapq_indexed_appender_uncommitted_iterator_checked(
        j->a, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
        &j->it);
    return NULL;
}

typedef struct {
    MmapqIndexedBatchAppender *a;
    MmapqUncommittedBatchIterator *it;
    int rc;
} BatchFactoryJob;

static void *batch_factory_main(void *p)
{
    BatchFactoryJob *j = p;
    j->rc = mmapq_indexed_batch_appender_uncommitted_batches_checked(j->a, &j->it);
    return NULL;
}

typedef struct {
    const char *name;
    MmapqRuntime *rt;
    MmapqIndexedAppender *a;
    int rc;
} JoinJob;

static void *join_main(void *p)
{
    JoinJob *j = p;
    j->rc = mmapq_indexed_appender_create_with_id(DIR, j->name, REGION, RING,
        MAXFILE, TIMEOUT, 1, j->rt, 0, 0, &j->a);
    return NULL;
}

/* drain state: the yielded offsets */
#define MAX_YIELDS 64
static int64_t yields[MAX_YIELDS];
static int     n_yields;

typedef struct {
    MmapqUncommittedPayloadIterator *it;
    int end_rc;
} DrainJob;

static void *drain_main(void *p)
{
    DrainJob *d = p;
    while (1) {
        const void *data; int32_t len; int64_t off;
        int r = mmapq_uncommitted_iterator_next(d->it, &data, &len, &off);
        if (r != 1) { d->end_rc = r; break; }
        if (n_yields >= MAX_YIELDS) { d->end_rc = -9999; break; }
        yields[n_yields++] = off;
    }
    return NULL;
}

typedef struct {
    MmapqUncommittedBatchIterator *it;
    int end_rc;
} BatchDrainJob;

static void *batch_drain_main(void *p)
{
    BatchDrainJob *d = p;
    while (1) {
        int64_t off; int32_t count; MmapqBatchPayloadIterator *msgs;
        int r = mmapqi_uncommitted_batch_iterator_next(d->it, &off, &count, &msgs);
        if (r != 1) { d->end_rc = r; break; }
        if (n_yields >= MAX_YIELDS) { d->end_rc = -9999; break; }
        yields[n_yields++] = off;
    }
    return NULL;
}

/* ── listing/reclaim helpers ── */

static void free_paths(char **paths, int n)
{
    for (int i = 0; i < n; i++) free(paths[i]);
}

static int list_and_reclaim(MmapqIndexedArchiver *arch)
{
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_THAN_INT(0, n);
    int drc = mmapq_indexed_archiver_reclaim_files(arch, paths, n);
    free_paths(paths, n);
    return drc;
}

static int batch_list_and_reclaim(MmapqIndexedBatchArchiver *arch)
{
    char *paths[64];
    int n = mmapq_indexed_batch_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_THAN_INT(0, n);
    int drc = mmapq_indexed_batch_archiver_reclaim_files(arch, paths, n);
    free_paths(paths, n);
    return drc;
}

static int listing_contains(MmapqIndexedArchiver *arch, const char *base)
{
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    int found = 0;
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], base) != NULL) found = 1;
    free_paths(paths, n);
    return found;
}

/* ── appender/archiver construction ── */

static MmapqIndexedAppender *mk_appender(const char *name, MmapqRuntime *r)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, name,
        REGION, RING, MAXFILE, TIMEOUT, 1, r, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedAppender *rejoin_appender(const char *name, MmapqRuntime *r)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, name,
        REGION, RING, MAXFILE, TIMEOUT, 1, r, 0, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedBatchAppender *mk_batch_appender(const char *name, MmapqRuntime *r)
{
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create(DIR, name,
        REGION, RING, MAXFILE, TIMEOUT, 1, r, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedBatchAppender *rejoin_batch_appender(const char *name, MmapqRuntime *r)
{
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_create_with_id(DIR,
        name, REGION, RING, MAXFILE, TIMEOUT, 1, r, 0, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedArchiver *mk_archiver(const char *name, MmapqRuntime *r)
{
    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, name, RING, r, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    return arch;
}

static MmapqIndexedBatchArchiver *mk_batch_archiver(const char *name, MmapqRuntime *r)
{
    MmapqIndexedBatchArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_create(DIR, name, RING, r, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    return arch;
}

/* stage one small/one big committed or uncommitted record, single-message */
static void stage_big_committed(MmapqIndexedAppender *a, int64_t expect_idx)
{
    TEST_ASSERT_EQUAL_INT64(expect_idx,
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

static void stage_big_committed_batch(MmapqIndexedBatchAppender *a, int64_t expect_idx)
{
    TEST_ASSERT_EQUAL_INT64(expect_idx,
        mmapq_indexed_batch_appender_append_and_commit(a, msg_big, sizeof msg_big));
}

/* ════════════════════════════════════════════════════════════════════════
 * Geometry builders. Single-message layout (measured against the executed
 * evidence pack's dumps, 2026-10-27):
 *   X (100 B) staged @64 ends 168; bigs idx 0..4 @168/@16172/@32768/@48772/
 *   @65536 (ends 81540 → file 2); X commits at idx 5; bigs idx 6..8 @81540/
 *   @98304/@114308 (ends 130312 → file 3); Z staged @130312 ends 130416.
 *   Round 1 archive to 5: si=5, so=64, line=81540 (the L(iv) state: X's live
 *   header pins start_offset at 64). Round 2 to 9: si=9, so=line=130312;
 *   files 0-2 wholly below.
 * No-X single layout: bigs idx 0..4 @64/@16068/@32768/@48772/@65536 (ends
 *   81540); idx 5..7 @81540/@98304/@114308 (ends 130312); Z @130312.
 * Batch layout (measured against the tree, 2026-10-28 — the batch's count
 *   slot and its message may straddle a file boundary with the padding
 *   INSIDE the span): X @64 ends 172; bigs idx 0..4 @172/@16180/@32188/
 *   @48772/@64780 (idx 4's span ends 81540); X commits at 5; idx 6..8
 *   @81540/@97548/@114308 (ends 130316); Z @130316.
 * No-X batch: bigs idx 0..4 @64/@16072/@32080/@48772/@64780 (ends 81540);
 *   idx 5..7 @81540/@97548/@114308 (ends 130316); Z @130316.
 * ════════════════════════════════════════════════════════════════════════ */

/* P1a/P1b/P4/P4b single fill through round 1 and the rejoin. */
typedef struct {
    MmapqIndexedAppender *w2;
    MmapqIndexedArchiver *arch;
    int64_t z_off;
} GeomS;

static void geom_x_single(const char *name, MmapqRuntime *r, GeomS *g,
                          int64_t z_expect)
{
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    TEST_ASSERT_EQUAL_INT64(64,
        mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small));
    for (int i = 0; i < 5; i++) stage_big_committed(w, i);
    TEST_ASSERT_EQUAL_INT64(5, mmapq_indexed_appender_commit(w, 64));
    for (int i = 6; i < 9; i++) stage_big_committed(w, i);
    g->z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(z_expect, g->z_off);

    g->arch = mk_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(g->arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(64,    meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_appender_close(w);
    g->w2 = rejoin_appender(name, r);
}

/* P2 single fill (no X) through round 1 and the rejoin. */
static void geom_nox_single(const char *name, MmapqRuntime *r, GeomS *g)
{
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    for (int i = 0; i < 5; i++) stage_big_committed(w, i);
    for (int i = 5; i < 8; i++) stage_big_committed(w, i);
    g->z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(130312, g->z_off);

    g->arch = mk_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(g->arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(81540, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_appender_close(w);
    g->w2 = rejoin_appender(name, r);
}

typedef struct {
    MmapqIndexedBatchAppender *w2;
    MmapqIndexedBatchArchiver *arch;
    int64_t z_off;
} GeomB;

/* P1a/P1b batch fill through round 1 and the rejoin. */
static void geom_x_batch(const char *name, MmapqRuntime *r, GeomB *g)
{
    rm_queue(name);
    MmapqIndexedBatchAppender *w = mk_batch_appender(name, r);
    TEST_ASSERT_EQUAL_INT64(64, stage_batch(w, msg_small, sizeof msg_small));
    for (int i = 0; i < 5; i++) stage_big_committed_batch(w, i);
    TEST_ASSERT_EQUAL_INT64(5, mmapq_indexed_batch_appender_commit_batch(w, 64));
    for (int i = 6; i < 9; i++) stage_big_committed_batch(w, i);
    g->z_off = stage_batch(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(130316, g->z_off);

    g->arch = mk_batch_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(g->arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(64,    meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_batch_appender_close(w);
    g->w2 = rejoin_batch_appender(name, r);
}

/* P2 batch fill (no X) through round 1 and the rejoin. */
static void geom_nox_batch(const char *name, MmapqRuntime *r, GeomB *g)
{
    rm_queue(name);
    MmapqIndexedBatchAppender *w = mk_batch_appender(name, r);
    for (int i = 0; i < 5; i++) stage_big_committed_batch(w, i);
    for (int i = 5; i < 8; i++) stage_big_committed_batch(w, i);
    g->z_off = stage_batch(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(130316, g->z_off);

    g->arch = mk_batch_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(g->arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(81540, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_batch_appender_close(w);
    g->w2 = rejoin_batch_appender(name, r);
}

/* ════════════════════════════════════════════════════════════════════════
 * P1a — the ruling's scenario (the executed round-2 shape)
 * ════════════════════════════════════════════════════════════════════════ */

static void p1a_single(const char *name, MmapqRuntime *r)
{
    GeomS g;
    geom_x_single(name, r, &g, 130312);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    FactoryJob j = { g.w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, factory_main, &j));
    wait_parked();

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(g.arch, 9));
    TEST_ASSERT_EQUAL_INT64(9,      meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(130312, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(130312, meta_line(name));
    TEST_ASSERT_EQUAL_INT(3, list_and_reclaim(g.arch));   /* files 0,1,2 */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P1a single: parked_off=%ld, factory rc=%d, restarts=%d, "
           "payload_0_0 exists=%d all_zero=%d\n",
           (long)parked_off, j.rc,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_0"), file_all_zero(name, "payload_0_0"));
    /* the probe target is recorded (vacuity); the clamp's skip is pinned
     * by P1b — P1a's green is the classification + restart (spec v2.2:
     * mutation (iv) is green here by geometry) */
    TEST_ASSERT_EQUAL_INT(1, n_fires);
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());   /* the retry pins' >= 1 */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));   /* no re-creation */
    TEST_ASSERT_EQUAL_INT(1, n_fires);   /* the restart's rescan sees nothing
                                          * below si=9 */

    /* the drain yields exactly the over-the-line tail (Z), nothing below */
    n_yields = 0;
    DrainJob d = { j.it, -999 };
    drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(130312, yields[0]);

    /* the drain's yield is committable at the exact next index; a below-line
     * commit is EINVAL (the membership gate) */
    TEST_ASSERT_EQUAL_INT64(9, mmapq_indexed_appender_commit(g.w2, yields[0]));
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, mmapq_indexed_appender_commit(g.w2, 64));

    /* the re-copy vector is gone: the next listing does not name file 0 */
    TEST_ASSERT_FALSE(listing_contains(g.arch, "payload_0_0"));

    mmapq_indexed_archiver_close(g.arch);
    mmapq_indexed_appender_close(g.w2);
}

static void p1a_batch(const char *name, MmapqRuntime *r)
{
    GeomB g;
    geom_x_batch(name, r, &g);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    BatchFactoryJob j = { g.w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_factory_main, &j));
    wait_parked();

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(g.arch, 9));
    TEST_ASSERT_EQUAL_INT64(9,      meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(130316, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(130316, meta_line(name));
    TEST_ASSERT_EQUAL_INT(3, batch_list_and_reclaim(g.arch));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P1a batch: parked_off=%ld, factory rc=%d, restarts=%d, "
           "payload_0_0 exists=%d all_zero=%d\n",
           (long)parked_off, j.rc,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_0"), file_all_zero(name, "payload_0_0"));
    TEST_ASSERT_EQUAL_INT(1, n_fires);   /* the restart's rescan (si=9) is empty */
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    n_yields = 0;
    BatchDrainJob d = { j.it, -999 };
    batch_drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(130316, yields[0]);

    TEST_ASSERT_EQUAL_INT64(9,
        mmapq_indexed_batch_appender_commit_batch(g.w2, yields[0]));
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(g.w2, 64));

    mmapq_indexed_batch_archiver_close(g.arch);
    mmapq_indexed_batch_appender_close(g.w2);
}

/* ════════════════════════════════════════════════════════════════════════
 * P1b — the §4.3 hand-deletion shape (the load-bearing-clamp pin)
 * ════════════════════════════════════════════════════════════════════════ */

static void p1b_single(const char *name, MmapqRuntime *r)
{
    GeomS g;
    geom_x_single(name, r, &g, 130312);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    FactoryJob j = { g.w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, factory_main, &j));
    wait_parked();

    /* the file vanishes WITHOUT the line advancing (§4.3): the library's
     * reclaim refuses file 0 in this state (the listing bound <= so = 64
     * protects it) — hand-delete, mirroring the demo's hand-delete mode */
    TEST_ASSERT_TRUE(file_exists(name, "payload_0_0"));
    hand_delete(name, "payload_0_0");
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P1b single: parked_off=%ld, factory rc=%d, restarts=%d, "
           "payload_0_0 exists=%d all_zero=%d\n",
           (long)parked_off, j.rc,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_0"), file_all_zero(name, "payload_0_0"));
    /* the clamp skips X's span: the first firing is the at-line span @81540 */
    TEST_ASSERT_EQUAL_INT64(81540, parked_off);
    TEST_ASSERT_EQUAL_INT(1, n_fires);
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    /* the clamp skips the below-line span on the FIRST pass — no restart */
    TEST_ASSERT_EQUAL_INT(restarts0, mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));   /* never re-created */

    /* the legitimate reclaim leg: archive to 7 (covers X and idx 6 — the
     * per-round candidate is the round's max-position end, 97544, lifting
     * start_offset past file 1's end), then file 1 is listed and reclaimed */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(g.arch, 7));
    TEST_ASSERT_EQUAL_INT64(7,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(97544, meta_line(name));
    TEST_ASSERT_TRUE(listing_contains(g.arch, "payload_0_1"));
    TEST_ASSERT_EQUAL_INT(1, list_and_reclaim(g.arch));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_1"));

    n_yields = 0;
    DrainJob d = { j.it, -999 };
    drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(130312, yields[0]);

    TEST_ASSERT_EQUAL_INT64(9, mmapq_indexed_appender_commit(g.w2, yields[0]));
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, mmapq_indexed_appender_commit(g.w2, 64));

    mmapq_indexed_archiver_close(g.arch);
    mmapq_indexed_appender_close(g.w2);
}

static void p1b_batch(const char *name, MmapqRuntime *r)
{
    GeomB g;
    geom_x_batch(name, r, &g);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    BatchFactoryJob j = { g.w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_factory_main, &j));
    wait_parked();

    TEST_ASSERT_TRUE(file_exists(name, "payload_0_0"));
    hand_delete(name, "payload_0_0");
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P1b batch: parked_off=%ld, factory rc=%d, restarts=%d, "
           "payload_0_0 exists=%d all_zero=%d\n",
           (long)parked_off, j.rc,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_0"), file_all_zero(name, "payload_0_0"));
    TEST_ASSERT_EQUAL_INT64(81540, parked_off);
    /* the batch scan measures every surviving span (no contiguous fast
     * path): hdr[6..8] — and never X's below-line span (the clamp) */
    TEST_ASSERT_EQUAL_INT(3, n_fires);
    assert_no_fires_below(81540);
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_EQUAL_INT(restarts0, mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(g.arch, 7));
    TEST_ASSERT_EQUAL_INT64(7,     meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(97548, meta_line(name));
    TEST_ASSERT_EQUAL_INT(1, batch_list_and_reclaim(g.arch));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_1"));

    n_yields = 0;
    BatchDrainJob d = { j.it, -999 };
    batch_drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(130316, yields[0]);

    TEST_ASSERT_EQUAL_INT64(9,
        mmapq_indexed_batch_appender_commit_batch(g.w2, yields[0]));
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL,
        mmapq_indexed_batch_appender_commit_batch(g.w2, 64));

    mmapq_indexed_batch_archiver_close(g.arch);
    mmapq_indexed_batch_appender_close(g.w2);
}

/* ════════════════════════════════════════════════════════════════════════
 * P2 — cold above-line probe (the executed midwalk shape)
 * ════════════════════════════════════════════════════════════════════════ */

static void p2_single(const char *name, MmapqRuntime *r)
{
    GeomS g;
    geom_nox_single(name, r, &g);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    FactoryJob j = { g.w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, factory_main, &j));
    wait_parked();

    /* parked between the span init and its advance at the line (@81540,
     * file 2 — never mapped by this incarnation) */
    TEST_ASSERT_EQUAL_INT64(81540, parked_off);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(g.arch, 8));
    TEST_ASSERT_EQUAL_INT64(8,      meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(130312, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(130312, meta_line(name));
    TEST_ASSERT_EQUAL_INT(3, list_and_reclaim(g.arch));   /* files 0,1,2 */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P2 single: factory rc=%d, restarts=%d, payload_0_2 exists=%d all_zero=%d\n",
           j.rc, mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_2"), file_all_zero(name, "payload_0_2"));
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    n_yields = 0;
    DrainJob d = { j.it, -999 };
    drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(130312, yields[0]);

    TEST_ASSERT_EQUAL_INT64(8, mmapq_indexed_appender_commit(g.w2, yields[0]));

    mmapq_indexed_archiver_close(g.arch);
    mmapq_indexed_appender_close(g.w2);
}

static void p2_batch(const char *name, MmapqRuntime *r)
{
    GeomB g;
    geom_nox_batch(name, r, &g);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    BatchFactoryJob j = { g.w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_factory_main, &j));
    wait_parked();

    TEST_ASSERT_EQUAL_INT64(81540, parked_off);
    assert_no_fires_below(81540);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(g.arch, 8));
    TEST_ASSERT_EQUAL_INT64(8,      meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(130316, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(130316, meta_line(name));
    TEST_ASSERT_EQUAL_INT(3, batch_list_and_reclaim(g.arch));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P2 batch: factory rc=%d, restarts=%d, payload_0_2 exists=%d all_zero=%d\n",
           j.rc, mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_2"), file_all_zero(name, "payload_0_2"));
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    n_yields = 0;
    BatchDrainJob d = { j.it, -999 };
    batch_drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(130316, yields[0]);

    TEST_ASSERT_EQUAL_INT64(8,
        mmapq_indexed_batch_appender_commit_batch(g.w2, yields[0]));

    mmapq_indexed_batch_archiver_close(g.arch);
    mmapq_indexed_batch_appender_close(g.w2);
}

/* ════════════════════════════════════════════════════════════════════════
 * P3 — hand-deletion at a known-written position >= the line
 * ════════════════════════════════════════════════════════════════════════ */

static void p3_single(const char *name, MmapqRuntime *r)
{
    /* no X; bigs idx 0..5 (idx 5 @81540 ends 97544, file 2); big idx 6
     * @98304 (file 3, ends 114308); Z staged @114308 (file 3). Round 1 to 5
     * (covers 0..4): si=5, so=line=81540. The rejoin's join walks from the
     * LAST live header (hdr[6] @98304) — file 3 only — so the parked span
     * @81540 (file 2) is a COLD probe in both runtimes. */
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    for (int i = 0; i < 7; i++) stage_big_committed(w, i);
    int64_t z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(114308, z_off);

    MmapqIndexedArchiver *arch = mk_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(81540, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_appender_close(w);
    MmapqIndexedAppender *w2 = rejoin_appender(name, r);

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(span_hook);
    FactoryJob j = { w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, factory_main, &j));
    wait_parked();

    TEST_ASSERT_TRUE(file_exists(name, "payload_0_2"));
    hand_delete(name, "payload_0_2");   /* external unlink mid-walk */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_span_pre_walk(NULL);

    printf("P3 single: parked_off=%ld, factory rc=%d (ECORRUPT=%d), "
           "payload_0_2 exists=%d all_zero=%d\n",
           (long)parked_off, j.rc, MMAPQ_ECORRUPT,
           file_exists(name, "payload_0_2"), file_all_zero(name, "payload_0_2"));
    /* parked at the committed span @81540 — AT the line, known-written (a
     * committed header's span start): the miss classifies ECORRUPT */
    TEST_ASSERT_EQUAL_INT64(81540, parked_off);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, j.rc);
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));   /* no re-creation */

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(w2);
}

/* ════════════════════════════════════════════════════════════════════════
 * P4 — the join walk overtaken mid-walk (below-line live header, L(iv))
 * ════════════════════════════════════════════════════════════════════════ */

static void p4_single(const char *name, MmapqRuntime *r)
{
    /* X staged @64; bigs idx 0..4 (@168..@65536, ends 81540); X commits at
     * idx 5 (resequenced — its position predates the bigs); Z staged @81540
     * (file 2, ends 81644). NO archive before the rejoin: the join snapshots
     * si=0/line=64 and its step-3 first walk probes X's record @64. The
     * single round to 6 (during the park) covers the bigs AND X: the
     * boundary candidate is idx 4's end (81540), so the listing bound
     * reaches 81540 and files 0,1 are legitimately reclaimed. (Covering X
     * alone would lift the boundary only to X's end — the per-round
     * candidate is the round's max-position end — and file 0 would stay
     * protected.) */
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    TEST_ASSERT_EQUAL_INT64(64,
        mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small));
    for (int i = 0; i < 5; i++) stage_big_committed(w, i);
    TEST_ASSERT_EQUAL_INT64(5, mmapq_indexed_appender_commit(w, 64));
    int64_t z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(81540, z_off);

    MmapqIndexedArchiver *arch = mk_archiver(name, r);
    mmapq_indexed_appender_close(w);

    /* the rejoin's join parks at the step-3 first walk (@64, the
     * resequenced X's record — below the coming line) */
    park_mode = PARK_FIRST;
    mmapq_test_hook_set_join_walk_step(join_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    JoinJob j = { name, r, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, join_main, &j));
    wait_parked();

    /* archive+reclaim past the probe: si=6, so=line=81540; files 0,1 */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 6));
    TEST_ASSERT_EQUAL_INT64(6,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(81540, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));
    TEST_ASSERT_EQUAL_INT(2, list_and_reclaim(arch));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_1"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_join_walk_step(NULL);

    /* evidence first (the fail-first transcripts) */
    printf("P4 single: parked_off=%ld, create rc=%d, restarts=%d, "
           "payload_0_0 exists=%d all_zero=%d\n",
           (long)parked_off, j.rc,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_0"), file_all_zero(name, "payload_0_0"));
    TEST_ASSERT_EQUAL_INT64(64, parked_off);
    TEST_ASSERT_EQUAL_INT(0, j.rc);            /* the join recomputed JoinBase */
    TEST_ASSERT_NOT_NULL(j.a);
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    /* the over-the-line tail is recoverable and committable */
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_appender_uncommitted_iterator_checked(j.a,
            MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, &it));
    const void *data; int32_t len; int64_t off;
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(81540, off);
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(6, mmapq_indexed_appender_commit(j.a, 81540));

    /* the append position equals the true tail (Z's end, 81644) */
    TEST_ASSERT_EQUAL_INT64(81644,
        mmapq_indexed_appender_append_uncommitted(j.a, msg_small, sizeof msg_small));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(j.a);
}

/* ════════════════════════════════════════════════════════════════════════
 * P4b — the reachable mutation-(ii) geometry: P4's L(iv) fill with the
 * round-1 archive BEFORE the rejoin (si=5, so=64, line=81540 — X live, the
 * legal L(iv) state); the join's step-3 walk parked before its first
 * advance (so the async ring's pre-map has not warmed file 1 either);
 * payload_0_1 hand-deleted during the park (strictly between X's file and
 * the line — the library's reclaim refuses it: so=64 protects it). The
 * failing probe is at 32768 ∈ [so, line) — the second file-0 wrap region's
 * skip target.
 * ════════════════════════════════════════════════════════════════════════ */

static void p4b_single(const char *name, MmapqRuntime *r)
{
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    TEST_ASSERT_EQUAL_INT64(64,
        mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small));
    for (int i = 0; i < 5; i++) stage_big_committed(w, i);
    TEST_ASSERT_EQUAL_INT64(5, mmapq_indexed_appender_commit(w, 64));
    int64_t z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(81540, z_off);

    MmapqIndexedArchiver *arch = mk_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(64,    meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_appender_close(w);

    /* park before the step-3 walk's first advance (@64) — nothing of the
     * lane's payload is mapped or pre-mapped yet (the async pre-map fires
     * on the first wrap), so the file-1 probe is cold in both runtimes */
    park_mode = PARK_FIRST;
    mmapq_test_hook_set_join_walk_step(join_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    JoinJob j = { name, r, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, join_main, &j));
    wait_parked();

    TEST_ASSERT_TRUE(file_exists(name, "payload_0_1"));
    hand_delete(name, "payload_0_1");   /* in (so, line): the library's
                                         * reclaim refuses it (so=64) */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_1"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_join_walk_step(NULL);

    /* the file state BEFORE the probe append (which would itself create the
     * file at a wrong tail) */
    int p1_exists  = file_exists(name, "payload_0_1");
    int p1_zero    = file_all_zero(name, "payload_0_1");
    int64_t probe_pos = j.rc == 0
        ? mmapq_indexed_appender_append_uncommitted(j.a, msg_small, sizeof msg_small)
        : -1;
    printf("P4b single: parked_off=%ld, create rc=%d, restarts=%d, "
           "payload_0_1 post-join exists=%d all_zero=%d, append probe=%ld "
           "(true tail 81644)\n",
           (long)parked_off, j.rc,
           mmapq_test_hook_walk_restart_count() - restarts0,
           p1_exists, p1_zero, (long)probe_pos);
    TEST_ASSERT_EQUAL_INT64(64, parked_off);
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_NOT_NULL(j.a);
    TEST_ASSERT_FALSE(p1_exists);   /* the pre-fix red: the hand-deleted
                                     * file is re-created (zeros) */
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());

    /* the restart floors the walk at the classifying line (81540): the
     * append position is the true tail (Z's end, 81644) — mutation (ii)
     * (flooring at start_offset=64) misclassifies the miss at 32768 as EOS
     * and lands the tail at 32768 */
    TEST_ASSERT_EQUAL_INT64(81644, probe_pos);

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(j.a);
}

/* ════════════════════════════════════════════════════════════════════════
 * P5 — the use phase overtaken mid-drain (the executed usephase shape)
 * ════════════════════════════════════════════════════════════════════════ */

/* P5 fill: no X; bigs idx 0..4 (ends 81540); U1 staged @81540 (the gap,
 * never committed); 14 more bigs idx 5..18 (the factory's scan walks files
 * 2..9, evicting file 2 from the payload ring (4) and the rolled per-file
 * cache (6) — the drain's probe is cold); Z staged at the tail. */
static void p5_single(const char *name, MmapqRuntime *r)
{
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    for (int i = 0; i < 5; i++) stage_big_committed(w, i);
    TEST_ASSERT_EQUAL_INT64(81540,
        mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small));
    for (int i = 5; i < 19; i++) stage_big_committed(w, i);
    int64_t z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(310916, z_off);

    MmapqIndexedArchiver *arch = mk_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(81540, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_appender_close(w);
    MmapqIndexedAppender *w2 = rejoin_appender(name, r);

    /* the factory completes WITHOUT a park */
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_appender_uncommitted_iterator_checked(w2,
            MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, &it));

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_use_phase_step(use_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    n_yields = 0;
    DrainJob d = { it, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, drain_main, &d));
    wait_parked();

    /* parked before probing U1's gap (81540, file 2) */
    TEST_ASSERT_EQUAL_INT64(81540, parked_off);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 8));
    TEST_ASSERT_EQUAL_INT64(8,      meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(130312, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(130312, meta_line(name));
    TEST_ASSERT_EQUAL_INT(3, list_and_reclaim(arch));   /* files 0,1,2 */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_use_phase_step(NULL);

    printf("P5 single: drain end rc=%d, yields=%d, restarts=%d, "
           "payload_0_2 exists=%d all_zero=%d\n", d.end_rc, n_yields,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_2"), file_all_zero(name, "payload_0_2"));
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    /* the drain completes: exactly the over-the-line tail (Z); U1 (below the
     * new line) is dropped by design (L(i)) */
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(310916, yields[0]);

    TEST_ASSERT_EQUAL_INT64(19, mmapq_indexed_appender_commit(w2, yields[0]));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(w2);
}

static void p5_batch(const char *name, MmapqRuntime *r)
{
    rm_queue(name);
    MmapqIndexedBatchAppender *w = mk_batch_appender(name, r);
    for (int i = 0; i < 5; i++) stage_big_committed_batch(w, i);
    TEST_ASSERT_EQUAL_INT64(81540, stage_batch(w, msg_small, sizeof msg_small));
    for (int i = 5; i < 19; i++) stage_big_committed_batch(w, i);
    int64_t z_off = stage_batch(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(310916, z_off);

    MmapqIndexedBatchArchiver *arch = mk_batch_archiver(name, r);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 5));
    TEST_ASSERT_EQUAL_INT64(5,     meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(81540, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(81540, meta_line(name));

    mmapq_indexed_batch_appender_close(w);
    MmapqIndexedBatchAppender *w2 = rejoin_batch_appender(name, r);

    MmapqUncommittedBatchIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_uncommitted_batches_checked(w2, &it));

    park_mode = PARK_FIRST;
    mmapq_test_hook_set_uncommitted_use_phase_step(use_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    n_yields = 0;
    BatchDrainJob d = { it, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, batch_drain_main, &d));
    wait_parked();

    TEST_ASSERT_EQUAL_INT64(81540, parked_off);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_archiver_archive_up_to_index(arch, 8));
    TEST_ASSERT_EQUAL_INT64(8,      meta_si(name, QUEUE_TYPE_BATCH));
    TEST_ASSERT_EQUAL_INT64(130316, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(130316, meta_line(name));
    TEST_ASSERT_EQUAL_INT(3, batch_list_and_reclaim(arch));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_use_phase_step(NULL);

    printf("P5 batch: drain end rc=%d, yields=%d, restarts=%d, "
           "payload_0_2 exists=%d all_zero=%d\n", d.end_rc, n_yields,
           mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "payload_0_2"), file_all_zero(name, "payload_0_2"));
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_2"));
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(310916, yields[0]);

    TEST_ASSERT_EQUAL_INT64(19,
        mmapq_indexed_batch_appender_commit_batch(w2, yields[0]));

    mmapq_indexed_batch_archiver_close(arch);
    mmapq_indexed_batch_appender_close(w2);
}

/* ════════════════════════════════════════════════════════════════════════
 * P6 — the factory's header scan overtaken (start_index passes a whole
 * header file) + the accessor-separation assertion
 * ════════════════════════════════════════════════════════════════════════ */

#define HDRFILL 4200

static void p6_single(const char *name, MmapqRuntime *r)
{
    rm_queue(name);
    MmapqIndexedAppender *w = mk_appender(name, r);
    for (int i = 0; i < HDRFILL; i++)
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_appender_append(w, msg_small, sizeof msg_small));
    int64_t z_off = mmapq_indexed_appender_append_uncommitted(w, msg_small, sizeof msg_small);
    TEST_ASSERT_EQUAL_INT64(437008, z_off);

    MmapqIndexedArchiver *arch = mk_archiver(name, r);

    mmapq_indexed_appender_close(w);
    MmapqIndexedAppender *w2 = rejoin_appender(name, r);

    TEST_ASSERT_TRUE(file_exists(name, "header_0"));

    park_mode = PARK_AT_OFFSET;
    park_target = 0;
    mmapq_test_hook_set_uncommitted_scan_header_pre_read(hdr_hook);
    int restarts0 = mmapq_test_hook_walk_restart_count();
    FactoryJob j = { w2, NULL, -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, factory_main, &j));
    wait_parked();

    /* si passes header_0's whole 4096-slot range */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 4097));
    TEST_ASSERT_EQUAL_INT64(4097,   meta_si(name, QUEUE_TYPE_NORMAL));
    TEST_ASSERT_EQUAL_INT64(426296, meta_so(name));
    TEST_ASSERT_EQUAL_INT64(426296, meta_line(name));
    TEST_ASSERT_EQUAL_INT(14, list_and_reclaim(arch));  /* header_0 + payload 0..12 */
    TEST_ASSERT_FALSE(file_exists(name, "header_0"));
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    release_park();
    pthread_join(t, NULL);
    mmapq_test_hook_set_uncommitted_scan_header_pre_read(NULL);

    printf("P6 single: factory rc=%d, restarts=%d, header_0 exists=%d "
           "all_zero=%d\n",
           j.rc, mmapq_test_hook_walk_restart_count() - restarts0,
           file_exists(name, "header_0"), file_all_zero(name, "header_0"));
    TEST_ASSERT_EQUAL_INT64(0, parked_off);
    TEST_ASSERT_EQUAL_INT(0, j.rc);
    TEST_ASSERT_GREATER_THAN_INT(restarts0,
        mmapq_test_hook_walk_restart_count());
    TEST_ASSERT_FALSE(file_exists(name, "header_0"));   /* no re-creation */
    TEST_ASSERT_FALSE(file_exists(name, "payload_0_0"));

    /* the separation assertion (the owner, v2.2): the factory's header
     * accessor is NOT the commit path's */
    TEST_ASSERT_NOT_NULL(j.it);
    TEST_ASSERT_TRUE(j.it->header_accessor != mmapqi_appender_header_acc(w2));

    /* the drain yields exactly the uncommitted tail, no duplicates */
    n_yields = 0;
    DrainJob d = { j.it, -999 };
    drain_main(&d);
    TEST_ASSERT_EQUAL_INT(0, d.end_rc);
    TEST_ASSERT_EQUAL_INT(1, n_yields);
    TEST_ASSERT_EQUAL_INT64(437008, yields[0]);

    TEST_ASSERT_EQUAL_INT64(4200, mmapq_indexed_appender_commit(w2, yields[0]));

    /* the zero header_0 is not re-listed either (the re-copy vector) */
    TEST_ASSERT_FALSE(listing_contains(arch, "header_0"));

    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(w2);
}

/* ── runtime-variant wrappers ── */

void test_c58_p1a_reclaim_single_sync(void)        { park_reset(); p1a_single("c58p_1a_s_sy", NULL); }
void test_c58_p1a_reclaim_single_async(void)       { park_reset(); p1a_single("c58p_1a_s_as", rt); }
void test_c58_p1a_reclaim_batch_sync(void)         { park_reset(); p1a_batch("c58p_1a_b_sy", NULL); }
void test_c58_p1a_reclaim_batch_async(void)        { park_reset(); p1a_batch("c58p_1a_b_as", rt); }
void test_c58_p1b_hand_delete_single_sync(void)    { park_reset(); p1b_single("c58p_1b_s_sy", NULL); }
void test_c58_p1b_hand_delete_single_async(void)   { park_reset(); p1b_single("c58p_1b_s_as", rt); }
void test_c58_p1b_hand_delete_batch_sync(void)     { park_reset(); p1b_batch("c58p_1b_b_sy", NULL); }
void test_c58_p1b_hand_delete_batch_async(void)    { park_reset(); p1b_batch("c58p_1b_b_as", rt); }
void test_c58_p2_midwalk_single_sync(void)         { park_reset(); p2_single("c58p_2_s_sy", NULL); }
void test_c58_p2_midwalk_single_async(void)        { park_reset(); p2_single("c58p_2_s_as", rt); }
void test_c58_p2_midwalk_batch_sync(void)          { park_reset(); p2_batch("c58p_2_b_sy", NULL); }
void test_c58_p2_midwalk_batch_async(void)         { park_reset(); p2_batch("c58p_2_b_as", rt); }
void test_c58_p3_known_written_single_sync(void)   { park_reset(); p3_single("c58p_3_s_sy", NULL); }
void test_c58_p3_known_written_single_async(void)  { park_reset(); p3_single("c58p_3_s_as", rt); }
void test_c58_p4_join_overtaken_single_sync(void)  { park_reset(); p4_single("c58p_4_s_sy", NULL); }
void test_c58_p4_join_overtaken_single_async(void) { park_reset(); p4_single("c58p_4_s_as", rt); }
void test_c58_p4b_join_below_line_single_sync(void)  { park_reset(); p4b_single("c58p_4b_s_sy", NULL); }
void test_c58_p4b_join_below_line_single_async(void) { park_reset(); p4b_single("c58p_4b_s_as", rt); }
void test_c58_p5_use_phase_single_sync(void)       { park_reset(); p5_single("c58p_5_s_sy", NULL); }
void test_c58_p5_use_phase_single_async(void)      { park_reset(); p5_single("c58p_5_s_as", rt); }
void test_c58_p5_use_phase_batch_sync(void)        { park_reset(); p5_batch("c58p_5_b_sy", NULL); }
void test_c58_p5_use_phase_batch_async(void)       { park_reset(); p5_batch("c58p_5_b_as", rt); }
void test_c58_p6_header_scan_single_sync(void)     { park_reset(); p6_single("c58p_6_s_sy", NULL); }
void test_c58_p6_header_scan_single_async(void)    { park_reset(); p6_single("c58p_6_s_as", rt); }

#endif /* MMAPQ_TEST_HOOKS */

void setUp(void)
{
#ifdef MMAPQ_TEST_HOOKS
    memset(msg_small, 'A', sizeof msg_small);
    memset(msg_big, 'B', sizeof msg_big);
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_region_mapper(&rt));
    TEST_ASSERT_NOT_NULL(rt);
#endif
}

void tearDown(void)
{
    /* The runtime is deliberately NOT closed: a failing pin leaves its queue
     * objects open, and closing the runtime under live async rings aborts
     * the process (region_async UNMANAGED) — the process exit reaps it. */
}

int main(void)
{
    UNITY_BEGIN();
#ifdef MMAPQ_TEST_HOOKS
    RUN_TEST(test_c58_p1a_reclaim_single_sync);
    RUN_TEST(test_c58_p1a_reclaim_single_async);
    RUN_TEST(test_c58_p1a_reclaim_batch_sync);
    RUN_TEST(test_c58_p1a_reclaim_batch_async);
    RUN_TEST(test_c58_p1b_hand_delete_single_sync);
    RUN_TEST(test_c58_p1b_hand_delete_single_async);
    RUN_TEST(test_c58_p1b_hand_delete_batch_sync);
    RUN_TEST(test_c58_p1b_hand_delete_batch_async);
    RUN_TEST(test_c58_p2_midwalk_single_sync);
    RUN_TEST(test_c58_p2_midwalk_single_async);
    RUN_TEST(test_c58_p2_midwalk_batch_sync);
    RUN_TEST(test_c58_p2_midwalk_batch_async);
    RUN_TEST(test_c58_p3_known_written_single_sync);
    RUN_TEST(test_c58_p3_known_written_single_async);
    RUN_TEST(test_c58_p4_join_overtaken_single_sync);
    RUN_TEST(test_c58_p4_join_overtaken_single_async);
    RUN_TEST(test_c58_p4b_join_below_line_single_sync);
    RUN_TEST(test_c58_p4b_join_below_line_single_async);
    RUN_TEST(test_c58_p5_use_phase_single_sync);
    RUN_TEST(test_c58_p5_use_phase_single_async);
    RUN_TEST(test_c58_p5_use_phase_batch_sync);
    RUN_TEST(test_c58_p5_use_phase_batch_async);
    RUN_TEST(test_c58_p6_header_scan_single_sync);
    RUN_TEST(test_c58_p6_header_scan_single_async);
#endif
    return UNITY_END();
}
