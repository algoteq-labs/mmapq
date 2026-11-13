/*
 * test_crash_composition.c — CAND-51e: bounded exhaustive crash-composition
 * test over the real code. Closes 51d §2(a) (region-boundary compositions)
 * in C after the owner ruled out a lane-level TLA+ model for 0.2.0.
 *
 * Sequence per case: write -> crash@k -> rejoin -> write -> crash@j ->
 * rejoin -> validate, both queue types, across the residue classes of the
 * region-boundary rules (exact fit; 1-4-byte tails; padding-marker
 * remainder; batch header at cap == 4; message-in-batch at cap == 4;
 * a recovered batch whose last message ends with exactly 4 bytes left).
 *
 * Kill reachability (spec §2): API-boundary points fork a child that
 * raise(SIGKILL)s itself after the target call (no pipes needed); the
 * padding-marker point (P4) arms the one-shot wrap-fault seam
 * (wrap_fault_oneshot.h, both loops); the single-message mid-copy and tail
 * -marker states are pwrite synthesis (no intra-function seam exists in
 * write_payload — the G-1(a) state-equivalence discipline); the
 * crash-during-recovery point (P8) uses the CAND-51e hook
 * mmapq_test_hook_set_batch_recover_walk_step (normal loop only).
 *
 * Oracles: batch_lane_validator.h / payload_lane_validator.h (no holes,
 * tail-zero, header cross-check + uniqueness), agreement of the rejoined
 * write position, the uncommitted walker and the reader, and
 * committed-records-unchanged byte compares.
 *
 * Children never return to Unity: they _exit(n) on any mismatch (the
 * parent fails on a non-signal exit) and raise(SIGKILL) at the kill point.
 * NULL runtime everywhere (the forked child must not inherit mapper
 * threads — the c35 discipline). All parent waits are bounded. Sessions go
 * through the queue object + create_appender (the house idiom — the
 * standalone appender factory does not bootstrap the lazy queue).
 */

#include "unity.h"
#include "mmap_platform.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_reader.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_reader.h"
#include "batch_payload_iterator.h"
#include "internal/header_codec.h"
#include "internal/test_hooks.h"
#include "batch_lane_validator.h"
#include "payload_lane_validator.h"
#include "wrap_fault_oneshot.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <poll.h>

/* ── Geometry (spec §1): the smallest legal real geometry — CAND-38 makes
 * region_size % 32768 == 0 mandatory. G1: boundary == file rollover; G2:
 * boundary inside a file. */
#define CC_REG      32768
#define CC_FILE_G1  32768
#define CC_FILE_G2  65536
#define CC_RING     4
#define CC_TIMEOUT  5000000000ULL
#define CC_ROOT     "/tmp/mmapq_cc"
#define CC_QUEUE    "q"

/* kill points (spec §2) */
enum { BP_OPEN_EMPTY = 0, BP_OPEN_MSGS, BP_OPEN_PARTIAL, BP_MARKER_FAULT,
       BP_STAGED, BP_DISCARDED, BP_COMMITTED };                 /* P1..P7 */
enum { SQ_UNCOMMITTED = 0, SQ_COMMITTED, SQ_SYNTH_PARTIAL,
       SQ_SYNTH_MARKER };                                       /* Q1..Q4 */
/* residue classes */
enum { R_EXACT = 0, R_TAIL1, R_TAIL2, R_TAIL3, R_CNT4, R_MARKER,
       R_MSG4, R_END4 };
/* pre-lane states */
enum { PRE_E = 0, PRE_C, PRE_CS };   /* batch: E = staged steering only;
                                        C = committed; CS = committed+staged.
                                        single: E/C/CU (CU == PRE_CS). */

#define CC_MAX_COMMITTED 10
#define CC_MAX_STAGED    10
#define CC_MAX_MSGS      4

typedef struct {
    int64_t offset;             /* batch start / record start */
    int32_t msg_count;
    int32_t msg_len[CC_MAX_MSGS];
    uint8_t msg_fill[CC_MAX_MSGS];
    int     partial;            /* single-message synthesized partial record */
} CcRec;

typedef struct {
    char     dir[640];
    int64_t  file_size;
    int      pre, residue, k, j;

    /* phase-1 steering + artifact lengths (parent-computed, child-executed) */
    int32_t  steer2_len;        /* CS/CU: the extra committed record's length */
    int32_t  steer_lens[2]; int n_steer;
    int32_t  art_lens[CC_MAX_MSGS]; int n_art;
    int64_t  exp_art_start;
    int64_t  exp_art_padto;     /* P4: the seam's wrap-fail position */

    /* expectations (mirror model) */
    int64_t  exp_pos1;          /* lane write position after rejoin 1 */
    int64_t  exp_pos3;          /* after rejoin 3 */
    int      exp_join1;  int64_t exp_join_off1;
    int      exp_join3;  int64_t exp_join_off3;
    CcRec    staged[CC_MAX_STAGED];  int n_staged;   /* expected at final */
    int      n_staged1;                              /* expected at mid */
    CcRec    committed[CC_MAX_COMMITTED]; int n_committed;
    int      n_committed1;                           /* committed at mid */
    int64_t  j_start;
    int      mid_staged;        /* R_END4: the middle write stays staged */
    int64_t  mid_start;
} CcCase;

static CcCase CUR;              /* fork shares the parent's image */
static int    cc_seq;           /* case counter for dirs/fills */

/* ── Mirror model of the padding rules (driver-side; self-checked) ─────── */

static int64_t cc_place(int64_t pos, int32_t len)
{
    int64_t cap = CC_REG - pos % CC_REG;
    if (cap < PAYLOAD_LENGTH_SIZE + len) pos += cap;   /* short skip or marker */
    return pos;
}
static int64_t cc_after(int64_t pos, int32_t len)
{
    return cc_place(pos, len) + PAYLOAD_LENGTH_SIZE + len;
}
static int64_t cc_batch_start(int64_t pos)
{
    int64_t cap = CC_REG - pos % CC_REG;
    /* CAND-51e unified tail rule: <= 4 (post-fix semantics — the fail-first
     * run against the pre-fix tree diverges exactly at cap == 4). */
    if (cap <= MMAPQ_BATCH_MSG_COUNT_SIZE) pos += cap;
    return pos;
}

/* Message lengths steering so the NEXT write starts at residue target_mod
 * (position % CC_REG == target_mod). 1-2 messages; verified by callers. */
static int cc_steer_msgs(int64_t pos, int64_t target_mod, int32_t lens[2])
{
    int n = 0;
    for (;;) {
        int64_t s = cc_place(pos, 1);
        int64_t L = (target_mod - (s % CC_REG) - PAYLOAD_LENGTH_SIZE) % CC_REG;
        if (L < 1) L += CC_REG;
        if (PAYLOAD_LENGTH_SIZE + L <= CC_REG - (s % CC_REG)) {
            lens[n++] = (int32_t)L;
            return n;
        }
        lens[n++] = 1;                 /* minimal record: move to next region */
        pos = cc_after(pos, 1);
    }
}

static uint8_t cc_fill(int tag) { return (uint8_t)(0x40 + ((cc_seq + tag) % 59)); }

/* ── fs helpers ────────────────────────────────────────────────────────── */

static void cc_rm_rf(const char *path)
{
    char cmd[800];
    snprintf(cmd, sizeof cmd, "rm -rf %s", path);
    int rc = system(cmd); (void)rc;
}

static void cc_case_dir(void)
{
    snprintf(CUR.dir, sizeof CUR.dir, "%s/case_%04d", CC_ROOT, cc_seq);
    cc_rm_rf(CUR.dir);
    char cmd[800];
    snprintf(cmd, sizeof cmd, "mkdir -p %s", CUR.dir);
    int rc = system(cmd); (void)rc;
}

/* One unbuffered line per case (stderr survives a hard crash — a SIGBUS
 * loses buffered stdout); the last line names the crashing case. */
static void cc_trace(const char *suite)
{
    fprintf(stderr, "case %04d %s pre=%d r=%d k=%d j=%d\n",
            cc_seq, suite, CUR.pre, CUR.residue, CUR.k, CUR.j);
}

/* CAND-51e owner ruling (the appender-pool item) / CAND-52 addendum 2 §4:
 * a killed child leaks its appender-id bit (a mmap'd bitmap with no lease
 * or liveness, both libraries). Zero the pool between incarnations — the
 * test-side mirror of the operational recreate/clear step. */
static void cc_reset_id_pool(void)
{
    char path[800];
    snprintf(path, sizeof path, "%s/%s/appender_id_pool", CUR.dir, CC_QUEUE);
    int fd = open(path, O_WRONLY);
    if (fd < 0) return;                          /* no pool file yet */
    char z[64] = {0};                            /* APPENDER_ID_POOL_REGION_SIZE */
    TEST_ASSERT_EQUAL_INT(64, (int)pwrite(fd, z, sizeof z, 0));
    close(fd);
}

/* Synthesis file handling: the writer would have created/extended the file
 * (region-granularity extension), so the synthesis does the same. */
static int cc_pwrite_open(int64_t pos)
{
    char path[800];
    snprintf(path, sizeof path, "%s/%s/payload_0_%lld", CUR.dir, CC_QUEUE,
             (long long)(pos / CUR.file_size));
    int fd = open(path, O_WRONLY | O_CREAT, 0664);
    TEST_ASSERT_GREATER_THAN_INT(0, fd);
    int64_t in_file = pos % CUR.file_size;
    int64_t region_end = (in_file / CC_REG + 1) * CC_REG;
    TEST_ASSERT_EQUAL_INT(0, ftruncate(fd, (off_t)region_end));
    return fd;
}

static void cc_pwrite32(int64_t pos, int32_t v)
{
    int fd = cc_pwrite_open(pos);
    TEST_ASSERT_EQUAL_INT(4, (int)pwrite(fd, &v, 4, (off_t)(pos % CUR.file_size)));
    close(fd);
}

static void cc_pwrite_bytes(int64_t pos, const void *buf, size_t len)
{
    int fd = cc_pwrite_open(pos);
    TEST_ASSERT_EQUAL_INT((int)len, (int)pwrite(fd, buf, len, (off_t)(pos % CUR.file_size)));
    close(fd);
}

/* ── fork machinery (c35 discipline: bounded reap; child never returns) ── */

/* Everything below setUp/tearDown is fork-based or crash-state synthesis;
 * under TSan only the ignore stubs compile (the c35 precedent: the parent
 * is multi-threaded at fork). */
#if !defined(__SANITIZE_THREAD__)
#define CC_DRIVERS 1
#endif

typedef void (*CcChildFn)(void);

#ifdef CC_DRIVERS

static void cc_fork_kill(CcChildFn fn)
{
    pid_t pid = fork();
    TEST_ASSERT_NOT_EQUAL(-1, pid);
    if (pid == 0) {
        alarm(15);
        fn();
        _exit(99);              /* returning from the child fn is a failure */
    }
    int st = 0, waited = 0;
    for (;;) {
        pid_t r = waitpid(pid, &st, WNOHANG);
        if (r == pid) break;
        TEST_ASSERT_EQUAL_INT(0, r);
        if (++waited > 200) {   /* 200 x 50 ms = 10 s bound */
            kill(pid, SIGKILL);
            waitpid(pid, &st, 0);
            TEST_FAIL_MESSAGE("cc: child did not die at its kill point");
        }
        struct pollfd pfd = { .fd = -1 };
        poll(&pfd, 0, 50);
    }
    cc_reset_id_pool();         /* the kill leaked the appender-id bit */
    if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL)) {
        char m[128];
        snprintf(m, sizeof m, "cc: child exit mismatch (status 0x%x — "
                 "a child-side _exit(n) names the failed step)", st);
        TEST_FAIL_MESSAGE(m);
    }
}

/* ── session helpers (house idiom: queue object + create_appender) ─────── */

static int cc_b_open(const char *dir, int64_t file,
                     MmapqIndexedBatchQueue **q, MmapqIndexedBatchAppender **a)
{
    int rc = mmapq_indexed_batch_queue_create(dir, CC_QUEUE, CC_REG, CC_RING,
        1, file, 0, CC_TIMEOUT, 0, NULL, NULL, q);
    if (rc != 0) return rc;
    rc = mmapq_indexed_batch_queue_create_appender(*q, a);
    if (rc != 0) { mmapq_indexed_batch_queue_close(*q); return rc; }
    return 0;
}
static void cc_b_close(MmapqIndexedBatchQueue *q, MmapqIndexedBatchAppender *a)
{
    if (a) mmapq_indexed_batch_appender_close(a);
    if (q) mmapq_indexed_batch_queue_close(q);
}

static int cc_s_open(const char *dir, int64_t file,
                     MmapqIndexedQueue **q, MmapqIndexedAppender **a)
{
    int rc = mmapq_indexed_queue_create(dir, CC_QUEUE, CC_REG, CC_RING, 1,
        file, 0, CC_TIMEOUT, 0, NULL, NULL, q);
    if (rc != 0) return rc;
    rc = mmapq_indexed_queue_create_appender(*q, a);
    if (rc != 0) { mmapq_indexed_queue_close(*q); return rc; }
    return 0;
}
static void cc_s_close(MmapqIndexedQueue *q, MmapqIndexedAppender *a)
{
    if (a) mmapq_indexed_appender_close(a);
    if (q) mmapq_indexed_queue_close(q);
}

/* ── child-side small helpers (no Unity; _exit codes name the step) ────── */

static void cc_die(void) { raise(SIGKILL); _exit(99); }

/* write a batch of the given message lengths, fill bytes by tag base;
 * completion per how: 0 leave open, 1 complete_uncommitted, 2 discard,
 * 3 complete (committed) */
static void cc_b_child_batch(MmapqIndexedBatchAppender *a,
                             const int32_t *lens, int n, int tag, int how,
                             int64_t exp_start)
{
    static char buf[CC_REG];              /* child-local scratch */
    MmapqMutableBatch *b = NULL;
    if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(3);
    if (b->batch_offset != exp_start) _exit(4);
    for (int i = 0; i < n; i++) {
        memset(buf, cc_fill(tag + i), (size_t)lens[i]);
        if (mmapq_mutable_batch_append(b, buf, lens[i]) != i) _exit(5);
    }
    switch (how) {
    case 0: return;                                    /* left open */
    case 1:
        if (mmapq_mutable_batch_complete_uncommitted(b) != exp_start) _exit(6);
        return;
    case 2:
        if (mmapq_mutable_batch_discard(b) != 0) _exit(7);
        return;
    default:
        if (mmapq_mutable_batch_complete(b) < 0) _exit(8);
        return;
    }
}

/* ── artifact builders (batch, child context) ──────────────────────────── */

static void cc_b_child_artifact(MmapqIndexedBatchAppender *a, int which,
                                const int32_t *lens, int n, int64_t exp_start)
{
    static char buf[CC_REG];
    switch (which) {
    case BP_OPEN_EMPTY: {
        MmapqMutableBatch *b = NULL;
        if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(20);
        if (b->batch_offset != exp_start) _exit(21);
        return;
    }
    case BP_OPEN_MSGS:
        cc_b_child_batch(a, lens, n, 100, 0, exp_start);
        return;
    case BP_OPEN_PARTIAL: {
        MmapqMutableBatch *b = NULL;
        if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(22);
        if (b->batch_offset != exp_start) _exit(23);
        memset(buf, cc_fill(110), (size_t)lens[0]);
        if (mmapq_mutable_batch_append(b, buf, lens[0]) != 0) _exit(24);
        void *out = NULL;
        if (mmapq_mutable_batch_append_reserved(b, lens[1], &out) != 1) _exit(25);
        memset(out, cc_fill(111), (size_t)lens[1] / 2);   /* partial content */
        return;                                            /* open, count 0 */
    }
    case BP_MARKER_FAULT: {
        MmapqMutableBatch *b = NULL;
        if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(26);
        if (b->batch_offset != exp_start) _exit(27);
        memset(buf, cc_fill(120), (size_t)lens[0]);
        if (mmapq_mutable_batch_append(b, buf, lens[0]) != 0) _exit(28);
        /* lens[1] forces padding (marker or short skip); the seam fails the
         * wrap at the post-padding position — the B4 on-disk state. */
        bq_arm_oneshot(b, CUR.exp_art_padto);
        memset(buf, cc_fill(121), (size_t)lens[1]);
        if (mmapq_mutable_batch_append(b, buf, lens[1]) != -1) _exit(29);
        if (bq_oneshot_hits != 1) _exit(30);
        return;                                            /* open, count 0 */
    }
    case BP_STAGED:
        cc_b_child_batch(a, lens, n, 130, 1, exp_start);
        return;
    case BP_DISCARDED:
        cc_b_child_batch(a, lens, n, 140, 2, exp_start);
        return;
    default: /* BP_COMMITTED */
        cc_b_child_batch(a, lens, n, 150, 3, exp_start);
        return;
    }
}

/* ── batch phase children ──────────────────────────────────────────────── */

static void cc_b_child_phase1(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    int64_t pos = INITIAL_PAYLOAD_POSITION;

    if (CUR.pre == PRE_C || CUR.pre == PRE_CS) {
        int32_t l[1] = { CUR.steer2_len };
        cc_b_child_batch(a, l, 1, 200, 3, cc_batch_start(pos));
        pos = cc_after(cc_batch_start(pos) + MMAPQ_BATCH_MSG_COUNT_SIZE,
                       CUR.steer2_len);
    }
    if (CUR.n_steer > 0) {   /* the steering batch: committed for C, staged else */
        int how = (CUR.pre == PRE_C) ? 3 : 1;
        cc_b_child_batch(a, CUR.steer_lens, CUR.n_steer, 210, how,
                         cc_batch_start(pos));
    }
    cc_b_child_artifact(a, CUR.k, CUR.art_lens, CUR.n_art, CUR.exp_art_start);
    cc_die();
}

static void cc_b_child_phase2(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    /* nothing to complete from rejoin 1 (the parent session did it) */
    int64_t off = -1;
    if (mmapq_indexed_batch_appender_join_completed_open_batch(a, &off) != 0)
        _exit(40);
    /* the middle write — its start asserts the rejoined position */
    int32_t ml[1] = { 16 };
    cc_b_child_batch(a, ml, 1, 300, CUR.mid_staged ? 1 : 3, CUR.mid_start);
    /* artifact j at interior geometry */
    cc_b_child_artifact(a, CUR.j, CUR.art_lens, CUR.n_art, CUR.j_start);
    cc_die();
}

/* ── single-message phase children ─────────────────────────────────────── */

static void cc_s_child_record(MmapqIndexedAppender *a, int32_t len, int tag,
                              int commit, int64_t exp_off)
{
    static char buf[CC_REG];
    memset(buf, cc_fill(tag), (size_t)len);
    int64_t off = mmapq_indexed_appender_append_uncommitted(a, buf, len);
    if (off != exp_off) _exit(50);
    if (commit) {
        if (mmapq_indexed_appender_commit(a, off) < 0) _exit(51);
    }
}

static void cc_s_child_phase1(void)
{
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    if (cc_s_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    int64_t pos = INITIAL_PAYLOAD_POSITION;

    if (CUR.pre == PRE_C || CUR.pre == PRE_CS) {
        cc_s_child_record(a, CUR.steer2_len, 400, 1, cc_place(pos, CUR.steer2_len));
        pos = cc_after(pos, CUR.steer2_len);
    }
    /* the steering record: committed for C, uncommitted for E/CU */
    cc_s_child_record(a, CUR.steer_lens[0], 401, CUR.pre == PRE_C,
                      cc_place(pos, CUR.steer_lens[0]));
    pos = cc_after(pos, CUR.steer_lens[0]);
    /* artifact k (child types only; synthesis is parent-side) */
    static char buf[CC_REG];
    memset(buf, cc_fill(410), (size_t)CUR.art_lens[0]);
    if (CUR.k == SQ_UNCOMMITTED) {
        if (mmapq_indexed_appender_append_uncommitted(a, buf, CUR.art_lens[0])
                != CUR.exp_art_start) _exit(52);
    } else {                   /* SQ_COMMITTED */
        if (mmapq_indexed_appender_append(a, buf, CUR.art_lens[0]) < 0)
            _exit(53);
    }
    cc_die();
}

static void cc_s_child_phase2(void)
{
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    if (cc_s_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    /* middle write: uncommitted + commit — the offset asserts the position */
    cc_s_child_record(a, 16, 500, 1, CUR.mid_start);
    static char buf[CC_REG];
    memset(buf, cc_fill(510), (size_t)CUR.art_lens[0]);
    if (CUR.j == SQ_UNCOMMITTED) {
        if (mmapq_indexed_appender_append_uncommitted(a, buf, CUR.art_lens[0])
                != CUR.j_start) _exit(54);
    } else {
        if (mmapq_indexed_appender_append(a, buf, CUR.art_lens[0]) < 0)
            _exit(55);
    }
    cc_die();
}

/* ── expectation recording (parent) ────────────────────────────────────── */

static void cc_rec_fill(CcRec *r, int64_t offset, int n,
                        const int32_t *lens, int tag_base)
{
    r->offset = offset;
    r->msg_count = n;
    r->partial = 0;
    for (int i = 0; i < n; i++) {
        r->msg_len[i] = lens[i];
        r->msg_fill[i] = cc_fill(tag_base + i);
    }
}

/* ── batch artifact geometry ───────────────────────────────────────────── */

/* The batch-start residue the artifact needs (steering target), or -1. */
static int64_t cc_b_art_target(int residue)
{
    switch (residue) {
    case R_TAIL1: return CC_REG - 1;
    case R_TAIL2: return CC_REG - 2;
    case R_TAIL3: return CC_REG - 3;
    case R_CNT4:  return CC_REG - 4;    /* unified rule: header skips 4 */
    case R_MARKER: return CC_REG - 12;  /* msg0 meets cap 8 -> marker */
    default:      return -1;            /* R_EXACT / R_MSG4 / R_END4: internal */
    }
}

/* Artifact message lengths per residue. */
static void cc_b_art_lens(int residue, int64_t art_start,
                          int32_t lens[CC_MAX_MSGS], int *n)
{
    int64_t m0 = art_start + MMAPQ_BATCH_MSG_COUNT_SIZE;
    switch (residue) {
    case R_EXACT: {
        lens[0] = 16;
        int64_t s1 = cc_after(m0, 16);
        int32_t L = (int32_t)(CC_REG - (s1 % CC_REG) - PAYLOAD_LENGTH_SIZE);
        if (L < 1) L += CC_REG;
        lens[1] = L;                       /* msg1 ends exactly at region end */
        *n = 2;
        return;
    }
    case R_MSG4: {
        /* msg0 ends leaving cap == 4; msg1 meets it (message rule: skip) */
        int32_t st[2];
        int ns = cc_steer_msgs(m0, CC_REG - 4, st);
        for (int i = 0; i < ns; i++) lens[i] = st[i];
        lens[ns] = 16;
        *n = ns + 1;
        return;
    }
    case R_END4: {
        /* the batch's LAST message ends with exactly 4 bytes left */
        lens[0] = 16;
        int64_t s1 = cc_after(m0, 16);
        int32_t L = (int32_t)(CC_REG - 4 - (s1 % CC_REG) - PAYLOAD_LENGTH_SIZE);
        if (L < 1) L += CC_REG;
        lens[1] = L;
        *n = 2;
        return;
    }
    default:
        lens[0] = 16; lens[1] = 24;
        *n = 2;
        return;
    }
}

/* lane position after the artifact's on-disk footprint */
static int64_t cc_b_art_end(int which, int64_t art_start,
                            const int32_t *lens, int n)
{
    if (which == BP_OPEN_EMPTY) return art_start;   /* slot untouched (zeros) */
    int64_t pos = art_start + MMAPQ_BATCH_MSG_COUNT_SIZE;
    if (which == BP_MARKER_FAULT) {
        pos = cc_after(pos, lens[0]);               /* msg0 */
        int64_t cap = CC_REG - pos % CC_REG;
        return pos + cap;                           /* marker/short skip + EOS */
    }
    for (int i = 0; i < n; i++) pos = cc_after(pos, lens[i]);
    /* Open artifacts are recovered at the rejoin: the recovery walk
     * normalizes a terminal <= 4-byte tail away (payload_walk's
     * short-padding skip), so the position lands past it. In-session
     * completed artifacts keep the content end (the skip happens at the
     * NEXT write's space rule, not at completion). */
    if (which == BP_OPEN_MSGS || which == BP_OPEN_PARTIAL) {
        int64_t cap = CC_REG - pos % CC_REG;
        if (cap <= MMAPQ_BATCH_MSG_COUNT_SIZE) pos += cap;
    }
    return pos;
}

static void cc_b_plan_artifact(int which, int residue, int64_t art_start)
{
    if (which == BP_OPEN_EMPTY) {
        CUR.n_art = 0;
        return;
    }
    if (which == BP_MARKER_FAULT) {
        int64_t m0 = art_start + MMAPQ_BATCH_MSG_COUNT_SIZE;
        if (residue == R_MSG4) {
            /* msg0 ends leaving cap == 4; the failed append short-skips */
            int32_t st[2];
            int ns = cc_steer_msgs(m0, CC_REG - 4, st);
            CUR.art_lens[0] = st[0];
            CUR.n_art = 2;
            int64_t p1 = cc_after(m0, st[0]);
            if (ns == 2) {   /* rare: minimal record first */
                CUR.art_lens[0] = st[0];
                CUR.art_lens[1] = st[1];
                CUR.n_art = 3;
                p1 = cc_after(cc_after(m0, st[0]), st[1]);
            }
            int64_t cap = CC_REG - p1 % CC_REG;
            CUR.art_lens[CUR.n_art - 1] = 16;
            CUR.exp_art_padto = p1 + cap;
            return;
        }
        if (residue == R_END4) {
            /* msg0 ends with exactly 4 bytes left; the failed append
             * short-skips the 4-byte tail */
            int32_t L = (int32_t)(CC_REG - 4 - (m0 % CC_REG) - PAYLOAD_LENGTH_SIZE);
            if (L < 1) L += CC_REG;
            CUR.art_lens[0] = L;
            CUR.art_lens[1] = 16;
            CUR.n_art = 2;
            int64_t p1 = cc_after(m0, L);
            CUR.exp_art_padto = p1 + (CC_REG - p1 % CC_REG);
            return;
        }
        CUR.art_lens[0] = 16;
        CUR.n_art = 2;
        int64_t p1 = cc_after(m0, 16);
        int64_t cap = CC_REG - p1 % CC_REG;
        /* the failed append must force padding: 4+len > cap */
        CUR.art_lens[1] = cap > PAYLOAD_LENGTH_SIZE ? (int32_t)cap : 16;
        CUR.exp_art_padto = p1 + cap;
        return;
    }
    cc_b_art_lens(residue, art_start, CUR.art_lens, &CUR.n_art);
}

/* artifact outcome: record expectations */
static void cc_b_record_artifact(int which, int64_t art_start,
                                 const int32_t *lens, int n,
                                 int *n_staged, int *n_committed)
{
    if (which == BP_OPEN_MSGS || which == BP_OPEN_PARTIAL ||
        which == BP_STAGED || which == BP_DISCARDED) {
        CcRec *r = &CUR.staged[(*n_staged)++];
        int nn = n;
        cc_rec_fill(r, art_start, nn, lens,
                    which == BP_OPEN_MSGS ? 100 : which == BP_OPEN_PARTIAL ? 110 :
                    which == BP_STAGED ? 130 : 140);
    } else if (which == BP_MARKER_FAULT) {
        /* join completion counts msg0..msgN-2 (all but the failed append) */
        CcRec *r = &CUR.staged[(*n_staged)++];
        cc_rec_fill(r, art_start, n - 1, lens, 120);
    } else if (which == BP_COMMITTED) {
        CcRec *r = &CUR.committed[(*n_committed)++];
        cc_rec_fill(r, art_start, n, lens, 150);
    }
}

/* ── batch case driver ─────────────────────────────────────────────────── */

static void run_batch_case(int pre, int residue, int k, int j, int64_t file_size)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = file_size;
    CUR.pre = pre; CUR.residue = residue; CUR.k = k; CUR.j = j;
    CUR.mid_staged = (residue == R_END4);
    cc_case_dir();
    cc_trace("batch");

    /* ── phase-1 plan (mirror model) ── */
    int64_t pos = INITIAL_PAYLOAD_POSITION;
    int n_staged = 0, n_committed = 0;

    if (pre == PRE_C || pre == PRE_CS) {
        CUR.steer2_len = 12;
        int64_t s = cc_batch_start(pos);
        cc_rec_fill(&CUR.committed[n_committed++], s, 1, &CUR.steer2_len, 200);
        pos = cc_after(s + MMAPQ_BATCH_MSG_COUNT_SIZE, CUR.steer2_len);
    }
    /* steering batch to the artifact's required start residue */
    {
        int64_t target = cc_b_art_target(residue);
        int64_t s_pre = cc_batch_start(pos);
        if (target >= 0) {
            /* the steering batch's messages end with pos % REG == target */
            CUR.n_steer = cc_steer_msgs(s_pre + MMAPQ_BATCH_MSG_COUNT_SIZE,
                                        target, CUR.steer_lens);
        } else {
            CUR.n_steer = 1;
            CUR.steer_lens[0] = 8;
        }
        CcRec *r;
        if (pre == PRE_C) r = &CUR.committed[n_committed++];
        else              r = &CUR.staged[n_staged++];
        cc_rec_fill(r, s_pre, CUR.n_steer, CUR.steer_lens, 210);
        pos = s_pre + MMAPQ_BATCH_MSG_COUNT_SIZE;
        for (int i = 0; i < CUR.n_steer; i++) pos = cc_after(pos, CUR.steer_lens[i]);
        if (target >= 0)
            TEST_ASSERT_EQUAL_INT64(target, pos % CC_REG);
    }
    int64_t art_start = cc_batch_start(pos);
    CUR.exp_art_start = art_start;

    cc_b_plan_artifact(k, residue, art_start);
    CUR.exp_pos1 = cc_b_art_end(k, art_start, CUR.art_lens, CUR.n_art);

    CUR.exp_join1 = (k == BP_OPEN_MSGS || k == BP_OPEN_PARTIAL ||
                     k == BP_MARKER_FAULT) ? 1 : 0;
    CUR.exp_join_off1 = art_start;
    cc_b_record_artifact(k, art_start, CUR.art_lens, CUR.n_art,
                         &n_staged, &n_committed);
    CUR.n_staged1 = n_staged;
    CUR.n_committed1 = n_committed;

    /* phase 1: child writes + dies */
    cc_fork_kill(cc_b_child_phase1);

    /* ── rejoin 1 (parent): mid oracles ── */
    {
        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, file_size, &q, &a));
        int64_t off = -1;
        int jr = mmapq_indexed_batch_appender_join_completed_open_batch(a, &off);
        TEST_ASSERT_EQUAL_INT(CUR.exp_join1, jr);
        if (jr == 1) TEST_ASSERT_EQUAL_INT64(CUR.exp_join_off1, off);
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));

        /* walker agreement (mid) */
        MmapqUncommittedBatchIterator *it = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
        for (int i = 0; i < CUR.n_staged1; i++) {
            int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
            TEST_ASSERT_EQUAL_INT(1,
                mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
            TEST_ASSERT_EQUAL_INT64(CUR.staged[i].offset, bo);
            TEST_ASSERT_EQUAL_INT32(CUR.staged[i].msg_count, mc);
        }
        {
            int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
        }
        cc_b_close(q, a);
    }
    assert_batch_lane(CUR.dir, CC_QUEUE, 0, CUR.exp_pos1);

    /* ── phase-2 plan: middle write + artifact j (interior geometry) ── */
    int64_t mid_start = cc_batch_start(CUR.exp_pos1);
    CUR.mid_start = mid_start;
    {
        int32_t ml[1] = { 16 };
        CcRec *r = CUR.mid_staged ? &CUR.staged[n_staged++]
                                  : &CUR.committed[n_committed++];
        cc_rec_fill(r, mid_start, 1, ml, 300);
        pos = cc_after(mid_start + MMAPQ_BATCH_MSG_COUNT_SIZE, 16);
    }
    int64_t j_start = cc_batch_start(pos);
    CUR.j_start = j_start;
    {
        cc_b_plan_artifact(j, -1, j_start);   /* j at interior geometry */
        CUR.exp_pos3 = cc_b_art_end(j, j_start, CUR.art_lens, CUR.n_art);

        CUR.exp_join3 = (j == BP_OPEN_MSGS || j == BP_OPEN_PARTIAL ||
                         j == BP_MARKER_FAULT) ? 1 : 0;
        CUR.exp_join_off3 = j_start;
        cc_b_record_artifact(j, j_start, CUR.art_lens, CUR.n_art,
                             &n_staged, &n_committed);
        CUR.n_staged = n_staged;
        CUR.n_committed = n_committed;
    }

    /* phase 2: child writes + dies */
    cc_fork_kill(cc_b_child_phase2);

    /* ── rejoin 3 (parent): final oracles ── */
    {
        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, file_size, &q, &a));
        int64_t off = -1;
        int jr = mmapq_indexed_batch_appender_join_completed_open_batch(a, &off);
        TEST_ASSERT_EQUAL_INT(CUR.exp_join3, jr);
        if (jr == 1) TEST_ASSERT_EQUAL_INT64(CUR.exp_join_off3, off);
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));

        /* walker agreement (final): the exact staged set, in position order */
        MmapqUncommittedBatchIterator *it = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
        for (int i = 0; i < CUR.n_staged; i++) {
            int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
            TEST_ASSERT_EQUAL_INT(1,
                mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
            TEST_ASSERT_EQUAL_INT64(CUR.staged[i].offset, bo);
            TEST_ASSERT_EQUAL_INT32(CUR.staged[i].msg_count, mc);
        }
        {
            int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
        }
        cc_b_close(q, a);
    }
    assert_batch_lane(CUR.dir, CC_QUEUE, 0, CUR.exp_pos3);

    /* reader: committed records unchanged, exact bytes, index order */
    {
        MmapqIndexedBatchReader *r = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_reader_create(CUR.dir, CC_QUEUE, CC_RING,
                CC_TIMEOUT, NULL, &r));
        for (int64_t i = 0; i < CUR.n_committed; i++) {
            MmapqBatchReadingContext ctx;
            TEST_ASSERT_NOT_NULL(mmapq_indexed_batch_reader_read(r, i, &ctx));
            TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
            TEST_ASSERT_EQUAL_INT64(CUR.committed[i].offset, ctx.batch_offset);
            TEST_ASSERT_EQUAL_INT32(CUR.committed[i].msg_count, ctx.message_count);
            for (int m = 0; m < CUR.committed[i].msg_count; m++) {
                const void *data = NULL; int32_t len = 0;
                TEST_ASSERT_EQUAL_INT(1,
                    mmapq_batch_iterator_next(ctx.iterator, &data, &len));
                TEST_ASSERT_EQUAL_INT32(CUR.committed[i].msg_len[m], len);
                const uint8_t *exp = malloc((size_t)len);
                TEST_ASSERT_NOT_NULL(exp);
                memset((void *)exp, CUR.committed[i].msg_fill[m], (size_t)len);
                TEST_ASSERT_EQUAL_MEMORY(exp, data, (size_t)len);
                free((void *)exp);
            }
            const void *data = NULL; int32_t len = 0;
            TEST_ASSERT_EQUAL_INT(0,
                mmapq_batch_iterator_next(ctx.iterator, &data, &len));
        }
        mmapq_indexed_batch_reader_close(r);
    }

    /* position agreement probe (last; the empty discard leaves the lane) */
    {
        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, file_size, &q, &a));
        MmapqMutableBatch *b = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
        TEST_ASSERT_NOT_NULL(b);
        TEST_ASSERT_EQUAL_INT64(cc_batch_start(CUR.exp_pos3), b->batch_offset);
        TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_discard(b));
        cc_b_close(q, a);
    }
}

/* ── single-message case driver ────────────────────────────────────────── */

static int64_t cc_s_art_target(int residue)
{
    switch (residue) {
    case R_TAIL1: return CC_REG - 1;
    case R_TAIL2: return CC_REG - 2;
    case R_TAIL3: return CC_REG - 3;
    case R_CNT4:  return CC_REG - 4;    /* cap == 4: message rule skip */
    case R_MARKER: return CC_REG - 8;   /* cap 8 > 4, unfit -> marker */
    default:      return -1;            /* R_EXACT: the length adjusts */
    }
}

/* artifact payload length for Q1/Q2/Q3 (R_EXACT: ends at the region end) */
static int32_t cc_s_art_len(int residue, int64_t pos)
{
    if (residue != R_EXACT) return 16;
    int64_t s = cc_place(pos, 1);
    int64_t L = CC_REG - (s % CC_REG) - PAYLOAD_LENGTH_SIZE;
    if (L < 1) L += CC_REG;
    return (int32_t)L;
}

static void run_sm_case(int pre, int residue, int k, int j, int64_t file_size)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = file_size;
    CUR.pre = pre; CUR.residue = residue; CUR.k = k; CUR.j = j;
    cc_case_dir();
    cc_trace("sm");

    /* ── phase-1 plan ── */
    int64_t pos = INITIAL_PAYLOAD_POSITION;
    int n_staged = 0, n_committed = 0;
    int64_t target = cc_s_art_target(residue);

    if (pre == PRE_C || pre == PRE_CS) {
        CUR.steer2_len = 12;
        cc_rec_fill(&CUR.committed[n_committed++], cc_place(pos, 12), 1,
                    &CUR.steer2_len, 400);
        pos = cc_after(pos, 12);
    }
    /* steering record */
    {
        int32_t sl;
        if (target >= 0) {
            int32_t st[2];
            int ns = cc_steer_msgs(pos, target, st);
            TEST_ASSERT_EQUAL_INT(1, ns);   /* a single record must suffice */
            sl = st[0];
        } else {
            sl = 8;
        }
        CUR.steer_lens[0] = sl; CUR.n_steer = 1;
        CcRec *r;
        if (pre == PRE_C) r = &CUR.committed[n_committed++];
        else              r = &CUR.staged[n_staged++];
        cc_rec_fill(r, cc_place(pos, sl), 1, &CUR.steer_lens[0], 401);
        pos = cc_after(pos, sl);
        if (target >= 0)
            TEST_ASSERT_EQUAL_INT64(target, pos % CC_REG);
    }

    /* artifact k */
    int32_t art_len = (k == SQ_SYNTH_MARKER) ? 0 : cc_s_art_len(residue, pos);
    int64_t art_pre   = pos;   /* pre-skip position (padding written here) */
    int64_t art_place = (k == SQ_SYNTH_MARKER) ? pos : cc_place(pos, art_len);
    CUR.exp_art_start = art_place;
    CUR.art_lens[0] = art_len; CUR.n_art = art_len ? 1 : 0;
    if (k == SQ_UNCOMMITTED || k == SQ_COMMITTED) {
        CcRec *r;
        if (k == SQ_COMMITTED) r = &CUR.committed[n_committed++];
        else                   r = &CUR.staged[n_staged++];
        cc_rec_fill(r, art_place, 1, &art_len, 410);
        pos = cc_after(pos, art_len);
    } else if (k == SQ_SYNTH_PARTIAL) {
        CcRec *r = &CUR.staged[n_staged++];
        cc_rec_fill(r, art_place, 1, &art_len, 410);
        r->partial = 1;
        pos = art_place + PAYLOAD_LENGTH_SIZE + art_len;
    } else {                 /* SQ_SYNTH_MARKER: skip to the region end */
        TEST_ASSERT_GREATER_THAN_INT64(PAYLOAD_LENGTH_SIZE,
                                       CC_REG - pos % CC_REG);
        pos += CC_REG - pos % CC_REG;
    }
    CUR.exp_pos1 = pos;
    CUR.n_staged1 = n_staged;
    CUR.n_committed1 = n_committed;

    /* phase 1 execution */
    if (k == SQ_UNCOMMITTED || k == SQ_COMMITTED) {
        cc_fork_kill(cc_s_child_phase1);
    } else {
        /* parent synthesis: pre-lane + steering via a clean session, close,
         * then pwrite the crash artifact at the EOS */
        MmapqIndexedQueue *q = NULL;
        MmapqIndexedAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, file_size, &q, &a));
        int64_t p = INITIAL_PAYLOAD_POSITION;
        char buf[CC_REG];
        if (pre == PRE_C || pre == PRE_CS) {
            memset(buf, cc_fill(400), 12);
            int64_t o = mmapq_indexed_appender_append_uncommitted(a, buf, 12);
            TEST_ASSERT_EQUAL_INT64(cc_place(p, 12), o);
            TEST_ASSERT_GREATER_THAN_INT64(-1, mmapq_indexed_appender_commit(a, o));
            p = cc_after(p, 12);
        }
        memset(buf, cc_fill(401), (size_t)CUR.steer_lens[0]);
        int64_t o = mmapq_indexed_appender_append_uncommitted(a, buf,
                                                              CUR.steer_lens[0]);
        TEST_ASSERT_EQUAL_INT64(cc_place(p, CUR.steer_lens[0]), o);
        if (pre == PRE_C)
            TEST_ASSERT_GREATER_THAN_INT64(-1, mmapq_indexed_appender_commit(a, o));
        cc_s_close(q, a);
        if (k == SQ_SYNTH_PARTIAL) {
            /* a real crash leaves the writer's marker when the record was
             * padded across the boundary (cap > 4); short tails (cap <= 4)
             * write nothing */
            int64_t cap0 = CC_REG - art_pre % CC_REG;
            if (art_place > art_pre && cap0 > PAYLOAD_LENGTH_SIZE)
                cc_pwrite32(art_pre, (int32_t)(PAYLOAD_LENGTH_SIZE - cap0));
            char pay[CC_REG / 2];
            memset(pay, cc_fill(410), (size_t)art_len / 2);
            cc_pwrite32(art_place, art_len);
            cc_pwrite_bytes(art_place + PAYLOAD_DATA_OFFSET, pay,
                            (size_t)art_len / 2);   /* half the declared content */
        } else {
            int64_t cap = CC_REG - art_place % CC_REG;
            cc_pwrite32(art_place, (int32_t)(PAYLOAD_LENGTH_SIZE - cap));
        }
    }

    /* ── rejoin 1 (parent): mid oracles + position probe (recorded) ── */
    int64_t probe_place;
    {
        MmapqIndexedQueue *q = NULL;
        MmapqIndexedAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, file_size, &q, &a));
        MmapqUncommittedPayloadIterator *it = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_appender_uncommitted_iterator_checked(a,
                MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
                &it));
        for (int i = 0; i < CUR.n_staged1; i++) {
            const void *data = NULL; int32_t len = 0; int64_t off = -2;
            TEST_ASSERT_EQUAL_INT(1,
                mmapq_uncommitted_iterator_next(it, &data, &len, &off));
            TEST_ASSERT_EQUAL_INT64(CUR.staged[i].offset, off);
            TEST_ASSERT_EQUAL_INT32(CUR.staged[i].msg_len[0], len);
        }
        {
            const void *data = NULL; int32_t len = 0; int64_t off = -2;
            TEST_ASSERT_EQUAL_INT(0,
                mmapq_uncommitted_iterator_next(it, &data, &len, &off));
        }
        /* position probe: an uncommitted probe record (joins the set) */
        int32_t probe_len = 20;
        char pb[20];
        memset(pb, cc_fill(600), sizeof pb);
        probe_place = cc_place(pos, probe_len);
        int64_t po = mmapq_indexed_appender_append_uncommitted(a, pb, probe_len);
        TEST_ASSERT_EQUAL_INT64(probe_place, po);
        cc_rec_fill(&CUR.staged[n_staged++], po, 1, &probe_len, 600);
        cc_s_close(q, a);
        pos = cc_after(pos, probe_len);
    }
    assert_payload_lane(CUR.dir, CC_QUEUE, 0,
                        probe_place + PAYLOAD_LENGTH_SIZE + 20);

    /* ── phase-2 plan: middle committed record + artifact j ── */
    {
        int32_t ml = 16;
        CUR.mid_start = cc_place(pos, 16);
        cc_rec_fill(&CUR.committed[n_committed++], CUR.mid_start, 1, &ml, 500);
        pos = cc_after(pos, 16);
    }
    int32_t jl_pre = (j == SQ_SYNTH_PARTIAL) ? 100 : 16;
    int64_t j_pre   = pos;              /* pre-skip position of the j artifact */
    int64_t j_place = (j == SQ_SYNTH_MARKER) ? pos : cc_place(pos, jl_pre);
    CUR.j_start = j_place;
    {
        int32_t jl = jl_pre;
        CUR.art_lens[0] = jl; CUR.n_art = (j == SQ_SYNTH_MARKER) ? 0 : 1;
        if (j == SQ_UNCOMMITTED || j == SQ_COMMITTED) {
            CcRec *r;
            if (j == SQ_COMMITTED) r = &CUR.committed[n_committed++];
            else                   r = &CUR.staged[n_staged++];
            cc_rec_fill(r, j_place, 1, &jl, 510);
            pos = cc_after(pos, jl);
        } else if (j == SQ_SYNTH_PARTIAL) {
            CcRec *r = &CUR.staged[n_staged++];
            cc_rec_fill(r, j_place, 1, &jl, 510);
            r->partial = 1;
            pos = j_place + PAYLOAD_LENGTH_SIZE + jl;
        } else {             /* SQ_SYNTH_MARKER */
            int64_t cap = CC_REG - pos % CC_REG;
            TEST_ASSERT_GREATER_THAN_INT64(PAYLOAD_LENGTH_SIZE, cap);
            pos += cap;
        }
        CUR.exp_pos3 = pos;
        CUR.n_staged = n_staged;
        CUR.n_committed = n_committed;
    }

    /* phase 2 execution */
    if (j == SQ_UNCOMMITTED || j == SQ_COMMITTED) {
        cc_fork_kill(cc_s_child_phase2);
    } else {
        MmapqIndexedQueue *q = NULL;
        MmapqIndexedAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, file_size, &q, &a));
        char mb[16];
        memset(mb, cc_fill(500), sizeof mb);
        int64_t mo = mmapq_indexed_appender_append_uncommitted(a, mb, 16);
        TEST_ASSERT_EQUAL_INT64(CUR.mid_start, mo);
        TEST_ASSERT_GREATER_THAN_INT64(-1, mmapq_indexed_appender_commit(a, mo));
        cc_s_close(q, a);
        if (j == SQ_SYNTH_PARTIAL) {
            /* the writer's marker when the record was padded across the
             * boundary (cap > 4); short tails write nothing */
            int64_t cap0 = CC_REG - j_pre % CC_REG;
            if (j_place > j_pre && cap0 > PAYLOAD_LENGTH_SIZE)
                cc_pwrite32(j_pre, (int32_t)(PAYLOAD_LENGTH_SIZE - cap0));
            char pay[50];
            memset(pay, cc_fill(510), sizeof pay);
            cc_pwrite32(j_place, 100);
            cc_pwrite_bytes(j_place + PAYLOAD_DATA_OFFSET, pay, sizeof pay);
        } else {
            int64_t cap = CC_REG - j_place % CC_REG;
            cc_pwrite32(j_place, (int32_t)(PAYLOAD_LENGTH_SIZE - cap));
        }
    }

    /* ── rejoin 3 (parent): final oracles ── */
    {
        MmapqIndexedQueue *q = NULL;
        MmapqIndexedAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, file_size, &q, &a));
        MmapqUncommittedPayloadIterator *it = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_appender_uncommitted_iterator_checked(a,
                MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
                &it));
        for (int i = 0; i < CUR.n_staged; i++) {
            const void *data = NULL; int32_t len = 0; int64_t off = -2;
            TEST_ASSERT_EQUAL_INT(1,
                mmapq_uncommitted_iterator_next(it, &data, &len, &off));
            TEST_ASSERT_EQUAL_INT64(CUR.staged[i].offset, off);
            TEST_ASSERT_EQUAL_INT32(CUR.staged[i].msg_len[0], len);
            if (CUR.staged[i].partial) {
                /* the documented partial-content tail (51c S1): the written
                 * prefix is intact, the rest reads zero */
                int32_t L = CUR.staged[i].msg_len[0];
                const uint8_t *d = data;
                for (int32_t x = 0; x < L / 2; x++)
                    TEST_ASSERT_EQUAL_UINT8(CUR.staged[i].msg_fill[0], d[x]);
                for (int32_t x = L / 2; x < L; x++)
                    TEST_ASSERT_EQUAL_UINT8(0, d[x]);
            }
        }
        {
            const void *data = NULL; int32_t len = 0; int64_t off = -2;
            TEST_ASSERT_EQUAL_INT(0,
                mmapq_uncommitted_iterator_next(it, &data, &len, &off));
        }
        cc_s_close(q, a);
    }
    assert_payload_lane(CUR.dir, CC_QUEUE, 0, CUR.exp_pos3);

    /* reader: committed records unchanged, exact bytes, index order */
    {
        MmapqIndexedReader *r = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_reader_create(CUR.dir, CC_QUEUE, CC_RING, CC_TIMEOUT,
                NULL, &r));
        for (int64_t i = 0; i < CUR.n_committed; i++) {
            MmapqReadingContext ctx = {0};
            TEST_ASSERT_NOT_NULL(mmapq_indexed_reader_read(r, i, &ctx));
            TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
            TEST_ASSERT_EQUAL_INT64(CUR.committed[i].offset, ctx.msg_offset);
            TEST_ASSERT_EQUAL_INT32(CUR.committed[i].msg_len[0], ctx.length);
            const uint8_t *exp = malloc((size_t)ctx.length);
            TEST_ASSERT_NOT_NULL(exp);
            memset((void *)exp, CUR.committed[i].msg_fill[0], (size_t)ctx.length);
            TEST_ASSERT_EQUAL_MEMORY(exp, ctx.data, (size_t)ctx.length);
            free((void *)exp);
        }
        mmapq_indexed_reader_close(r);
    }
}

/* ── P8: crash during join completion (hooks-gated, normal loop only) ──── */

#if defined(MMAPQ_TEST_HOOKS) && !defined(__SANITIZE_THREAD__)
static int cc_p8_die_at;
static void cc_p8_hook(int walked)
{
    if (walked == cc_p8_die_at) { raise(SIGKILL); _exit(99); }
}

static void cc_p8_child_write(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    int64_t pos = INITIAL_PAYLOAD_POSITION;
    if (CUR.pre == PRE_C) {
        int32_t l[1] = { 12 };
        cc_b_child_batch(a, l, 1, 700, 3, cc_batch_start(pos));
        pos = cc_after(cc_batch_start(pos) + MMAPQ_BATCH_MSG_COUNT_SIZE, 12);
    }
    cc_b_child_batch(a, CUR.art_lens, CUR.n_art, 710, 0,
                     cc_batch_start(pos));
    cc_die();
}

static void cc_p8_child_rejoin(void)
{
    mmapq_test_hook_set_batch_recover_walk_step(cc_p8_hook);
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    _exit(60);                /* the hook never fired — non-vacuity guard */
}

static void run_p8_case(int pre, int marker, int s)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    CUR.pre = pre;
    cc_case_dir();

    int64_t pos = INITIAL_PAYLOAD_POSITION;
    int n_committed = 0;
    if (pre == PRE_C) {
        int32_t l = 12;
        cc_rec_fill(&CUR.committed[n_committed++], cc_batch_start(pos), 1, &l, 700);
        pos = cc_after(cc_batch_start(pos) + MMAPQ_BATCH_MSG_COUNT_SIZE, 12);
    }
    int64_t bstart = cc_batch_start(pos);
    /* three messages; marker variant: msg1 meets cap 8 -> long marker */
    if (marker) {
        int32_t st[2];
        int ns = cc_steer_msgs(bstart + MMAPQ_BATCH_MSG_COUNT_SIZE,
                               CC_REG - 8, st);
        int n = 0;
        for (int i = 0; i < ns; i++) CUR.art_lens[n++] = st[i];
        CUR.art_lens[n++] = 16;
        CUR.art_lens[n++] = 16;
        CUR.n_art = n;
    } else {
        CUR.art_lens[0] = 16; CUR.art_lens[1] = 16; CUR.art_lens[2] = 16;
        CUR.n_art = 3;
    }
    int64_t pos1 = bstart + MMAPQ_BATCH_MSG_COUNT_SIZE;
    for (int i = 0; i < CUR.n_art; i++) pos1 = cc_after(pos1, CUR.art_lens[i]);
    CUR.exp_pos1 = pos1;
    CUR.n_committed1 = n_committed;
    cc_rec_fill(&CUR.staged[0], bstart, CUR.n_art, CUR.art_lens, 710);
    CUR.n_staged = 1;

    cc_p8_die_at = s;
    cc_fork_kill(cc_p8_child_write);
    cc_fork_kill(cc_p8_child_rejoin);   /* dies at the s-th walked message */

    /* re-rejoin: completion is idempotent — same offset, same state */
    {
        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));
        int64_t off = -1;
        TEST_ASSERT_EQUAL_INT(1,
            mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));
        TEST_ASSERT_EQUAL_INT64(bstart, off);
        MmapqUncommittedBatchIterator *it = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
        int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
        TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
        TEST_ASSERT_EQUAL_INT64(bstart, bo);
        TEST_ASSERT_EQUAL_INT32(CUR.n_art, mc);
        TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
        cc_b_close(q, a);
    }
    assert_batch_lane(CUR.dir, CC_QUEUE, 0, CUR.exp_pos1);
}
#endif /* MMAPQ_TEST_HOOKS && !__SANITIZE_THREAD__ */

/* ── misfit synthesis (the M1 fail-first target) ───────────────────────── */

static void run_misfit_case(int batch, int at_boundary)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();

    if (batch) {
        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));
        MmapqMutableBatch *b = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
        TEST_ASSERT_NOT_NULL(b);
        TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, b->batch_offset);
        int64_t m0 = INITIAL_PAYLOAD_POSITION + MMAPQ_BATCH_MSG_COUNT_SIZE;
        int32_t l0 = 16;
        if (at_boundary) {
            /* msg0 steered so the misfit position has cap 16 */
            int32_t st[2];
            int ns = cc_steer_msgs(m0, CC_REG - 16, st);
            TEST_ASSERT_EQUAL_INT(1, ns);
            l0 = st[0];
        }
        char buf[CC_REG];
        memset(buf, 0x55, (size_t)l0);
        TEST_ASSERT_EQUAL_INT32(0, mmapq_mutable_batch_append(b, buf, l0));
        cc_b_close(q, a);       /* open batch abandoned (the crash shape) */

        int64_t m1 = cc_after(m0, l0);
        cc_pwrite32(m1, at_boundary ? 1000 : (1 << 22));

        MmapqIndexedBatchAppender *a2 = NULL;
        MmapqIndexedBatchQueue *q2 = NULL;
        TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,
            cc_b_open(CUR.dir, CC_FILE_G1, &q2, &a2));
    } else {
        MmapqIndexedQueue *q = NULL;
        MmapqIndexedAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, CC_FILE_G1, &q, &a));
        int32_t l0 = 16;
        if (at_boundary) {
            int32_t st[2];
            int ns = cc_steer_msgs(INITIAL_PAYLOAD_POSITION, CC_REG - 16, st);
            TEST_ASSERT_EQUAL_INT(1, ns);
            l0 = st[0];
        }
        char buf[CC_REG];
        memset(buf, 0x55, (size_t)l0);
        int64_t o = mmapq_indexed_appender_append_uncommitted(a, buf, l0);
        TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, o);
        TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, o));
        cc_s_close(q, a);

        int64_t m1 = cc_after(INITIAL_PAYLOAD_POSITION, l0);
        cc_pwrite32(m1, at_boundary ? 1000 : (1 << 22));

        MmapqIndexedQueue *q2 = NULL;
        MmapqIndexedAppender *a2 = NULL;
        TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,
            cc_s_open(CUR.dir, CC_FILE_G1, &q2, &a2));
    }
}

/* ── Dedicated pins (the owner-listed CAND-51e pins) ───────────────────── */

#if !defined(__SANITIZE_THREAD__)
/* The fork/kill child for the R4 pin: lane position at REG-4, then
 * start_batch — post-fix the header goes to the next region. */
static void cc_r4_child(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    /* steering committed batch whose message ends at REG-4 */
    int64_t m0 = INITIAL_PAYLOAD_POSITION + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int32_t L = (int32_t)(CC_REG - 4 - (m0 % CC_REG) - PAYLOAD_LENGTH_SIZE);
    int32_t l[1] = { L };
    cc_b_child_batch(a, l, 1, 800, 3, INITIAL_PAYLOAD_POSITION);
    /* the next batch: the unified rule skips the 4-byte tail */
    MmapqMutableBatch *b = NULL;
    if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(3);
    if (b->batch_offset != CC_REG) _exit(41);   /* pre-fix: REG-4 */
    static char buf[CC_REG];
    memset(buf, cc_fill(801), 8);
    if (mmapq_mutable_batch_append(b, buf, 8) != 0) _exit(5);
    if (mmapq_mutable_batch_complete(b) < 0) _exit(8);
    cc_die();
}

/* The R_END4 canonical wedge pin (fail-first: pre-fix the final rejoin is
 * ECORRUPT — the CAND-51e wedge; probe-landmine.log is the discovery
 * transcript). */
static void cc_end4_child1(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    /* open batch whose only message ends exactly at REG-4 */
    int64_t m0 = INITIAL_PAYLOAD_POSITION + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int32_t L = (int32_t)(CC_REG - 4 - (m0 % CC_REG) - PAYLOAD_LENGTH_SIZE);
    int32_t l[1] = { L };
    cc_b_child_batch(a, l, 1, 810, 0, INITIAL_PAYLOAD_POSITION);
    cc_die();
}
static void cc_end4_child2(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    /* rejoin completes the open batch; position = next region (the walk's
     * <= 4 skip, both trees). A staged batch follows above the 4 zero
     * bytes stranded at [REG-4, REG). */
    int64_t off = -1;
    if (mmapq_indexed_batch_appender_join_completed_open_batch(a, &off) != 1)
        _exit(42);
    if (off != INITIAL_PAYLOAD_POSITION) _exit(43);
    int32_t l[1] = { 16 };
    cc_b_child_batch(a, l, 1, 820, 1, CC_REG);
    cc_die();
}
#endif

#endif /* CC_DRIVERS */

/* ── Unity entry points (table-driven loops; exact case counts) ────────── */

void setUp(void)
{
    cc_rm_rf(CC_ROOT);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "mkdir -p %s", CC_ROOT);
    int rc = system(cmd); (void)rc;
}

void tearDown(void)
{
    cc_rm_rf(CC_ROOT);
}

#if !defined(__SANITIZE_THREAD__)
void test_cc_batch_compose_g1(void)
{
    int n = 0;
    for (int pre = 0; pre < 3; pre++)
        for (int k = 0; k < 7; k++)
            for (int j = 0; j < 7; j++) {
                /* P1 (empty open batch) has no message sequence: R_MARKER/
                 * R_MSG4/R_END4 are message-level (impossible); R_EXACT is
                 * its interior control — pruned, stated in the spec. */
                static const int r_p1[]   = { R_EXACT, R_TAIL1, R_TAIL2,
                                              R_TAIL3, R_CNT4 };
                static const int r_open[] = { R_EXACT, R_TAIL1, R_TAIL2,
                                              R_TAIL3, R_CNT4, R_MARKER,
                                              R_MSG4, R_END4 };
                static const int r_done[] = { R_EXACT, R_TAIL1, R_TAIL2,
                                              R_TAIL3, R_CNT4, R_MARKER,
                                              R_MSG4 };
                const int *rs;
                int nr;
                if (k == BP_OPEN_EMPTY)              { rs = r_p1;   nr = 5; }
                else if (k <= BP_MARKER_FAULT)       { rs = r_open; nr = 8; }
                else                                 { rs = r_done; nr = 7; }
                for (int ri = 0; ri < nr; ri++) {
                    run_batch_case(pre, rs[ri], k, j, CC_FILE_G1);
                    n++;
                }
            }
    TEST_ASSERT_EQUAL_INT(1050, n);
}

void test_cc_batch_compose_g2(void)
{
    int n = 0;
    static const int rs[] = { R_EXACT, R_TAIL1, R_TAIL2, R_TAIL3, R_CNT4,
                              R_MARKER, R_MSG4, R_END4 };
    for (int ri = 0; ri < 8; ri++) {
        run_batch_case(PRE_E, rs[ri], BP_OPEN_MSGS, BP_OPEN_MSGS, CC_FILE_G2);
        n++;
    }
    TEST_ASSERT_EQUAL_INT(8, n);
}

void test_cc_sm_compose_g1(void)
{
    int n = 0;
    for (int pre = 0; pre < 3; pre++)
        for (int k = 0; k < 4; k++)
            for (int j = 0; j < 4; j++) {
                /* Q4 (a marker) needs cap > 4 to exist: the cap <= 4 tail
                 * cells are impossible by construction (the writer
                 * short-skips; that shape is R_TAIL* for the next record,
                 * covered by Q1/Q2) — pruned, stated in the spec. */
                static const int r_q4[]   = { R_EXACT, R_MARKER };
                static const int r_rest[] = { R_EXACT, R_TAIL1, R_TAIL2,
                                              R_TAIL3, R_CNT4, R_MARKER };
                const int *rs = (k == SQ_SYNTH_MARKER) ? r_q4 : r_rest;
                int nr = (k == SQ_SYNTH_MARKER) ? 2 : 6;
                for (int ri = 0; ri < nr; ri++) {
                    run_sm_case(pre, rs[ri], k, j, CC_FILE_G1);
                    n++;
                }
            }
    TEST_ASSERT_EQUAL_INT(240, n);
}

void test_cc_sm_compose_g2(void)
{
    int n = 0;
    static const int rs[] = { R_EXACT, R_TAIL1, R_TAIL2, R_TAIL3, R_CNT4,
                              R_MARKER };
    for (int ri = 0; ri < 6; ri++) {
        run_sm_case(PRE_E, rs[ri], SQ_UNCOMMITTED, SQ_UNCOMMITTED, CC_FILE_G2);
        n++;
    }
    TEST_ASSERT_EQUAL_INT(6, n);
}

/* R4 pin: a batch header with exactly 4 bytes left goes to the next
 * region (the unified tail rule; exact positions). */
void test_cc_batch_header_cap4_next_region(void)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();

    cc_fork_kill(cc_r4_child);

    /* post-fix lane: batch0 @64 (count 1, msg [68, REG-4)); 4 zero bytes
     * at [REG-4, REG); batch1 @REG (count 1, msg 8). Write position
     * REG+4+8. */
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));
    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT64(CC_REG + 2 * MMAPQ_BATCH_MSG_COUNT_SIZE + 8,
                            b->batch_offset);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_discard(b));
    cc_b_close(q, a);
    assert_batch_lane(CUR.dir, CC_QUEUE, 0,
                      CC_REG + 2 * MMAPQ_BATCH_MSG_COUNT_SIZE + 8);
}

/* R_END4 canonical pin: a recovered batch whose last message ends with
 * exactly 4 bytes left, staged content above the tail, then rejoin.
 * Pre-fix: the final rejoin is ECORRUPT (the wedge). Post-fix: clean. */
void test_cc_recovered_end4_rejoin(void)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();

    cc_fork_kill(cc_end4_child1);
    cc_fork_kill(cc_end4_child2);

    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));
    int64_t off = -1;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));
    /* the walker yields both staged batches, in position order */
    MmapqUncommittedBatchIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
    int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, bo);
    TEST_ASSERT_EQUAL_INT32(1, mc);
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(CC_REG, bo);
    TEST_ASSERT_EQUAL_INT32(1, mc);
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    cc_b_close(q, a);
    /* lane: batch0 @64 count 1 (msg ends REG-4); 4 zero bytes; batch1
     * @REG count 1, msg [REG+4, REG+24); EOS at REG+24 */
    assert_batch_lane(CUR.dir, CC_QUEUE, 0,
                      CC_REG + 2 * MMAPQ_BATCH_MSG_COUNT_SIZE + 16);
}

/* R_END4 via the start_batch orphan-recovery caller (in-session): the
 * discard pre-store failure leaves ORPHANED; the next start_batch runs
 * recover_open_batch on a batch whose message ends at REG-4. */
void test_cc_orphan_recovery_end4(void)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();

    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));

    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    TEST_ASSERT_NOT_NULL(b);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, b->batch_offset);
    int64_t m0 = INITIAL_PAYLOAD_POSITION + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int32_t L = (int32_t)(CC_REG - 4 - (m0 % CC_REG) - PAYLOAD_LENGTH_SIZE);
    static char buf[CC_REG];
    memset(buf, 0x51, (size_t)L);
    TEST_ASSERT_EQUAL_INT32(0, mmapq_mutable_batch_append(b, buf, L));
    /* the discard pre-store failure -> ORPHANED */
    bq_arm_oneshot(b, INITIAL_PAYLOAD_POSITION);
    TEST_ASSERT_EQUAL_INT(-1, mmapq_mutable_batch_discard(b));
    TEST_ASSERT_EQUAL_INT(1, bq_oneshot_hits);
    /* the next start_batch recovers the orphan to staged (count 1) and
     * starts the new batch past the recovered batch — post-fix past the
     * 4-byte tail as well */
    MmapqMutableBatch *b2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b2));
    TEST_ASSERT_NOT_NULL(b2);
    TEST_ASSERT_EQUAL_INT64(CC_REG, b2->batch_offset);
    int64_t off = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off);
    memset(buf, 0x52, 16);
    TEST_ASSERT_EQUAL_INT32(0, mmapq_mutable_batch_append(b2, buf, 16));
    TEST_ASSERT_EQUAL_INT64(CC_REG,
                            mmapq_mutable_batch_complete_uncommitted(b2));
    cc_b_close(q, a);

    /* rejoin: the batch-level walk crosses the 4 zero bytes */
    q = NULL; a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));
    MmapqUncommittedBatchIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
    int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, bo);
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(CC_REG, bo);
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    cc_b_close(q, a);
    assert_batch_lane(CUR.dir, CC_QUEUE, 0,
                      CC_REG + 2 * MMAPQ_BATCH_MSG_COUNT_SIZE + 16);
}

/* Framing pin (added as the D2.3 torn-tail pin, CAND-50 item 2; recast
 * 2026-11-13 when the crash contract became framing only — the queue
 * makes no promise about uncommitted bodies). State: a committed record,
 * an earlier uncommitted record, and a last record whose length is
 * complete and whose data is half written. The pin: the rejoin walk
 * frames every record by its declared length — it yields the earlier
 * record and the partly written one at their offsets with their declared
 * lengths, returns the bytes on disk unchanged, and ends exactly after the
 * partly written record's declared extent. The kill is real (SIGKILL at
 * the API boundary after the earlier record's append); the partly written
 * record is pwrite synthesis — no intra-function seam exists in
 * write_payload (the G-1(a) state-equivalence discipline, matrix
 * SQ_SYNTH_PARTIAL). The write side of framing (each record's length
 * stored before its data) is the trailing atomic_signal_fence in
 * write_payload (indexed_appender.c) and its batch twins — a
 * compiler-order property no runtime pin can fail-first. */
static void cc_d2_child(void)
{
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    if (cc_s_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    static char buf[64];
    /* A: committed, 24 bytes, fill 0x71 */
    memset(buf, 0x71, 24);
    int64_t ap = cc_place(INITIAL_PAYLOAD_POSITION, 24);
    if (mmapq_indexed_appender_append_uncommitted(a, buf, 24) != ap) _exit(63);
    if (mmapq_indexed_appender_commit(a, ap) < 0) _exit(64);
    /* B: uncommitted, 24 bytes, fill 0x72 — the earlier uncommitted record */
    memset(buf, 0x72, 24);
    int64_t bp = cc_place(cc_after(INITIAL_PAYLOAD_POSITION, 24), 24);
    if (mmapq_indexed_appender_append_uncommitted(a, buf, 24) != bp) _exit(65);
    cc_die();
}

void test_cc_sm_torn_tail_earlier_intact(void)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();
    cc_trace("sm-torn-tail");

    cc_fork_kill(cc_d2_child);

    /* C: the record being written at the kill — declared length 100,
     * half the payload written, the rest never touched */
    int64_t after_a = cc_after(INITIAL_PAYLOAD_POSITION, 24);
    int64_t bp = cc_place(after_a, 24);
    int64_t cp = cc_place(cc_after(after_a, 24), 100);
    cc_pwrite32(cp, 100);
    char pay[50];
    memset(pay, 0x73, sizeof pay);
    cc_pwrite_bytes(cp + PAYLOAD_DATA_OFFSET, pay, sizeof pay);

    /* rejoin: the walk yields B, then C framed by its declared length */
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, CUR.file_size, &q, &a));
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_appender_uncommitted_iterator_checked(a,
            MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
            &it));
    const void *data = NULL; int32_t len = 0; int64_t off = -2;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(bp, off);
    TEST_ASSERT_EQUAL_INT32(24, len);
    const uint8_t *d = data;
    for (int32_t x = 0; x < 24; x++)
        TEST_ASSERT_EQUAL_UINT8(0x72, d[x]);     /* B: bytes as on disk */
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(cp, off);
    TEST_ASSERT_EQUAL_INT32(100, len);
    d = data;
    for (int32_t x = 0; x < 50; x++)
        TEST_ASSERT_EQUAL_UINT8(0x73, d[x]);     /* C: written prefix intact */
    for (int32_t x = 50; x < 100; x++)
        TEST_ASSERT_EQUAL_UINT8(0, d[x]);        /* C: unwritten rest zero */
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    cc_s_close(q, a);
    assert_payload_lane(CUR.dir, CC_QUEUE, 0,
                        cp + PAYLOAD_LENGTH_SIZE + 100);
}

/* Repeated-crash framing pin: a record partly written at crash 1 is never
 * removed. The rejoin steps over it by its declared length and appends
 * after it, so after crash 2 the walk yields it again — framed by its
 * declared length, no longer the lane's last record, its bytes unchanged —
 * and then the newer record. Guards existing behaviour (the
 * walk-past-the-tail rejoin) — fail-first not applicable; the mutant check
 * is in the card report. */
static void cc_rep_child2(void)
{
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    if (cc_s_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    static char buf[64];
    /* U: appended after the rejoin, 24 bytes, fill 0x74 */
    memset(buf, 0x74, 24);
    int64_t after_a = cc_after(INITIAL_PAYLOAD_POSITION, 24);
    int64_t cp = cc_place(cc_after(after_a, 24), 100);
    int64_t up = cc_place(cc_after(cp, 100), 24);
    if (mmapq_indexed_appender_append_uncommitted(a, buf, 24) != up) _exit(66);
    cc_die();
}

void test_cc_sm_torn_tail_repeated_crash(void)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();
    cc_trace("sm-torn-tail-repeated");

    cc_fork_kill(cc_d2_child);                    /* crash 1 */

    /* T1: partly written at crash 1 — declared length 100, half written */
    int64_t after_a = cc_after(INITIAL_PAYLOAD_POSITION, 24);
    int64_t bp = cc_place(after_a, 24);
    int64_t cp = cc_place(cc_after(after_a, 24), 100);
    cc_pwrite32(cp, 100);
    char pay[50];
    memset(pay, 0x73, sizeof pay);
    cc_pwrite_bytes(cp + PAYLOAD_DATA_OFFSET, pay, sizeof pay);

    cc_fork_kill(cc_rep_child2);                  /* rejoin, append U, crash 2 */

    /* rejoin 2: B, then T1 framed by its declared length and no longer
     * the last record, then U */
    int64_t up = cc_place(cc_after(cp, 100), 24);
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_s_open(CUR.dir, CUR.file_size, &q, &a));
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_appender_uncommitted_iterator_checked(a,
            MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
            &it));
    const void *data = NULL; int32_t len = 0; int64_t off = -2;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(bp, off);
    TEST_ASSERT_EQUAL_INT32(24, len);
    const uint8_t *d = data;
    for (int32_t x = 0; x < 24; x++)
        TEST_ASSERT_EQUAL_UINT8(0x72, d[x]);      /* B: bytes as on disk */
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(cp, off);
    TEST_ASSERT_EQUAL_INT32(100, len);
    d = data;
    for (int32_t x = 0; x < 50; x++)
        TEST_ASSERT_EQUAL_UINT8(0x73, d[x]);      /* T1: written prefix */
    for (int32_t x = 50; x < 100; x++)
        TEST_ASSERT_EQUAL_UINT8(0, d[x]);         /* T1: unwritten rest zero */
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_EQUAL_INT64(up, off);             /* T1 is not the last */
    TEST_ASSERT_EQUAL_INT32(24, len);
    d = data;
    for (int32_t x = 0; x < 24; x++)
        TEST_ASSERT_EQUAL_UINT8(0x74, d[x]);      /* U: bytes as on disk */
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    cc_s_close(q, a);
    assert_payload_lane(CUR.dir, CC_QUEUE, 0, up + PAYLOAD_LENGTH_SIZE + 24);
}

/* Batch repeated-crash pin: crash 1 with a batch open; the rejoin completes
 * it to staged and join_completed_open_batch reports it once (read-and-clear).
 * The batch is skipped, the writer stages another and crashes again; the
 * walk after crash 2 yields the queue-completed batch again, unreported,
 * then the newer one. Guards existing behaviour — fail-first not applicable;
 * the mutant check is in the card report. */
static void cc_bo_child1(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    static char buf[64];
    memset(buf, 0x51, 40);
    MmapqMutableBatch *b = NULL;
    if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(3);
    if (mmapq_mutable_batch_append(b, buf, 40) != 0) _exit(4);
    if (mmapq_mutable_batch_append(b, buf, 40) != 1) _exit(5);
    cc_die();                                     /* crash 1: batch open */
}

static void cc_bo_child2(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (cc_b_open(CUR.dir, CUR.file_size, &q, &a) != 0) _exit(2);
    static char buf[64];
    memset(buf, 0x52, 24);
    MmapqMutableBatch *b = NULL;
    if (mmapq_indexed_batch_appender_start_batch_checked(a, &b) != 0) _exit(6);
    if (mmapq_mutable_batch_append(b, buf, 24) != 0) _exit(7);
    if (mmapq_mutable_batch_complete_uncommitted(b) < 0) _exit(8);
    cc_die();                                     /* crash 2: Y staged */
}

void test_cc_batch_open_repeated_crash(void)
{
    cc_seq++;
    memset(&CUR, 0, sizeof CUR);
    CUR.file_size = CC_FILE_G1;
    cc_case_dir();
    cc_trace("batch-open-repeated");

    cc_fork_kill(cc_bo_child1);

    /* rejoin 1: the open batch X is completed to staged and reported once */
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CUR.file_size, &q, &a));
    int64_t x = -1, off = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_batch_appender_join_completed_open_batch(a, &x));
    TEST_ASSERT_EQUAL_INT64(cc_batch_start(INITIAL_PAYLOAD_POSITION), x);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));
    MmapqUncommittedBatchIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
    int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(x, bo);
    TEST_ASSERT_EQUAL_INT32(2, mc);
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    cc_b_close(q, a);                             /* X skipped */

    cc_fork_kill(cc_bo_child2);

    /* rejoin 2: nothing reported; the walk still yields X, then Y */
    int64_t y = cc_batch_start(cc_after(cc_after(x + MMAPQ_BATCH_MSG_COUNT_SIZE, 40), 40));
    TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CUR.file_size, &q, &a));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_join_completed_open_batch(a, &off));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(x, bo);               /* X again, unreported */
    TEST_ASSERT_EQUAL_INT32(2, mc);
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    TEST_ASSERT_EQUAL_INT64(y, bo);
    TEST_ASSERT_EQUAL_INT32(1, mc);
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
    cc_b_close(q, a);
}
#else
void test_cc_batch_compose_g1(void) { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan (test_runtime.c precedent)"); }
void test_cc_batch_compose_g2(void) { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan"); }
void test_cc_sm_compose_g1(void)    { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan"); }
void test_cc_sm_compose_g2(void)    { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan"); }
void test_cc_batch_header_cap4_next_region(void) { TEST_IGNORE_MESSAGE("excluded under TSan"); }
void test_cc_recovered_end4_rejoin(void)         { TEST_IGNORE_MESSAGE("excluded under TSan"); }
void test_cc_orphan_recovery_end4(void)          { TEST_IGNORE_MESSAGE("excluded under TSan"); }
void test_cc_sm_torn_tail_earlier_intact(void)   { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan"); }
void test_cc_sm_torn_tail_repeated_crash(void)   { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan"); }
void test_cc_batch_open_repeated_crash(void)     { TEST_IGNORE_MESSAGE("fork suite: excluded under TSan"); }
#endif

/* Guard pin (owner, finding #4 ruling): a staged batch followed by a
 * committed batch in the next region, with padding between them — short
 * (<= 4) in arm 1, a long-padding marker in arm 2. The batch twin's gap
 * walker skips both kinds inline and re-tests the gap end at its loop top
 * (uncommitted_batch_iterator.c), so the WALK is correct on every tree —
 * this pin guards that while the single-message side gets the fix. (On the
 * pre-unified-tail-rule tree the pin still fails, on the committed batch's
 * position: the < 4 rule placed its count slot at REG-4 — the CAND-51e
 * rule change, not the gap walk.) */
void test_cc_batch_bounded_gap_end_padding(void)
{
#if !defined(__SANITIZE_THREAD__)
    for (int arm = 0; arm < 2; arm++) {
        cc_seq++;
        memset(&CUR, 0, sizeof CUR);
        CUR.file_size = CC_FILE_G1;
        cc_case_dir();
        cc_trace("batch-gap-guard");

        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));

        MmapqMutableBatch *b = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
        TEST_ASSERT_NOT_NULL(b);
        TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, b->batch_offset);
        static char buf[CC_REG];
        if (arm == 0) {
            /* msg0 ends with exactly 4 bytes left in the region */
            int64_t m0 = INITIAL_PAYLOAD_POSITION + MMAPQ_BATCH_MSG_COUNT_SIZE;
            int32_t L = (int32_t)(CC_REG - 4 - (m0 % CC_REG) - PAYLOAD_LENGTH_SIZE);
            memset(buf, 0x61, (size_t)L);
            TEST_ASSERT_EQUAL_INT32(0, mmapq_mutable_batch_append(b, buf, L));
        } else {
            /* msg0 short; msg1 forces a long-padding marker, then the
             * re-wrap is faulted — the marker stays as S's tail (the D-F
             * error path) */
            memset(buf, 0x62, 16);
            TEST_ASSERT_EQUAL_INT32(0, mmapq_mutable_batch_append(b, buf, 16));
            int64_t m1 = INITIAL_PAYLOAD_POSITION + MMAPQ_BATCH_MSG_COUNT_SIZE + 4 + 16;
            int64_t cap = CC_REG - m1 % CC_REG;
            int32_t L = (int32_t)cap;   /* 4+L > cap -> marker at m1 */
            bq_arm_oneshot(b, m1 + cap);
            memset(buf, 0x63, (size_t)L);
            TEST_ASSERT_EQUAL_INT32(-1, mmapq_mutable_batch_append(b, buf, L));
            TEST_ASSERT_EQUAL_INT(1, bq_oneshot_hits);
        }
        /* S staged (count 1) */
        TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION,
                                mmapq_mutable_batch_complete_uncommitted(b));
        /* C committed at the next region start */
        MmapqMutableBatch *c = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &c));
        TEST_ASSERT_NOT_NULL(c);
        TEST_ASSERT_EQUAL_INT64(CC_REG, c->batch_offset);
        memset(buf, 0x64, 16);
        TEST_ASSERT_EQUAL_INT32(0, mmapq_mutable_batch_append(c, buf, 16));
        TEST_ASSERT_EQUAL_INT64(0, mmapq_mutable_batch_complete(c));
        cc_b_close(q, a);

        /* rejoin: the walker yields S, then the gap ending in padding is
         * exhausted cleanly (never ECORRUPT) */
        q = NULL; a = NULL;
        TEST_ASSERT_EQUAL_INT(0, cc_b_open(CUR.dir, CC_FILE_G1, &q, &a));
        MmapqUncommittedBatchIterator *it = NULL;
        TEST_ASSERT_EQUAL_INT(0,
            mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
        int64_t bo = -2; int32_t mc = -2; MmapqBatchPayloadIterator *mi = NULL;
        TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
        TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, bo);
        TEST_ASSERT_EQUAL_INT32(1, mc);
        TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_batch_next(it, &bo, &mc, &mi));
        cc_b_close(q, a);
    }
#else
    TEST_IGNORE_MESSAGE("excluded under TSan");
#endif
}

void test_cc_batch_recovery_crash(void)
{
#if defined(MMAPQ_TEST_HOOKS) && !defined(__SANITIZE_THREAD__)
    int n = 0;
    for (int pre = 0; pre < 2; pre++)        /* E, C */
        for (int marker = 0; marker < 2; marker++)
            for (int s = 1; s <= 3; s++) {
                run_p8_case(pre == 0 ? PRE_E : PRE_C, marker, s);
                n++;
            }
    TEST_ASSERT_EQUAL_INT(12, n);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

void test_cc_misfit_synthesis(void)
{
#if !defined(__SANITIZE_THREAD__)
    int n = 0;
    run_misfit_case(1, 0); n++;
    run_misfit_case(1, 1); n++;
    run_misfit_case(0, 0); n++;
    run_misfit_case(0, 1); n++;
    TEST_ASSERT_EQUAL_INT(4, n);
#else
    TEST_IGNORE_MESSAGE("excluded under TSan");
#endif
}

int main(void)
{
    UNITY_BEGIN();
    /* the named pins first (fast feedback), then the matrices; the two
     * single-message matrices last — their Q4-marker and G2 exact-fit cells
     * pin the two open CAND-51e rejoin-walk findings (owner ruling
     * pending), so they are the ones that go red/hard-stop the binary. */
    RUN_TEST(test_cc_batch_header_cap4_next_region);
    RUN_TEST(test_cc_recovered_end4_rejoin);
    RUN_TEST(test_cc_orphan_recovery_end4);
    RUN_TEST(test_cc_sm_torn_tail_earlier_intact);
    RUN_TEST(test_cc_sm_torn_tail_repeated_crash);
    RUN_TEST(test_cc_batch_open_repeated_crash);
    RUN_TEST(test_cc_batch_compose_g1);
    RUN_TEST(test_cc_batch_compose_g2);
    RUN_TEST(test_cc_batch_recovery_crash);
    RUN_TEST(test_cc_batch_bounded_gap_end_padding);
    RUN_TEST(test_cc_misfit_synthesis);
    RUN_TEST(test_cc_sm_compose_g1);
    RUN_TEST(test_cc_sm_compose_g2);
    return UNITY_END();
}
