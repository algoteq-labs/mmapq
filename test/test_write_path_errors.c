/*
 * The write path reports a permanent file error at once, as MMAPQ_EIO
 * (CAND-41, the append-side half of PERMANENT-NOT-TRANSIENT).
 *
 * An appender creates, opens, extends and maps the next header or payload
 * file when it crosses a file boundary. Before 0.2.1 the read-write file
 * mapper turned every failure there into a bare NULL address with no
 * errno, the region retried it until its timeout, and the append returned
 * MMAPQ_ETIMEDOUT ("transient, retry"): a permission error cost one full
 * timeout per append, forever. A failed header-file create returned
 * MMAPQ_ETIMEDOUT too, at once.
 *
 *   - mmapqi_write_errno_rc: a transient errno (EINTR, EAGAIN, EMFILE,
 *     ENFILE, EBUSY, ENOLCK) stays MMAPQ_ETIMEDOUT; anything else is EIO.
 *   - mmapqi_write_wrap_rc: WRAP_IO_ERROR is EIO; a timeout stays ETIMEDOUT.
 *
 * Each end-to-end test makes the queue directory read-only under a live
 * appender (as root: in a child that drops to uid/gid 65534, the
 * test_readonly_open pattern; the child first proves the restriction
 * bites), appends until the next file is needed, and checks: the exact
 * code and index of the failing call, that it failed well inside the
 * timeout, that a retry fails the same way, that the append succeeds at
 * the same index once the directory is writable again, and that a poller
 * then reads every message in order. The two transient pins check the
 * other side of the rule at both boundaries: a descriptor limit (EMFILE)
 * is still retried until the timeout and reported as MMAPQ_ETIMEDOUT.
 *
 * A full filesystem used to kill the process: ftruncate leaves the file
 * sparse and the first write to an unallocated page raised SIGBUS. The
 * read-write mapper now reserves the blocks (fallocate) before the
 * mapping is written, so ENOSPC is MMAPQ_EIO. The full-disk pins (hook
 * build only) make the next reservation fail with ENOSPC and check EIO at
 * once, then success at the same index once the "space" is back.
 */
#include "unity.h"
#include "mmapq.h"
#include "internal/platform_internal.h"
#include "internal/region_accessor.h"
#include "internal/test_hooks.h"

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define WP_REGION    ((int32_t)32768)
#define WP_FILE      ((int64_t)32768 * 8)        /* 8 regions per file */
#define WP_TIMEOUT   ((uint64_t)2000000000)      /* 2 s */
#define WP_FAST_MS   1000                        /* "at once": well inside WP_TIMEOUT */
#define WP_UID       ((uid_t)65534)
#define WP_GID       ((gid_t)65534)
#define WP_BIG       1000                        /* payload file fills first: 256 per file */
#define WP_HDR_SI    ((int64_t)32768 - 10)       /* header file 0 has 10 free slots */
#define WP_HDR_FIRST ((int64_t)32768)            /* first index in header file 1 */
#define WPF_AT ((int64_t)32)                       /* first write needing region 1 */

static char base_dir[64];

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_wp_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
    if (geteuid() == 0) TEST_ASSERT_EQUAL_INT(0, chown(base_dir, WP_UID, WP_GID));
}

void tearDown(void)
{
    char cmd[256];
    snprintf(cmd, sizeof cmd, "chmod -R u+rwx '%s' 2>/dev/null; rm -rf '%s'", base_dir, base_dir);
    TEST_ASSERT_EQUAL_INT(0, system(cmd));
}

/* ── the two mapping helpers, exhaustively ──────────────────────────────── */

void test_write_errno_rc_table(void)
{
    const int transient[] = { EINTR, EAGAIN, EMFILE, ENFILE, EBUSY, ENOLCK };
    for (size_t i = 0; i < sizeof transient / sizeof transient[0]; i++)
        TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_write_errno_rc(transient[i]));
    const int permanent[] = { EACCES, EPERM, EROFS, ENOSPC, EDQUOT, ENODEV, EFBIG,
                              ENOTDIR, ENOENT, ENOMEM, EIO };
    for (size_t i = 0; i < sizeof permanent / sizeof permanent[0]; i++)
        TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, mmapqi_write_errno_rc(permanent[i]));
}

void test_write_wrap_rc_table(void)
{
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_write_wrap_rc(WRAP_IO_ERROR));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_write_wrap_rc(WRAP_MAP_TIMED_OUT));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_write_wrap_rc(WRAP_FILE_NOT_FOUND));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_write_wrap_rc(WRAP_BEYOND_FILE_SIZE));
}

/* ── end to end: a live appender crossing a file boundary ───────────────── */

typedef struct {
    int     setup;       /* 0 = ok; else the failing setup step (101 = no restriction) */
    int64_t first_rc;    /* the first failing call's code */
    int64_t first_at;    /* the message/batch index it was writing */
    int64_t first_ms;    /* how long that call took */
    int64_t retry_rc;    /* the same call again, still restricted */
    int64_t retry_ms;
    int64_t after_rc;    /* the same call once the restriction is lifted */
    int64_t delivered;   /* messages then read back in order by a poller */
} WpResult;

typedef enum { WP_SINGLE, WP_BATCH } WpLane;
typedef enum { WP_READONLY_DIR, WP_FD_LIMIT } WpCause;

static int64_t wp_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static void wp_fill(unsigned char *buf, int64_t index, int32_t len)
{
    memset(buf, (int)(index & 0xff), (size_t)len);
    memcpy(buf, &index, len < 8 ? (size_t)len : sizeof index);
}

/* One message (single lane) or one one-message batch (batch lane). A
 * failed batch is discarded (it holds no message, so discard resets it). */
static int64_t wp_write(WpLane lane, void *app, int64_t index, int32_t len)
{
    unsigned char buf[WP_BIG];
    wp_fill(buf, index, len);
    if (lane == WP_SINGLE)
        return mmapq_indexed_appender_append((MmapqIndexedAppender *)app, buf, len);
    MmapqMutableBatch *b = NULL;
    int rc = mmapq_indexed_batch_appender_start_batch_checked(
        (MmapqIndexedBatchAppender *)app, &b);
    if (rc != 0) return rc;
    int32_t arc = mmapq_mutable_batch_append(b, buf, len);
    if (arc < 0) { (void)mmapq_mutable_batch_discard(b); return arc; }
    return mmapq_mutable_batch_complete(b);
}

typedef struct { int64_t next; int32_t len; int bad; } WpRead;

static MmapqNextMove wp_single_handler(int64_t index, uint8_t aid, const void *data,
                                       int32_t length, void *ctx)
{
    (void)aid;
    WpRead *c = ctx;
    unsigned char want[WP_BIG];
    wp_fill(want, index, c->len);
    if (index != c->next || length != c->len || memcmp(data, want, (size_t)length) != 0) c->bad++;
    c->next++;
    return MMAPQ_NEXT_ADVANCE;
}

static MmapqNextMove wp_batch_handler(int64_t batch_index, MmapqBatchReadingContext *bctx,
                                      void *ctx)
{
    WpRead *c = ctx;
    unsigned char want[WP_BIG];
    wp_fill(want, batch_index, c->len);
    int n = 0, rc;
    while ((rc = mmapq_batch_iterator_advance(bctx->iterator)) == 1) {
        if (mmapq_batch_iterator_current_length_ffi(bctx->iterator) != c->len ||
            memcmp(mmapq_batch_iterator_current_data_ffi(bctx->iterator), want,
                   (size_t)c->len) != 0)
            c->bad++;
        n++;
    }
    if (rc != 0 || n != 1 || batch_index != c->next) c->bad++;
    c->next++;
    return MMAPQ_NEXT_ADVANCE;
}

/* The child: build the queue, restrict, write until the next file is
 * needed, retry, lift the restriction, write again, read back. */
static void wp_child(WpLane lane, WpCause cause, int async, int32_t len,
                     int64_t start_index, uint64_t timeout_ns, WpResult *res)
{
    if (geteuid() == 0 &&
        (setgroups(0, NULL) != 0 || setgid(WP_GID) != 0 || setuid(WP_UID) != 0)) {
        res->setup = 100;
        return;
    }
    MmapqRuntime *rt = NULL;
    if (async) {
        MmapqIdleStrategy *idle = NULL;
        if (mmapq_idle_strategy_yield_create(&idle) != 0 ||
            mmapq_runtime_create("wp-async", idle, MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS,
                                 NULL, NULL, &rt) != 0) { res->setup = 1; return; }
    }
    int ahead = async ? 1 : 0;
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedBatchQueue *bq = NULL;
    void *app = NULL;
    if (lane == WP_SINGLE) {
        if (mmapq_indexed_queue_create(base_dir, "q", WP_REGION, mmapq_min_ring_size(WP_REGION),
                                       ahead, WP_FILE, start_index, timeout_ns, 0, rt, rt,
                                       &q) != 0 ||
            mmapq_indexed_queue_create_appender(q, (MmapqIndexedAppender **)&app) != 0) {
            res->setup = 2; return;
        }
    } else {
        if (mmapq_indexed_batch_queue_create(base_dir, "q", WP_REGION,
                                             mmapq_min_ring_size(WP_REGION), ahead, WP_FILE,
                                             start_index, timeout_ns, 0, rt, rt, &bq) != 0 ||
            mmapq_indexed_batch_queue_create_appender(bq, (MmapqIndexedBatchAppender **)&app) != 0) {
            res->setup = 2; return;
        }
    }

    char qdir[128], probe[160];
    snprintf(qdir, sizeof qdir, "%s/q", base_dir);
    snprintf(probe, sizeof probe, "%s/probe", qdir);
    struct rlimit saved;
    if (cause == WP_READONLY_DIR) {
        if (chmod(qdir, 0555) != 0) { res->setup = 3; return; }
        int fd = open(probe, O_RDWR | O_CREAT, 0644);
        if (fd >= 0 || errno != EACCES) { res->setup = 101; return; }  /* does not bite */
    } else {
        /* the next descriptor this process opens fails with EMFILE */
        int next = dup(0);
        if (next < 0) { res->setup = 4; return; }
        close(next);
        if (getrlimit(RLIMIT_NOFILE, &saved) != 0) { res->setup = 4; return; }
        struct rlimit lim = saved;
        lim.rlim_cur = (rlim_t)next;
        if (setrlimit(RLIMIT_NOFILE, &lim) != 0) { res->setup = 4; return; }
        int fd = open(probe, O_RDWR | O_CREAT, 0644);
        if (fd >= 0 || errno != EMFILE) { res->setup = 102; return; }   /* does not bite */
    }

    int64_t i = start_index, rc = 0, t0 = 0;
    for (; i < start_index + 1000000; i++) {
        t0 = wp_ms();
        rc = wp_write(lane, app, i, len);
        if (rc != i) break;
    }
    res->first_rc = rc;
    res->first_at = i;
    res->first_ms = wp_ms() - t0;
    t0 = wp_ms();
    res->retry_rc = wp_write(lane, app, i, len);
    res->retry_ms = wp_ms() - t0;

    if (cause == WP_READONLY_DIR) {
        if (chmod(qdir, 0755) != 0) { res->setup = 5; return; }
    } else if (setrlimit(RLIMIT_NOFILE, &saved) != 0) {
        res->setup = 5; return;
    }
    res->after_rc = wp_write(lane, app, i, len);

    WpRead c = { start_index, len, 0 };
    if (lane == WP_SINGLE) {
        MmapqIndexedPoller *p = NULL;
        if (mmapq_indexed_queue_create_poller(q, &p) != 0) { res->setup = 6; return; }
        while (mmapq_indexed_poller_poll(p, wp_single_handler, &c) == MMAPQ_POLL_ADVANCED) { }
        mmapq_indexed_poller_close(p);
        mmapq_indexed_appender_close((MmapqIndexedAppender *)app);
        mmapq_indexed_queue_close(q);
    } else {
        MmapqIndexedBatchPoller *p = NULL;
        if (mmapq_indexed_batch_queue_create_poller(bq, &p) != 0) { res->setup = 6; return; }
        while (mmapq_indexed_batch_poller_poll(p, wp_batch_handler, &c) == MMAPQ_POLL_ADVANCED) { }
        mmapq_indexed_batch_poller_close(p);
        mmapq_indexed_batch_appender_close((MmapqIndexedBatchAppender *)app);
        mmapq_indexed_batch_queue_close(bq);
    }
    res->delivered = c.bad ? -c.bad : c.next - start_index;
    if (rt) { (void)mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
}

static WpResult wp_run(WpLane lane, WpCause cause, int async, int32_t len,
                       int64_t start_index, uint64_t timeout_ns)
{
    int fds[2];
    TEST_ASSERT_EQUAL_INT(0, pipe(fds));
    fflush(stdout);
    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        close(fds[0]);
        WpResult res;
        memset(&res, 0, sizeof res);
        wp_child(lane, cause, async, len, start_index, timeout_ns, &res);
        ssize_t w = write(fds[1], &res, sizeof res);
        _exit(w == (ssize_t)sizeof res ? 0 : 1);
    }
    close(fds[1]);
    WpResult res;
    memset(&res, 0, sizeof res);
    ssize_t n = read(fds[0], &res, sizeof res);
    close(fds[0]);
    int st = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &st, 0));
    TEST_ASSERT_TRUE(WIFEXITED(st));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(st));
    TEST_ASSERT_EQUAL_INT((int)sizeof res, (int)n);
    printf("write path %s %s %s: setup=%d first rc=%lld at=%lld %lldms; retry rc=%lld %lldms; "
           "after=%lld delivered=%lld\n",
           lane == WP_SINGLE ? "single" : "batch", async ? "async" : "sync",
           cause == WP_READONLY_DIR ? "read-only dir" : "fd limit", res.setup,
           (long long)res.first_rc, (long long)res.first_at, (long long)res.first_ms,
           (long long)res.retry_rc, (long long)res.retry_ms, (long long)res.after_rc,
           (long long)res.delivered);
    TEST_ASSERT_EQUAL_INT(0, res.setup);
    return res;
}

/* A permanent failure: EIO at once, again on retry; recoverable. */
static void wp_expect_eio(WpLane lane, int async, int32_t len, int64_t start_index,
                          int64_t expect_at)
{
    WpResult r = wp_run(lane, WP_READONLY_DIR, async, len, start_index, WP_TIMEOUT);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EIO, r.first_rc);
    TEST_ASSERT_EQUAL_INT64(expect_at, r.first_at);
    TEST_ASSERT_TRUE(r.first_ms < WP_FAST_MS);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EIO, r.retry_rc);
    TEST_ASSERT_TRUE(r.retry_ms < WP_FAST_MS);
    TEST_ASSERT_EQUAL_INT64(expect_at, r.after_rc);
    TEST_ASSERT_EQUAL_INT64(expect_at - start_index + 1, r.delivered);
}

/* Payload file boundary: 1000-byte messages fill payload file 0 (256 per
 * file) long before header file 0 (32,768 slots). */
void test_payload_file_boundary_readonly_dir_is_eio_single_sync(void)
{ wp_expect_eio(WP_SINGLE, 0, WP_BIG, 0, 256); }
void test_payload_file_boundary_readonly_dir_is_eio_single_async(void)
{ wp_expect_eio(WP_SINGLE, 1, WP_BIG, 0, 256); }
void test_payload_file_boundary_readonly_dir_is_eio_batch_sync(void)
{ wp_expect_eio(WP_BATCH, 0, WP_BIG, 0, 256); }
void test_payload_file_boundary_readonly_dir_is_eio_batch_async(void)
{ wp_expect_eio(WP_BATCH, 1, WP_BIG, 0, 256); }

/* Header file boundary: a queue that starts 10 slots before the end of
 * header file 0, with small messages, needs header file 1 first. */
void test_header_file_boundary_readonly_dir_is_eio_single_sync(void)
{ wp_expect_eio(WP_SINGLE, 0, 8, WP_HDR_SI, WP_HDR_FIRST); }
void test_header_file_boundary_readonly_dir_is_eio_single_async(void)
{ wp_expect_eio(WP_SINGLE, 1, 8, WP_HDR_SI, WP_HDR_FIRST); }
void test_header_file_boundary_readonly_dir_is_eio_batch_sync(void)
{ wp_expect_eio(WP_BATCH, 0, 8, WP_HDR_SI, WP_HDR_FIRST); }
void test_header_file_boundary_readonly_dir_is_eio_batch_async(void)
{ wp_expect_eio(WP_BATCH, 1, 8, WP_HDR_SI, WP_HDR_FIRST); }

/* The other side of the rule: a descriptor limit is transient — still
 * retried until the timeout and reported as MMAPQ_ETIMEDOUT. A guard: it
 * also passes before the fix. Short timeout to keep the suite fast. */
void test_payload_file_boundary_fd_limit_stays_etimedout(void)
{
    const uint64_t timeout_ns = 300000000;   /* 300 ms */
    WpResult r = wp_run(WP_SINGLE, WP_FD_LIMIT, 0, WP_BIG, 0, timeout_ns);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT, r.first_rc);
    TEST_ASSERT_EQUAL_INT64(256, r.first_at);
    TEST_ASSERT_TRUE(r.first_ms >= 250);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT, r.retry_rc);
    TEST_ASSERT_EQUAL_INT64(256, r.after_rc);
    TEST_ASSERT_EQUAL_INT64(257, r.delivered);
}

/* The same at a header-file boundary. The first call to fail there is the
 * header mapper's open-existing of the next file, and the kernel takes the
 * descriptor before it looks up the path, so it is EMFILE (not ENOENT):
 * retried by the region until the timeout, as on the payload side — the
 * explicit create is never reached. A guard: it also passes before the
 * fix. */
void test_header_file_boundary_fd_limit_stays_etimedout(void)
{
    const uint64_t timeout_ns = 300000000;   /* 300 ms */
    WpResult r = wp_run(WP_SINGLE, WP_FD_LIMIT, 0, 8, WP_HDR_SI, timeout_ns);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT, r.first_rc);
    TEST_ASSERT_EQUAL_INT64(WP_HDR_FIRST, r.first_at);
    TEST_ASSERT_TRUE(r.first_ms >= 250);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT, r.retry_rc);
    TEST_ASSERT_EQUAL_INT64(WP_HDR_FIRST, r.after_rc);
    TEST_ASSERT_EQUAL_INT64(WP_HDR_FIRST - WP_HDR_SI + 1, r.delivered);
}

#ifdef MMAPQ_TEST_HOOKS
/* Full filesystem, by hook: the next block reservation fails with ENOSPC.
 * The hook is armed after the queue and appender exist and before the
 * first append, so the first reservation it meets is region 1's: in sync
 * mode the map at index 32; in async mode the premap of region 1, issued
 * by the first append's wrap of region 0 — the premap fails and its error
 * is reported when index 32 wraps region 1. Either way index 32 gets
 * MMAPQ_EIO at once, and the same call succeeds once the hook is spent
 * (the space is back). Reviewer: 40 runs, index 32 in all four modes. */
static void wp_full_disk(WpLane lane, int async, int64_t expect_at)
{
    MmapqRuntime *rt = NULL;
    if (async) {
        MmapqIdleStrategy *idle = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_yield_create(&idle));
        TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_create("wp-full", idle,
            MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, NULL, NULL, &rt));
    }
    int ahead = async ? 1 : 0;
    MmapqIndexedQueue *q = NULL; MmapqIndexedBatchQueue *bq = NULL; void *app = NULL;
    if (lane == WP_SINGLE) {
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(base_dir, "q", WP_REGION,
            mmapq_min_ring_size(WP_REGION), ahead, WP_FILE, 0, WP_TIMEOUT, 0, rt, rt, &q));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, (MmapqIndexedAppender **)&app));
    } else {
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(base_dir, "q", WP_REGION,
            mmapq_min_ring_size(WP_REGION), ahead, WP_FILE, 0, WP_TIMEOUT, 0, rt, rt, &bq));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(bq, (MmapqIndexedBatchAppender **)&app));
    }
    mmapq_test_hook_set_wpe_reserve_enospc(1);
    int64_t i = 0, rc = 0, t0 = 0;
    for (; i < 100000; i++) {
        t0 = wp_ms();
        rc = wp_write(lane, app, i, WP_BIG);
        if (rc != i) break;
    }
    int64_t ms = wp_ms() - t0;
    mmapq_test_hook_set_wpe_reserve_enospc(0);
    printf("full disk %s %s: rc=%lld at=%lld %lldms\n", lane == WP_SINGLE ? "single" : "batch",
           async ? "async" : "sync", (long long)rc, (long long)i, (long long)ms);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EIO, rc);
    TEST_ASSERT_EQUAL_INT64(expect_at, i);
    TEST_ASSERT_TRUE(ms < WP_FAST_MS);
    TEST_ASSERT_EQUAL_INT64(i, wp_write(lane, app, i, WP_BIG));   /* space is back */
    WpRead c = { 0, WP_BIG, 0 };
    if (lane == WP_SINGLE) {
        MmapqIndexedPoller *p = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
        while (mmapq_indexed_poller_poll(p, wp_single_handler, &c) == MMAPQ_POLL_ADVANCED) { }
        mmapq_indexed_poller_close(p);
        mmapq_indexed_appender_close((MmapqIndexedAppender *)app);
        mmapq_indexed_queue_close(q);
    } else {
        MmapqIndexedBatchPoller *p = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(bq, &p));
        while (mmapq_indexed_batch_poller_poll(p, wp_batch_handler, &c) == MMAPQ_POLL_ADVANCED) { }
        mmapq_indexed_batch_poller_close(p);
        mmapq_indexed_batch_appender_close((MmapqIndexedBatchAppender *)app);
        mmapq_indexed_batch_queue_close(bq);
    }
    if (rt) { (void)mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
    TEST_ASSERT_EQUAL_INT(0, c.bad);
    TEST_ASSERT_EQUAL_INT64(expect_at + 1, c.next);
}

void test_full_disk_is_eio_single_sync(void)  { wp_full_disk(WP_SINGLE, 0, WPF_AT); }
void test_full_disk_is_eio_single_async(void) { wp_full_disk(WP_SINGLE, 1, WPF_AT); }
void test_full_disk_is_eio_batch_sync(void)   { wp_full_disk(WP_BATCH, 0, WPF_AT); }
void test_full_disk_is_eio_batch_async(void)  { wp_full_disk(WP_BATCH, 1, WPF_AT); }
#endif

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_write_errno_rc_table);
    RUN_TEST(test_write_wrap_rc_table);
    RUN_TEST(test_payload_file_boundary_readonly_dir_is_eio_single_sync);
    RUN_TEST(test_payload_file_boundary_readonly_dir_is_eio_single_async);
    RUN_TEST(test_payload_file_boundary_readonly_dir_is_eio_batch_sync);
    RUN_TEST(test_payload_file_boundary_readonly_dir_is_eio_batch_async);
    RUN_TEST(test_header_file_boundary_readonly_dir_is_eio_single_sync);
    RUN_TEST(test_header_file_boundary_readonly_dir_is_eio_single_async);
    RUN_TEST(test_header_file_boundary_readonly_dir_is_eio_batch_sync);
    RUN_TEST(test_header_file_boundary_readonly_dir_is_eio_batch_async);
    RUN_TEST(test_payload_file_boundary_fd_limit_stays_etimedout);
    RUN_TEST(test_header_file_boundary_fd_limit_stays_etimedout);
#ifdef MMAPQ_TEST_HOOKS
    RUN_TEST(test_full_disk_is_eio_single_sync);
    RUN_TEST(test_full_disk_is_eio_single_async);
    RUN_TEST(test_full_disk_is_eio_batch_sync);
    RUN_TEST(test_full_disk_is_eio_batch_async);
#endif
    return UNITY_END();
}
