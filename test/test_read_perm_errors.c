/*
 * The read side reports a queue file it cannot open as an error, not as a
 * missing file (READ-PERM-EOS-01).
 *
 * Before 0.2.1 the read-only file mapper reported every failure to stat,
 * open or initialise an existing file as FILE_NOT_FOUND. On the read side
 * FILE_NOT_FOUND means "not written yet" (the frontier) or "archived", so:
 *
 *   - a header file without read permission read as END_OF_STREAM with
 *     last_error 0 — a poller stopped mid-stream as if caught up, a reader
 *     saw END_OF_STREAM for committed entries, and last_index reported the
 *     last index before the unreadable file (or -1, "empty");
 *   - a payload file without read permission read as CORRUPT + ECORRUPT —
 *     "damage, recreate the queue" for a permission problem.
 *
 * The mapper now classifies the errno with the write side's rule
 * (mmapqi_write_errno_rc): ENOENT and a zero-length file stay FILE_NOT_FOUND
 * (not created yet, archived, or created but not yet extended by the
 * writer); a transient errno (EINTR, EAGAIN, EMFILE, ENFILE, EBUSY, ENOLCK)
 * is retried until the timeout (MMAPQ_ETIMEDOUT); anything else (EACCES,
 * ELOOP, ENOTDIR, ENODEV, ...) is WRAP_IO_ERROR, reported at once as
 * MMAPQ_EIO: poll ERROR, reader ENTRY_ERROR, last_index -1.
 *
 * Each end-to-end test builds a committed queue, makes one file unreadable
 * (EACCES — as root, in a child that drops to uid/gid 65534 and first
 * proves the restriction bites — or ELOOP, a symlink loop where the file
 * was), then checks: the poll outcome, code and count delivered before it,
 * that it failed well inside the timeout, that a retry fails the same way,
 * what a reader and last_index say, and that once the file is readable
 * again the same poller delivers every message in order.
 */
#include "unity.h"
#include "mmapq.h"
#include "internal/file_mapper.h"
#include "internal/region_accessor.h"

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

#define RP_REGION    ((int32_t)32768)
#define RP_FILE      ((int64_t)32768 * 8)        /* 8 regions per file */
#define RP_TIMEOUT   ((uint64_t)2000000000)      /* 2 s */
#define RP_SHORT     ((uint64_t)300000000)       /* 300 ms: the transient pin */
#define RP_FAST_MS   1000                        /* "at once": well inside RP_TIMEOUT */
#define RP_UID       ((uid_t)65534)
#define RP_GID       ((gid_t)65534)
#define RP_BIG       1000                        /* 256 per payload file */
#define RP_SMALL     8
#define RP_HDR_SI    ((int64_t)32768 - 10)       /* header file 0 has 10 slots */
#define RP_HDR_FIRST ((int64_t)32768)            /* first index in header file 1 */

static char base_dir[64];

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_rp_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
    TEST_ASSERT_EQUAL_INT(0, chmod(base_dir, 0755));
}

void tearDown(void)
{
    char cmd[256];
    snprintf(cmd, sizeof cmd, "chmod -R u+rwx '%s' 2>/dev/null; rm -rf '%s'", base_dir, base_dir);
    TEST_ASSERT_EQUAL_INT(0, system(cmd));
}

static int64_t rp_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

/* ── the shared mapping helper ──────────────────────────────────────────── */

void test_committed_wrap_rc_table(void)
{
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_committed_wrap_rc(WRAP_MAP_TIMED_OUT));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_committed_wrap_rc(WRAP_IO_ERROR));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,  mmapqi_committed_wrap_rc(WRAP_FILE_NOT_FOUND));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,  mmapqi_committed_wrap_rc(WRAP_BEYOND_FILE_SIZE));
}

/* ── the read-only mapper's classification, one cause at a time ─────────── */

static uintptr_t ro_map_once(const char *path)
{
    FileMapper *fm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_single_ro_create(
        path, mmapqi_file_init_none, NULL, 1, &fm));
    uintptr_t a = file_mapper_map(fm, 0, 4096);
    if (file_mapper_is_address(a)) file_mapper_unmap(fm, a, 0, 4096);
    file_mapper_close(fm);
    mmapqi_file_mapper_destroy(fm);
    return a;
}

static void write_file(const char *path, size_t size)
{
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT_TRUE(fd >= 0);
    TEST_ASSERT_EQUAL_INT(0, ftruncate(fd, (off_t)size));
    close(fd);
}

void test_ro_mapper_not_found_is_only_enoent_and_empty(void)
{
    char p[160];
    snprintf(p, sizeof p, "%s/missing", base_dir);
    TEST_ASSERT_EQUAL_UINT64(FILE_MAPPER_FILE_NOT_FOUND, ro_map_once(p));
    snprintf(p, sizeof p, "%s/empty", base_dir);            /* created, not yet extended */
    write_file(p, 0);
    TEST_ASSERT_EQUAL_UINT64(FILE_MAPPER_FILE_NOT_FOUND, ro_map_once(p));
    snprintf(p, sizeof p, "%s/ok", base_dir);
    write_file(p, 4096);
    TEST_ASSERT_TRUE(file_mapper_is_address(ro_map_once(p)));
}

void test_ro_mapper_permanent_is_io_error(void)
{
    char p[160], q[160];
    snprintf(p, sizeof p, "%s/loop", base_dir);              /* stat: ELOOP */
    TEST_ASSERT_EQUAL_INT(0, symlink("loop", p));
    TEST_ASSERT_EQUAL_UINT64(FILE_MAPPER_IO_ERROR, ro_map_once(p));
    snprintf(q, sizeof q, "%s/ok", base_dir);
    write_file(q, 4096);
    snprintf(p, sizeof p, "%s/ok/child", base_dir);          /* stat: ENOTDIR */
    TEST_ASSERT_EQUAL_UINT64(FILE_MAPPER_IO_ERROR, ro_map_once(p));
    snprintf(p, sizeof p, "%s/dir", base_dir);               /* mmap: ENODEV */
    TEST_ASSERT_EQUAL_INT(0, mkdir(p, 0755));
    snprintf(q, sizeof q, "%s/dir/entry", base_dir);         /* a non-zero size everywhere */
    write_file(q, 0);
    TEST_ASSERT_EQUAL_UINT64(FILE_MAPPER_IO_ERROR, ro_map_once(p));
}

/* A child as uid 65534 when root (the owner otherwise): open fails EACCES. */
void test_ro_mapper_eacces_is_io_error_and_recovers(void)
{
    char p[160];
    snprintf(p, sizeof p, "%s/locked", base_dir);
    write_file(p, 4096);
    TEST_ASSERT_EQUAL_INT(0, chmod(p, 0000));
    fflush(stdout);
    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        if (geteuid() == 0 &&
            (setgroups(0, NULL) != 0 || setgid(RP_GID) != 0 || setuid(RP_UID) != 0)) _exit(10);
        int fd = open(p, O_RDONLY);
        if (fd >= 0 || errno != EACCES) _exit(11);            /* does not bite */
        FileMapper *fm = NULL;
        if (mmapqi_file_mapper_single_ro_create(p, mmapqi_file_init_none, NULL, 1, &fm) != 0)
            _exit(12);
        if (file_mapper_map(fm, 0, 4096) != FILE_MAPPER_IO_ERROR) _exit(13);
        _exit(0);
    }
    int st = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &st, 0));
    TEST_ASSERT_TRUE(WIFEXITED(st));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(st));
    /* the same mapper object retries the open: readable again → mapped */
    FileMapper *fm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_single_ro_create(
        p, mmapqi_file_init_none, NULL, 1, &fm));
    TEST_ASSERT_EQUAL_INT(0, chmod(p, 0644));
    uintptr_t a = file_mapper_map(fm, 0, 4096);
    TEST_ASSERT_TRUE(file_mapper_is_address(a));
    file_mapper_unmap(fm, a, 0, 4096);
    file_mapper_close(fm);
    mmapqi_file_mapper_destroy(fm);
}

void test_ro_mapper_emfile_is_retried(void)
{
    char p[160];
    snprintf(p, sizeof p, "%s/ok", base_dir);
    write_file(p, 4096);
    FileMapper *fm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_single_ro_create(
        p, mmapqi_file_init_none, NULL, 1, &fm));
    int next = dup(0);
    TEST_ASSERT_TRUE(next >= 0);
    close(next);
    struct rlimit saved, lim;
    TEST_ASSERT_EQUAL_INT(0, getrlimit(RLIMIT_NOFILE, &saved));
    lim = saved;
    lim.rlim_cur = (rlim_t)next;
    TEST_ASSERT_EQUAL_INT(0, setrlimit(RLIMIT_NOFILE, &lim));
    uintptr_t a = file_mapper_map(fm, 0, 4096);
    TEST_ASSERT_EQUAL_INT(0, setrlimit(RLIMIT_NOFILE, &saved));
    TEST_ASSERT_EQUAL_UINT64(FILE_MAPPER_NULL_ADDRESS, a);   /* transient: the region retries */
    a = file_mapper_map(fm, 0, 4096);
    TEST_ASSERT_TRUE(file_mapper_is_address(a));
    file_mapper_unmap(fm, a, 0, 4096);
    file_mapper_close(fm);
    mmapqi_file_mapper_destroy(fm);
}

/* ── end to end: a consumer of a committed queue ───────────────────────── */

typedef enum { RP_SINGLE, RP_BATCH } RpLane;
typedef enum { RP_EACCES, RP_ELOOP, RP_EMFILE } RpCause;

typedef struct {
    int     setup;       /* 0 = ok; else the failing step (101 = no restriction) */
    int     poll;        /* the first poll that did not advance */
    int     poll_err;    /* its last_error */
    int64_t delivered;   /* messages before it */
    int64_t poll_ms;
    int     retry;       /* the same poll again, still restricted */
    int     retry_err;
    int64_t retry_ms;
    int     read_status; /* reader at read_at */
    int     read_err;
    int64_t last_index;
    int     last_err;
    int     after;       /* polls once readable again: the final result */
    int64_t total;       /* messages delivered in all, in order (-bad on mismatch) */
} RpResult;

static void rp_fill(unsigned char *buf, int64_t index, int32_t len)
{
    memset(buf, (int)(index & 0xff), (size_t)len);
    memcpy(buf, &index, len < 8 ? (size_t)len : sizeof index);
}

typedef struct { int64_t next; int32_t len; int bad; } RpRead;

static MmapqNextMove rp_single_handler(int64_t index, uint8_t aid, const void *data,
                                       int32_t length, void *ctx)
{
    (void)aid;
    RpRead *c = ctx;
    unsigned char want[RP_BIG];
    rp_fill(want, index, c->len);
    if (index != c->next || length != c->len || memcmp(data, want, (size_t)length) != 0) c->bad++;
    c->next++;
    return MMAPQ_NEXT_ADVANCE;
}

static MmapqNextMove rp_batch_handler(int64_t batch_index, MmapqBatchReadingContext *bctx,
                                      void *ctx)
{
    RpRead *c = ctx;
    unsigned char want[RP_BIG];
    rp_fill(want, batch_index, c->len);
    int n = 0, rc;
    while ((rc = mmapq_batch_iterator_advance(bctx->iterator)) == 1) {
        if (mmapq_batch_iterator_current_length_ffi(bctx->iterator) != c->len ||
            memcmp(mmapq_batch_iterator_current_data_ffi(bctx->iterator), want,
                   (size_t)c->len) != 0)
            c->bad++;
        n++;
    }
    /* A mid-batch read failure (the batch count in payload file 0, the
     * message in the unreadable file 1): the poller reports it after the
     * handler returns and does not advance, so the batch comes again —
     * not consumed here. advance() ends the batch (0) with no message;
     * the error is the iterator's, which the poller reads. */
    if (rc == 0 && n == 0 && bctx->message_count > 0) return MMAPQ_NEXT_ADVANCE;
    if (rc != 0 || n != 1 || batch_index != c->next) c->bad++;
    c->next++;
    return MMAPQ_NEXT_ADVANCE;
}

/* The writer: count messages (single) or one-message batches (batch). */
static void rp_build(RpLane lane, int64_t start_index, int32_t len, int64_t count)
{
    unsigned char buf[RP_BIG];
    if (lane == RP_SINGLE) {
        MmapqIndexedQueue *q = NULL;
        MmapqIndexedAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
            base_dir, "q", RP_REGION, mmapq_min_ring_size(RP_REGION), 0, RP_FILE,
            start_index, RP_TIMEOUT, 0, NULL, NULL, &q));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
        for (int64_t i = start_index; i < start_index + count; i++) {
            rp_fill(buf, i, len);
            TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, buf, len));
        }
        mmapq_indexed_appender_close(a);
        mmapq_indexed_queue_close(q);
    } else {
        MmapqIndexedBatchQueue *q = NULL;
        MmapqIndexedBatchAppender *a = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
            base_dir, "q", RP_REGION, mmapq_min_ring_size(RP_REGION), 0, RP_FILE,
            start_index, RP_TIMEOUT, 0, NULL, NULL, &q));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
        for (int64_t i = start_index; i < start_index + count; i++) {
            MmapqMutableBatch *b = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
            rp_fill(buf, i, len);
            TEST_ASSERT_TRUE(mmapq_mutable_batch_append(b, buf, len) >= 0);
            TEST_ASSERT_EQUAL_INT64(i, mmapq_mutable_batch_complete(b));
        }
        mmapq_indexed_batch_appender_close(a);
        mmapq_indexed_batch_queue_close(q);
    }
    char qdir[128];
    snprintf(qdir, sizeof qdir, "%s/q", base_dir);
    TEST_ASSERT_EQUAL_INT(0, chmod(qdir, 0755));
}

static int rp_poll(RpLane lane, void *p, RpRead *c)
{
    int r;
    if (lane == RP_SINGLE) {
        do r = (int)mmapq_indexed_poller_poll(p, rp_single_handler, c);
        while (r == MMAPQ_POLL_ADVANCED);
    } else {
        do r = (int)mmapq_indexed_batch_poller_poll(p, rp_batch_handler, c);
        while (r == MMAPQ_POLL_ADVANCED);
    }
    return r;
}

static int rp_poll_err(RpLane lane, void *p)
{
    return lane == RP_SINGLE ? mmapq_indexed_poller_last_error(p)
                             : mmapq_indexed_batch_poller_last_error(p);
}

/* The consumer (child): restrict, poll until it stops, retry, ask a reader
 * and last_index, then hand the restriction back to the parent to lift and
 * poll to the end. */
static void rp_child(RpLane lane, RpCause cause, int async, int32_t len, int64_t start_index,
                     const char *file, int64_t read_at, uint64_t timeout_ns,
                     int to_parent, int from_parent, RpResult *res)
{
    char path[160];
    snprintf(path, sizeof path, "%s/q/%s", base_dir, file);
    if (cause == RP_EACCES) {
        if (geteuid() == 0 &&
            (setgroups(0, NULL) != 0 || setgid(RP_GID) != 0 || setuid(RP_UID) != 0)) {
            res->setup = 100; return;
        }
        int fd = open(path, O_RDONLY);
        if (fd >= 0 || errno != EACCES) { res->setup = 101; return; }  /* does not bite */
    }
    MmapqRuntime *rt = NULL;
    if (async) {
        MmapqIdleStrategy *idle = NULL;
        if (mmapq_idle_strategy_yield_create(&idle) != 0 ||
            mmapq_runtime_create("rp-async", idle, MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS,
                                 NULL, NULL, &rt) != 0) { res->setup = 1; return; }
    }
    int ahead = async ? 1 : 0;
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedBatchQueue *bq = NULL;
    void *p = NULL, *rd = NULL;
    if (lane == RP_SINGLE) {
        if (mmapq_indexed_queue_create(base_dir, "q", RP_REGION, mmapq_min_ring_size(RP_REGION),
                                       ahead, RP_FILE, start_index, timeout_ns, 0, rt, rt,
                                       &q) != 0 ||
            mmapq_indexed_queue_create_poller(q, (MmapqIndexedPoller **)&p) != 0 ||
            mmapq_indexed_queue_create_reader(q, (MmapqIndexedReader **)&rd) != 0) {
            res->setup = 2; return;
        }
    } else {
        if (mmapq_indexed_batch_queue_create(base_dir, "q", RP_REGION,
                                             mmapq_min_ring_size(RP_REGION), ahead, RP_FILE,
                                             start_index, timeout_ns, 0, rt, rt, &bq) != 0 ||
            mmapq_indexed_batch_queue_create_poller(bq, (MmapqIndexedBatchPoller **)&p) != 0 ||
            mmapq_indexed_batch_queue_create_reader(bq, (MmapqIndexedBatchReader **)&rd) != 0) {
            res->setup = 2; return;
        }
    }

    struct rlimit saved;
    RpRead c = { start_index, len, 0 };
    if (cause == RP_EMFILE) {
        /* the next descriptor this process opens fails with EMFILE: one
         * poll first opens the poller's first header and payload files,
         * then the limit is capped before it needs header file 1 */
        int first = lane == RP_SINGLE
            ? (int)mmapq_indexed_poller_poll(p, rp_single_handler, &c)
            : (int)mmapq_indexed_batch_poller_poll(p, rp_batch_handler, &c);
        if (first != MMAPQ_POLL_ADVANCED) { res->setup = 3; return; }
        int next = dup(0);
        if (next < 0) { res->setup = 4; return; }
        close(next);
        if (getrlimit(RLIMIT_NOFILE, &saved) != 0) { res->setup = 4; return; }
        struct rlimit lim = saved;
        lim.rlim_cur = (rlim_t)next;
        if (setrlimit(RLIMIT_NOFILE, &lim) != 0) { res->setup = 4; return; }
        int fd = open(path, O_RDONLY);
        if (fd >= 0 || errno != EMFILE) { res->setup = 102; return; }   /* does not bite */
    }

    int64_t t0 = rp_ms();
    res->poll = rp_poll(lane, p, &c);
    res->poll_ms = rp_ms() - t0;
    res->poll_err = rp_poll_err(lane, p);
    res->delivered = c.next - start_index;
    t0 = rp_ms();
    res->retry = rp_poll(lane, p, &c);
    res->retry_ms = rp_ms() - t0;
    res->retry_err = rp_poll_err(lane, p);

    if (cause != RP_EMFILE) {
        if (lane == RP_SINGLE) {
            MmapqReadingContext rc;
            memset(&rc, 0, sizeof rc);
            (void)mmapq_indexed_reader_read(rd, read_at, &rc);
            res->read_status = (int)rc.status;
            res->read_err = mmapq_indexed_reader_last_error(rd);
            res->last_index = mmapq_indexed_reader_last_index(rd);
            res->last_err = mmapq_indexed_reader_last_error(rd);
        } else {
            MmapqBatchReadingContext rc;
            memset(&rc, 0, sizeof rc);
            (void)mmapq_indexed_batch_reader_read(rd, read_at, &rc);
            res->read_status = (int)rc.status;
            res->read_err = mmapq_indexed_batch_reader_last_error(rd);
            res->last_index = mmapq_indexed_batch_reader_last_index(rd);
            res->last_err = mmapq_indexed_batch_reader_last_error(rd);
        }
    } else if (setrlimit(RLIMIT_NOFILE, &saved) != 0) {
        res->setup = 5; return;
    }

    /* the parent lifts the restriction (it owns the file) */
    char b = 'x';
    if (write(to_parent, &b, 1) != 1 || read(from_parent, &b, 1) != 1) { res->setup = 6; return; }
    res->after = rp_poll(lane, p, &c);
    res->total = c.bad ? -c.bad : c.next - start_index;

    if (lane == RP_SINGLE) {
        mmapq_indexed_reader_close(rd);
        mmapq_indexed_poller_close(p);
        mmapq_indexed_queue_close(q);
    } else {
        mmapq_indexed_batch_reader_close(rd);
        mmapq_indexed_batch_poller_close(p);
        mmapq_indexed_batch_queue_close(bq);
    }
    if (rt) { (void)mmapq_runtime_close(rt); mmapq_runtime_destroy(rt); }
}

static RpResult rp_run(RpLane lane, RpCause cause, int async, int32_t len,
                       int64_t start_index, int64_t count, const char *file,
                       int64_t read_at, uint64_t timeout_ns)
{
    rp_build(lane, start_index, len, count);
    char path[160], aside[176];
    snprintf(path, sizeof path, "%s/q/%s", base_dir, file);
    snprintf(aside, sizeof aside, "%s.aside", path);
    if (cause == RP_EACCES) {
        TEST_ASSERT_EQUAL_INT(0, chmod(path, 0000));
    } else if (cause == RP_ELOOP) {
        TEST_ASSERT_EQUAL_INT(0, rename(path, aside));
        TEST_ASSERT_EQUAL_INT(0, symlink(file, path));      /* a link to itself */
    }

    int up[2], down[2], out[2];
    TEST_ASSERT_EQUAL_INT(0, pipe(up));
    TEST_ASSERT_EQUAL_INT(0, pipe(down));
    TEST_ASSERT_EQUAL_INT(0, pipe(out));
    fflush(stdout);
    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        close(up[0]); close(down[1]); close(out[0]);
        RpResult res;
        memset(&res, 0, sizeof res);
        rp_child(lane, cause, async, len, start_index, file, read_at, timeout_ns,
                 up[1], down[0], &res);
        if (res.setup != 0) { char b = 'x'; (void)!write(up[1], &b, 1); }
        ssize_t w = write(out[1], &res, sizeof res);
        _exit(w == (ssize_t)sizeof res ? 0 : 1);
    }
    close(up[1]); close(down[0]); close(out[1]);
    char b;
    if (read(up[0], &b, 1) == 1) {
        if (cause == RP_EACCES) {
            TEST_ASSERT_EQUAL_INT(0, chmod(path, 0644));
        } else if (cause == RP_ELOOP) {
            TEST_ASSERT_EQUAL_INT(0, unlink(path));
            TEST_ASSERT_EQUAL_INT(0, rename(aside, path));
        }
        (void)!write(down[1], &b, 1);
    }
    RpResult res;
    memset(&res, 0, sizeof res);
    ssize_t n = read(out[0], &res, sizeof res);
    close(up[0]); close(down[1]); close(out[0]);
    int st = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &st, 0));
    TEST_ASSERT_TRUE(WIFEXITED(st));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(st));
    TEST_ASSERT_EQUAL_INT((int)sizeof res, (int)n);
    printf("read side %s %s %s %s: setup=%d poll=%d err=%d after %lld in %lldms; "
           "retry=%d err=%d %lldms; read=%d err=%d; last_index=%lld err=%d; after=%d total=%lld\n",
           lane == RP_SINGLE ? "single" : "batch", async ? "async" : "sync",
           cause == RP_EACCES ? "EACCES" : cause == RP_ELOOP ? "ELOOP" : "EMFILE", file,
           res.setup, res.poll, res.poll_err, (long long)res.delivered, (long long)res.poll_ms,
           res.retry, res.retry_err, (long long)res.retry_ms, res.read_status, res.read_err,
           (long long)res.last_index, res.last_err, res.after, (long long)res.total);
    TEST_ASSERT_EQUAL_INT(0, res.setup);
    return res;
}

/* A permanent failure: ERROR + EIO at once, again on retry, the reader
 * agrees; once readable, every message in order. */
static void rp_expect_eio(RpLane lane, RpCause cause, int async, int32_t len,
                          int64_t start_index, int64_t count, const char *file,
                          int64_t delivered, int64_t read_at, int header)
{
    RpResult r = rp_run(lane, cause, async, len, start_index, count, file, read_at, RP_TIMEOUT);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ERROR, r.poll);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, r.poll_err);
    TEST_ASSERT_EQUAL_INT64(delivered, r.delivered);
    TEST_ASSERT_TRUE(r.poll_ms < RP_FAST_MS);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ERROR, r.retry);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, r.retry_err);
    TEST_ASSERT_TRUE(r.retry_ms < RP_FAST_MS);
    TEST_ASSERT_EQUAL_INT(lane == RP_SINGLE ? (int)MMAPQ_ENTRY_ERROR
                                            : (int)MMAPQ_BATCH_ENTRY_ERROR, r.read_status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, r.read_err);
    if (header) {                       /* last_index cannot see past the file */
        TEST_ASSERT_EQUAL_INT64(-1, r.last_index);
        TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, r.last_err);
    } else {                            /* headers readable: the true last index */
        TEST_ASSERT_EQUAL_INT64(start_index + count - 1, r.last_index);
        TEST_ASSERT_EQUAL_INT(0, r.last_err);
    }
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r.after);
    TEST_ASSERT_EQUAL_INT64(count, r.total);
}

/* Header file 1 unreadable: 10 delivered from header file 0, then EIO. */
void test_single_sync_header_eacces_is_eio(void)
{
    rp_expect_eio(RP_SINGLE, RP_EACCES, 0, RP_SMALL, RP_HDR_SI, 20, "header_1",
                  10, RP_HDR_FIRST + 2, 1);
}

void test_single_async_header_eacces_is_eio(void)
{
    rp_expect_eio(RP_SINGLE, RP_EACCES, 1, RP_SMALL, RP_HDR_SI, 20, "header_1",
                  10, RP_HDR_FIRST + 2, 1);
}

/* Header file 0 unreadable: the queue used to look empty. */
void test_single_sync_first_header_eloop_is_eio(void)
{
    rp_expect_eio(RP_SINGLE, RP_ELOOP, 0, RP_SMALL, 0, 20, "header_0", 0, 5, 1);
}

/* Payload file 1 unreadable: 256 delivered from payload file 0, then EIO
 * (was CORRUPT + ECORRUPT). */
void test_single_sync_payload_eacces_is_eio(void)
{
    rp_expect_eio(RP_SINGLE, RP_EACCES, 0, RP_BIG, 0, 300, "payload_0_1", 256, 299, 0);
}

void test_single_async_payload_eacces_is_eio(void)
{
    rp_expect_eio(RP_SINGLE, RP_EACCES, 1, RP_BIG, 0, 300, "payload_0_1", 256, 299, 0);
}

void test_single_sync_payload_eloop_is_eio(void)
{
    rp_expect_eio(RP_SINGLE, RP_ELOOP, 0, RP_BIG, 0, 300, "payload_0_1", 256, 299, 0);
}

void test_batch_sync_header_eacces_is_eio(void)
{
    rp_expect_eio(RP_BATCH, RP_EACCES, 0, RP_SMALL, RP_HDR_SI, 20, "header_1",
                  10, RP_HDR_FIRST + 2, 1);
}

void test_batch_async_header_eacces_is_eio(void)
{
    rp_expect_eio(RP_BATCH, RP_EACCES, 1, RP_SMALL, RP_HDR_SI, 20, "header_1",
                  10, RP_HDR_FIRST + 2, 1);
}

void test_batch_sync_payload_eacces_is_eio(void)
{
    rp_expect_eio(RP_BATCH, RP_EACCES, 0, RP_BIG, 0, 300, "payload_0_1", 256, 299, 0);
}

void test_batch_async_payload_eacces_is_eio(void)
{
    rp_expect_eio(RP_BATCH, RP_EACCES, 1, RP_BIG, 0, 300, "payload_0_1", 256, 299, 0);
}

/* A transient errno (EMFILE) opening the next header file is retried until
 * the timeout: ERROR + ETIMEDOUT, never a silent END_OF_STREAM. */
static void rp_expect_timeout(RpLane lane)
{
    RpResult r = rp_run(lane, RP_EMFILE, 0, RP_SMALL, RP_HDR_SI, 20, "header_1", 0, RP_SHORT);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ERROR, r.poll);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, r.poll_err);
    TEST_ASSERT_EQUAL_INT64(10, r.delivered);
    TEST_ASSERT_TRUE(r.poll_ms >= (int64_t)(RP_SHORT / 1000000) - 50);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ERROR, r.retry);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, r.retry_err);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r.after);
    TEST_ASSERT_EQUAL_INT64(20, r.total);
}

void test_single_header_emfile_is_timeout(void) { rp_expect_timeout(RP_SINGLE); }
void test_batch_header_emfile_is_timeout(void)  { rp_expect_timeout(RP_BATCH); }

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_committed_wrap_rc_table);
    RUN_TEST(test_ro_mapper_not_found_is_only_enoent_and_empty);
    RUN_TEST(test_ro_mapper_permanent_is_io_error);
    RUN_TEST(test_ro_mapper_eacces_is_io_error_and_recovers);
    RUN_TEST(test_ro_mapper_emfile_is_retried);
    RUN_TEST(test_single_sync_header_eacces_is_eio);
    RUN_TEST(test_single_async_header_eacces_is_eio);
    RUN_TEST(test_single_sync_first_header_eloop_is_eio);
    RUN_TEST(test_single_sync_payload_eacces_is_eio);
    RUN_TEST(test_single_async_payload_eacces_is_eio);
    RUN_TEST(test_single_sync_payload_eloop_is_eio);
    RUN_TEST(test_batch_sync_header_eacces_is_eio);
    RUN_TEST(test_batch_async_header_eacces_is_eio);
    RUN_TEST(test_batch_sync_payload_eacces_is_eio);
    RUN_TEST(test_batch_async_payload_eacces_is_eio);
    RUN_TEST(test_single_header_emfile_is_timeout);
    RUN_TEST(test_batch_header_emfile_is_timeout);
    return UNITY_END();
}
