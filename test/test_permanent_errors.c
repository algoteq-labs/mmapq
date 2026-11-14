/*
 * Permanent failures are never MMAPQ_ETIMEDOUT (PERMANENT-NOT-TRANSIENT).
 *
 * MMAPQ_ETIMEDOUT means "transient, retry". Before 0.2.1 it was also what
 * every failed open/mkdir/stat/mmap of a queue file returned (the int-return
 * conversion mapped each failure to it without looking at errno), and what
 * the single-message reader and poller returned for a payload file missing
 * under a committed header — so a caller that retried, retried forever.
 *
 *   - mmapqi_fs_errno_rc: ENOENT -> ENOENT, ENOMEM -> ENOMEM, the transient
 *     set (EINTR, EAGAIN, EMFILE, ENFILE, EBUSY, ENOLCK) -> ETIMEDOUT,
 *     everything else -> EIO.
 *   - mmapqi_committed_wrap_rc: only a mapping timeout is ETIMEDOUT; a
 *     missing or short file behind a committed header is ECORRUPT, as on
 *     the batch path.
 *
 * Each end-to-end test asserts the exact code; before the fix every one of
 * them saw MMAPQ_ETIMEDOUT (-1). The permission test forks and, as root,
 * drops to uid/gid 65534 (the test_readonly_open pattern) and first proves
 * the restriction bites — never a silent skip.
 */
#include "unity.h"
#include "mmapq.h"
#include "internal/platform_internal.h"
#include "internal/region_accessor.h"

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define PE_REGION   ((int32_t)32768)
#define PE_FILE     ((int64_t)32768 * 8)
#define PE_TIMEOUT  ((uint64_t)1000000000)
#define PE_UID      ((uid_t)65534)
#define PE_GID      ((gid_t)65534)

static char base_dir[64];

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_pe_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
}

static void rm_rf(const char *p)
{
    char cmd[256];
    snprintf(cmd, sizeof cmd, "chmod -R u+rwx '%s' 2>/dev/null; rm -rf '%s'", p, p);
    TEST_ASSERT_EQUAL_INT(0, system(cmd));
}

void tearDown(void) { rm_rf(base_dir); }

static void make_file(const char *path)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT_TRUE(fd >= 0);
    close(fd);
}

static MmapqNextMove count_handler(int64_t index, uint8_t aid, const void *data,
                                   int32_t length, void *ctx)
{
    (void)index; (void)aid; (void)data; (void)length;
    (*(int *)ctx)++;
    return MMAPQ_NEXT_ADVANCE;
}

/* ── the two mapping helpers, exhaustively ──────────────────────────────── */

void test_fs_errno_rc_table(void)
{
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOENT,    mmapqi_fs_errno_rc(ENOENT));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM,    mmapqi_fs_errno_rc(ENOMEM));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_fs_errno_rc(EINTR));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_fs_errno_rc(EAGAIN));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_fs_errno_rc(EMFILE));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_fs_errno_rc(ENFILE));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_fs_errno_rc(EBUSY));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_fs_errno_rc(ENOLCK));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EACCES));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EPERM));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(ENOTDIR));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(ELOOP));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(ENAMETOOLONG));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EROFS));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EISDIR));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(ENODEV));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(ENOSPC));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EDQUOT));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EFBIG));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO,       mmapqi_fs_errno_rc(EIO));
}

void test_committed_wrap_rc_table(void)
{
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_committed_wrap_rc(WRAP_MAP_TIMED_OUT));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,  mmapqi_committed_wrap_rc(WRAP_FILE_NOT_FOUND));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,  mmapqi_committed_wrap_rc(WRAP_BEYOND_FILE_SIZE));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,  mmapqi_committed_wrap_rc(WRAP_UNWRAPPED));
}

/* ── setup failures, end to end ─────────────────────────────────────────── */

/* The queue handle creates the queue's own directory; a missing BASE
 * directory is "does not exist" (walk-through round 2: it used to be
 * ETIMEDOUT on every attempt). Single and batch. */
void test_missing_base_dir_is_enoent(void)
{
    char missing[160];
    snprintf(missing, sizeof missing, "%s/no/such/base", base_dir);

    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(missing, "q", PE_REGION, 4, 1,
        PE_FILE, 0, PE_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOENT, mmapq_indexed_queue_create_appender(q, &a));
    TEST_ASSERT_NULL(a);
    mmapq_indexed_queue_close(q);

    MmapqIndexedBatchQueue *bq = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(missing, "b", PE_REGION, 4, 1,
        PE_FILE, 0, PE_TIMEOUT, 0, NULL, NULL, &bq));
    MmapqIndexedBatchAppender *ba = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOENT, mmapq_indexed_batch_queue_create_appender(bq, &ba));
    TEST_ASSERT_NULL(ba);
    mmapq_indexed_batch_queue_close(bq);
}

/* The direct appender factory does not create the queue directory: a queue
 * that was never created does not exist. */
void test_appender_on_uncreated_queue_is_enoent(void)
{
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOENT, mmapq_indexed_appender_create(base_dir, "never",
        PE_REGION, 4, PE_FILE, PE_TIMEOUT, 0, NULL, 0, &a));
    TEST_ASSERT_NULL(a);
}

/* A path component that is a regular file (ENOTDIR) is a bad path, not a
 * missing queue and not a timeout: EIO, for every participant. A reader or
 * poller reports it on every attempt (sticky init error). */
void test_path_through_a_file_is_eio(void)
{
    char afile[160];
    snprintf(afile, sizeof afile, "%s/afile", base_dir);
    make_file(afile);

    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(afile, "q", PE_REGION, 4, 1,
        PE_FILE, 0, PE_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, mmapq_indexed_queue_create_appender(q, &a));

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
    int seen = 0;
    for (int k = 0; k < 2; k++) {
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ERROR, mmapq_indexed_poller_poll(p, count_handler, &seen));
        TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, mmapq_indexed_poller_last_error(p));
    }
    TEST_ASSERT_EQUAL_INT(0, seen);
    mmapq_indexed_poller_close(p);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    MmapqReadingContext ctx;
    for (int k = 0; k < 2; k++) {
        mmapq_indexed_reader_read(r, 0, &ctx);
        TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
        TEST_ASSERT_EQUAL_INT(MMAPQ_EIO, mmapq_indexed_reader_last_error(r));
    }
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);
}

/* Permission denied is EIO. As root the check runs in a child that drops to
 * uid/gid 65534; it first proves the restriction bites. */
void test_permission_denied_is_eio(void)
{
    /* The queue directory exists but the participant may not enter it. */
    char qdir[160];
    snprintf(qdir, sizeof qdir, "%s/q", base_dir);
    TEST_ASSERT_EQUAL_INT(0, mkdir(qdir, 0700));
    TEST_ASSERT_EQUAL_INT(0, chmod(base_dir, 0755));
    if (geteuid() != 0) TEST_ASSERT_EQUAL_INT(0, chmod(qdir, 0000));

    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (pid == 0) {
        if (geteuid() == 0) {
            if (setgroups(0, NULL) != 0 || setgid(PE_GID) != 0 || setuid(PE_UID) != 0) _exit(100);
        }
        char probe[200];
        snprintf(probe, sizeof probe, "%s/x", qdir);
        int fd = open(probe, O_RDWR | O_CREAT, 0644);
        if (fd >= 0 || errno != EACCES) _exit(101);        /* restriction does not bite */
        MmapqIndexedAppender *a = NULL;
        int rc = mmapq_indexed_appender_create(base_dir, "q", PE_REGION, 4, PE_FILE,
                                               PE_TIMEOUT, 0, NULL, 0, &a);
        _exit(rc == MMAPQ_EIO ? 0 : 10 + (rc < 0 ? -rc : 50));
    }
    int st = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &st, 0));
    TEST_ASSERT_TRUE(WIFEXITED(st));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(st));
}

/* ── committed reference, single lane ───────────────────────────────────── */

/* A payload file deleted under a committed message (outside the persistence
 * contract) never comes back: the poller reports CORRUPT + ECORRUPT and the
 * reader ENTRY_ERROR + ECORRUPT — as the batch path always did. Non-
 * archivable queue, so no reclaim can explain it. */
void test_missing_payload_under_committed_header_is_corrupt(void)
{
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(base_dir, "q", PE_REGION, 4, 1,
        PE_FILE, 0, PE_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    const char msg[] = "hello";
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, msg, (int32_t)sizeof msg));
    unsigned aid = mmapq_indexed_appender_id(a);
    mmapq_indexed_appender_close(a);

    char payload[200];
    snprintf(payload, sizeof payload, "%s/q/payload_%u_0", base_dir, aid);
    TEST_ASSERT_EQUAL_INT(0, unlink(payload));

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
    int seen = 0;
    for (int k = 0; k < 2; k++) {
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_CORRUPT, mmapq_indexed_poller_poll(p, count_handler, &seen));
        TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_poller_last_error(p));
    }
    TEST_ASSERT_EQUAL_INT(0, seen);
    mmapq_indexed_poller_close(p);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(q, &r));
    MmapqReadingContext ctx;
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);
}

/* ── stress pin: the rule never fires on a live queue ───────────────────── */

/* A payload miss under a committed header is ECORRUPT only because the
 * writer creates and sizes the payload file before it commits the header.
 * Pin that: one-region files (a file boundary every few dozen messages),
 * a writer thread flat out, a poller right behind it on the same queue —
 * exactly 0 ERROR and 0 CORRUPT outcomes, every message in order. */
#define ST_REGION   ((int32_t)32768)
#define ST_FILE     ((int64_t)32768)
#define ST_SINGLE_N 20000
#define ST_BATCH_N  6000
#define ST_DEADLINE_S 120

static int32_t st_len(int64_t i) { return 16 + (int32_t)((i * 37) % 900); }

static double st_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

typedef struct { void *a; int batch; int failed; } StWriter;

static void *st_write(void *arg)
{
    StWriter *w = arg;
    unsigned char buf[1024];
    if (!w->batch) {
        for (int64_t i = 0; i < ST_SINGLE_N; i++) {
            if ((i & 31) == 0) sched_yield();   /* keep the poller close behind */
            memset(buf, (int)(i & 0xff), sizeof buf);
            memcpy(buf, &i, sizeof i);
            if (mmapq_indexed_appender_append(w->a, buf, st_len(i)) != i) { w->failed = 1; break; }
        }
    } else {
        for (int64_t i = 0; i < ST_BATCH_N; i++) {
            if ((i & 31) == 0) sched_yield();   /* keep the poller close behind */
            MmapqMutableBatch *b = NULL;
            if (mmapq_indexed_batch_appender_start_batch_checked(w->a, &b) != 0) { w->failed = 1; break; }
            int n = 1 + (int)(i % 4);
            for (int m = 0; m < n; m++) {
                memset(buf, m, sizeof buf);
                memcpy(buf, &i, sizeof i);
                if (mmapq_mutable_batch_append(b, buf, st_len(i * 4 + m) / 2) < 0) { w->failed = 1; break; }
            }
            if (w->failed || mmapq_mutable_batch_complete(b) != i) { w->failed = 1; break; }
        }
    }
    return NULL;
}

typedef struct { int64_t next; int bad; } StSingle;

static MmapqNextMove st_single_handler(int64_t index, uint8_t aid, const void *data,
                                       int32_t length, void *ctx)
{
    (void)aid;
    StSingle *c = ctx;
    int64_t tag;
    memcpy(&tag, data, sizeof tag);
    if (index != c->next || tag != index || length != st_len(index)) c->bad++;
    c->next++;
    return MMAPQ_NEXT_ADVANCE;
}

typedef struct { int64_t next; int64_t msgs; int bad; } StBatch;

static MmapqNextMove st_batch_handler(int64_t batch_index, MmapqBatchReadingContext *bctx,
                                      void *ctx)
{
    StBatch *c = ctx;
    int n = 0, rc;
    while ((rc = mmapq_batch_iterator_advance(bctx->iterator)) == 1) {
        int64_t tag;
        memcpy(&tag, mmapq_batch_iterator_current_data_ffi(bctx->iterator), sizeof tag);
        if (tag != batch_index ||
            mmapq_batch_iterator_current_length_ffi(bctx->iterator) != st_len(batch_index * 4 + n) / 2)
            c->bad++;
        n++;
    }
    if (rc != 0 || batch_index != c->next || n != 1 + (int)(batch_index % 4)) c->bad++;
    c->msgs += n;
    c->next++;
    return MMAPQ_NEXT_ADVANCE;
}

static void st_single(MmapqRuntime *rt)
{
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(base_dir, "q", ST_REGION,
        mmapq_min_ring_size(ST_REGION), 1, ST_FILE, 0, PE_TIMEOUT, 0, rt, rt, &q));
    StWriter w = { 0 };
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, (MmapqIndexedAppender **)&w.a));
    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));

    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, st_write, &w));
    StSingle c = { 0 };
    int errors = 0, corrupt = 0, other = 0;
    long eos = 0;
    double end = st_now() + ST_DEADLINE_S;
    while (c.next < ST_SINGLE_N && st_now() < end) {
        MmapqPollResult pr = mmapq_indexed_poller_poll(p, st_single_handler, &c);
        if (pr == MMAPQ_POLL_ERROR) errors++;
        else if (pr == MMAPQ_POLL_CORRUPT) corrupt++;
        else if (pr == MMAPQ_POLL_END_OF_STREAM) { eos++; sched_yield(); }
        else if (pr != MMAPQ_POLL_ADVANCED) other++;
        if (errors + corrupt + other) break;
    }
    pthread_join(t, NULL);
    printf("stress single %s: delivered=%lld eos=%ld errors=%d corrupt=%d other=%d bad=%d\n",
           rt ? "async" : "sync", (long long)c.next, eos, errors, corrupt, other, c.bad);
    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(w.a);
    mmapq_indexed_queue_close(q);
    TEST_ASSERT_EQUAL_INT(0, w.failed);
    TEST_ASSERT_EQUAL_INT(0, errors);
    TEST_ASSERT_EQUAL_INT(0, corrupt);
    TEST_ASSERT_EQUAL_INT(0, other);
    TEST_ASSERT_EQUAL_INT(0, c.bad);
    TEST_ASSERT_EQUAL_INT64(ST_SINGLE_N, c.next);
}

static void st_batch(MmapqRuntime *rt)
{
    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(base_dir, "q", ST_REGION,
        mmapq_min_ring_size(ST_REGION), 1, ST_FILE, 0, PE_TIMEOUT, 0, rt, rt, &q));
    StWriter w = { .batch = 1 };
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, (MmapqIndexedBatchAppender **)&w.a));
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(q, &p));

    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, st_write, &w));
    StBatch c = { 0 };
    int errors = 0, corrupt = 0, other = 0;
    long eos = 0;
    double end = st_now() + ST_DEADLINE_S;
    while (c.next < ST_BATCH_N && st_now() < end) {
        BatchPollResult pr = mmapq_indexed_batch_poller_poll(p, st_batch_handler, &c);
        if (pr == MMAPQ_POLL_ERROR) errors++;
        else if (pr == MMAPQ_POLL_CORRUPT) corrupt++;
        else if (pr == MMAPQ_POLL_END_OF_STREAM) { eos++; sched_yield(); }
        else if (pr != MMAPQ_POLL_ADVANCED) other++;
        if (errors + corrupt + other) break;
    }
    pthread_join(t, NULL);
    printf("stress batch %s: batches=%lld messages=%lld eos=%ld errors=%d corrupt=%d other=%d bad=%d\n",
           rt ? "async" : "sync", (long long)c.next, (long long)c.msgs, eos, errors, corrupt, other, c.bad);
    mmapq_indexed_batch_poller_close(p);
    mmapq_indexed_batch_appender_close(w.a);
    mmapq_indexed_batch_queue_close(q);
    TEST_ASSERT_EQUAL_INT(0, w.failed);
    TEST_ASSERT_EQUAL_INT(0, errors);
    TEST_ASSERT_EQUAL_INT(0, corrupt);
    TEST_ASSERT_EQUAL_INT(0, other);
    TEST_ASSERT_EQUAL_INT(0, c.bad);
    TEST_ASSERT_EQUAL_INT64(ST_BATCH_N, c.next);
}

static MmapqRuntime *st_runtime(void)
{
    MmapqIdleStrategy *idle = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_yield_create(&idle));
    MmapqRuntime *rt = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_create("pe-stress", idle,
        MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, NULL, NULL, &rt));
    return rt;
}

static void st_runtime_done(MmapqRuntime *rt)
{
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_close(rt));
    mmapq_runtime_destroy(rt);
}

/* sync: regions mapped on the calling thread; async: by a runtime thread */
void test_stress_live_queue_never_error_or_corrupt_single_sync(void) { st_single(NULL); }
void test_stress_live_queue_never_error_or_corrupt_batch_sync(void)  { st_batch(NULL); }
void test_stress_live_queue_never_error_or_corrupt_single_async(void)
{
    MmapqRuntime *rt = st_runtime();
    st_single(rt);
    st_runtime_done(rt);
}
void test_stress_live_queue_never_error_or_corrupt_batch_async(void)
{
    MmapqRuntime *rt = st_runtime();
    st_batch(rt);
    st_runtime_done(rt);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_fs_errno_rc_table);
    RUN_TEST(test_committed_wrap_rc_table);
    RUN_TEST(test_missing_base_dir_is_enoent);
    RUN_TEST(test_appender_on_uncreated_queue_is_enoent);
    RUN_TEST(test_path_through_a_file_is_eio);
    RUN_TEST(test_permission_denied_is_eio);
    RUN_TEST(test_missing_payload_under_committed_header_is_corrupt);
    RUN_TEST(test_stress_live_queue_never_error_or_corrupt_single_sync);
    RUN_TEST(test_stress_live_queue_never_error_or_corrupt_batch_sync);
    RUN_TEST(test_stress_live_queue_never_error_or_corrupt_single_async);
    RUN_TEST(test_stress_live_queue_never_error_or_corrupt_batch_async);
    return UNITY_END();
}
