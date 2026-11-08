/*
 * Read-only participants on a read-only queue (READONLY-OPEN-01).
 *
 * Readers and pollers, single and batch, must work for a user who has
 * read permission only — files 0444, queue directory 0555 (r-x) — and
 * must create and modify nothing. Before the fix the meta was opened
 * O_RDWR|O_CREAT, so every read and poll failed with ETIMEDOUT for such a
 * user.
 *
 * Each test builds a queue as the current user, makes it read-only,
 * then reads it in a forked child that cannot write: as root the child
 * drops to uid/gid 65534; in every case it first proves the restriction
 * bites (open(meta, O_RDWR) fails with EACCES) and fails the test if it
 * does not — never a silent skip. The child reports a bitmask of failed
 * checks through its exit status. The parent then compares the queue
 * directory with a snapshot taken before (names, sizes, modes, mtimes).
 *
 * Runtimes are NULL (synchronous mapping) in parent and child, so no
 * library thread exists at fork().
 */
#include "unity.h"
#include "mmapq.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define RO_REGION   ((int32_t)32768)
#define RO_FILE     ((int64_t)32768 * 8)
#define RO_TIMEOUT  ((uint64_t)1000000000)
#define RO_UID      ((uid_t)65534)
#define RO_GID      ((gid_t)65534)

/* child exit codes outside the check bitmask */
#define EXIT_DROP_FAILED   100   /* could not drop root */
#define EXIT_NOT_RESTRICTED 101  /* open(meta, O_RDWR) did not fail with EACCES */

static char base_dir[64];
static char queue_dir[160];

/* ── directory snapshot ──────────────────────────────────────────────────── */

#define SNAP_MAX 32
typedef struct {
    char            name[256];
    off_t           size;
    mode_t          mode;
    struct timespec mtime;
} SnapEntry;
typedef struct {
    int       n;
    SnapEntry e[SNAP_MAX];
    struct timespec dir_mtime;
} Snapshot;

static int snap_cmp(const void *a, const void *b)
{
    return strcmp(((const SnapEntry *)a)->name, ((const SnapEntry *)b)->name);
}

static void snapshot(const char *dir, Snapshot *s)
{
    memset(s, 0, sizeof *s);
    struct stat st;
    TEST_ASSERT_EQUAL_INT(0, stat(dir, &st));
    s->dir_mtime = st.st_mtim;
    DIR *d = opendir(dir);
    TEST_ASSERT_NOT_NULL(d);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        TEST_ASSERT_TRUE(s->n < SNAP_MAX);
        SnapEntry *e = &s->e[s->n++];
        snprintf(e->name, sizeof e->name, "%s", de->d_name);
        char p[512];
        snprintf(p, sizeof p, "%s/%s", dir, de->d_name);
        TEST_ASSERT_EQUAL_INT(0, stat(p, &st));
        e->size = st.st_size;
        e->mode = st.st_mode & 07777;
        e->mtime = st.st_mtim;
    }
    closedir(d);
    qsort(s->e, (size_t)s->n, sizeof s->e[0], snap_cmp);
}

/* compare ignoring modes (the parent changes them around the child run) */
static void assert_same_content(const Snapshot *a, const Snapshot *b)
{
    TEST_ASSERT_EQUAL_INT_MESSAGE(a->n, b->n, "file count changed");
    for (int i = 0; i < a->n; i++) {
        TEST_ASSERT_EQUAL_STRING(a->e[i].name, b->e[i].name);
        TEST_ASSERT_EQUAL_INT64_MESSAGE((int64_t)a->e[i].size, (int64_t)b->e[i].size, a->e[i].name);
        TEST_ASSERT_EQUAL_INT64_MESSAGE((int64_t)a->e[i].mtime.tv_sec, (int64_t)b->e[i].mtime.tv_sec, a->e[i].name);
        TEST_ASSERT_EQUAL_INT64_MESSAGE((int64_t)a->e[i].mtime.tv_nsec, (int64_t)b->e[i].mtime.tv_nsec, a->e[i].name);
    }
    TEST_ASSERT_EQUAL_INT64_MESSAGE((int64_t)a->dir_mtime.tv_sec, (int64_t)b->dir_mtime.tv_sec, "queue dir mtime");
    TEST_ASSERT_EQUAL_INT64_MESSAGE((int64_t)a->dir_mtime.tv_nsec, (int64_t)b->dir_mtime.tv_nsec, "queue dir mtime");
}

static void set_modes(const char *dir, mode_t file_mode, mode_t dir_mode)
{
    Snapshot s;
    snapshot(dir, &s);
    for (int i = 0; i < s.n; i++) {
        char p[512];
        snprintf(p, sizeof p, "%s/%s", dir, s.e[i].name);
        TEST_ASSERT_EQUAL_INT(0, chmod(p, file_mode));
    }
    TEST_ASSERT_EQUAL_INT(0, chmod(dir, dir_mode));
}

/* ── setup ───────────────────────────────────────────────────────────────── */

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_ro_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
    TEST_ASSERT_EQUAL_INT(0, chmod(base_dir, 0755));   /* traversable by uid 65534 */
    queue_dir[0] = '\0';
}

void tearDown(void)
{
    if (queue_dir[0] != '\0') {
        chmod(queue_dir, 0755);
        DIR *d = opendir(queue_dir);
        if (d != NULL) {
            struct dirent *de;
            while ((de = readdir(d)) != NULL) {
                if (de->d_name[0] == '.') continue;
                char p[512];
                snprintf(p, sizeof p, "%s/%s", queue_dir, de->d_name);
                unlink(p);
            }
            closedir(d);
        }
        rmdir(queue_dir);
    }
    rmdir(base_dir);
}

/* ── child side ──────────────────────────────────────────────────────────── */

static void child_become_restricted(void)
{
    if (geteuid() == 0) {
        if (setgroups(0, NULL) != 0) _exit(EXIT_DROP_FAILED);
        if (setgid(RO_GID) != 0)     _exit(EXIT_DROP_FAILED);
        if (setuid(RO_UID) != 0)     _exit(EXIT_DROP_FAILED);
        if (geteuid() != RO_UID)     _exit(EXIT_DROP_FAILED);
    }
    char p[256];
    snprintf(p, sizeof p, "%s/meta", queue_dir);
    int fd = open(p, O_RDWR);
    if (fd >= 0) { close(fd); _exit(EXIT_NOT_RESTRICTED); }
    if (errno != EACCES) _exit(EXIT_NOT_RESTRICTED);
}

static int same_bytes(const void *d, int32_t len, const char *want)
{
    return d != NULL && len == (int32_t)strlen(want) && memcmp(d, want, (size_t)len) == 0;
}

/* single: m0, m1, m2 */
static const char *const SINGLE_MSGS[] = { "m0", "m1", "m2" };

typedef struct { int n; int bad; } SinglePollCtx;

static MmapqNextMove single_handler(int64_t index, uint8_t appender_id,
                                    const void *data, int32_t length, void *ctx)
{
    (void)appender_id;
    SinglePollCtx *c = (SinglePollCtx *)ctx;
    if (index != c->n || c->n >= 3 || !same_bytes(data, length, SINGLE_MSGS[c->n])) c->bad = 1;
    c->n++;
    return MMAPQ_NEXT_ADVANCE;
}

static int child_single_checks(void)
{
    int bits = 0;
    MmapqIndexedQueue *q = NULL;
    if (mmapq_indexed_queue_create(base_dir, "s", RO_REGION, 4, 1, RO_FILE, 0,
                                   RO_TIMEOUT, 0, NULL, NULL, &q) != 0) return 1 << 0;

    MmapqIndexedReader *r = NULL;
    if (mmapq_indexed_queue_create_reader(q, &r) != 0) bits |= 1 << 1;
    else {
        for (int i = 0; i < 3; i++) {
            MmapqReadingContext ctx = {0};
            mmapq_indexed_reader_read(r, i, &ctx);
            if (ctx.status != MMAPQ_ENTRY_AVAILABLE ||
                !same_bytes(ctx.data, ctx.length, SINGLE_MSGS[i])) bits |= 1 << 2;
        }
        MmapqReadingContext end = {0};
        mmapq_indexed_reader_read(r, 3, &end);
        if (end.status != MMAPQ_ENTRY_END_OF_STREAM) bits |= 1 << 3;
        mmapq_indexed_reader_close(r);
    }

    MmapqIndexedPoller *p = NULL;
    if (mmapq_indexed_queue_create_poller(q, &p) != 0) bits |= 1 << 4;
    else {
        SinglePollCtx c = {0, 0};
        for (int i = 0; i < 3; i++)
            if (mmapq_indexed_poller_poll(p, single_handler, &c) != MMAPQ_POLL_ADVANCED) bits |= 1 << 5;
        if (c.n != 3 || c.bad) bits |= 1 << 6;
        if (mmapq_indexed_poller_poll(p, single_handler, &c) != MMAPQ_POLL_END_OF_STREAM) bits |= 1 << 7;
        mmapq_indexed_poller_close(p);
    }
    mmapq_indexed_queue_close(q);
    return bits;
}

/* batch: batch 0 = (a0, a1), batch 1 = (b0) */
static const char *const BATCH0[] = { "a0", "a1" };
static const char *const BATCH1[] = { "b0" };

/* 1 iff the iterator yields exactly want[0..n) */
static int batch_matches(MmapqBatchPayloadIterator *it, const char *const *want, int n)
{
    for (int i = 0; i < n; i++) {
        if (mmapq_batch_iterator_advance(it) != 1) return 0;
        if (!same_bytes(mmapq_batch_iterator_current_data_ffi(it),
                        mmapq_batch_iterator_current_length_ffi(it), want[i])) return 0;
    }
    return mmapq_batch_iterator_advance(it) == 0;
}

typedef struct { int n; int bad; } BatchPollCtx;

static MmapqNextMove batch_handler(int64_t index, MmapqBatchReadingContext *x, void *ctx)
{
    BatchPollCtx *c = (BatchPollCtx *)ctx;
    int ok = (index == c->n) &&
             ((c->n == 0 && x->message_count == 2 && batch_matches(x->iterator, BATCH0, 2)) ||
              (c->n == 1 && x->message_count == 1 && batch_matches(x->iterator, BATCH1, 1)));
    if (!ok) c->bad = 1;
    c->n++;
    return MMAPQ_NEXT_ADVANCE;
}

static int child_batch_checks(void)
{
    int bits = 0;
    MmapqIndexedBatchQueue *q = NULL;
    if (mmapq_indexed_batch_queue_create(base_dir, "b", RO_REGION, 4, 1, RO_FILE, 0,
                                         RO_TIMEOUT, 0, NULL, NULL, &q) != 0) return 1 << 0;

    MmapqIndexedBatchReader *r = NULL;
    if (mmapq_indexed_batch_queue_create_reader(q, &r) != 0) bits |= 1 << 1;
    else {
        MmapqBatchReadingContext c0 = {0};
        mmapq_indexed_batch_reader_read(r, 0, &c0);
        if (c0.status != MMAPQ_BATCH_ENTRY_AVAILABLE || c0.message_count != 2 ||
            !batch_matches(c0.iterator, BATCH0, 2)) bits |= 1 << 2;
        MmapqBatchReadingContext c1 = {0};
        mmapq_indexed_batch_reader_read(r, 1, &c1);
        if (c1.status != MMAPQ_BATCH_ENTRY_AVAILABLE || c1.message_count != 1 ||
            !batch_matches(c1.iterator, BATCH1, 1)) bits |= 1 << 2;
        MmapqBatchReadingContext c2 = {0};
        mmapq_indexed_batch_reader_read(r, 2, &c2);
        if (c2.status != MMAPQ_BATCH_ENTRY_END_OF_STREAM) bits |= 1 << 3;
        mmapq_indexed_batch_reader_close(r);
    }

    MmapqIndexedBatchPoller *p = NULL;
    if (mmapq_indexed_batch_queue_create_poller(q, &p) != 0) bits |= 1 << 4;
    else {
        BatchPollCtx c = {0, 0};
        for (int i = 0; i < 2; i++)
            if (mmapq_indexed_batch_poller_poll(p, batch_handler, &c) != MMAPQ_POLL_ADVANCED) bits |= 1 << 5;
        if (c.n != 2 || c.bad) bits |= 1 << 6;
        if (mmapq_indexed_batch_poller_poll(p, batch_handler, &c) != MMAPQ_POLL_END_OF_STREAM) bits |= 1 << 7;
        mmapq_indexed_batch_poller_close(p);
    }
    mmapq_indexed_batch_queue_close(q);
    return bits;
}

/* ── parent side ─────────────────────────────────────────────────────────── */

static void write_single_queue(void)
{
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(base_dir, "s", RO_REGION, 4, 1,
        RO_FILE, 0, RO_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    for (int i = 0; i < 3; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, SINGLE_MSGS[i],
                                (int32_t)strlen(SINGLE_MSGS[i])));
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

static void append_batch(MmapqIndexedBatchAppender *a, const char *const *m, int n, int64_t want)
{
    MmapqMutableBatch *b = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(a, &b));
    for (int i = 0; i < n; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_mutable_batch_append(b, m[i], (int32_t)strlen(m[i])));
    TEST_ASSERT_EQUAL_INT64(want, mmapq_mutable_batch_complete(b));
}

static void write_batch_queue(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(base_dir, "b", RO_REGION, 4, 1,
        RO_FILE, 0, RO_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    append_batch(a, BATCH0, 2, 0);
    append_batch(a, BATCH1, 1, 1);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);
}

static void run_readonly_case(const char *queue_name, void (*write_queue)(void),
                              int (*child_checks)(void))
{
    snprintf(queue_dir, sizeof queue_dir, "%s/%s", base_dir, queue_name);
    write_queue();

    Snapshot before, after;
    snapshot(queue_dir, &before);
    set_modes(queue_dir, 0444, 0555);

    fflush(NULL);
    pid_t pid = fork();
    TEST_ASSERT_NOT_EQUAL(-1, pid);
    if (pid == 0) {
        child_become_restricted();
        _exit(child_checks());
    }
    int st = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &st, 0));

    set_modes(queue_dir, 0644, 0755);
    snapshot(queue_dir, &after);

    if (!(WIFEXITED(st) && WEXITSTATUS(st) == 0)) {
        char m[200];
        if (WIFSIGNALED(st))
            snprintf(m, sizeof m, "child killed by signal %d", WTERMSIG(st));
        else if (WEXITSTATUS(st) == EXIT_DROP_FAILED)
            snprintf(m, sizeof m, "child could not drop root to uid %d", (int)RO_UID);
        else if (WEXITSTATUS(st) == EXIT_NOT_RESTRICTED)
            snprintf(m, sizeof m, "permissions do not bite: open(meta, O_RDWR) did not "
                     "fail with EACCES (CAP_DAC_OVERRIDE?) — the test cannot run here");
        else
            snprintf(m, sizeof m, "child check bitmask 0x%02x (bit 0 queue, 1 reader create, "
                     "2 read contents, 3 read EOS, 4 poller create, 5 poll ADVANCED, "
                     "6 poll contents, 7 poll EOS)", WEXITSTATUS(st));
        TEST_FAIL_MESSAGE(m);
    }
    assert_same_content(&before, &after);
}

void test_ro_single_reader_poller_readonly_queue(void)
{
    run_readonly_case("s", write_single_queue, child_single_checks);
}

void test_ro_batch_reader_poller_readonly_queue(void)
{
    run_readonly_case("b", write_batch_queue, child_batch_checks);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_ro_single_reader_poller_readonly_queue);
    RUN_TEST(test_ro_batch_reader_poller_readonly_queue);
    return UNITY_END();
}
