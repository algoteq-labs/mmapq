/*
 * test_ids_integration — real-queue integration for the mmapq-ids CLI
 * (CAND-50 tools import; replaces the retired mmapq-py-driven
 * test_integration.py from the standalone mmapq-tools repo, b740fd4).
 *
 * Shape (per the approved spec §3.2): a child holds appender id N=7 with an
 * uncommitted record outstanding and is SIGKILLed; the parent observes
 * MMAPQ_EIDCONFLICT on re-acquisition, runs `mmapq-ids <dir> <queue>
 * --unset N` (the real binary, path via MMAPQ_IDS_BIN — fail-closed when
 * unset), asserts the byte-contract (only the pool changes, only bit N),
 * re-attaches rc 0, and recovers the record through the uncommitted
 * iterators + commit, both queue types. A third case corrupts the queue
 * meta before --unset: the tool never opens meta and still releases
 * (damaged-metadata independence).
 *
 * The pool-zeroing rule does NOT apply here: the leaked bit is the fixture
 * under test — the tool is the surgery.
 *
 * Fork/kill discipline (the CAND-51e harness): NULL runtime in the child
 * (never inherit the mapper thread), bounded parent waits, children _exit
 * and never return to Unity, bodies excluded under TSan.
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

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define ID_REG      32768
#define ID_RING     4
#define ID_TIMEOUT  5000000000ULL
#define ID_QUEUE    "q"
#define ID_LANE     7
/* Lane payload streams start at offset 64 (lane preamble). */
#define ID_LANE_START 64

static char DIR_BUF[256];       /* per-test scratch parent dir */
static char QUEUE_DIR[288];     /* DIR_BUF/ID_QUEUE */

/* ── scratch dirs ────────────────────────────────────────────────────────── */

static void rm_rf(const char *path)
{
    char cmd[600];
    snprintf(cmd, sizeof cmd, "rm -rf \"%s\"", path);
    if (system(cmd) != 0) TEST_FAIL_MESSAGE("ids: scratch cleanup failed");
}

void setUp(void)
{
    snprintf(DIR_BUF, sizeof DIR_BUF, "/tmp/mmapq_ids_itest_%d_%d",
             (int)getpid(), (int)rand());
    snprintf(QUEUE_DIR, sizeof QUEUE_DIR, "%s/%s", DIR_BUF, ID_QUEUE);
    rm_rf(DIR_BUF);
    char cmd[600];
    snprintf(cmd, sizeof cmd, "mkdir -p \"%s\"", DIR_BUF);
    if (system(cmd) != 0) TEST_FAIL_MESSAGE("ids: scratch mkdir failed");
}

void tearDown(void)
{
    rm_rf(DIR_BUF);
}

/* ── queue-dir snapshot (the byte-contract) ──────────────────────────────── */

#define ID_MAX_FILES 32

typedef struct {
    char    name[128];
    uint8_t bytes[1 << 20];
    size_t  len;
} IdFile;

typedef struct {
    IdFile files[ID_MAX_FILES];
    int    n;
} IdSnapshot;

/* heap-resident: two live snapshots are ~64 MB, far over stack budget */
static IdSnapshot *id_snap_new(void)
{
    IdSnapshot *s = calloc(1, sizeof *s);
    TEST_ASSERT_NOT_NULL(s);
    return s;
}

static void id_snapshot(const char *queue_dir, IdSnapshot *snap)
{
    snap->n = 0;
    DIR *d = opendir(queue_dir);
    TEST_ASSERT_NOT_NULL(d);
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (e->d_name[0] == '.') continue;
        char path[512];
        TEST_ASSERT_LESS_THAN(sizeof path,
                              strlen(queue_dir) + 1 + strlen(e->d_name) + 1);
        strcpy(path, queue_dir);
        strcat(path, "/");
        strcat(path, e->d_name);
        struct stat st;
        TEST_ASSERT_EQUAL_INT(0, stat(path, &st));
        if (!S_ISREG(st.st_mode)) continue;
        TEST_ASSERT_LESS_THAN(ID_MAX_FILES, snap->n);
        TEST_ASSERT_LESS_OR_EQUAL(sizeof(snap->files[0].bytes), (size_t)st.st_size);
        IdFile *f = &snap->files[snap->n++];
        TEST_ASSERT_LESS_THAN(sizeof f->name, strlen(e->d_name) + 1);
        strcpy(f->name, e->d_name);
        int fd = open(path, O_RDONLY);
        TEST_ASSERT_NOT_EQUAL(-1, fd);
        size_t off = 0;
        while (off < (size_t)st.st_size) {
            ssize_t r = read(fd, f->bytes + off, (size_t)st.st_size - off);
            TEST_ASSERT_NOT_EQUAL(-1, r);
            if (r == 0) break;
            off += (size_t)r;
        }
        TEST_ASSERT_EQUAL_INT(0, close(fd));
        f->len = off;
        TEST_ASSERT_EQUAL_UINT64((uint64_t)st.st_size, (uint64_t)f->len);
    }
    closedir(d);
    /* insertion order is readdir order — sort by name for stable compare */
    for (int i = 0; i < snap->n; i++)
        for (int j = i + 1; j < snap->n; j++)
            if (strcmp(snap->files[i].name, snap->files[j].name) > 0) {
                IdFile t = snap->files[i];
                snap->files[i] = snap->files[j];
                snap->files[j] = t;
            }
}

/* After --unset N: identical file set, every file byte-identical except
 * appender_id_pool, whose image differs only in bit N cleared. */
static void id_assert_only_bit_cleared(const IdSnapshot *before,
                                       const IdSnapshot *after, unsigned id)
{
    TEST_ASSERT_EQUAL_INT(before->n, after->n);
    int pool_seen = 0;
    for (int i = 0; i < before->n; i++) {
        const IdFile *b = &before->files[i];
        const IdFile *a = &after->files[i];
        TEST_ASSERT_EQUAL_STRING(b->name, a->name);
        TEST_ASSERT_EQUAL_UINT64(b->len, a->len);
        if (!strcmp(b->name, "appender_id_pool")) {
            pool_seen = 1;
            uint8_t expected[64];
            memcpy(expected, b->bytes, 64);
            expected[id / 32 * 4 + (id % 32) / 8] &= (uint8_t)~(1u << (id % 8));
            TEST_ASSERT_EQUAL_UINT64(64, b->len);
            TEST_ASSERT_EQUAL_MEMORY(expected, a->bytes, 64);
        } else {
            TEST_ASSERT_EQUAL_MEMORY(b->bytes, a->bytes, b->len);
        }
    }
    TEST_ASSERT_TRUE_MESSAGE(pool_seen, "ids: pool file missing from snapshot");
}

/* ── fork machinery (c35 discipline: bounded reap; child never returns) ──── */

#if !defined(__SANITIZE_THREAD__)
#define ID_DRIVERS 1
#endif

typedef void (*IdChildFn)(void);

#ifdef ID_DRIVERS

static void id_fork_kill(IdChildFn fn)
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
            TEST_FAIL_MESSAGE("ids: child did not die at its kill point");
        }
        struct pollfd pfd = { .fd = -1 };
        poll(&pfd, 0, 50);
    }
    /* NB: the appender-id pool is NOT zeroed here — the leaked bit is the
     * fixture under test (spec §3.2 step 2). */
    if (!(WIFSIGNALED(st) && WTERMSIG(st) == SIGKILL)) {
        char m[128];
        snprintf(m, sizeof m, "ids: child exit mismatch (status 0x%x — "
                 "a child-side _exit(n) names the failed step)", st);
        TEST_FAIL_MESSAGE(m);
    }
}

/* Run the real mmapq-ids binary: exit code exactly 0, the operator warning
 * on stderr. Fail-closed when MMAPQ_IDS_BIN is unset (a test.sh wiring bug,
 * not a skip). */
static void id_run_tool_unset(unsigned id)
{
    const char *bin = getenv("MMAPQ_IDS_BIN");
    TEST_ASSERT_NOT_NULL_MESSAGE(bin,
        "ids: MMAPQ_IDS_BIN unset — test.sh must build the tool first");
    char id_text[8];
    snprintf(id_text, sizeof id_text, "%u", id);
    char err_path[600];
    snprintf(err_path, sizeof err_path, "%s/tool-stderr.txt", DIR_BUF);

    pid_t pid = fork();
    TEST_ASSERT_NOT_EQUAL(-1, pid);
    if (pid == 0) {
        alarm(15);
        int ef = open(err_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (ef < 0) _exit(41);
        if (dup2(ef, STDERR_FILENO) < 0) _exit(42);
        execl(bin, bin, DIR_BUF, ID_QUEUE, "--unset", id_text, (char *)NULL);
        _exit(43);              /* exec failed */
    }
    int st = 0, waited = 0;
    for (;;) {
        pid_t r = waitpid(pid, &st, WNOHANG);
        if (r == pid) break;
        TEST_ASSERT_EQUAL_INT(0, r);
        if (++waited > 200) {
            kill(pid, SIGKILL);
            waitpid(pid, &st, 0);
            TEST_FAIL_MESSAGE("ids: mmapq-ids did not exit in 10 s");
        }
        struct pollfd pfd = { .fd = -1 };
        poll(&pfd, 0, 50);
    }
    TEST_ASSERT_TRUE_MESSAGE(WIFEXITED(st), "ids: mmapq-ids killed by signal");
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, WEXITSTATUS(st),
        "ids: mmapq-ids --unset must exit exactly 0");

    char warning[512] = {0};
    int ef = open(err_path, O_RDONLY);
    TEST_ASSERT_NOT_EQUAL(-1, ef);
    ssize_t n = read(ef, warning, sizeof(warning) - 1);
    TEST_ASSERT_NOT_EQUAL(-1, n);
    close(ef);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(warning, "does not verify ownership"),
        "ids: the operator warning must be printed to stderr");
}

/* ── children (no Unity; _exit codes name the step) ──────────────────────── */

static void id_child_normal(void)
{
    MmapqIndexedQueue *q = NULL;
    MmapqIndexedAppender *a = NULL;
    if (mmapq_indexed_queue_create(DIR_BUF, ID_QUEUE, ID_REG, ID_RING, 1,
            16LL * ID_REG, 0, ID_TIMEOUT, 0, NULL, NULL, &q) != 0) _exit(3);
    if (mmapq_indexed_queue_create_appender_with_id(q, ID_LANE, &a) != 0) _exit(4);
    if (mmapq_indexed_appender_append(a, "before", 6) != 0) _exit(5);
    if (mmapq_indexed_appender_append_uncommitted(a, "unc", 3) < 0) _exit(6);
    raise(SIGKILL);
    _exit(99);
}

static void id_child_batch(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    MmapqIndexedBatchAppender *a = NULL;
    if (mmapq_indexed_batch_queue_create(DIR_BUF, ID_QUEUE, ID_REG, ID_RING, 1,
            16LL * ID_REG, 0, ID_TIMEOUT, 0, NULL, NULL, &q) != 0) _exit(3);
    if (mmapq_indexed_batch_queue_create_appender_with_id(q, ID_LANE, &a) != 0)
        _exit(4);
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    if (!b) _exit(7);
    if (mmapq_mutable_batch_append(b, "before", 6) != 0) _exit(8);
    if (mmapq_mutable_batch_complete(b) != 0) _exit(9);     /* batch index 0 */
    b = mmapq_indexed_batch_appender_start_batch(a);
    if (!b) _exit(10);
    if (mmapq_mutable_batch_append(b, "uncb", 4) != 0) _exit(11);
    if (mmapq_mutable_batch_complete_uncommitted(b) < 0) _exit(12);
    raise(SIGKILL);
    _exit(99);
}

/* ── tests ───────────────────────────────────────────────────────────────── */

static void test_ids_release_reattach_normal(void)
{
    id_fork_kill(id_child_normal);

    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(DIR_BUF, ID_QUEUE,
        ID_REG, ID_RING, 1, 16LL * ID_REG, 0, ID_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIDCONFLICT,
        mmapq_indexed_queue_create_appender_with_id(q, ID_LANE, &a));
    TEST_ASSERT_NULL(a);

    IdSnapshot *before = id_snap_new(), *after = id_snap_new();
    id_snapshot(QUEUE_DIR, before);
    id_run_tool_unset(ID_LANE);
    id_snapshot(QUEUE_DIR, after);
    id_assert_only_bit_cleared(before, after, ID_LANE);
    free(before);
    free(after);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_queue_create_appender_with_id(q, ID_LANE, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* Recovery: exactly the uncommitted record, exact bytes, then commit
     * through the refilled outstanding set. */
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_appender_uncommitted_iterator_checked(a,
            MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, &it));
    TEST_ASSERT_NOT_NULL(it);
    const void *data = NULL;
    int32_t length = 0;
    int64_t offset = 0;
    TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_iterator_next(it, &data, &length, &offset));
    TEST_ASSERT_EQUAL_INT32(3, length);
    TEST_ASSERT_EQUAL_MEMORY("unc", data, 3);
    /* Lane start 64; "before" (4+6) at 64 → the uncommitted record at 74. */
    TEST_ASSERT_EQUAL_INT64(ID_LANE_START + 4 + 6, offset);
    int64_t unc_offset = offset;    /* next() clears its outputs on exhaustion */
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_iterator_next(it, &data, &length, &offset));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, unc_offset));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(DIR_BUF, ID_QUEUE,
        ID_RING, ID_TIMEOUT, NULL, &r));
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(6, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("before", ctx.data, 6);
    mmapq_indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(3, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("unc", ctx.data, 3);

    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

static void test_ids_release_reattach_batch(void)
{
    id_fork_kill(id_child_batch);

    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(DIR_BUF, ID_QUEUE,
        ID_REG, ID_RING, 1, 16LL * ID_REG, 0, ID_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIDCONFLICT,
        mmapq_indexed_batch_queue_create_appender_with_id(q, ID_LANE, &a));
    TEST_ASSERT_NULL(a);

    IdSnapshot *before = id_snap_new(), *after = id_snap_new();
    id_snapshot(QUEUE_DIR, before);
    id_run_tool_unset(ID_LANE);
    id_snapshot(QUEUE_DIR, after);
    id_assert_only_bit_cleared(before, after, ID_LANE);
    free(before);
    free(after);

    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_queue_create_appender_with_id(q, ID_LANE, &a));
    TEST_ASSERT_NOT_NULL(a);

    /* Recovery: the staged batch is reported with exact bytes, then
     * committed through the re-sequenced path. */
    MmapqUncommittedBatchIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &it));
    TEST_ASSERT_NOT_NULL(it);
    int64_t batch_offset = 0;
    int32_t message_count = 0;
    MmapqBatchPayloadIterator *msgs = NULL;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_uncommitted_batch_next(it, &batch_offset, &message_count, &msgs));
    TEST_ASSERT_EQUAL_INT32(1, message_count);
    /* Lane start 64; batch 0: count@64 + "before" (4+6) → staged batch at 78. */
    TEST_ASSERT_EQUAL_INT64(ID_LANE_START + 4 + 4 + 6, batch_offset);
    TEST_ASSERT_NOT_NULL(msgs);
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_advance(msgs));
    TEST_ASSERT_EQUAL_INT32(4, mmapq_batch_iterator_current_length_ffi(msgs));
    TEST_ASSERT_EQUAL_MEMORY("uncb", mmapq_batch_iterator_current_data_ffi(msgs), 4);
    TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_advance(msgs));
    int64_t staged_offset = batch_offset;   /* next clears its outputs */
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_uncommitted_batch_next(it, &batch_offset, &message_count, &msgs));

    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_batch_appender_commit_batch(a, staged_offset));

    MmapqIndexedBatchReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(DIR_BUF, ID_QUEUE,
        ID_RING, ID_TIMEOUT, NULL, &r));
    MmapqBatchReadingContext ctx = {0};
    mmapq_indexed_batch_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(1, ctx.message_count);
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("before",
        mmapq_batch_iterator_current_data_ffi(ctx.iterator), 6);
    mmapq_indexed_batch_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT32(1, ctx.message_count);
    TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_advance(ctx.iterator));
    TEST_ASSERT_EQUAL_MEMORY("uncb",
        mmapq_batch_iterator_current_data_ffi(ctx.iterator), 4);

    mmapq_indexed_batch_reader_close(r);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);
}

/* Damaged-metadata independence (spec §3.2 step 8, owner ruling: one queue
 * type, one extra test function): the queue meta is truncated before
 * --unset; the tool never opens meta and still releases the bit. No
 * reattach — the corrupt queue cannot be opened. */
static void test_ids_release_damaged_meta_normal(void)
{
    id_fork_kill(id_child_normal);

    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(DIR_BUF, ID_QUEUE,
        ID_REG, ID_RING, 1, 16LL * ID_REG, 0, ID_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_EIDCONFLICT,
        mmapq_indexed_queue_create_appender_with_id(q, ID_LANE, &a));
    TEST_ASSERT_NULL(a);
    mmapq_indexed_queue_close(q);

    char meta_path[600];
    snprintf(meta_path, sizeof meta_path, "%s/meta", QUEUE_DIR);
    TEST_ASSERT_EQUAL_INT(0, truncate(meta_path, 16));

    IdSnapshot *before = id_snap_new(), *after = id_snap_new();
    id_snapshot(QUEUE_DIR, before);     /* post-corruption baseline */
    id_run_tool_unset(ID_LANE);
    id_snapshot(QUEUE_DIR, after);
    id_assert_only_bit_cleared(before, after, ID_LANE);
    free(before);
    free(after);
}

#endif /* ID_DRIVERS */

#ifndef ID_DRIVERS
static void test_ids_release_reattach_normal(void)        { TEST_IGNORE_MESSAGE("excluded under TSan"); }
static void test_ids_release_reattach_batch(void)         { TEST_IGNORE_MESSAGE("excluded under TSan"); }
static void test_ids_release_damaged_meta_normal(void)    { TEST_IGNORE_MESSAGE("excluded under TSan"); }
#endif

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_ids_release_reattach_normal);
    RUN_TEST(test_ids_release_reattach_batch);
    RUN_TEST(test_ids_release_damaged_meta_normal);
    return UNITY_END();
}
