/* HDR-RECREATE-01 pins: the commit path's residual window (hook-only suite).
 *
 * The commit path checks start_index before its explicit header create
 * (O_CREAT|O_EXCL, CAND-59). A writer that stalls between that check and
 * the create, while the header file is filled by another lane, archived
 * past and reclaimed, re-creates the file empty below start_index. The
 * frontier_pre_create hook parks the writer exactly there and runs the
 * other lane, the archive and the reclaim inside the hook.
 *
 * What the pins fix in place (documented in doc/reference/guarantees.md,
 * "The commit floor"): the stale writer's append still lands at the
 * frontier (the floor check rejects the re-created file); the file is
 * empty; a poller left inside the reclaimed range gets ARCHIVED; a poller
 * from start_index sees only the stale writer's entry; the next
 * archivable_files() lists the file and reclaim_files() deletes it.
 *
 * Geometry: REGION 32768, file 65536 → 8192 header slots per header file.
 * Lane 1 fills 0..8191 (header_0) and its next append needs header_1. In
 * the hook lane 2 fills 8192..16384 (header_1 full, 16384 in header_2), the
 * archiver archives to 16385 — the listing takes header files below
 * (start - 1) / 8192, so 16385 is the first line that lists header_1 —
 * and reclaims header_0 and header_1.
 *
 * Both pins pass on the code before and after HDR-RECREATE-01 (it adds
 * the hook and docs only): they are documentation pins. They are seen to
 * bite by the reviewer's mutation (commit_header_below_floor returning 0:
 * the stale writer then commits at 8192 into the re-created file).
 */
#include "unity.h"
#include "mmapq.h"
#include "internal/test_hooks.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef MMAPQ_TEST_HOOKS

#define HR_REGION   ((int32_t)32768)
#define HR_FILE     ((int64_t)65536)
#define HR_PER_FILE ((int64_t)8192)          /* HR_FILE / 8-byte header */
#define HR_LINE     (2 * HR_PER_FILE + 1)    /* 16385 */
#define HR_TIMEOUT  ((uint64_t)2000000000)

static char base_dir[64];
static int  hook_calls;
static int  hook_rc;                          /* first failure inside the hook */

static int file_size(const char *name, long long *size)
{
    char p[256]; struct stat st;
    snprintf(p, sizeof p, "%s/q/%s", base_dir, name);
    if (stat(p, &st) != 0) return 0;
    *size = (long long)st.st_size;
    return 1;
}

static int file_all_zero(const char *name)
{
    char p[256]; snprintf(p, sizeof p, "%s/q/%s", base_dir, name);
    FILE *f = fopen(p, "rb");
    if (f == NULL) return 0;
    int c, zero = 1;
    while ((c = fgetc(f)) != EOF) if (c != 0) { zero = 0; break; }
    fclose(f);
    return zero;
}

static const char *base_name(const char *path)
{
    const char *s = strrchr(path, '/');
    return s ? s + 1 : path;
}

static int listed(char **paths, int n, const char *name)
{
    for (int i = 0; i < n; i++) if (strcmp(base_name(paths[i]), name) == 0) return 1;
    return 0;
}

static MmapqNextMove count_cb(int64_t i, uint8_t id, const void *d, int32_t l, void *c)
{ (void)id; int64_t *v = c; v[v[0] + 1] = i; v[3] = (l == 1) ? ((const char *)d)[0] : -1; v[0]++; return MMAPQ_NEXT_ADVANCE; }
static MmapqNextMove count_batch_cb(int64_t i, MmapqBatchReadingContext *x, void *c)
{
    int64_t *v = c; v[v[0] + 1] = i; v[0]++;
    const void *d = NULL; int32_t l = 0;
    v[3] = (x->message_count == 1 && mmapq_batch_iterator_next(x->iterator, &d, &l) == 1 && l == 1)
         ? ((const char *)d)[0] : -1;
    return MMAPQ_NEXT_ADVANCE;
}

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_hr_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
    hook_calls = 0;
    hook_rc = 0;
}

void tearDown(void)
{
    mmapq_test_hook_set_frontier_pre_create(NULL);
    char cmd[256];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", base_dir);
    if (system(cmd) != 0) { /* best effort */ }
}

#define HOOK_CHECK(cond) do { if (!(cond) && hook_rc == 0) hook_rc = __LINE__; } while (0)

/* ── single lane ────────────────────────────────────────────────────────── */

static MmapqIndexedQueue    *sq;
static MmapqIndexedAppender *s_other;

static void single_hook(void)
{
    mmapq_test_hook_set_frontier_pre_create(NULL);   /* one shot */
    hook_calls++;
    long long sz;
    HOOK_CHECK(!file_size("header_1", &sz));
    for (int64_t i = HR_PER_FILE; i < HR_LINE; i++)
        HOOK_CHECK(mmapq_indexed_appender_append(s_other, "b", 1) == i);
    MmapqIndexedArchiver *ar = NULL;
    HOOK_CHECK(mmapq_indexed_queue_create_archiver(sq, &ar) == 0);
    HOOK_CHECK(mmapq_indexed_archiver_archive_up_to_index(ar, HR_LINE) == 0);
    char *paths[16];
    int n = mmapq_indexed_archiver_archivable_files(ar, paths, 16);
    HOOK_CHECK(n == 2);
    for (int i = 0; i < n && i < 16; i++)
        HOOK_CHECK(strcmp(base_name(paths[i]), i == 0 ? "header_0" : "header_1") == 0);
    if (n > 0) HOOK_CHECK(mmapq_indexed_archiver_reclaim_files(ar, paths, n) == 2);
    for (int i = 0; i < n && i < 16; i++) free(paths[i]);
    mmapq_indexed_archiver_close(ar);
    HOOK_CHECK(!file_size("header_1", &sz));
}

static void test_single_stale_writer_recreates_header_below_start_index(void)
{
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        base_dir, "q", HR_REGION, mmapq_min_ring_size(HR_REGION), 0, HR_FILE, 0,
        HR_TIMEOUT, 1, NULL, NULL, &sq));
    MmapqIndexedAppender *stale = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(sq, 1, &stale));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(sq, 2, &s_other));
    for (int64_t i = 0; i < HR_PER_FILE; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(stale, "a", 1));
    MmapqIndexedPoller *lag = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(sq, &lag));
    int64_t v[4] = {0};
    for (int64_t i = 0; i < HR_PER_FILE; i++) {
        v[0] = 0;
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_poller_poll(lag, count_cb, v));
    }

    mmapq_test_hook_set_frontier_pre_create(single_hook);
    int64_t idx = mmapq_indexed_appender_append(stale, "a", 1);
    TEST_ASSERT_EQUAL_INT(1, hook_calls);
    TEST_ASSERT_EQUAL_INT(0, hook_rc);
    TEST_ASSERT_EQUAL_INT64(HR_LINE, idx);           /* at the frontier, not 8192 */

    long long sz = -1;
    TEST_ASSERT_TRUE(file_size("header_1", &sz));    /* re-created ... */
    TEST_ASSERT_EQUAL_INT64(HR_REGION, sz);
    TEST_ASSERT_TRUE(file_all_zero("header_1"));      /* ... and empty */

    v[0] = 0;
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, mmapq_indexed_poller_poll(lag, count_cb, v));
    TEST_ASSERT_EQUAL_INT64(0, v[0]);
    mmapq_indexed_poller_close(lag);

    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(sq, &p));
    v[0] = 0;
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_poller_poll(p, count_cb, v));
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, mmapq_indexed_poller_poll(p, count_cb, v));
    TEST_ASSERT_EQUAL_INT64(1, v[0]);
    TEST_ASSERT_EQUAL_INT64(HR_LINE, v[1]);
    TEST_ASSERT_EQUAL_INT64('a', v[3]);              /* the stale writer's record */
    mmapq_indexed_poller_close(p);

    MmapqIndexedArchiver *ar = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_archiver(sq, &ar));
    char *paths[16];
    int n = mmapq_indexed_archiver_archivable_files(ar, paths, 16);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_EQUAL_STRING("header_1", base_name(paths[0]));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_reclaim_files(ar, paths, n));
    free(paths[0]);
    mmapq_indexed_archiver_close(ar);
    TEST_ASSERT_FALSE(file_size("header_1", &sz));

    mmapq_indexed_appender_close(stale);
    mmapq_indexed_appender_close(s_other);
    mmapq_indexed_queue_close(sq);
}

/* ── batch lane ─────────────────────────────────────────────────────────── */

static MmapqIndexedBatchQueue    *bq;
static MmapqIndexedBatchAppender *b_other;

static void batch_hook(void)
{
    mmapq_test_hook_set_frontier_pre_create(NULL);
    hook_calls++;
    long long sz;
    HOOK_CHECK(!file_size("header_1", &sz));
    for (int64_t i = HR_PER_FILE; i < HR_LINE; i++)
        HOOK_CHECK(mmapq_indexed_batch_appender_append_and_commit(b_other, "b", 1) == i);
    MmapqIndexedBatchArchiver *ar = NULL;
    HOOK_CHECK(mmapq_indexed_batch_queue_create_archiver(bq, &ar) == 0);
    HOOK_CHECK(mmapq_indexed_batch_archiver_archive_up_to_index(ar, HR_LINE) == 0);
    char *paths[16];
    int n = mmapq_indexed_batch_archiver_archivable_files(ar, paths, 16);
    /* the batch lanes' first payload files are full as well (a batch
     * record is larger than a single-lane one); the stale lane's open
     * payload file is payload_1_1, which is not listed */
    HOOK_CHECK(n == 4);
    HOOK_CHECK(listed(paths, n, "header_0") && listed(paths, n, "header_1") &&
               listed(paths, n, "payload_1_0") && listed(paths, n, "payload_2_0"));
    if (n > 0) HOOK_CHECK(mmapq_indexed_batch_archiver_reclaim_files(ar, paths, n) == 4);
    for (int i = 0; i < n && i < 16; i++) free(paths[i]);
    mmapq_indexed_batch_archiver_close(ar);
    HOOK_CHECK(!file_size("header_1", &sz));
}

static void test_batch_stale_writer_recreates_header_below_start_index(void)
{
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(
        base_dir, "q", HR_REGION, mmapq_min_ring_size(HR_REGION), 0, HR_FILE, 0,
        HR_TIMEOUT, 1, NULL, NULL, &bq));
    MmapqIndexedBatchAppender *stale = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(bq, 1, &stale));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(bq, 2, &b_other));
    for (int64_t i = 0; i < HR_PER_FILE; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_batch_appender_append_and_commit(stale, "a", 1));
    MmapqIndexedBatchPoller *lag = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(bq, &lag));
    int64_t v[4] = {0};
    for (int64_t i = 0; i < HR_PER_FILE; i++) {
        v[0] = 0;
        TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(lag, count_batch_cb, v));
    }

    mmapq_test_hook_set_frontier_pre_create(batch_hook);
    int64_t idx = mmapq_indexed_batch_appender_append_and_commit(stale, "a", 1);
    TEST_ASSERT_EQUAL_INT(1, hook_calls);
    TEST_ASSERT_EQUAL_INT(0, hook_rc);
    TEST_ASSERT_EQUAL_INT64(HR_LINE, idx);

    long long sz = -1;
    TEST_ASSERT_TRUE(file_size("header_1", &sz));
    TEST_ASSERT_EQUAL_INT64(HR_REGION, sz);
    TEST_ASSERT_TRUE(file_all_zero("header_1"));

    v[0] = 0;
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, mmapq_indexed_batch_poller_poll(lag, count_batch_cb, v));
    TEST_ASSERT_EQUAL_INT64(0, v[0]);
    mmapq_indexed_batch_poller_close(lag);

    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(bq, &p));
    v[0] = 0;
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, mmapq_indexed_batch_poller_poll(p, count_batch_cb, v));
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, mmapq_indexed_batch_poller_poll(p, count_batch_cb, v));
    TEST_ASSERT_EQUAL_INT64(1, v[0]);
    TEST_ASSERT_EQUAL_INT64(HR_LINE, v[1]);
    TEST_ASSERT_EQUAL_INT64('a', v[3]);
    mmapq_indexed_batch_poller_close(p);

    MmapqIndexedBatchArchiver *ar = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_archiver(bq, &ar));
    char *paths[16];
    int n = mmapq_indexed_batch_archiver_archivable_files(ar, paths, 16);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_EQUAL_STRING("header_1", base_name(paths[0]));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_batch_archiver_reclaim_files(ar, paths, n));
    free(paths[0]);
    mmapq_indexed_batch_archiver_close(ar);
    TEST_ASSERT_FALSE(file_size("header_1", &sz));

    mmapq_indexed_batch_appender_close(stale);
    mmapq_indexed_batch_appender_close(b_other);
    mmapq_indexed_batch_queue_close(bq);
}

#else
void setUp(void) { }
void tearDown(void) { }
#endif /* MMAPQ_TEST_HOOKS */

int main(void)
{
    UNITY_BEGIN();
#ifdef MMAPQ_TEST_HOOKS
    RUN_TEST(test_single_stale_writer_recreates_header_below_start_index);
    RUN_TEST(test_batch_stale_writer_recreates_header_below_start_index);
#endif
    return UNITY_END();
}
