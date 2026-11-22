/*
 * MOVE-STATUS-01 pins (0.2.1).
 *
 * (1) The batch poller's cursor and lookup functions take the NULL guards
 *     the single-message poller has (indexed_poller.c): move_to_index,
 *     move_to_end, move_to_last_existing_batch, move_to_start, exists,
 *     start_index and current_batch_index return 0 for a NULL poller. They
 *     used to dereference it. Each call runs in a forked child, so the
 *     pre-fix crash is reported as a failed test, not a dead runner.
 *     (The closed check that came with them cannot be pinned, nor can it
 *     protect anything: close frees the handle. The bindings refuse a
 *     closed handle themselves.)
 * (2) A mapping timeout inside a move does not abort (the header said it
 *     did, MMAP_ASSERT): the header read treats the slot as unwritten, so
 *     move_to_index returns 0 and leaves the cursor alone. Hook builds only
 *     (header_wrap_inject); both lanes.
 */
#include "mmapq.h"
#include "internal/region_accessor.h"
#include "internal/test_hooks.h"
#include "unity.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#define MS_REGION  ((int32_t)32768)
#define MS_FILE    ((int64_t)32768 * 8)
#define MS_TIMEOUT ((uint64_t)2000000000)

static char base_dir[64];

void setUp(void)
{
    snprintf(base_dir, sizeof base_dir, "/tmp/mmapq_ms_XXXXXX");
    TEST_ASSERT_NOT_NULL(mkdtemp(base_dir));
}

void tearDown(void)
{
#ifdef MMAPQ_TEST_HOOKS
    mmapq_test_hook_header_wrap_inject(0);
#endif
    char cmd[256];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", base_dir);
    if (system(cmd) != 0) { /* best effort */ }
}

/* Runs fn in a child; the child exits 0 iff fn returned `want`. */
static void expect_in_child(int64_t (*fn)(void), int64_t want, const char *what)
{
    fflush(stdout);
    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (pid == 0) _exit(fn() == want ? 0 : 1);
    int st = 0;
    TEST_ASSERT_EQUAL_INT(pid, waitpid(pid, &st, 0));
    char msg[160];
    if (WIFSIGNALED(st))
        snprintf(msg, sizeof msg, "%s: child killed by signal %d", what, WTERMSIG(st));
    else
        snprintf(msg, sizeof msg, "%s: child exit %d", what, WEXITSTATUS(st));
    TEST_ASSERT_TRUE_MESSAGE(WIFEXITED(st) && WEXITSTATUS(st) == 0, msg);
}

static int64_t null_move_to_index(void) { return mmapq_indexed_batch_poller_move_to_index(NULL, 0); }
static int64_t null_move_to_end(void)   { return mmapq_indexed_batch_poller_move_to_end(NULL); }
static int64_t null_move_to_last(void)  { return mmapq_indexed_batch_poller_move_to_last_existing_batch(NULL); }
static int64_t null_move_to_start(void) { return mmapq_indexed_batch_poller_move_to_start(NULL); }
static int64_t null_exists(void)        { return mmapq_indexed_batch_poller_exists(NULL, 0); }
static int64_t null_start_index(void)   { return mmapq_indexed_batch_poller_start_index(NULL); }
static int64_t null_current(void)       { return mmapq_indexed_batch_poller_current_batch_index(NULL); }

static void test_batch_poller_null_move_to_index_returns_0(void) { expect_in_child(null_move_to_index, 0, "move_to_index(NULL, 0)"); }
static void test_batch_poller_null_move_to_end_returns_0(void)   { expect_in_child(null_move_to_end, 0, "move_to_end(NULL)"); }
static void test_batch_poller_null_move_to_last_returns_0(void)  { expect_in_child(null_move_to_last, 0, "move_to_last_existing_batch(NULL)"); }
static void test_batch_poller_null_move_to_start_returns_0(void) { expect_in_child(null_move_to_start, 0, "move_to_start(NULL)"); }
static void test_batch_poller_null_exists_returns_0(void)        { expect_in_child(null_exists, 0, "exists(NULL, 0)"); }
static void test_batch_poller_null_start_index_returns_0(void)   { expect_in_child(null_start_index, 0, "start_index(NULL)"); }
static void test_batch_poller_null_current_returns_0(void)       { expect_in_child(null_current, 0, "current_batch_index(NULL)"); }

#ifdef MMAPQ_TEST_HOOKS

static void test_single_move_to_index_timeout_returns_0(void)
{
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(base_dir, "q", MS_REGION,
        mmapq_min_ring_size(MS_REGION), 0, MS_FILE, 0, MS_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    for (int64_t i = 0; i < 10; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "m", 1));
    MmapqIndexedPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_poller(q, &p));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_index(p, 2));

    mmapq_test_hook_header_wrap_inject(WRAP_MAP_TIMED_OUT);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_poller_move_to_index(p, 5));   /* no abort */
    mmapq_test_hook_header_wrap_inject(0);
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_poller_current_index(p));    /* cursor kept */
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_poller_move_to_index(p, 5));
    TEST_ASSERT_EQUAL_INT64(5, mmapq_indexed_poller_current_index(p));

    mmapq_indexed_poller_close(p);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);
}

static void test_batch_move_to_index_timeout_returns_0(void)
{
    MmapqIndexedBatchQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(base_dir, "q", MS_REGION,
        mmapq_min_ring_size(MS_REGION), 0, MS_FILE, 0, MS_TIMEOUT, 0, NULL, NULL, &q));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(q, &a));
    for (int64_t i = 0; i < 10; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_batch_appender_append_and_commit(a, "m", 1));
    MmapqIndexedBatchPoller *p = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_poller(q, &p));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_batch_poller_move_to_index(p, 2));

    mmapq_test_hook_header_wrap_inject(WRAP_MAP_TIMED_OUT);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_poller_move_to_index(p, 5));
    mmapq_test_hook_header_wrap_inject(0);
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_batch_poller_current_batch_index(p));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_batch_poller_move_to_index(p, 5));
    TEST_ASSERT_EQUAL_INT64(5, mmapq_indexed_batch_poller_current_batch_index(p));

    mmapq_indexed_batch_poller_close(p);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(q);
}

#endif /* MMAPQ_TEST_HOOKS */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_batch_poller_null_move_to_index_returns_0);
    RUN_TEST(test_batch_poller_null_move_to_end_returns_0);
    RUN_TEST(test_batch_poller_null_move_to_last_returns_0);
    RUN_TEST(test_batch_poller_null_move_to_start_returns_0);
    RUN_TEST(test_batch_poller_null_exists_returns_0);
    RUN_TEST(test_batch_poller_null_start_index_returns_0);
    RUN_TEST(test_batch_poller_null_current_returns_0);
#ifdef MMAPQ_TEST_HOOKS
    RUN_TEST(test_single_move_to_index_timeout_returns_0);
    RUN_TEST(test_batch_move_to_index_timeout_returns_0);
#endif
    return UNITY_END();
}
