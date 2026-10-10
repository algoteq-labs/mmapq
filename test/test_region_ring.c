#include "unity.h"
#include "mmap_platform.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static const char    *DIR       = "/tmp";
static const char    *QUEUE     = "mmap_test_rring";
static const int32_t  REGION_SZ = 4096 * 16;
static const int      RING_SZ   = 4;
static const int64_t  MAX_FILE  = (int64_t)4096 * 16 * 64;
static const uint64_t TIMEOUT   = 5000000000ULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

void setUp(void)    { cleanup(); }
void tearDown(void) { cleanup(); }

/* Child body: close the runtime before the appender. Pre-fix (C-01),
 * async_ring_close registers the deregistration executable with the
 * already-closed runtime, never checks the 0 return, and spins forever
 * waiting for it to run. Exit codes: 0 = full close sequence completed;
 * 2 = a setup step failed. */
static void child_close_appender_after_runtime_close(void)
{
    MmapqRuntime *rt = NULL;
    if (mmapq_runtime_region_mapper(&rt) != 0 || rt == NULL) _exit(2);

    MmapqIndexedQueue *q = NULL;
    if (mmapq_indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                   MAX_FILE, 0, TIMEOUT, 0, rt, rt, &q) != 0)
        _exit(2);

    MmapqIndexedAppender *a = NULL;
    if (mmapq_indexed_queue_create_appender(q, &a) != 0 || a == NULL) _exit(2);

    mmapq_runtime_close(rt);         /* joins the runtime thread */
    mmapq_indexed_appender_close(a); /* pre-fix: never returns */
    mmapq_indexed_queue_close(q);
    mmapq_runtime_destroy(rt);
    _exit(0);
}

void test_region_ring_close_after_runtime_close_returns(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_close_appender_after_runtime_close();

    /* Parent watchdog: the pre-fix behavior is a HANG, so an assertion in
     * the child would never fire — kill the child after 10 s instead. */
    int status = 0;
    int reaped = 0;
    for (int i = 0; i < 100; i++) {
        if (waitpid(pid, &status, WNOHANG) == pid) { reaped = 1; break; }
        usleep(100000);
    }
    if (!reaped) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        TEST_FAIL_MESSAGE("child hung: appender close after runtime close did not return in 10 s");
    }
    TEST_ASSERT(WIFEXITED(status));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(status));
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_region_ring_close_after_runtime_close_returns);
    return UNITY_END();
}
