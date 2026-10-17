#include "unity.h"
#include "mmap_platform.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdatomic.h>
#include <sched.h>
#include <pthread.h>
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

/* Parent watchdog: the pre-fix behaviors are a HANG or a child crash, so
 * an assertion in the child would never fire — kill the child after 10 s
 * instead and require a clean exit 0. */
static void assert_child_exits_zero(pid_t pid)
{
    int status = 0;
    int reaped = 0;
    for (int i = 0; i < 100; i++) {
        if (waitpid(pid, &status, WNOHANG) == pid) { reaped = 1; break; }
        usleep(100000);
    }
    if (!reaped) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        TEST_FAIL_MESSAGE("child hung or crashed: close sequence did not finish in 10 s");
    }
    TEST_ASSERT(WIFEXITED(status));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(status));
}

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
    assert_child_exits_zero(pid);
}


/* Runs under TSan too, unlike the test_runtime.c forks: this binary's
 * parent process never creates threads, so the fork is single-threaded
 * and TSan's die_after_fork does not fire (verified by execution). */

typedef struct {
    atomic_int      entered;  /* set once the runtime thread is inside execute */
    atomic_int      release;  /* 0 = keep spinning */
    atomic_int      closed;   /* set by the drain's close() on the resumed thread */
    MmapqExecutable exec;
} SlowExec;

static int slow_mapper_execute(MmapqExecutable *self)
{
    SlowExec *se = (SlowExec *)self->ctx;
    atomic_store_explicit(&se->entered, 1, memory_order_release);
    while (!atomic_load_explicit(&se->release, memory_order_acquire))
        sched_yield();
    return 1;
}

static int wedged_mapper_execute(MmapqExecutable *self)
{
    SlowExec *se = (SlowExec *)self->ctx;
    atomic_store_explicit(&se->entered, 1, memory_order_release);
    for (;;) sched_yield();   /* never returns, never observes closed */
    /* unreachable; keeps -Wreturn-type quiet */
    return 0;
}

static void slow_mapper_close(MmapqExecutable *self)
{
    SlowExec *se = (SlowExec *)self->ctx;
    atomic_store_explicit(&se->closed, 1, memory_order_release);
}

/* Child body: a runtime with a 200 ms close timeout drives the queue's
 * async rings; a slow executable wedges the runtime thread so
 * mmapq_runtime_close detaches instead of joining (C-02). Closing the
 * appender afterwards must not free the mapping state the live thread
 * can still reach: pre-fix async_ring_close's register-failed branch
 * frees mapping_impl and ring destroy frees the regions and file mapper,
 * and the resumed thread walks straight into them (ASan:
 * heap-use-after-free in async_mapping_close). Exit codes: 0 = survived;
 * 2 = a setup step failed. */
static void child_ring_close_after_detached_runtime_close(void)
{
    MmapqIdleStrategy *idle = NULL;
    if (mmapq_idle_strategy_busy_spin_create(&idle) != 0) _exit(2);

    MmapqRuntime *rt = NULL;
    if (mmapq_runtime_create("slow-mapper-rt", idle, 200000000ULL,
                             NULL, NULL, &rt) != 0)
        _exit(2);

    MmapqIndexedQueue *q = NULL;
    if (mmapq_indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                   MAX_FILE, 0, TIMEOUT, 0, rt, rt, &q) != 0)
        _exit(2);

    MmapqIndexedAppender *a = NULL;
    if (mmapq_indexed_queue_create_appender(q, &a) != 0 || a == NULL) _exit(2);

    static SlowExec slow;
    atomic_store(&slow.entered, 0);
    atomic_store(&slow.release, 0);
    atomic_store(&slow.closed,  0);
    slow.exec.execute = slow_mapper_execute;
    slow.exec.close   = slow_mapper_close;
    slow.exec.ctx     = &slow;
    if (mmapq_runtime_register(rt, &slow.exec) != 1) _exit(2);

    /* Wait until the runtime thread is observably inside execute() —
     * without the handshake close() could win the race and the test
     * would pass without exercising the detach. */
    int entered = 0;
    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&slow.entered, memory_order_acquire)) { entered = 1; break; }
        usleep(1000);
    }
    if (!entered) _exit(2);

    mmapq_runtime_close(rt);          /* join budget exhausts → detach */
    mmapq_indexed_appender_close(a);  /* ring close + ring destroy */
    mmapq_indexed_queue_close(q);

    atomic_store_explicit(&slow.release, 1, memory_order_release);
    /* Wait for the resumed thread to reach the drain's close() — the
     * freed-state reads this pin targets (mapping_impl in
     * async_mapping_close, regions, file mapper) all happen before it —
     * and waiting (instead of a bare sleep + _exit) lets a sanitizer
     * report complete before the process dies. */
    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&slow.closed, memory_order_acquire)) break;
        usleep(1000);
    }
    usleep(100000);                   /* drain tail */
    _exit(0);
}

void test_async_ring_close_after_detached_runtime_close_no_uaf(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_ring_close_after_detached_runtime_close();
    assert_child_exits_zero(pid);
}

/* Child body (CAND-24 P1): the ring is closed while a never-returning
 * executable wedges the runtime thread (probe H1 — ring closed BEFORE the
 * wedged runtime, the opposite ordering of C-02). Pre-fix,
 * async_ring_close's deregister wait has no deadline and the appender
 * close never returns. Post-fix the wait times out at the ring's
 * timeout_ns (500 ms) and takes the B2 mapper_detached leak path.
 * Exit codes: 0 = full close sequence completed; 2 = a setup step
 * failed. */
static void child_ring_close_wedged_runtime(void)
{
    MmapqIdleStrategy *idle = NULL;
    if (mmapq_idle_strategy_busy_spin_create(&idle) != 0) _exit(2);

    MmapqRuntime *rt = NULL;
    if (mmapq_runtime_create("c24-wedged-rt", idle, 500000000ULL,
                             NULL, NULL, &rt) != 0)
        _exit(2);

    MmapqIndexedQueue *q = NULL;
    if (mmapq_indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                   MAX_FILE, 0, 500000000ULL, 0, rt, rt, &q) != 0)
        _exit(2);

    MmapqIndexedAppender *a = NULL;
    if (mmapq_indexed_queue_create_appender(q, &a) != 0 || a == NULL) _exit(2);

    static SlowExec wedged;
    atomic_store(&wedged.entered, 0);
    atomic_store(&wedged.release, 0);
    atomic_store(&wedged.closed,  0);
    wedged.exec.execute = wedged_mapper_execute;
    wedged.exec.close   = slow_mapper_close;
    wedged.exec.ctx     = &wedged;
    if (mmapq_runtime_register(rt, &wedged.exec) != 1) _exit(2);

    /* Same entered-handshake as the C-02 child: without it the ring close
     * could win the race against the wedge and pass vacuously. */
    int entered = 0;
    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&wedged.entered, memory_order_acquire)) { entered = 1; break; }
        usleep(1000);
    }
    if (!entered) _exit(2);

    mmapq_indexed_appender_close(a);  /* pre-fix: never returns */
    mmapq_indexed_queue_close(q);
    mmapq_runtime_close(rt);          /* budget exhausts → detach */
    mmapq_runtime_destroy(rt);        /* leaks the detached runtime */
    _exit(0);                         /* the wedged thread dies with the process */
}

void test_async_ring_close_wedged_runtime_returns_after_deadline(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_ring_close_wedged_runtime();
    assert_child_exits_zero(pid);
}

/* Releaser for the H2 control: lift the wedge 500 ms into the close. */
static void *release_mapper_after_500ms(void *arg)
{
    usleep(500000);
    atomic_store_explicit(&((SlowExec *)arg)->release, 1, memory_order_release);
    return NULL;
}

/* Child body (CAND-24 P2, H2 control — passes pre AND post-fix): same
 * shape as P1 but the wedge lifts 500 ms into the close, well inside the
 * ring's 2 s timeout. The deregister wait must complete NORMALLY (no
 * deadline, no leak path): the ring's mapping state is freed by the
 * deregistration executable, the runtime close joins. Exit codes:
 * 0 = full close sequence completed; 2 = a setup step failed;
 * 3 = appender close returned in < 400 ms (deadline fired on a healthy
 * close — the leak path taken on the normal path). */
static void child_ring_close_slow_runtime(void)
{
    MmapqIdleStrategy *idle = NULL;
    if (mmapq_idle_strategy_busy_spin_create(&idle) != 0) _exit(2);

    MmapqRuntime *rt = NULL;
    if (mmapq_runtime_create("c24-slow-rt", idle, 2000000000ULL,
                             NULL, NULL, &rt) != 0)
        _exit(2);

    MmapqIndexedQueue *q = NULL;
    if (mmapq_indexed_queue_create(DIR, QUEUE, REGION_SZ, RING_SZ, 1,
                                   MAX_FILE, 0, 2000000000ULL, 0, rt, rt, &q) != 0)
        _exit(2);

    MmapqIndexedAppender *a = NULL;
    if (mmapq_indexed_queue_create_appender(q, &a) != 0 || a == NULL) _exit(2);

    static SlowExec slow;
    atomic_store(&slow.entered, 0);
    atomic_store(&slow.release, 0);
    atomic_store(&slow.closed,  0);
    slow.exec.execute = slow_mapper_execute;
    slow.exec.close   = slow_mapper_close;
    slow.exec.ctx     = &slow;
    if (mmapq_runtime_register(rt, &slow.exec) != 1) _exit(2);

    int entered = 0;
    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&slow.entered, memory_order_acquire)) { entered = 1; break; }
        usleep(1000);
    }
    if (!entered) _exit(2);

    pthread_t rel;
    if (pthread_create(&rel, NULL, release_mapper_after_500ms, &slow) != 0) _exit(2);
    pthread_detach(rel);

    /* Rule-11 review: assert the close actually WAITED for the slow task
     * (the normal path), not merely that it returned — a deadline that
     * fires on every close (reviewer mutant MX-b) returns at once and
     * leaks; without this assertion only LSan caught that. */
    uint64_t t0 = mmapq_monotonic_nanos();
    mmapq_indexed_appender_close(a);  /* waits out the wedge (~500 ms) */
    uint64_t waited_ms = (mmapq_monotonic_nanos() - t0) / 1000000ULL;
    if (waited_ms < 400) _exit(3);    /* deadline fired early — leak path on a healthy close */

    mmapq_indexed_queue_close(q);
    mmapq_runtime_close(rt);          /* joins */
    mmapq_runtime_destroy(rt);
    _exit(0);
}

void test_async_ring_close_slow_runtime_completes_without_deadline(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_ring_close_slow_runtime();
    assert_child_exits_zero(pid);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_region_ring_close_after_runtime_close_returns);
    RUN_TEST(test_async_ring_close_after_detached_runtime_close_no_uaf);
    RUN_TEST(test_async_ring_close_wedged_runtime_returns_after_deadline);
    RUN_TEST(test_async_ring_close_slow_runtime_completes_without_deadline);
    return UNITY_END();
}
