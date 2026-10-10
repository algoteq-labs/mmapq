#include "unity.h"
#include "runtime.h"

#include <stdint.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <signal.h>
#include <sched.h>
#include <sys/types.h>
#include <sys/wait.h>

static MmapqRuntime *rt = NULL;

void setUp(void)
{
    MmapqIdleStrategy *idle = NULL;
    int rc_idle = mmapq_idle_strategy_busy_spin_create(&idle);
    TEST_ASSERT_EQUAL_INT(0, rc_idle);
    TEST_ASSERT_NOT_NULL(idle);
    int rc_rt = mmapq_runtime_create("test-rt",
                               idle,
                               MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS,
                               NULL, NULL, &rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
}

void tearDown(void)
{
    if (rt) {
        mmapq_runtime_destroy(rt);
        rt = NULL;
    }
}

/* ── MmapqExecutable helpers ──────────────────────────────────────────────────── */

typedef struct {
    atomic_int execute_count;
    atomic_int close_count;
    int        work_to_return;
    MmapqExecutable exec;
} CountingExec;

static int counting_execute(MmapqExecutable *self)
{
    CountingExec *ce = (CountingExec *)self->ctx;
    atomic_fetch_add(&ce->execute_count, 1);
    return ce->work_to_return;
}

static void counting_close(MmapqExecutable *self)
{
    CountingExec *ce = (CountingExec *)self->ctx;
    atomic_fetch_add(&ce->close_count, 1);
}

static CountingExec *counting_exec_create(int work_to_return)
{
    CountingExec *ce = calloc(1, sizeof(CountingExec));
    atomic_store(&ce->execute_count, 0);
    atomic_store(&ce->close_count,   0);
    ce->work_to_return  = work_to_return;
    ce->exec.execute    = counting_execute;
    ce->exec.close      = counting_close;
    ce->exec.ctx        = ce;
    return ce;
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_runtime_is_not_closed_on_create(void)
{
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_is_closed(rt));
}

void test_runtime_register_returns_true(void)
{
    CountingExec *ce = counting_exec_create(1);
    int result = mmapq_runtime_register(rt, &ce->exec);
    TEST_ASSERT_EQUAL_INT(1, result);
    /* Don't free ce here — runtime thread still holds reference.
     * tearDown() calls mmapq_runtime_destroy() which calls close() on all
     * executables before we free. Safe to free after destroy. */
    mmapq_runtime_destroy(rt);
    rt = NULL;
    free(ce);
}

void test_runtime_executes_registered_executable(void)
{
    CountingExec *ce = counting_exec_create(1);
    mmapq_runtime_register(rt, &ce->exec);

    /* Wait for at least 10 executions */
    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 10 && waited_ms < 1000) {
        usleep(1000);
        waited_ms++;
    }

    TEST_ASSERT_GREATER_OR_EQUAL_INT(10, atomic_load(&ce->execute_count));

    /* Must destroy runtime before freeing ce — runtime thread holds a pointer
     * to ce->exec and will use-after-free if we free ce first. */
    mmapq_runtime_destroy(rt);
    rt = NULL;
    free(ce);
}

void test_runtime_close_stops_execution(void)
{
    CountingExec *ce = counting_exec_create(1);
    mmapq_runtime_register(rt, &ce->exec);

    usleep(10000);  /* let it run briefly */

    mmapq_runtime_close(rt);

    /* After close() returns, the thread has fully stopped — count is frozen */
    int count_after_close = atomic_load(&ce->execute_count);
    usleep(20000);
    int count_final = atomic_load(&ce->execute_count);

    /* Count must not grow after close() returns — thread is done */
    TEST_ASSERT_EQUAL_INT(count_after_close, count_final);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_is_closed(rt));
    TEST_ASSERT_GREATER_OR_EQUAL(1, atomic_load(&ce->close_count));

    free(ce);
    mmapq_runtime_destroy(rt);
    rt = NULL;
}

void test_runtime_register_after_close_returns_false(void)
{
    mmapq_runtime_close(rt);
    CountingExec *ce = counting_exec_create(1);
    int result = mmapq_runtime_register(rt, &ce->exec);
    TEST_ASSERT_EQUAL_INT(0, result);
    free(ce);

    mmapq_runtime_destroy(rt);
    rt = NULL;
}

void test_runtime_multiple_executables(void)
{
    CountingExec *ce1 = counting_exec_create(1);
    CountingExec *ce2 = counting_exec_create(0);
    CountingExec *ce3 = counting_exec_create(1);

    mmapq_runtime_register(rt, &ce1->exec);
    mmapq_runtime_register(rt, &ce2->exec);
    mmapq_runtime_register(rt, &ce3->exec);

    int waited_ms = 0;
    while ((atomic_load(&ce1->execute_count) < 5 ||
            atomic_load(&ce3->execute_count) < 5) && waited_ms < 1000) {
        usleep(1000);
        waited_ms++;
    }

    TEST_ASSERT_GREATER_OR_EQUAL_INT(5, atomic_load(&ce1->execute_count));
    TEST_ASSERT_GREATER_OR_EQUAL_INT(5, atomic_load(&ce2->execute_count));
    TEST_ASSERT_GREATER_OR_EQUAL_INT(5, atomic_load(&ce3->execute_count));

    /* Destroy runtime before freeing — runtime thread still holds pointers */
    mmapq_runtime_destroy(rt);
    rt = NULL;
    free(ce1); free(ce2); free(ce3);
}

void test_runtime_is_closed_after_close(void)
{
    mmapq_runtime_close(rt);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_is_closed(rt));
    mmapq_runtime_destroy(rt);
    rt = NULL;
}

void test_idle_strategy_busy_spin(void)
{
    MmapqIdleStrategy *s = NULL;
    int rc = mmapq_idle_strategy_busy_spin_create(&s);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(s);
    /* Should not crash with any work count */
    s->idle(s, 0);
    s->idle(s, 1);
    s->idle(s, 100);
    mmapq_idle_strategy_destroy(s);
}

void test_idle_strategy_yield(void)
{
    MmapqIdleStrategy *s = NULL;
    int rc = mmapq_idle_strategy_yield_create(&s);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(s);
    s->idle(s, 0);
    s->idle(s, 1);
    mmapq_idle_strategy_destroy(s);
}

void test_idle_strategy_backoff(void)
{
    MmapqIdleStrategy *s = NULL;
    int rc = mmapq_idle_strategy_backoff_create(10, 10, 1000000ULL, &s);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(s);
    for (int i = 0; i < 30; i++) {
        s->idle(s, 0);  /* progress through spin → yield → sleep phases */
    }
    s->idle(s, 1);  /* reset */
    mmapq_idle_strategy_destroy(s);
}

void test_deregister_executable(void)
{
    CountingExec *ce = counting_exec_create(1);
    mmapq_runtime_register(rt, &ce->exec);

    usleep(5000);  /* let it execute */

    /* Use mmapq_runtime_unregister_and_wait — blocks until runtime thread confirms
     * it has finished with ce->exec. No racy sleep required. */
    mmapq_runtime_unregister_and_wait(rt, &ce->exec);

    int count_after_dereg = atomic_load(&ce->execute_count);

    /* MmapqRuntime thread has confirmed it is done — count is frozen */
    usleep(20000);
    int count_later = atomic_load(&ce->execute_count);

    TEST_ASSERT_EQUAL_INT(count_after_dereg, count_later);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, count_after_dereg);
    free(ce);  /* safe: mmapq_runtime_unregister_and_wait guarantees no further access */
}

void test_unregister_and_wait_safe_free(void)
{
    /* Verify that after mmapq_runtime_unregister_and_wait returns, the executable
     * context can be freed immediately without data races. */
    CountingExec *ce = counting_exec_create(1);
    mmapq_runtime_register(rt, &ce->exec);

    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 5 && waited_ms < 1000) {
        usleep(1000);
        waited_ms++;
    }

    mmapq_runtime_unregister_and_wait(rt, &ce->exec);

    /* Immediate free — no sleep, no grace period needed */
    int final_count = atomic_load(&ce->execute_count);
    free(ce);

    /* Verify runtime still runs (can register a new executable) */
    CountingExec *ce2 = counting_exec_create(1);
    mmapq_runtime_register(rt, &ce2->exec);
    usleep(5000);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, atomic_load(&ce2->execute_count));
    TEST_ASSERT_GREATER_OR_EQUAL_INT(5, final_count);
    mmapq_runtime_unregister_and_wait(rt, &ce2->exec);
    free(ce2);
}

void test_runtime_double_acquire(void)
{
    /* Verify that mmapq_runtime_get_instance/clone/destroy cycle doesn't
     * orphan the registry entry. Regression for P0.4 refcount fix. */
    MmapqRuntime *rt1 = NULL;
    int rc = mmapq_runtime_region_mapper(&rt1);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(rt1);
    TEST_ASSERT_FALSE(mmapq_runtime_is_closed(rt1));

    MmapqRuntime *rt2 = NULL;
    rc = mmapq_runtime_clone(rt1, &rt2);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(rt2);

    mmapq_runtime_destroy(rt1);
    mmapq_runtime_destroy(rt2);

    /* Re-acquire — should return a valid entry (registry still holds ref) */
    MmapqRuntime *rt3 = NULL;
    rc = mmapq_runtime_region_mapper(&rt3);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(rt3);
    TEST_ASSERT_FALSE(mmapq_runtime_is_closed(rt3));

    mmapq_runtime_destroy(rt3);
}

/* ── C-02: close with a wedged executable ───────────────────────────────────── */

#ifndef __SANITIZE_THREAD__
/* Excluded under TSan: the region-mapper singleton (acquired by
 * test_runtime_double_acquire) keeps its thread alive for the process
 * lifetime, so the forks below are multi-threaded and TSan kills any
 * child that starts a thread afterwards (die_after_fork). The fix's close
 * path is still TSan-exercised by every other test's runtime teardown. */

typedef struct {
    atomic_int        entered;  /* set once the runtime thread is inside execute */
    atomic_int        release;  /* 0 = keep spinning (slow), or spin forever (wedged) */
    atomic_int        closed;   /* set by the drain's close() on the resumed thread */
    MmapqExecutable   exec;
} WedgedExec;

static int wedged_execute(MmapqExecutable *self)
{
    WedgedExec *we = (WedgedExec *)self->ctx;
    atomic_store_explicit(&we->entered, 1, memory_order_release);
    for (;;) sched_yield();   /* never returns, never observes closed */
    /* unreachable; keeps -Wreturn-type quiet */
    return 0;
}

static int slow_execute(MmapqExecutable *self)
{
    WedgedExec *we = (WedgedExec *)self->ctx;
    atomic_store_explicit(&we->entered, 1, memory_order_release);
    while (!atomic_load_explicit(&we->release, memory_order_acquire))
        sched_yield();
    return 1;
}

static void wedged_close(MmapqExecutable *self)
{
    WedgedExec *we = (WedgedExec *)self->ctx;
    atomic_store_explicit(&we->closed, 1, memory_order_release);
}

/* Register `exec` on a fresh child-local runtime with a 200 ms close
 * timeout and wait until the runtime thread is observably inside
 * execute() (bounded — without the handshake a slow machine could let
 * close() win the race and the test would pass vacuously). */
static MmapqRuntime *child_runtime_with_wedged_exec(const char *name,
                                                    int (*fn)(MmapqExecutable *),
                                                    WedgedExec *we)
{
    MmapqIdleStrategy *idle = NULL;
    if (mmapq_idle_strategy_busy_spin_create(&idle) != 0) _exit(2);

    MmapqRuntime *crt = NULL;
    if (mmapq_runtime_create(name, idle, 200000000ULL, NULL, NULL, &crt) != 0)
        _exit(2);

    atomic_store(&we->entered, 0);
    atomic_store(&we->release, 0);
    atomic_store(&we->closed,  0);
    we->exec.execute = fn;
    we->exec.close   = wedged_close;
    we->exec.ctx     = we;
    if (mmapq_runtime_register(crt, &we->exec) != 1) _exit(2);

    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&we->entered, memory_order_acquire)) return crt;
        usleep(1000);
    }
    _exit(2);   /* thread never entered execute(): setup environment broken */
}

/* Child body: runtimes are per-process after fork, so the child builds its
 * own runtime, wedges its thread in an execute() that never returns, then
 * closes. Pre-fix (C-02) the close hangs in pthread_join. Exit codes:
 * 0 = close returned; 2 = a setup step failed. */
static void child_close_with_wedged_executable(void)
{
    static WedgedExec we;
    MmapqRuntime *crt = child_runtime_with_wedged_exec("wedged-rt", wedged_execute, &we);

    mmapq_runtime_close(crt);   /* pre-fix: never returns */
    _exit(0);                   /* the detached wedged thread dies with the process */
}

/* Child body: close detaches the thread (200 ms budget vs a slow
 * executable), destroy() runs, then the thread is released to resume.
 * Without the thread_detached destroy guard the resumed thread touches
 * the freed runtime (ASan: heap-use-after-free); with it destroy leaks
 * the runtime intact. Exit codes: 0 = survived; 2 = setup failed. */
static void child_destroy_after_detach(void)
{
    static WedgedExec we;
    MmapqRuntime *crt = child_runtime_with_wedged_exec("slow-rt", slow_execute, &we);

    mmapq_runtime_close(crt);    /* budget exhausts; thread detached */
    mmapq_runtime_destroy(crt);  /* guarded: leaks crt; unguarded: frees it */

    atomic_store_explicit(&we.release, 1, memory_order_release);
    /* Wait for the resumed thread to reach the drain's close() — every
     * use of rt the UAF pin targets happens before that point, and waiting
     * (instead of a bare sleep + _exit) lets a sanitizer report complete
     * before the process dies. */
    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&we.closed, memory_order_acquire)) break;
        usleep(1000);
    }
    usleep(100000);              /* drain tail (thread_done/close_mutex) */
    _exit(0);
}

/* Parent watchdog: the pre-fix behavior is a HANG or a child crash, so an
 * assertion in the child would never fire — kill the child after 10 s
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
        TEST_FAIL_MESSAGE("child hung: close with a wedged/slow executable did not return in 10 s");
    }
    TEST_ASSERT(WIFEXITED(status));
    TEST_ASSERT_EQUAL_INT(0, WEXITSTATUS(status));
}

void test_runtime_close_with_wedged_executable_returns(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_close_with_wedged_executable();
    assert_child_exits_zero(pid);
}

void test_runtime_destroy_after_detach_no_uaf(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_destroy_after_detach();
    assert_child_exits_zero(pid);
}
#endif /* __SANITIZE_THREAD__ */

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_runtime_is_not_closed_on_create);
    RUN_TEST(test_runtime_register_returns_true);
    RUN_TEST(test_runtime_executes_registered_executable);
    RUN_TEST(test_runtime_close_stops_execution);
    RUN_TEST(test_runtime_register_after_close_returns_false);
    RUN_TEST(test_runtime_multiple_executables);
    RUN_TEST(test_runtime_is_closed_after_close);
    RUN_TEST(test_idle_strategy_busy_spin);
    RUN_TEST(test_idle_strategy_yield);
    RUN_TEST(test_idle_strategy_backoff);
    RUN_TEST(test_deregister_executable);
    RUN_TEST(test_unregister_and_wait_safe_free);
    RUN_TEST(test_runtime_double_acquire);
#ifndef __SANITIZE_THREAD__
    RUN_TEST(test_runtime_close_with_wedged_executable_returns);
    RUN_TEST(test_runtime_destroy_after_detach_no_uaf);
#endif
    return UNITY_END();
}
