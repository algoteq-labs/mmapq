#include "unity.h"
#include "runtime.h"
#include "mmap_platform.h"

#include <stdint.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>
#include <sched.h>
#include <pthread.h>
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

/* CAND-24: close reports its outcome. Healthy close joins and returns
 * exactly 0; a sequential repeat close reports the same; NULL is EINVAL.
 * (The ETIMEDOUT path is asserted in the C-02 fork children above —
 * exact values, not loose bounds.) */
void test_runtime_close_status_values(void)
{
    MmapqIdleStrategy *idle = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_busy_spin_create(&idle));

    MmapqRuntime *crt = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_runtime_create("status-rt", idle, 1000000000ULL, NULL, NULL, &crt));
    TEST_ASSERT_NOT_NULL(crt);

    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_close(crt));
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_close(crt));   /* sequential repeat */
    mmapq_runtime_destroy(crt);

    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_runtime_close(NULL));
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

/* ── COV-01: direct tests for previously-untested public functions ───────── */

/* An executable that records which thread executed it (proves runtimes
 * share a thread without relying on pointer equality, which the header
 * does not promise). */
typedef struct {
    pthread_t       ran_on;
    atomic_int      execute_count;
    MmapqExecutable exec;
} ThreadProbe;

static int thread_probe_execute(MmapqExecutable *self)
{
    ThreadProbe *tp = (ThreadProbe *)self->ctx;
    pthread_t me = pthread_self();
    /* ran_on is written exactly once, and BEFORE the count's release
     * RMW, so a main-thread read that acquire-observes execute_count >= 1
     * is happens-before-ordered with the write — placing the write after
     * the RMW (or writing on every execution) leaves a window where the
     * observer reads ran_on unordered with the write (TSan). */
    if (atomic_load_explicit(&tp->execute_count, memory_order_relaxed) == 0)
        tp->ran_on = me;
    atomic_fetch_add_explicit(&tp->execute_count, 1, memory_order_acq_rel);
    return 1;
}

static void thread_probe_close(MmapqExecutable *self) { (void)self; }

static void thread_probe_init(ThreadProbe *tp)
{
    atomic_store(&tp->execute_count, 0);
    tp->exec.execute = thread_probe_execute;
    tp->exec.close   = thread_probe_close;
    tp->exec.ctx     = tp;
}

/* Bounded wait until the probe has executed at least `want` times. */
static void wait_probe_runs(ThreadProbe *tp, int want)
{
    int waited_ms = 0;
    while (atomic_load(&tp->execute_count) < want && waited_ms < 2000) {
        usleep(1000);
        waited_ms++;
    }
    TEST_ASSERT_GREATER_OR_EQUAL_INT(want, atomic_load(&tp->execute_count));
}

/* Bounded wait until a counting executable's count stops increasing (the
 * busy-spin setUp runtime re-executes a registered target every few
 * microseconds, so 50 ms of no change means it is unregistered). */
static void wait_counting_frozen(CountingExec *ce)
{
    int frozen = 0;
    for (int i = 0; i < 40 && !frozen; i++) {
        int c1 = atomic_load(&ce->execute_count);
        usleep(50000);
        int c2 = atomic_load(&ce->execute_count);
        if (c1 == c2) frozen = 1;
    }
    TEST_ASSERT_EQUAL_INT(1, frozen);
}

/* First CPU in the process's current affinity mask. */
static int first_allowed_cpu(void)
{
    cpu_set_t allowed;
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) return -1;
    for (int i = 0; i < CPU_SETSIZE; i++) {
        if (CPU_ISSET(i, &allowed)) return i;
    }
    return -1;
}

/* A CPU index in range for cpu_set_t but not schedulable for this
 * process: at or beyond the number of configured processors (schedulable
 * cpu indices never reach it) and outside the process's allowed mask;
 * CPU_SETSIZE-1 as the fallback. Deterministic on any host, unlike a
 * literal small index that a wide cgroup might legitimately allow. */
static int first_disallowed_cpu(void)
{
    long conf = sysconf(_SC_NPROCESSORS_CONF);
    cpu_set_t allowed;
    if (conf > 0 && conf < CPU_SETSIZE) {
        int start = (int)conf;
        if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0) {
            for (int i = start; i < CPU_SETSIZE; i++) {
                if (!CPU_ISSET(i, &allowed)) return i;
            }
        }
        return start;
    }
    return CPU_SETSIZE - 1;
}

/* runtime.h:324-343 — get_instance: "Get or create a singleton MmapqRuntime
 * by ID. If a MmapqRuntime with runtime_id already exists, returns it...
 * If an existing singleton was found, the caller-supplied idle strategy is
 * destroyed (never used)." */
void test_runtime_get_instance_returns_one_runtime(void)
{
    /* runtime_id 15: none of the well-known IDs, unused by other suites. */
    MmapqIdleStrategy *idle1 = NULL, *idle2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_sleep_create(1000000ULL, 4000000ULL, &idle1));
    MmapqRuntime *h1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_get_instance(15, "cov01-gi", idle1,
        MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, &h1));
    TEST_ASSERT_NOT_NULL(h1);

    /* Second call with a heap-allocated idle: get_instance must destroy it
     * (never used) — a leak here fails the ASan/LSan loop. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_sleep_create(1000000ULL, 4000000ULL, &idle2));
    MmapqRuntime *h2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_get_instance(15, "cov01-gi2", idle2,
        MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, &h2));
    TEST_ASSERT_NOT_NULL(h2);

    /* One runtime, shown by behaviour: executables registered via the two
     * handles execute on the SAME thread. */
    ThreadProbe e1, e2;
    thread_probe_init(&e1);
    thread_probe_init(&e2);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(h1, &e1.exec));
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(h2, &e2.exec));
    wait_probe_runs(&e1, 1);
    wait_probe_runs(&e2, 1);
    TEST_ASSERT_TRUE(pthread_equal(e1.ran_on, e2.ran_on));

    /* Destroying both caller handles does NOT make the next call create a
     * fresh runtime: the registry keeps its own reference (runtime.c,
     * mmapq_runtime_get_instance), so the same singleton is returned —
     * proven by passing NULL as the idle (a fresh create would fail with
     * MMAPQ_EINVAL) and by e1 still executing afterwards. */
    mmapq_runtime_destroy(h1);
    mmapq_runtime_destroy(h2);
    MmapqRuntime *h3 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_get_instance(15, "cov01-gi3", NULL,
        MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, &h3));
    TEST_ASSERT_NOT_NULL(h3);
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_is_closed(h3));
    int c1 = atomic_load(&e1.execute_count);
    usleep(100000);
    TEST_ASSERT_GREATER_THAN_INT(c1, atomic_load(&e1.execute_count));

    mmapq_runtime_unregister_and_wait(h3, &e1.exec);
    mmapq_runtime_unregister_and_wait(h3, &e2.exec);
    mmapq_runtime_destroy(h3);
}

/* ASAN-01 (C): closing a singleton runtime must release the registry's
 * permanent reference. Pre-fix, registry_on_close removed the registry
 * entry when the runtime closed but never released the reference the
 * registry holds, so after close + every caller's destroy the runtime
 * struct and its thread state were unreachable — the 240-byte leak (plus
 * indirects) seen under --asan in the c59 pins and the indexed-queue
 * suites. Once closed and out of the registry the runtime can never be
 * handed out again, so the registry's reference has no purpose left. A
 * singleton that is NOT closed keeps its registry reference — pinned by
 * test_runtime_get_instance_returns_one_runtime above. Under --asan this
 * test leaks at the base and is clean after the fix. */
void test_runtime_get_instance_close_destroy_frees_singleton(void)
{
    /* runtime_id 14: none of the well-known IDs, unused by other tests */
    MmapqIdleStrategy *idle = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_idle_strategy_sleep_create(1000000ULL, 4000000ULL, &idle));
    MmapqRuntime *h = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_get_instance(14, "asan01-c", idle,
        MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, &h));
    TEST_ASSERT_NOT_NULL(h);

    /* exact outcome for a healthy empty runtime: the thread joins */
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_close(h));
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_is_closed(h));

    /* releases the caller's reference; with the registry's released in
     * on_close at close time this is the last one — the runtime is freed
     * here, not leaked. */
    mmapq_runtime_destroy(h);
}

/* runtime.h:345-359 — metrics_processor: "Get the singleton metrics
 * processor MmapqRuntime. Creates it with backoff idle strategy if it
 * does not exist." */
void test_runtime_metrics_processor_returns_same_singleton(void)
{
    MmapqRuntime *m1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_metrics_processor(&m1));
    TEST_ASSERT_NOT_NULL(m1);
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_is_closed(m1));

    MmapqRuntime *m2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_metrics_processor(&m2));
    TEST_ASSERT_NOT_NULL(m2);
    /* a clone hands back the same pointer: the same singleton */
    TEST_ASSERT_EQUAL_PTR(m1, m2);

    /* both caller references released cleanly (the registry keeps its own) */
    mmapq_runtime_destroy(m1);
    mmapq_runtime_destroy(m2);
}

/* runtime.h:216-223 — set_affinity: "Pin the background thread of this
 * MmapqRuntime to a specific CPU core... Returns 0 on success, or an error
 * code." The pin is applied on the runtime thread by a one-shot
 * executable; the probe below only runs after it (list order plus the
 * one-shot's unregister-and-restart). */
void test_runtime_set_affinity_pins_runtime_thread(void)
{
    int cpu = first_allowed_cpu();
    TEST_ASSERT_GREATER_OR_EQUAL_INT(0, cpu);

    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_set_affinity(rt, cpu));

    ThreadProbe probe;
    thread_probe_init(&probe);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &probe.exec));
    wait_probe_runs(&probe, 1);

    cpu_set_t actual;
    TEST_ASSERT_EQUAL_INT(0, pthread_getaffinity_np(probe.ran_on, sizeof(actual), &actual));
    TEST_ASSERT_EQUAL_INT(1, CPU_COUNT(&actual));
    TEST_ASSERT_TRUE(CPU_ISSET(cpu, &actual));

    mmapq_runtime_unregister_and_wait(rt, &probe.exec);
}

void test_runtime_set_affinity_out_of_range_cpu_runtime_survives(void)
{
    /* cpu_core outside [0, CPU_SETSIZE) is rejected synchronously with
     * MMAPQ_EINVAL (runtime.h, RUNTIME-AFFINITY-01). */
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL,
        mmapq_runtime_set_affinity(rt, CPU_SETSIZE));
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL,
        mmapq_runtime_set_affinity(rt, -1));

    /* A cpu_core in range but not schedulable for this process (>= the
     * configured processor count) fails only asynchronously: rc 0, the
     * one-shot's pin is dropped, the runtime thread survives with its
     * affinity unchanged. */
    int bad = first_disallowed_cpu();
    if (bad < CPU_SETSIZE) {
        ThreadProbe probe;
        thread_probe_init(&probe);
        TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &probe.exec));
        wait_probe_runs(&probe, 1);

        cpu_set_t before;
        TEST_ASSERT_EQUAL_INT(0,
            pthread_getaffinity_np(probe.ran_on, sizeof(before), &before));

        TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_set_affinity(rt, bad));

        /* the one-shot runs within an iteration (registered after the
         * probe, so list order guarantees it); the probe keeps running */
        wait_probe_runs(&probe, 3);
        usleep(50000);
        cpu_set_t after;
        TEST_ASSERT_EQUAL_INT(0,
            pthread_getaffinity_np(probe.ran_on, sizeof(after), &after));
        TEST_ASSERT_TRUE(CPU_EQUAL(&before, &after));

        mmapq_runtime_unregister_and_wait(rt, &probe.exec);
    }
}

/* runtime.h:106-112 — sleep idle: "Sleep — calls nanosleep() with
 * increasing duration when idle. min_sleep_ns: initial sleep duration,
 * max_sleep_ns: maximum sleep duration (doubles each idle cycle)." */
void test_idle_strategy_sleep_runs_executables(void)
{
    MmapqIdleStrategy *s = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_sleep_create(1000000ULL, 2000000ULL, &s));
    TEST_ASSERT_NOT_NULL(s);

    /* a runtime using the sleep strategy runs a registered counting
     * executable (work 0, so the idle/nanosleep path actually executes) */
    MmapqRuntime *srt = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_create("cov01-sleep", s,
        MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS, NULL, NULL, &srt));
    TEST_ASSERT_NOT_NULL(srt);
    CountingExec *ce = counting_exec_create(0);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(srt, &ce->exec));
    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 3 && waited_ms < 2000) {
        usleep(1000);
        waited_ms++;
    }
    TEST_ASSERT_GREATER_OR_EQUAL_INT(3, atomic_load(&ce->execute_count));

    mmapq_runtime_destroy(srt);
    free(ce);
}

void test_idle_strategy_sleep_min_above_max_refused(void)
{
    /* The header documents only "Returns: 0 on success, or an MMAPQ_*
     * error code". Observed behaviour for min > max is MMAPQ_EINVAL with
     * *out cleared to NULL (pinned as found; noted in the COV-01 report). */
    MmapqIdleStrategy *s = (MmapqIdleStrategy *)1;   /* sentinel: must be cleared */
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL,
        mmapq_idle_strategy_sleep_create(2000000ULL, 1000000ULL, &s));
    TEST_ASSERT_NULL(s);
}

/* runtime.h:283-298 — deregister_executable_create: "Create a one-shot
 * MmapqExecutable that, when executed on the MmapqRuntime thread,
 * unregisters target from the MmapqRuntime and then unregisters itself...
 * The DeregisterExecutable frees itself after execution." */
void test_deregister_executable_stops_target(void)
{
    CountingExec *ce = counting_exec_create(1);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &ce->exec));
    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 1 && waited_ms < 2000) {
        usleep(1000);
        waited_ms++;
    }
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, atomic_load(&ce->execute_count));

    MmapqExecutable *dereg = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_deregister_executable_create(rt, &ce->exec, &dereg));
    TEST_ASSERT_NOT_NULL(dereg);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, dereg));

    /* the dereg ran on the runtime thread: the target's count freezes */
    wait_counting_frozen(ce);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, atomic_load(&ce->execute_count));

    /* the DeregisterExecutable freed itself — nothing in this test frees
     * `dereg`; the ASan/LSan loop proves it */
    free(ce);
}

/* runtime.h:301-309 — deregister_executable_create_with_free: "Like
 * mmapq_deregister_executable_create() but also frees ctx_to_free after
 * unregistration." */
void test_deregister_executable_with_free_frees_ctx(void)
{
    CountingExec *ce = counting_exec_create(1);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &ce->exec));
    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 1 && waited_ms < 2000) {
        usleep(1000);
        waited_ms++;
    }
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, atomic_load(&ce->execute_count));

    void *ctx_to_free = malloc(64);
    TEST_ASSERT_NOT_NULL(ctx_to_free);
    MmapqExecutable *dereg = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_deregister_executable_create_with_free(rt, &ce->exec, ctx_to_free, &dereg));
    TEST_ASSERT_NOT_NULL(dereg);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, dereg));

    wait_counting_frozen(ce);

    /* ctx_to_free was freed by the dereg executable after unregistration —
     * nothing in this test frees it; the ASan/LSan loop proves no leak */
    free(ce);
}

/* mmap_platform.h:157-162 — thread_set_affinity: "pin the current thread
 * to a specific CPU core." */
void test_thread_set_affinity_pins_current_thread(void)
{
    cpu_set_t original;
    TEST_ASSERT_EQUAL_INT(0, sched_getaffinity(0, sizeof(original), &original));
    int cpu = first_allowed_cpu();
    TEST_ASSERT_GREATER_OR_EQUAL_INT(0, cpu);

    TEST_ASSERT_EQUAL_INT(0, mmapq_thread_set_affinity(cpu));

    cpu_set_t now;
    TEST_ASSERT_EQUAL_INT(0, sched_getaffinity(0, sizeof(now), &now));
    TEST_ASSERT_EQUAL_INT(1, CPU_COUNT(&now));
    TEST_ASSERT_TRUE(CPU_ISSET(cpu, &now));

    /* restore the process's original mask for the remaining tests */
    TEST_ASSERT_EQUAL_INT(0,
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &original));
}

void test_thread_set_affinity_out_of_range_cpu_fails(void)
{
    cpu_set_t before;
    TEST_ASSERT_EQUAL_INT(0, sched_getaffinity(0, sizeof(before), &before));
    int bad = first_disallowed_cpu();
    TEST_ASSERT_NOT_EQUAL_INT(0, mmapq_thread_set_affinity(bad));

    /* the current affinity is unchanged */
    cpu_set_t after;
    TEST_ASSERT_EQUAL_INT(0, sched_getaffinity(0, sizeof(after), &after));
    TEST_ASSERT_TRUE(CPU_EQUAL(&before, &after));
}

static void *parked_thread_main(void *arg)
{
    atomic_int *release = (atomic_int *)arg;
    while (!atomic_load(release)) usleep(1000);
    return NULL;
}

/* mmap_platform.h:137-142 — thread_set_affinity_of: "pin a specific
 * thread to a specific CPU core. Linux: uses pthread_setaffinity_np." */
void test_thread_set_affinity_of_pins_second_thread(void)
{
    int cpu = first_allowed_cpu();
    TEST_ASSERT_GREATER_OR_EQUAL_INT(0, cpu);

    atomic_int release;
    atomic_store(&release, 0);
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, parked_thread_main, &release));

    TEST_ASSERT_EQUAL_INT(0, mmapq_thread_set_affinity_of(t, cpu));
    cpu_set_t mask;
    TEST_ASSERT_EQUAL_INT(0, pthread_getaffinity_np(t, sizeof(mask), &mask));
    TEST_ASSERT_EQUAL_INT(1, CPU_COUNT(&mask));
    TEST_ASSERT_TRUE(CPU_ISSET(cpu, &mask));

    /* out-of-range CPU: nonzero return, affinity untouched */
    cpu_set_t before;
    TEST_ASSERT_EQUAL_INT(0, pthread_getaffinity_np(t, sizeof(before), &before));
    int bad = first_disallowed_cpu();
    TEST_ASSERT_NOT_EQUAL_INT(0, mmapq_thread_set_affinity_of(t, bad));
    cpu_set_t after;
    TEST_ASSERT_EQUAL_INT(0, pthread_getaffinity_np(t, sizeof(after), &after));
    TEST_ASSERT_TRUE(CPU_EQUAL(&before, &after));

    atomic_store(&release, 1);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
}

/* mmap_platform.h:126-134 — thread_set_name: "Set the name of a pthread
 * (for debugger visibility). Linux: pthread_setname_np(thread, name)." */
void test_thread_set_name_roundtrips(void)
{
    atomic_int release;
    atomic_store(&release, 0);
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, parked_thread_main, &release));

    mmapq_thread_set_name(t, "cov01-thr");   /* 8 chars: Linux caps names at 15 */
    char name[16] = {0};
    TEST_ASSERT_EQUAL_INT(0, pthread_getname_np(t, name, sizeof(name)));
    TEST_ASSERT_EQUAL_STRING("cov01-thr", name);

    atomic_store(&release, 1);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));

    /* NOTE (reported, not changed — COV-01 is tests-only): the function
     * returns void, so pthread_setname_np's ERANGE for names longer than
     * 15 bytes is silently dropped. */
}

/* mmap_platform.h:49-63 — mmapq_version: "return the library version
 * string... Never NULL." It must equal MMAPQ_VERSION_STR and begin with
 * MAJOR.MINOR.PATCH assembled from the MMAPQ_VERSION_* macros, so a
 * release bump that forgets one of them fails here. */
void test_mmapq_version_string_matches_macros(void)
{
    const char *v = mmapq_version();
    TEST_ASSERT_NOT_NULL(v);
    TEST_ASSERT_EQUAL_STRING(MMAPQ_VERSION_STR, v);

    char built[32];
    snprintf(built, sizeof(built), "%d.%d.%d",
        MMAPQ_VERSION_MAJOR, MMAPQ_VERSION_MINOR, MMAPQ_VERSION_PATCH);
    TEST_ASSERT_EQUAL_INT(0, strncmp(v, built, strlen(built)));
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

    int rc = mmapq_runtime_close(crt);   /* pre-C-02: never returns */
    /* CAND-24: the wedged thread exhausts the budget and detaches —
     * close reports exactly MMAPQ_ETIMEDOUT. */
    _exit(rc == MMAPQ_ETIMEDOUT ? 0 : 3);
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

    int rc = mmapq_runtime_close(crt);    /* budget exhausts; thread detached */
    if (rc != MMAPQ_ETIMEDOUT) _exit(3);  /* CAND-24: exact status */
    /* CAND-24: a sequential repeat close reports the first close's outcome */
    if (mmapq_runtime_close(crt) != MMAPQ_ETIMEDOUT) _exit(3);
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

/* ── CAND-24 5(b): unregister_and_wait racing a joined close ──────────────── */

static int trivial_execute(MmapqExecutable *self)
{
    (void)self;
    return 0;
}

typedef struct {
    MmapqRuntime   *rt;
    WedgedExec      target;  /* T: trivial execute, close records it ran */
    WedgedExec      wedge;   /* G: slow — holds the runtime thread */
    atomic_int      waiter_returned;
    atomic_int      waiter_saw_target_closed;
} UwRace;

static void *unregister_waiter(void *arg)
{
    UwRace *s = (UwRace *)arg;
    mmapq_runtime_unregister_and_wait(s->rt, &s->target.exec);
    /* The drain closes in list order and T registered before the helper,
     * so on the joined path T's close() must have run before the waiter
     * returns — this load is the pin's ordering assertion. */
    atomic_store_explicit(&s->waiter_saw_target_closed,
        atomic_load_explicit(&s->target.closed, memory_order_acquire),
        memory_order_release);
    atomic_store_explicit(&s->waiter_returned, 1, memory_order_release);
    return NULL;
}

static void *release_wedge_after_300ms(void *arg)
{
    usleep(300000);
    atomic_store_explicit(&((WedgedExec *)arg)->release, 1, memory_order_release);
    return NULL;
}

/* Child body (CAND-24 P4 — the reviewer's c24_5b B2 shape, executed on the
 * baseline): the unregister helper registers while the runtime thread is
 * wedged in G; the runtime close then JOINS (3 s budget, wedge lifted
 * 300 ms in). Pre-fix the helper never runs and the drain's
 * unregister_wait_close is a no-op — done is never set and the waiter
 * spins forever (parent watchdog kills the child). Post-fix the drain's
 * close sets done and the waiter returns. Exit codes: 0 = waiter
 * returned with T's close already run; 2 = setup failed;
 * 3 = the close did not join (rc != 0); 4 = waiter returned before T's
 * close ran. */
static void child_unregister_and_wait_joined_close(void)
{
    static UwRace s;   /* static: shared with the waiter/releaser threads */

    MmapqIdleStrategy *idle = NULL;
    if (mmapq_idle_strategy_busy_spin_create(&idle) != 0) _exit(2);
    if (mmapq_runtime_create("c24-uw-rt", idle, 3000000000ULL,
                             NULL, NULL, &s.rt) != 0) _exit(2);

    atomic_store(&s.target.closed, 0);
    s.target.exec.execute = trivial_execute;
    s.target.exec.close   = wedged_close;
    s.target.exec.ctx     = &s.target;
    if (mmapq_runtime_register(s.rt, &s.target.exec) != 1) _exit(2);

    atomic_store(&s.wedge.entered, 0);
    atomic_store(&s.wedge.release, 0);
    atomic_store(&s.wedge.closed,  0);
    s.wedge.exec.execute = slow_execute;
    s.wedge.exec.close   = wedged_close;
    s.wedge.exec.ctx     = &s.wedge;
    if (mmapq_runtime_register(s.rt, &s.wedge.exec) != 1) _exit(2);

    /* Wait until the thread is observably inside G's execute() — without
     * the handshake the helper could register before the wedge and the
     * test would exercise nothing. */
    int entered = 0;
    for (int i = 0; i < 5000; i++) {
        if (atomic_load_explicit(&s.wedge.entered, memory_order_acquire)) { entered = 1; break; }
        usleep(1000);
    }
    if (!entered) _exit(2);

    atomic_store(&s.waiter_returned, 0);
    atomic_store(&s.waiter_saw_target_closed, 0);
    pthread_t w;
    if (pthread_create(&w, NULL, unregister_waiter, &s) != 0) _exit(2);
    usleep(100000);   /* helper registers while the thread is wedged (c24_5b timing) */

    pthread_t r;
    if (pthread_create(&r, NULL, release_wedge_after_300ms, &s.wedge) != 0) _exit(2);
    pthread_detach(r);

    /* 3 s budget, wedge lifted 300 ms in: must JOIN — exact status
     * (spec §6 P4). This also proves the test exercises a joined close. */
    if (mmapq_runtime_close(s.rt) != 0) _exit(3);

    pthread_join(w, NULL);          /* pre-fix: never returns — watchdog kills */
    if (!atomic_load_explicit(&s.waiter_saw_target_closed, memory_order_acquire))
        _exit(4);

    mmapq_runtime_destroy(s.rt);
    _exit(0);
}

void test_unregister_and_wait_returns_after_joined_close(void)
{
    pid_t pid = fork();
    TEST_ASSERT(pid >= 0);
    if (pid == 0) child_unregister_and_wait_joined_close();
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
    RUN_TEST(test_runtime_close_status_values);
    RUN_TEST(test_idle_strategy_busy_spin);
    RUN_TEST(test_idle_strategy_yield);
    RUN_TEST(test_idle_strategy_backoff);
    RUN_TEST(test_deregister_executable);
    RUN_TEST(test_unregister_and_wait_safe_free);
    RUN_TEST(test_runtime_double_acquire);

    /* COV-01: direct tests for previously-untested public functions */
    RUN_TEST(test_runtime_get_instance_returns_one_runtime);
    RUN_TEST(test_runtime_get_instance_close_destroy_frees_singleton);
    RUN_TEST(test_runtime_metrics_processor_returns_same_singleton);
    RUN_TEST(test_runtime_set_affinity_pins_runtime_thread);
    RUN_TEST(test_runtime_set_affinity_out_of_range_cpu_runtime_survives);
    RUN_TEST(test_idle_strategy_sleep_runs_executables);
    RUN_TEST(test_idle_strategy_sleep_min_above_max_refused);
    RUN_TEST(test_deregister_executable_stops_target);
    RUN_TEST(test_deregister_executable_with_free_frees_ctx);
    RUN_TEST(test_thread_set_affinity_pins_current_thread);
    RUN_TEST(test_thread_set_affinity_out_of_range_cpu_fails);
    RUN_TEST(test_thread_set_affinity_of_pins_second_thread);
    RUN_TEST(test_thread_set_name_roundtrips);
    RUN_TEST(test_mmapq_version_string_matches_macros);
#ifndef __SANITIZE_THREAD__
    RUN_TEST(test_runtime_close_with_wedged_executable_returns);
    RUN_TEST(test_runtime_destroy_after_detach_no_uaf);
    RUN_TEST(test_unregister_and_wait_returns_after_joined_close);
#endif
    return UNITY_END();
}
