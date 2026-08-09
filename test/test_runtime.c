#include "unity.h"
#include "runtime.h"

#include <stdint.h>
#include <unistd.h>
#include <stdatomic.h>
#include <stdlib.h>

static Runtime *rt = NULL;

void setUp(void)
{
    IdleStrategy *idle = NULL;
    int rc_idle = idle_strategy_busy_spin_create(&idle);
    TEST_ASSERT_EQUAL_INT(0, rc_idle);
    TEST_ASSERT_NOT_NULL(idle);
    int rc_rt = runtime_create("test-rt",
                               idle,
                               RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS,
                               NULL, NULL, &rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
}

void tearDown(void)
{
    if (rt) {
        runtime_destroy(rt);
        rt = NULL;
    }
}

/* ── Executable helpers ──────────────────────────────────────────────────── */

typedef struct {
    atomic_int execute_count;
    atomic_int close_count;
    int        work_to_return;
    Executable exec;
} CountingExec;

static int counting_execute(Executable *self)
{
    CountingExec *ce = (CountingExec *)self->ctx;
    atomic_fetch_add(&ce->execute_count, 1);
    return ce->work_to_return;
}

static void counting_close(Executable *self)
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
    TEST_ASSERT_EQUAL_INT(0, runtime_is_closed(rt));
}

void test_runtime_register_returns_true(void)
{
    CountingExec *ce = counting_exec_create(1);
    int result = runtime_register(rt, &ce->exec);
    TEST_ASSERT_EQUAL_INT(1, result);
    /* Don't free ce here — runtime thread still holds reference.
     * tearDown() calls runtime_destroy() which calls close() on all
     * executables before we free. Safe to free after destroy. */
    runtime_destroy(rt);
    rt = NULL;
    free(ce);
}

void test_runtime_executes_registered_executable(void)
{
    CountingExec *ce = counting_exec_create(1);
    runtime_register(rt, &ce->exec);

    /* Wait for at least 10 executions */
    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 10 && waited_ms < 1000) {
        usleep(1000);
        waited_ms++;
    }

    TEST_ASSERT_GREATER_OR_EQUAL_INT(10, atomic_load(&ce->execute_count));

    /* Must destroy runtime before freeing ce — runtime thread holds a pointer
     * to ce->exec and will use-after-free if we free ce first. */
    runtime_destroy(rt);
    rt = NULL;
    free(ce);
}

void test_runtime_close_stops_execution(void)
{
    CountingExec *ce = counting_exec_create(1);
    runtime_register(rt, &ce->exec);

    usleep(10000);  /* let it run briefly */

    runtime_close(rt);

    /* After close() returns, the thread has fully stopped — count is frozen */
    int count_after_close = atomic_load(&ce->execute_count);
    usleep(20000);
    int count_final = atomic_load(&ce->execute_count);

    /* Count must not grow after close() returns — thread is done */
    TEST_ASSERT_EQUAL_INT(count_after_close, count_final);
    TEST_ASSERT_EQUAL_INT(1, runtime_is_closed(rt));
    TEST_ASSERT_GREATER_OR_EQUAL(1, atomic_load(&ce->close_count));

    free(ce);
    runtime_destroy(rt);
    rt = NULL;
}

void test_runtime_register_after_close_returns_false(void)
{
    runtime_close(rt);
    CountingExec *ce = counting_exec_create(1);
    int result = runtime_register(rt, &ce->exec);
    TEST_ASSERT_EQUAL_INT(0, result);
    free(ce);

    runtime_destroy(rt);
    rt = NULL;
}

void test_runtime_multiple_executables(void)
{
    CountingExec *ce1 = counting_exec_create(1);
    CountingExec *ce2 = counting_exec_create(0);
    CountingExec *ce3 = counting_exec_create(1);

    runtime_register(rt, &ce1->exec);
    runtime_register(rt, &ce2->exec);
    runtime_register(rt, &ce3->exec);

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
    runtime_destroy(rt);
    rt = NULL;
    free(ce1); free(ce2); free(ce3);
}

void test_runtime_is_closed_after_close(void)
{
    runtime_close(rt);
    TEST_ASSERT_EQUAL_INT(1, runtime_is_closed(rt));
    runtime_destroy(rt);
    rt = NULL;
}

void test_idle_strategy_busy_spin(void)
{
    IdleStrategy *s = NULL;
    int rc = idle_strategy_busy_spin_create(&s);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(s);
    /* Should not crash with any work count */
    s->idle(s, 0);
    s->idle(s, 1);
    s->idle(s, 100);
    idle_strategy_destroy(s);
}

void test_idle_strategy_yield(void)
{
    IdleStrategy *s = NULL;
    int rc = idle_strategy_yield_create(&s);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(s);
    s->idle(s, 0);
    s->idle(s, 1);
    idle_strategy_destroy(s);
}

void test_idle_strategy_backoff(void)
{
    IdleStrategy *s = NULL;
    int rc = idle_strategy_backoff_create(10, 10, 1000000ULL, &s);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(s);
    for (int i = 0; i < 30; i++) {
        s->idle(s, 0);  /* progress through spin → yield → sleep phases */
    }
    s->idle(s, 1);  /* reset */
    idle_strategy_destroy(s);
}

void test_deregister_executable(void)
{
    CountingExec *ce = counting_exec_create(1);
    runtime_register(rt, &ce->exec);

    usleep(5000);  /* let it execute */

    /* Use runtime_unregister_and_wait — blocks until runtime thread confirms
     * it has finished with ce->exec. No racy sleep required. */
    runtime_unregister_and_wait(rt, &ce->exec);

    int count_after_dereg = atomic_load(&ce->execute_count);

    /* Runtime thread has confirmed it is done — count is frozen */
    usleep(20000);
    int count_later = atomic_load(&ce->execute_count);

    TEST_ASSERT_EQUAL_INT(count_after_dereg, count_later);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, count_after_dereg);
    free(ce);  /* safe: runtime_unregister_and_wait guarantees no further access */
}

void test_unregister_and_wait_safe_free(void)
{
    /* Verify that after runtime_unregister_and_wait returns, the executable
     * context can be freed immediately without data races. */
    CountingExec *ce = counting_exec_create(1);
    runtime_register(rt, &ce->exec);

    int waited_ms = 0;
    while (atomic_load(&ce->execute_count) < 5 && waited_ms < 1000) {
        usleep(1000);
        waited_ms++;
    }

    runtime_unregister_and_wait(rt, &ce->exec);

    /* Immediate free — no sleep, no grace period needed */
    int final_count = atomic_load(&ce->execute_count);
    free(ce);

    /* Verify runtime still runs (can register a new executable) */
    CountingExec *ce2 = counting_exec_create(1);
    runtime_register(rt, &ce2->exec);
    usleep(5000);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(1, atomic_load(&ce2->execute_count));
    TEST_ASSERT_GREATER_OR_EQUAL_INT(5, final_count);
    runtime_unregister_and_wait(rt, &ce2->exec);
    free(ce2);
}

void test_runtime_double_acquire(void)
{
    /* Verify that runtime_get_instance/clone/destroy cycle doesn't
     * orphan the registry entry. Regression for P0.4 refcount fix. */
    Runtime *rt1 = NULL;
    int rc = runtime_region_mapper(&rt1);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(rt1);
    TEST_ASSERT_FALSE(runtime_is_closed(rt1));

    Runtime *rt2 = NULL;
    rc = runtime_clone(rt1, &rt2);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(rt2);

    runtime_destroy(rt1);
    runtime_destroy(rt2);

    /* Re-acquire — should return a valid entry (registry still holds ref) */
    Runtime *rt3 = NULL;
    rc = runtime_region_mapper(&rt3);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(rt3);
    TEST_ASSERT_FALSE(runtime_is_closed(rt3));

    runtime_destroy(rt3);
}

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
    return UNITY_END();
}
