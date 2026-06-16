#include "unity.h"
#include "appender_id_pool.h"

#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>

static const char *DIR   = "/tmp";
static const char *QUEUE = "mmap_test_id_pool_q";
static AppenderIdPool *pool = NULL;

static void cleanup(void)
{
    char path[256];
    snprintf(path, sizeof(path), "%s/%s/%s", DIR, QUEUE, APPENDER_ID_POOL_FILE_NAME);
    unlink(path);
    snprintf(path, sizeof(path), "%s/%s", DIR, QUEUE);
    rmdir(path);
}

void setUp(void)
{
    cleanup();
    mkdir("/tmp/mmap_test_id_pool_q", 0755);
    pool = appender_id_pool_open(DIR, QUEUE);
}

void tearDown(void)
{
    if (pool) { appender_id_pool_close(pool); pool = NULL; }
    cleanup();
}

void test_acquire_returns_valid_id(void)
{
    int id = appender_id_pool_acquire(pool);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(0, id);
    TEST_ASSERT_LESS_THAN_INT(MAX_APPENDERS, id);
}

void test_acquire_returns_unique_ids(void)
{
    int id0 = appender_id_pool_acquire(pool);
    int id1 = appender_id_pool_acquire(pool);
    int id2 = appender_id_pool_acquire(pool);
    TEST_ASSERT_NOT_EQUAL(id0, id1);
    TEST_ASSERT_NOT_EQUAL(id1, id2);
    TEST_ASSERT_NOT_EQUAL(id0, id2);
}

void test_release_and_reacquire(void)
{
    int id = appender_id_pool_acquire(pool);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(0, id);

    int released = appender_id_pool_release(pool, (uint8_t)id);
    TEST_ASSERT_EQUAL_INT(1, released);

    /* Next acquire should be able to return the same id (likely first free) */
    int id2 = appender_id_pool_acquire(pool);
    TEST_ASSERT_EQUAL_INT(id, id2);
}

void test_release_not_held_returns_zero(void)
{
    /* ID 5 was never acquired */
    int result = appender_id_pool_release(pool, 5);
    TEST_ASSERT_EQUAL_INT(0, result);
}

void test_try_acquire_specific_id(void)
{
    int ok = appender_id_pool_try_acquire(pool, 7);
    TEST_ASSERT_EQUAL_INT(1, ok);
    appender_id_pool_release(pool, 7);
}

void test_try_acquire_already_taken_fails(void)
{
    appender_id_pool_try_acquire(pool, 3);
    int ok = appender_id_pool_try_acquire(pool, 3);
    TEST_ASSERT_EQUAL_INT(0, ok);
    appender_id_pool_release(pool, 3);
}

void test_acquire_skips_taken_ids(void)
{
    /* Reserve IDs 0,1,2 via try_acquire */
    appender_id_pool_try_acquire(pool, 0);
    appender_id_pool_try_acquire(pool, 1);
    appender_id_pool_try_acquire(pool, 2);

    int id = appender_id_pool_acquire(pool);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(3, id);

    appender_id_pool_release(pool, 0);
    appender_id_pool_release(pool, 1);
    appender_id_pool_release(pool, 2);
    appender_id_pool_release(pool, (uint8_t)id);
}

void test_persists_across_reopen(void)
{
    /* Acquire ID 4 */
    appender_id_pool_try_acquire(pool, 4);
    appender_id_pool_close(pool);

    /* Reopen — ID 4 should still be marked as taken */
    pool = appender_id_pool_open(DIR, QUEUE);
    int ok = appender_id_pool_try_acquire(pool, 4);
    TEST_ASSERT_EQUAL_INT(0, ok);  /* still held */
    appender_id_pool_release(pool, 4);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_acquire_returns_valid_id);
    RUN_TEST(test_acquire_returns_unique_ids);
    RUN_TEST(test_release_and_reacquire);
    RUN_TEST(test_release_not_held_returns_zero);
    RUN_TEST(test_try_acquire_specific_id);
    RUN_TEST(test_try_acquire_already_taken_fails);
    RUN_TEST(test_acquire_skips_taken_ids);
    RUN_TEST(test_persists_across_reopen);
    return UNITY_END();
}
