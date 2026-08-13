#include "unity.h"
#include "internal/region.h"
#include "internal/region_ring.h"
#include "runtime.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"

#include <stdint.h>
#include <unistd.h>
#include <pthread.h>

static const char   *FILE_PREFIX   = "/tmp/mmap_test_region_async";
static const int32_t REGION_SIZE   = 4096;
static const int64_t MAX_FILE_SIZE = 4096 * 8;
static const uint64_t TIMEOUT_NS   = 2000000000ULL;  /* 2 seconds */

static MmapqRuntime    *rt  = NULL;
static FileMapper *fm  = NULL;
static Region     *reg = NULL;

static void cleanup_files(void)
{
    char path[256];
    for (int i = 0; i < 4; i++) {
        snprintf(path, sizeof(path), "%s_%d", FILE_PREFIX, i);
        unlink(path);
    }
}

void setUp(void)
{
    cleanup_files();
    MmapqIdleStrategy *idle = NULL;
    int rc_idle = mmapq_idle_strategy_busy_spin_create(&idle);
    TEST_ASSERT_EQUAL_INT(0, rc_idle);
    TEST_ASSERT_NOT_NULL(idle);
    int rc_rt = mmapq_runtime_create("test-mapper",
                               idle,
                               MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS,
                               NULL, NULL, &rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
    int fmerr = mmapqi_file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        mmapqi_file_init_zeros, NULL, MAP_MODE_READ_WRITE, 4, 1, &fm);
    TEST_ASSERT_EQUAL_INT(0, fmerr);
    TEST_ASSERT_NOT_NULL(fm);
    int rerr = mmapqi_region_async_create(fm, REGION_SIZE, TIMEOUT_NS,
                               REGION_MAX_FILE_NOT_FOUND_ERRORS,
                               REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
                               &reg);
    TEST_ASSERT_EQUAL_INT(0, rerr);
    TEST_ASSERT_NOT_NULL(reg);
    /* Region is driven manually in tests via mmapqi_region_async_process_request() */
}

void tearDown(void)
{
    if (reg) { mmapqi_region_destroy(reg); reg = NULL; }
    if (fm)  { file_mapper_close(fm); mmapqi_file_mapper_destroy(fm); fm = NULL; }
    if (rt)  { mmapq_runtime_destroy(rt); rt = NULL; }
    cleanup_files();
}

/* ── Helper — drive region mapper manually ───────────────────────────────── */

static void drive_until_mapped(Region *r, int64_t position, int max_iters)
{
    region_map(r, position);
    for (int i = 0; i < max_iters; i++) {
        if (mmapqi_region_async_process_request(r)) {
            /* Check if now mapped */
            Buffer buf = {0};
            if (region_wrap(r, position, &buf) == WRAP_SUCCESS) {
                return;
            }
        }
        usleep(100);
    }
}

/* ── State machine tests ─────────────────────────────────────────────────── */

void test_async_wrap_success_after_mapping(void)
{
    drive_until_mapped(reg, 0, 100);
    Buffer buf = {0};
    WrapResult r = region_wrap(reg, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_NOT_NULL(buf.data);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf.capacity);
}

void test_async_wrap_returns_timeout_when_not_mapped(void)
{
    /* Create a fresh region with very short timeout — no mapping requested */
    Region *fast_timeout = NULL;
    int rerr2 = mmapqi_region_async_create(
        fm, REGION_SIZE, 1000ULL,  /* 1 microsecond */
        REGION_MAX_FILE_NOT_FOUND_ERRORS,
        REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
        &fast_timeout);
    TEST_ASSERT_EQUAL_INT(0, rerr2);
    TEST_ASSERT_NOT_NULL(fast_timeout);

    Buffer buf = {0};
    /* Don't call map() — region stays UNMAPPED */
    /* wrap() will request map and spin until timeout */
    WrapResult r = region_wrap(fast_timeout, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_MAP_TIMED_OUT, r);

    mmapqi_region_destroy(fast_timeout);
}

void test_async_map_request_is_idempotent(void)
{
    /* Call map() multiple times — should not crash or corrupt state */
    region_map(reg, 0);
    region_map(reg, 0);
    region_map(reg, 0);

    /* Drive to completion */
    drive_until_mapped(reg, 0, 100);

    Buffer buf = {0};
    WrapResult r = region_wrap(reg, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
}

void test_async_write_and_read(void)
{
    drive_until_mapped(reg, 0, 100);

    Buffer buf = {0};
    region_wrap(reg, 0, &buf);

    uint64_t *p = (uint64_t *)buf.data;
    *p = 0xABCDEF0123456789ULL;

    /* Re-wrap same position */
    Buffer buf2 = {0};
    region_wrap(reg, 0, &buf2);
    TEST_ASSERT_EQUAL_HEX64(0xABCDEF0123456789ULL, *(uint64_t *)buf2.data);
}

void test_async_unmap_and_remap(void)
{
    drive_until_mapped(reg, 0, 100);

    Buffer buf = {0};
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, region_wrap(reg, 0, &buf));

    region_unmap(reg);
    mmapqi_region_async_process_request(reg);

    /* Remap at different position */
    drive_until_mapped(reg, REGION_SIZE, 100);
    Buffer buf2 = {0};
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, region_wrap(reg, REGION_SIZE, &buf2));
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf2.capacity);
}

void test_async_size_returns_region_size(void)
{
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, region_size(reg));
}

void test_async_process_request_returns_work_done(void)
{
    region_map(reg, 0);
    /* First process should do work (MAP_REQUESTED → MAPPED) */
    int work = mmapqi_region_async_process_request(reg);
    TEST_ASSERT_GREATER_THAN(0, work);

    /* Second process — already MAPPED, no work */
    int work2 = mmapqi_region_async_process_request(reg);
    TEST_ASSERT_EQUAL_INT(0, work2);
}

void test_async_close_unmaps_region(void)
{
    drive_until_mapped(reg, 0, 100);
    /* close() should not crash and should unmap */
    mmapqi_region_async_process_request(reg);  /* ensure state is settled */
    /* mmapqi_region_destroy calls close() internally */
    mmapqi_region_destroy(reg);
    reg = NULL;  /* prevent double-free in tearDown */
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_async_wrap_success_after_mapping);
    RUN_TEST(test_async_wrap_returns_timeout_when_not_mapped);
    RUN_TEST(test_async_map_request_is_idempotent);
    RUN_TEST(test_async_write_and_read);
    RUN_TEST(test_async_unmap_and_remap);
    RUN_TEST(test_async_size_returns_region_size);
    RUN_TEST(test_async_process_request_returns_work_done);
    RUN_TEST(test_async_close_unmaps_region);
    return UNITY_END();
}
