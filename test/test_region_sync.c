#include "unity.h"
#include "internal/region.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"

#include <stdint.h>
#include <unistd.h>

static const char   *FILE_PREFIX  = "/tmp/mmap_test_region_sync";
static const int32_t REGION_SIZE  = 4096;
static const int64_t MAX_FILE_SIZE = 4096 * 8;
static const uint64_t TIMEOUT_NS  = 1000000000ULL;  /* 1 second */

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
    int fmerr = mmapqi_file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        mmapqi_file_init_zeros, NULL, MAP_MODE_READ_WRITE, 4, 1, 0, &fm);
    TEST_ASSERT_EQUAL_INT(0, fmerr);
    TEST_ASSERT_NOT_NULL(fm);
    int rerr = mmapqi_region_sync_create(fm, REGION_SIZE, TIMEOUT_NS,
                              REGION_MAX_FILE_NOT_FOUND_ERRORS,
                              REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
                              &reg);
    TEST_ASSERT_EQUAL_INT(0, rerr);
    TEST_ASSERT_NOT_NULL(reg);
}

void tearDown(void)
{
    if (reg) { mmapqi_region_destroy(reg); reg = NULL; }
    if (fm)  { file_mapper_close(fm); mmapqi_file_mapper_destroy(fm); fm = NULL; }
    cleanup_files();
}

/* ── wrap tests ──────────────────────────────────────────────────────────── */

void test_sync_wrap_success(void)
{
    Buffer buf = {0};
    WrapResult r = region_wrap(reg, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_NOT_NULL(buf.data);
    TEST_ASSERT_GREATER_THAN(0, buf.capacity);
}

void test_sync_wrap_capacity_is_remaining_bytes(void)
{
    Buffer buf = {0};
    region_wrap(reg, 0, &buf);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf.capacity);
}

void test_sync_wrap_mid_region_reduces_capacity(void)
{
    Buffer buf = {0};
    int32_t offset = 100;
    region_wrap(reg, offset, &buf);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE - offset, buf.capacity);
}

void test_sync_wrap_at_region_boundary(void)
{
    Buffer buf = {0};
    /* Last byte of region */
    WrapResult r = region_wrap(reg, REGION_SIZE - 1, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_EQUAL_INT(1, buf.capacity);
}

void test_sync_wrap_write_read(void)
{
    Buffer buf = {0};
    region_wrap(reg, 0, &buf);

    /* Write via buffer */
    uint64_t *p = (uint64_t *)buf.data;
    *p = 0xDEADBEEFCAFEBABEULL;

    /* Read back — same region still mapped */
    Buffer buf2 = {0};
    region_wrap(reg, 0, &buf2);
    TEST_ASSERT_EQUAL_HEX64(0xDEADBEEFCAFEBABEULL, *(uint64_t *)buf2.data);
}

void test_sync_wrap_crosses_region_boundary(void)
{
    Buffer buf = {0};

    /* First region */
    WrapResult r1 = region_wrap(reg, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r1);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf.capacity);

    /* Second region */
    WrapResult r2 = region_wrap(reg, REGION_SIZE, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r2);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf.capacity);
}

void test_sync_map_returns_true_when_mapped(void)
{
    int result = region_map(reg, 0);
    TEST_ASSERT_EQUAL_INT(1, result);
}

void test_sync_unmap_returns_true(void)
{
    region_map(reg, 0);
    int result = region_unmap(reg);
    TEST_ASSERT_EQUAL_INT(1, result);
}

void test_sync_size_returns_region_size(void)
{
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, region_size(reg));
}

void test_sync_wrap_no_premap_succeeds(void)
{
    Buffer buf = {0};
    WrapResult r = region_wrap_no_premap(reg, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_NOT_NULL(buf.data);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_sync_wrap_success);
    RUN_TEST(test_sync_wrap_capacity_is_remaining_bytes);
    RUN_TEST(test_sync_wrap_mid_region_reduces_capacity);
    RUN_TEST(test_sync_wrap_at_region_boundary);
    RUN_TEST(test_sync_wrap_write_read);
    RUN_TEST(test_sync_wrap_crosses_region_boundary);
    RUN_TEST(test_sync_map_returns_true_when_mapped);
    RUN_TEST(test_sync_unmap_returns_true);
    RUN_TEST(test_sync_size_returns_region_size);
    RUN_TEST(test_sync_wrap_no_premap_succeeds);
    return UNITY_END();
}
