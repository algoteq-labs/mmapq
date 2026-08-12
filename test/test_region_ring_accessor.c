#include "unity.h"
#include "internal/region_ring_accessor.h"
#include "internal/region_ring.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"
#include "runtime.h"

#include <stdint.h>
#include <unistd.h>
#include <string.h>

static const char    *FILE_PREFIX   = "/tmp/mmap_test_ring_acc";
static const int32_t  REGION_SIZE   = 4096;
static const int64_t  MAX_FILE_SIZE = 4096 * 16;
static const int      RING_SIZE     = 4;
static const uint64_t TIMEOUT_NS    = 2000000000ULL;

static Runtime         *rt  = NULL;
static FileMapper      *fm  = NULL;
static RegionAccessor  *acc = NULL;

static void cleanup_files(void)
{
    char path[256];
    for (int i = 0; i < 8; i++) {
        snprintf(path, sizeof(path), "%s_%d", FILE_PREFIX, i);
        unlink(path);
    }
}

void setUp(void)
{
    cleanup_files();
    int rc_rt = runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);

    int fmerr = file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        file_init_zeros, NULL, MAP_MODE_READ_WRITE,
        RING_SIZE + 2, 1, &fm);
    TEST_ASSERT_EQUAL_INT(0, fmerr);
    TEST_ASSERT_NOT_NULL(fm);

    RegionRing *ring = NULL;
    int rerr = region_ring_async_create(
        RING_SIZE, REGION_SIZE, fm, TIMEOUT_NS, rt, &ring);
    TEST_ASSERT_EQUAL_INT(0, rerr);
    TEST_ASSERT_NOT_NULL(ring);

    int rerr2 = region_ring_accessor_create(
        ring, REGION_SIZE,
        REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD,
        REGION_RING_ACCESSOR_DEFAULT_PREMAP_RATIO,
        &acc);
    TEST_ASSERT_EQUAL_INT(0, rerr2);
    TEST_ASSERT_NOT_NULL(acc);
}

void tearDown(void)
{
    if (acc) { region_ring_accessor_destroy(acc); acc = NULL; fm = NULL; }
    else if (fm) { file_mapper_close(fm); file_mapper_destroy(fm); fm = NULL; }
    cleanup_files();
    /* Note: runtime_region_mapper() is a singleton — don't destroy it */
}

/* ── Basic wrap tests ────────────────────────────────────────────────────── */

void test_accessor_wrap_position_zero(void)
{
    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(acc, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_NOT_NULL(buf.data);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf.capacity);
}

void test_accessor_wrap_mid_region(void)
{
    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(acc, 100, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE - 100, buf.capacity);
}

void test_accessor_wrap_at_region_end(void)
{
    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(acc, REGION_SIZE - 1, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_EQUAL_INT(1, buf.capacity);
}

/* ── Sequential access ───────────────────────────────────────────────────── */

void test_accessor_sequential_regions(void)
{
    for (int i = 0; i < RING_SIZE; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        Buffer buf = {0};
        WrapResult r = region_accessor_wrap(acc, pos, &buf);
        TEST_ASSERT_EQUAL_MESSAGE(WRAP_SUCCESS, r, "sequential wrap failed");
        TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf.capacity);
    }
}

void test_accessor_write_sequential_read_back(void)
{
    /* Write region index at start of each region */
    for (int i = 0; i < RING_SIZE; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        Buffer buf = {0};
        region_accessor_wrap(acc, pos, &buf);
        *(int32_t *)buf.data = i;
    }

    /* Read back in order */
    for (int i = 0; i < RING_SIZE; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        Buffer buf = {0};
        region_accessor_wrap(acc, pos, &buf);
        TEST_ASSERT_EQUAL_INT(i, *(int32_t *)buf.data);
    }
}

/* ── Region boundary skip ────────────────────────────────────────────────── */

void test_accessor_boundary_skip_capacity(void)
{
    /* Position 3 bytes before region end — capacity should be 3 */
    int64_t pos = REGION_SIZE - 3;
    Buffer buf  = {0};
    WrapResult r = region_accessor_wrap(acc, pos, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_EQUAL_INT(3, buf.capacity);

    /* Queue would detect buf.capacity < MIN_NEEDED and skip:
     * next wrap at pos + buf.capacity = REGION_SIZE (start of next region) */
    Buffer buf2 = {0};
    WrapResult r2 = region_accessor_wrap(acc, pos + buf.capacity, &buf2);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r2);
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, buf2.capacity);
}

/* ── wrap_no_premap ──────────────────────────────────────────────────────── */

void test_accessor_wrap_no_premap_succeeds(void)
{
    Buffer buf = {0};
    WrapResult r = region_accessor_wrap_no_premap(acc, 0, &buf);
    TEST_ASSERT_EQUAL(WRAP_SUCCESS, r);
    TEST_ASSERT_NOT_NULL(buf.data);
}

void test_accessor_wrap_no_premap_random_access(void)
{
    /* Access in non-sequential order */
    int64_t positions[] = { 2 * REGION_SIZE, 0, 3 * REGION_SIZE, REGION_SIZE };
    for (int i = 0; i < 4; i++) {
        Buffer buf = {0};
        WrapResult r = region_accessor_wrap_no_premap(acc, positions[i], &buf);
        TEST_ASSERT_EQUAL_MESSAGE(WRAP_SUCCESS, r, "random access wrap failed");
    }
}

/* ── size ────────────────────────────────────────────────────────────────── */

void test_accessor_size_returns_region_size(void)
{
    TEST_ASSERT_EQUAL_INT(REGION_SIZE, region_accessor_size(acc));
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_accessor_wrap_position_zero);
    RUN_TEST(test_accessor_wrap_mid_region);
    RUN_TEST(test_accessor_wrap_at_region_end);
    RUN_TEST(test_accessor_sequential_regions);
    RUN_TEST(test_accessor_write_sequential_read_back);
    RUN_TEST(test_accessor_boundary_skip_capacity);
    RUN_TEST(test_accessor_wrap_no_premap_succeeds);
    RUN_TEST(test_accessor_wrap_no_premap_random_access);
    RUN_TEST(test_accessor_size_returns_region_size);
    return UNITY_END();
}
