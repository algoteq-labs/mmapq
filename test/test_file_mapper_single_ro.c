#include "unity.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>

static const char *TEST_FILE    = "/tmp/mmap_test_ro.dat";
static const char *MISSING_FILE = "/tmp/mmap_test_ro_missing.dat";
static const int32_t REGION_SIZE = 4096;

static FileMapper *fm = NULL;

void setUp(void)
{
    /* Create a test file with known content */
    int fd = open(TEST_FILE, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    mmapqi_file_init_zeros(fd, REGION_SIZE * 2, NULL);

    /* Write a known value at offset 0 */
    uint64_t magic = 0xDEADBEEFCAFEBABEULL;
    { ssize_t _w = pwrite(fd, &magic, sizeof(magic), 0); (void)_w; }

    /* Write another at offset REGION_SIZE */
    uint64_t magic2 = 0x0102030405060708ULL;
    { ssize_t _w = pwrite(fd, &magic2, sizeof(magic2), REGION_SIZE); (void)_w; }

    close(fd);

    int fmerr = mmapqi_file_mapper_single_ro_create(TEST_FILE, mmapqi_file_init_none, NULL, 1, &fm);
    TEST_ASSERT_EQUAL_INT(0, fmerr);
    TEST_ASSERT_NOT_NULL(fm);
}

void tearDown(void)
{
    if (fm != NULL) {
        file_mapper_close(fm);
        mmapqi_file_mapper_destroy(fm);
        fm = NULL;
    }
    unlink(TEST_FILE);
    unlink(MISSING_FILE);
}

/* ── map tests ───────────────────────────────────────────────────────────── */

void test_ro_map_returns_valid_address(void)
{
    uintptr_t addr = file_mapper_map(fm, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS,     addr);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_FILE_NOT_FOUND,   addr);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_BEYOND_FILE_SIZE, addr);
    file_mapper_unmap(fm, addr, 0, REGION_SIZE);
}

void test_ro_map_content_matches_file(void)
{
    uintptr_t addr = file_mapper_map(fm, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);

    uint64_t *val = (uint64_t *)addr;
    TEST_ASSERT_EQUAL_HEX64(0xDEADBEEFCAFEBABEULL, *val);

    file_mapper_unmap(fm, addr, 0, REGION_SIZE);
}

void test_ro_map_second_region(void)
{
    uintptr_t addr = file_mapper_map(fm, REGION_SIZE, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);

    uint64_t *val = (uint64_t *)addr;
    TEST_ASSERT_EQUAL_HEX64(0x0102030405060708ULL, *val);

    file_mapper_unmap(fm, addr, REGION_SIZE, REGION_SIZE);
}

void test_ro_map_missing_file_returns_file_not_found(void)
{
    FileMapper *missing = NULL;
    int fmerr2 = mmapqi_file_mapper_single_ro_create(
        MISSING_FILE, mmapqi_file_init_none, NULL, 1, &missing);
    TEST_ASSERT_EQUAL_INT(0, fmerr2);
    TEST_ASSERT_NOT_NULL(missing);

    uintptr_t addr = file_mapper_map(missing, 0, REGION_SIZE);
    TEST_ASSERT_EQUAL(FILE_MAPPER_FILE_NOT_FOUND, addr);

    file_mapper_close(missing);
    mmapqi_file_mapper_destroy(missing);
}

void test_ro_map_beyond_file_size_returns_sentinel(void)
{
    uintptr_t addr = file_mapper_map(fm, REGION_SIZE * 10, REGION_SIZE);
    TEST_ASSERT_EQUAL(FILE_MAPPER_BEYOND_FILE_SIZE, addr);
}

void test_ro_map_unmap_multiple_times(void)
{
    for (int i = 0; i < 10; i++) {
        uintptr_t addr = file_mapper_map(fm, 0, REGION_SIZE);
        TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);
        file_mapper_unmap(fm, addr, 0, REGION_SIZE);
    }
}

void test_ro_close_is_idempotent(void)
{
    file_mapper_close(fm);
    file_mapper_close(fm);  /* should not crash */
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_ro_map_returns_valid_address);
    RUN_TEST(test_ro_map_content_matches_file);
    RUN_TEST(test_ro_map_second_region);
    RUN_TEST(test_ro_map_missing_file_returns_file_not_found);
    RUN_TEST(test_ro_map_beyond_file_size_returns_sentinel);
    RUN_TEST(test_ro_map_unmap_multiple_times);
    RUN_TEST(test_ro_close_is_idempotent);
    return UNITY_END();
}
