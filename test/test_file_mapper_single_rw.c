#include "unity.h"
#include "file_mapper.h"
#include "file_initialiser.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdint.h>

static const char   *TEST_FILE   = "/tmp/mmap_test_rw.dat";
static const int32_t REGION_SIZE = 4096;
static const int64_t MAX_SIZE    = 4096 * 16;

static FileMapper *fm = NULL;

void setUp(void)
{
    unlink(TEST_FILE);
    fm = file_mapper_single_rw_create(
        TEST_FILE, MAX_SIZE, file_init_zeros, NULL);
}

void tearDown(void)
{
    if (fm != NULL) {
        file_mapper_close(fm);
        file_mapper_destroy(fm);
        fm = NULL;
    }
    unlink(TEST_FILE);
}

/* ── map tests ───────────────────────────────────────────────────────────── */

void test_rw_map_creates_file(void)
{
    uintptr_t addr = file_mapper_map(fm, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);

    struct stat st;
    stat(TEST_FILE, &st);
    TEST_ASSERT_GREATER_OR_EQUAL(REGION_SIZE, st.st_size);

    file_mapper_unmap(fm, addr, 0, REGION_SIZE);
}

void test_rw_map_returns_valid_address(void)
{
    uintptr_t addr = file_mapper_map(fm, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS,     addr);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_FILE_NOT_FOUND,   addr);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_BEYOND_FILE_SIZE, addr);
    file_mapper_unmap(fm, addr, 0, REGION_SIZE);
}

void test_rw_map_write_persists(void)
{
    /* Write via mmap */
    uintptr_t addr = file_mapper_map(fm, 0, REGION_SIZE);
    uint64_t *p = (uint64_t *)addr;
    *p = 0xCAFEBABEDEADBEEFULL;
    file_mapper_unmap(fm, addr, 0, REGION_SIZE);
    file_mapper_close(fm);
    file_mapper_destroy(fm);
    fm = NULL;

    /* Read back with RO mapper */
    FileMapper *ro = file_mapper_single_ro_create(
        TEST_FILE, file_init_none, NULL, 0);
    uintptr_t ro_addr = file_mapper_map(ro, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, ro_addr);

    uint64_t *read_val = (uint64_t *)ro_addr;
    TEST_ASSERT_EQUAL_HEX64(0xCAFEBABEDEADBEEFULL, *read_val);

    file_mapper_unmap(ro, ro_addr, 0, REGION_SIZE);
    file_mapper_close(ro);
    file_mapper_destroy(ro);
}

void test_rw_map_extends_file_on_demand(void)
{
    /* Map second region — file must extend */
    uintptr_t addr = file_mapper_map(fm, REGION_SIZE, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);

    struct stat st;
    stat(TEST_FILE, &st);
    TEST_ASSERT_GREATER_OR_EQUAL(REGION_SIZE * 2, st.st_size);

    file_mapper_unmap(fm, addr, REGION_SIZE, REGION_SIZE);
}

void test_rw_map_sequential_regions(void)
{
    for (int i = 0; i < 4; i++) {
        int64_t pos  = (int64_t)i * REGION_SIZE;
        uintptr_t addr = file_mapper_map(fm, pos, REGION_SIZE);
        TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);

        /* Write region index at start of each region */
        *(int32_t *)addr = i;
        file_mapper_unmap(fm, addr, pos, REGION_SIZE);
    }

    /* Read back */
    FileMapper *ro = file_mapper_single_ro_create(
        TEST_FILE, file_init_none, NULL, 0);
    for (int i = 0; i < 4; i++) {
        int64_t pos  = (int64_t)i * REGION_SIZE;
        uintptr_t addr = file_mapper_map(ro, pos, REGION_SIZE);
        TEST_ASSERT_EQUAL_INT(i, *(int32_t *)addr);
        file_mapper_unmap(ro, addr, pos, REGION_SIZE);
    }
    file_mapper_close(ro);
    file_mapper_destroy(ro);
}

void test_rw_close_is_idempotent(void)
{
    file_mapper_close(fm);
    file_mapper_close(fm);  /* should not crash */
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_rw_map_creates_file);
    RUN_TEST(test_rw_map_returns_valid_address);
    RUN_TEST(test_rw_map_write_persists);
    RUN_TEST(test_rw_map_extends_file_on_demand);
    RUN_TEST(test_rw_map_sequential_regions);
    RUN_TEST(test_rw_close_is_idempotent);
    return UNITY_END();
}
