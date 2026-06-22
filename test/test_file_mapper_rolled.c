#include "unity.h"
#include "file_mapper.h"
#include "file_initialiser.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdio.h>

static const char   *FILE_PREFIX  = "/tmp/mmap_test_rolled";
static const int32_t REGION_SIZE  = 4096;
static const int64_t MAX_FILE_SIZE = 4096 * 4;  /* 4 regions per file */
static const int     MAX_CACHE    = 4;

static FileMapper *fm_rw = NULL;
static FileMapper *fm_ro = NULL;

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
    fm_rw = file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        file_init_zeros, NULL, MAP_MODE_READ_WRITE, MAX_CACHE, 1);
}

void tearDown(void)
{
    if (fm_rw) { file_mapper_close(fm_rw); file_mapper_destroy(fm_rw); fm_rw = NULL; }
    if (fm_ro) { file_mapper_close(fm_ro); file_mapper_destroy(fm_ro); fm_ro = NULL; }
    cleanup_files();
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_rolled_map_first_region(void)
{
    uintptr_t addr = file_mapper_map(fm_rw, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS,     addr);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_FILE_NOT_FOUND,   addr);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_BEYOND_FILE_SIZE, addr);
    file_mapper_unmap(fm_rw, addr, 0, REGION_SIZE);
}

void test_rolled_map_spans_multiple_files(void)
{
    /* Write sequential values across file boundary */
    for (int i = 0; i < 6; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        uintptr_t addr = file_mapper_map(fm_rw, pos, REGION_SIZE);
        TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);
        *(int32_t *)addr = i;
        file_mapper_unmap(fm_rw, addr, pos, REGION_SIZE);
    }

    /* Read back — regions 0-3 in file_0, 4-5 in file_1 */
    fm_ro = file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        file_init_none, NULL, MAP_MODE_READ_ONLY, MAX_CACHE, 1);

    for (int i = 0; i < 6; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        uintptr_t addr = file_mapper_map(fm_ro, pos, REGION_SIZE);
        TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addr);
        TEST_ASSERT_EQUAL_INT(i, *(int32_t *)addr);
        file_mapper_unmap(fm_ro, addr, pos, REGION_SIZE);
    }
}

void test_rolled_missing_ro_file_returns_file_not_found(void)
{
    fm_ro = file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        file_init_none, NULL, MAP_MODE_READ_ONLY, MAX_CACHE, 1);

    uintptr_t addr = file_mapper_map(fm_ro, 0, REGION_SIZE);
    TEST_ASSERT_EQUAL(FILE_MAPPER_FILE_NOT_FOUND, addr);
}

void test_rolled_ref_count_prevents_premature_eviction(void)
{
    /* Map 4 regions simultaneously — all refs open */
    uintptr_t addrs[4];
    for (int i = 0; i < 4; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        addrs[i] = file_mapper_map(fm_rw, pos, REGION_SIZE);
        TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, addrs[i]);
    }

    /* All 4 still valid — unmap in order */
    for (int i = 0; i < 4; i++) {
        int64_t pos = (int64_t)i * REGION_SIZE;
        file_mapper_unmap(fm_rw, addrs[i], pos, REGION_SIZE);
    }
}

void test_rolled_close_is_idempotent(void)
{
    file_mapper_close(fm_rw);
    file_mapper_close(fm_rw);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_rolled_map_first_region);
    RUN_TEST(test_rolled_map_spans_multiple_files);
    RUN_TEST(test_rolled_missing_ro_file_returns_file_not_found);
    RUN_TEST(test_rolled_ref_count_prevents_premature_eviction);
    RUN_TEST(test_rolled_close_is_idempotent);
    return UNITY_END();
}
