#include "unity.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"

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
    /* CAND-28 pin: the file_index 2^32 file lives outside the loop range */
    snprintf(path, sizeof(path), "%s_%ld", FILE_PREFIX, (long)((int64_t)1 << 32));
    unlink(path);
}

void setUp(void)
{
    cleanup_files();
    int fmerr = mmapqi_file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        mmapqi_file_init_zeros, NULL, MAP_MODE_READ_WRITE, MAX_CACHE, 1, &fm_rw);
    TEST_ASSERT_EQUAL_INT(0, fmerr);
    TEST_ASSERT_NOT_NULL(fm_rw);
}

void tearDown(void)
{
    if (fm_rw) { file_mapper_close(fm_rw); mmapqi_file_mapper_destroy(fm_rw); fm_rw = NULL; }
    if (fm_ro) { file_mapper_close(fm_ro); mmapqi_file_mapper_destroy(fm_ro); fm_ro = NULL; }
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
    int fmerr3 = mmapqi_file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_ONLY, MAX_CACHE, 1, &fm_ro);
    TEST_ASSERT_EQUAL_INT(0, fmerr3);
    TEST_ASSERT_NOT_NULL(fm_ro);

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
    int fmerr4 = mmapqi_file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE_SIZE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_ONLY, MAX_CACHE, 1, &fm_ro);
    TEST_ASSERT_EQUAL_INT(0, fmerr4);
    TEST_ASSERT_NOT_NULL(fm_ro);

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

void test_mapper_rolled_file_index_not_truncated(void)
{
    /* CAND-28: position 2^32 * MAX_FILE_SIZE gives file_index exactly 2^32.
     * The rolled mapper's int file_index truncated it to 0, aliasing
     * <prefix>_0. The map must create the true-named file and leave file 0
     * undisturbed. Pre-fix: the true-named file does not exist. */
    const int64_t hi_pos = ((int64_t)1 << 32) * MAX_FILE_SIZE;

    uintptr_t hi = file_mapper_map(fm_rw, hi_pos, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, hi);
    *(int32_t *)hi = 0x11111111;
    file_mapper_unmap(fm_rw, hi, hi_pos, REGION_SIZE);

    uintptr_t lo = file_mapper_map(fm_rw, 0, REGION_SIZE);
    TEST_ASSERT_NOT_EQUAL(FILE_MAPPER_NULL_ADDRESS, lo);
    *(int32_t *)lo = 0x22222222;
    file_mapper_unmap(fm_rw, lo, 0, REGION_SIZE);

    char path[256];
    snprintf(path, sizeof(path), "%s_%ld", FILE_PREFIX, (long)((int64_t)1 << 32));
    struct stat st;
    TEST_ASSERT_EQUAL_INT(0, stat(path, &st));

    snprintf(path, sizeof(path), "%s_0", FILE_PREFIX);
    int fd = open(path, O_RDONLY);
    TEST_ASSERT_GREATER_OR_EQUAL(0, fd);
    int32_t marker = 0;
    TEST_ASSERT_EQUAL_INT((int)sizeof(marker),
        (int)read(fd, &marker, sizeof(marker)));
    TEST_ASSERT_EQUAL_INT(0, close(fd));
    TEST_ASSERT_EQUAL_INT32(0x22222222, marker);
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
    RUN_TEST(test_mapper_rolled_file_index_not_truncated);
    return UNITY_END();
}
