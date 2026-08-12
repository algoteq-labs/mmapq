#include "unity.h"
#include "internal/file_initialiser.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>

static const char *TEST_FILE = "/tmp/mmap_test_init.dat";
static int test_fd = -1;

void setUp(void)
{
    test_fd = open(TEST_FILE, O_RDWR | O_CREAT | O_TRUNC,
                   S_IRUSR | S_IWUSR);
    TEST_ASSERT_GREATER_THAN(-1, test_fd);
}

void tearDown(void)
{
    if (test_fd >= 0) {
        close(test_fd);
        test_fd = -1;
    }
    unlink(TEST_FILE);
}

/* ── file_init_zeros ─────────────────────────────────────────────────────── */

void test_file_init_zeros_sets_file_size(void)
{
    int result = file_init_zeros(test_fd, 4096, NULL);
    TEST_ASSERT_EQUAL_INT(0, result);

    struct stat st;
    fstat(test_fd, &st);
    TEST_ASSERT_EQUAL(4096, st.st_size);
}

void test_file_init_zeros_multiple_pages(void)
{
    int result = file_init_zeros(test_fd, 4096 * 4, NULL);
    TEST_ASSERT_EQUAL_INT(0, result);

    struct stat st;
    fstat(test_fd, &st);
    TEST_ASSERT_EQUAL(4096 * 4, st.st_size);
}

void test_file_init_zeros_content_is_zero(void)
{
    size_t size = 4096;
    file_init_zeros(test_fd, size, NULL);

    /* Read back and verify zeros */
    lseek(test_fd, 0, SEEK_SET);
    char buf[4096] = {0};
    ssize_t n = read(test_fd, buf, size);
    TEST_ASSERT_EQUAL((ssize_t)size, n);

    for (size_t i = 0; i < size; i++) {
        TEST_ASSERT_EQUAL_INT(0, buf[i]);
    }
}

/* ── file_init_none ──────────────────────────────────────────────────────── */

void test_file_init_none_does_not_change_size(void)
{
    /* Write some data first */
    const char data[] = "hello";
    { ssize_t _w = write(test_fd, data, sizeof(data)); (void)_w; }

    struct stat st_before;
    fstat(test_fd, &st_before);

    int result = file_init_none(test_fd, 99999, NULL);
    TEST_ASSERT_EQUAL_INT(0, result);

    struct stat st_after;
    fstat(test_fd, &st_after);
    TEST_ASSERT_EQUAL(st_before.st_size, st_after.st_size);
}

void test_file_init_none_ignores_ctx(void)
{
    int ctx_value = 42;
    int result = file_init_none(test_fd, 4096, &ctx_value);
    TEST_ASSERT_EQUAL_INT(0, result);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_file_init_zeros_sets_file_size);
    RUN_TEST(test_file_init_zeros_multiple_pages);
    RUN_TEST(test_file_init_zeros_content_is_zero);
    RUN_TEST(test_file_init_none_does_not_change_size);
    RUN_TEST(test_file_init_none_ignores_ctx);
    return UNITY_END();
}
