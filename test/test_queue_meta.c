#include "unity.h"
#include "internal/queue_meta.h"

#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>

static const char *DIR   = "/tmp";
static const char *QUEUE = "mmap_test_qmeta_q";
static QueueMeta  *meta  = NULL;
static int qmerr;

static void cleanup(void)
{
    char path[256];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    unlink(path);
    snprintf(path, sizeof(path), "%s/%s", DIR, QUEUE);
    rmdir(path);
}

void setUp(void)
{
    cleanup();
    mkdir("/tmp/mmap_test_qmeta_q", 0775);
}

void tearDown(void)
{
    if (meta) { mmapqi_queue_meta_close(meta); meta = NULL; }
    cleanup();
}

void test_open_rw_creates_file(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, 4096 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_NOT_NULL(meta);

    char path[256];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    struct stat st;
    TEST_ASSERT_EQUAL_INT(0, stat(path, &st));
    TEST_ASSERT_GREATER_OR_EQUAL(QUEUE_META_ENCODED_LENGTH, st.st_size);
}

void test_open_rw_stores_region_size(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 8192, 8192 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT32(8192, queue_meta_region_size(meta));
}

void test_open_rw_stores_max_file_size(void)
{
    int64_t mfs = 4096LL * 512;
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, mfs, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT64(mfs, queue_meta_max_file_size(meta));
}

void test_open_rw_schema_version(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, 4096 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT32(QUEUE_META_SCHEMA_VERSION, queue_meta_schema_version(meta));
}

void test_open_ro_returns_null_when_missing(void)
{
    QueueMeta *ro = NULL;
    int roerr = mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOENT, roerr);
    TEST_ASSERT_NULL(ro);
}

void test_open_ro_reads_existing_file(void)
{
    int qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, 4096 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    mmapqi_queue_meta_close(meta);
    meta = NULL;

    QueueMeta *ro = NULL;
    int roerr = mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro);
    TEST_ASSERT_EQUAL_INT(0, roerr);
    TEST_ASSERT_NOT_NULL(ro);
    TEST_ASSERT_EQUAL_INT32(4096, queue_meta_region_size(ro));
    mmapqi_queue_meta_close(ro);
}

void test_start_index_zero_non_archivable(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, 4096 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT64(0, queue_meta_start_index(meta));
}

void test_archivable_false(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, 4096 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT(0, queue_meta_archivable(meta));
}

void test_reopen_rw_preserves_values(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 4096, 4096 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    mmapqi_queue_meta_close(meta);

    /* Re-open with different params — existing values should be preserved */
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 8192, 8192 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    /* File already existed — region_size from file is 4096, not 8192 */
    TEST_ASSERT_EQUAL_INT32(4096, queue_meta_region_size(meta));
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_open_rw_creates_file);
    RUN_TEST(test_open_rw_stores_region_size);
    RUN_TEST(test_open_rw_stores_max_file_size);
    RUN_TEST(test_open_rw_schema_version);
    RUN_TEST(test_open_ro_returns_null_when_missing);
    RUN_TEST(test_open_ro_reads_existing_file);
    RUN_TEST(test_start_index_zero_non_archivable);
    RUN_TEST(test_archivable_false);
    RUN_TEST(test_reopen_rw_preserves_values);
    return UNITY_END();
}
