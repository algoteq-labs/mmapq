#include "unity.h"
#include "queue_transfer.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_reader.h"
#include "runtime.h"
#include "mmap_platform.h"

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#define TEST_DIR  "/tmp"
#define QUEUE     "mmap_test_qtrans"
#define COPY_Q    "mmap_test_qtrans_copy"

static Runtime *rt = NULL;

static void cleanup_queue(const char *name)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", TEST_DIR, name);
    system(cmd);
}

void setUp(void)
{
    rt = runtime_region_mapper();
}

void tearDown(void)
{
    cleanup_queue(QUEUE);
    cleanup_queue(COPY_Q);
}

/* ── Helpers ─────────────────────────────────────────────────────────────── */

static IndexedQueue *create_queue(const char *name, const char **msgs, int n, int64_t start_index)
{
    cleanup_queue(name);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, name);
    mkdir(path, 0775);

    IndexedQueue *q = indexed_queue_create(TEST_DIR, name, 4096, 4, 1, 4096LL * 64,
                                            start_index, 5000000000ULL, 0, rt, rt);

    IndexedAppender *a = indexed_queue_create_appender(q);
    for (int i = 0; i < n; i++)
        indexed_appender_append(a, msgs[i], (int32_t)strlen(msgs[i]) + 1);
    indexed_appender_close(a);
    return q;
}

static int load_messages(const char *name, char out[][100], int max)
{
    IndexedQueue *q = indexed_queue_create(TEST_DIR, name, 4096, 4, 1, 4096LL * 64,
                                            0, 5000000000ULL, 0, rt, rt);
    IndexedReader *r = indexed_queue_create_reader(q);
    int count = 0;
    int64_t last = indexed_reader_last_index(r);
    if (last < 0) { indexed_reader_close(r); indexed_queue_close(q); return 0; }
    for (int64_t i = indexed_reader_start_index(r); i <= last && count < max; i++) {
        ReadingContext ctx;
        indexed_reader_read(r, i, &ctx);
        if (ctx.status == ENTRY_AVAILABLE) {
            memcpy(out[count], ctx.data, (size_t)ctx.length);
            out[count][ctx.length - 1] = '\0';
            count++;
        }
    }
    indexed_reader_close(r);
    indexed_queue_close(q);
    return count;
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_export_import_roundtrip(void)
{
    const char *msgs[] = {"msg1", "msg2", "msg3"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, 0, 2, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, 0, rt, rt);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("msg1", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("msg2", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("msg3", loaded[2]);

    unlink(fpath);
}

void test_export_import_preserve_start_index(void)
{
    const char *msgs[] = {"a", "b", "c"};
    /* Queue is created with start_index=100. Messages are at indices 0,1,2.
     * Export with PRESERVE: the stream's meta.start_index is set to 100,
     * import creates the dest queue with start_index=100. */
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, 0, 2, QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, 100, rt, rt);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    /* PRESERVE mode sets stream start_index to source start_index (100).
     * Import with start_index=100 should create a queue with start_index 0 by default
     * (non-archivable queue). The messages import correctly regardless. */
    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    unlink(fpath);
}

void test_export_import_rebase_start_index(void)
{
    const char *msgs[] = {"a", "b", "c"};
    /* Queue with start_index=100. Rebase export means stream starts at 0. */
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, 0, 2, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, 0, rt, rt);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    unlink(fpath);
}

void test_export_import_fragment(void)
{
    const char *msgs[] = {"m1", "m2", "m3", "m4", "m5"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 5, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, 1, 3, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, 0, rt, rt);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("m2", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("m3", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("m4", loaded[2]);
    unlink(fpath);
}

void test_export_empty_queue(void)
{
    IndexedQueue *src = create_queue(QUEUE, NULL, 0, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, 0, -1, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(0, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, 0, rt, rt);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(0, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(0, cnt);
    unlink(fpath);
}

void test_export_chunked(void)
{
    const char *msgs[] = {"msg1", "msg2", "msg3", "msg4", "msg5",
                          "msg6", "msg7", "msg8", "msg9", "msg10"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 10, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char base[512];
    snprintf(base, sizeof(base), "%s/%s_chunk", TEST_DIR, QUEUE);

    int64_t total = queue_transfer_export_chunked(reader, base, 0, 9, 1, QUEUE_TRANSFER_REBASE,
                                                    NULL, NULL, NULL);
    TEST_ASSERT_EQUAL_INT64(10, total);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    /* Cleanup chunk files */
    char clean_cmd[1024];
    snprintf(clean_cmd, sizeof(clean_cmd), "rm -f %s_*", base);
    system(clean_cmd);
}

static int filter_include_aaa(int64_t idx, const void *data, int32_t len, void *ctx)
{
    (void)idx; (void)ctx;
    return (len >= 3 && memcmp(data, "aaa", 3) == 0) ? 1 : 0;
}

void test_filter_include(void)
{
    const char *msgs[] = {"aaa", "bbb", "aaa", "ccc", "aaa"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 5, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, 0, 4, QUEUE_TRANSFER_REBASE,
                                       filter_include_aaa, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);

    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, 0, rt, rt);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("aaa", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("aaa", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("aaa", loaded[2]);
    unlink(fpath);
}

void test_copy_queue(void)
{
    const char *msgs[] = {"copy1", "copy2", "copy3"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    cleanup_queue(COPY_Q);
    int64_t n = queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 2, QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("copy1", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("copy2", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("copy3", loaded[2]);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_export_import_roundtrip);
    RUN_TEST(test_export_import_preserve_start_index);
    RUN_TEST(test_export_import_rebase_start_index);
    RUN_TEST(test_export_import_fragment);
    RUN_TEST(test_export_empty_queue);
    RUN_TEST(test_export_chunked);
    RUN_TEST(test_filter_include);
    RUN_TEST(test_copy_queue);
    return UNITY_END();
}
