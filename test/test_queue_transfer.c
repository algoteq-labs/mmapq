#include "unity.h"
#include "queue_transfer.h"
#include "batch_queue_transfer.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_poller.h"
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
#define BQ_QUEUE  "mmap_test_bqtrans"
#define BQ_COPY   "mmap_test_bqtrans_copy"
#define CROSS_Q   "mmap_test_cross"

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
    cleanup_queue(BQ_QUEUE);
    cleanup_queue(BQ_COPY);
    cleanup_queue(CROSS_Q);
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

static void verify_batch_index_space(const char *name, int64_t expected_start, int64_t expected_end)
{
    IndexedBatchQueue *vq = indexed_batch_queue_create(TEST_DIR, name, 4096, 4, 1, 4096LL * 64,
                                                         0, 5000000000ULL, 0, rt, rt);
    IndexedBatchReader *vr = indexed_batch_queue_create_reader(vq);
    int64_t si = indexed_batch_reader_start_index(vr);
    int64_t li = indexed_batch_reader_last_index(vr);
    TEST_ASSERT_EQUAL_INT64(expected_start, si);
    TEST_ASSERT_EQUAL_INT64(expected_end, li);

    BatchReadingContext vctx;
    indexed_batch_reader_read(vr, expected_start, &vctx);
    TEST_ASSERT_EQUAL(BATCH_ENTRY_AVAILABLE, vctx.status);

    if (expected_start > 0) {
        indexed_batch_reader_read(vr, expected_start - 1, &vctx);
        TEST_ASSERT_EQUAL(BATCH_ENTRY_ARCHIVED, vctx.status);
    }

    indexed_batch_reader_close(vr);
    indexed_batch_queue_close(vq);
}

static void verify_index_space(const char *name, int64_t expected_start, int64_t expected_end)
{
    IndexedQueue *vq = indexed_queue_create(TEST_DIR, name, 4096, 4, 1, 4096LL * 64,
                                              expected_start, 5000000000ULL, 0, rt, rt);
    IndexedReader *vr = indexed_queue_create_reader(vq);
    TEST_ASSERT_EQUAL_INT64(expected_start, indexed_reader_start_index(vr));
    TEST_ASSERT_EQUAL_INT64(expected_end, indexed_reader_last_index(vr));

    ReadingContext vctx;
    indexed_reader_read(vr, expected_start, &vctx);
    TEST_ASSERT_EQUAL(ENTRY_AVAILABLE, vctx.status);

    if (expected_start > 0) {
        indexed_reader_read(vr, expected_start - 1, &vctx);
        TEST_ASSERT_EQUAL(ENTRY_ARCHIVED, vctx.status);
    }

    indexed_reader_close(vr);
    indexed_queue_close(vq);
}

static IndexedBatchQueue *create_batch_queue(const char *name, const char *batches[][3],
                                               int batch_sizes[], int nb)
{
    cleanup_queue(name);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, name);
    mkdir(path, 0775);

    IndexedBatchQueue *q = indexed_batch_queue_create(TEST_DIR, name, 4096, 4, 1, 4096LL * 64,
                                                       0, 5000000000ULL, 0, rt, rt);
    IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
    for (int b = 0; b < nb; b++) {
        MutableBatch *mb = indexed_batch_appender_start_batch(a);
        for (int m = 0; m < batch_sizes[b]; m++) {
            mutable_batch_append(mb, batches[b][m], (int32_t)strlen(batches[b][m]) + 1);
        }
        int64_t off = mutable_batch_complete(mb);
        indexed_batch_appender_commit_batch(a, off);
    }
    indexed_batch_appender_close(a);
    return q;
}

/* ── Same-type transfer tests ────────────────────────────────────────────── */

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
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
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
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);

    /* Messages are at indices 100, 101, 102 */
    int64_t actual_start = indexed_reader_start_index(reader);
    TEST_ASSERT_EQUAL_INT64(100, actual_start);
    int64_t actual_end   = indexed_reader_last_index(reader);
    TEST_ASSERT_EQUAL_INT64(102, actual_end);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    /* Export the range the queue actually has */
    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, actual_start, actual_end,
                                       QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    /* Import with PRESERVE: new queue starts at index 100 */
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q,
                                              QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    /* Verify payloads */
    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);

    /* Verify index space is preserved */
    IndexedQueue *vq = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                              actual_start, 5000000000ULL, 0, rt, rt);
    IndexedReader *vr = indexed_queue_create_reader(vq);
    TEST_ASSERT_EQUAL_INT64(actual_start, indexed_reader_start_index(vr));
    TEST_ASSERT_EQUAL_INT64(actual_end, indexed_reader_last_index(vr));
    ReadingContext vctx;
    indexed_reader_read(vr, actual_start, &vctx);
    TEST_ASSERT_EQUAL(ENTRY_AVAILABLE, vctx.status);
    indexed_reader_read(vr, actual_start - 1, &vctx);
    TEST_ASSERT_EQUAL(ENTRY_ARCHIVED, vctx.status);
    indexed_reader_close(vr);
    indexed_queue_close(vq);

    unlink(fpath);
}

static NextMove poll_handler(int64_t index, uint8_t aid, const void *data,
                              int32_t length, void *ctx)
{
    (void)aid; (void)data; (void)length;
    if (ctx) *(int64_t *)ctx = index;
    return NEXT_ADVANCE;
}

void test_poller_starts_at_preserved_index(void)
{
    const char *msgs[] = {"a", "b", "c"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);
    int64_t si = indexed_reader_start_index(reader);
    int64_t ei = indexed_reader_last_index(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, si, ei, QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);

    /* Now poll the imported queue — first entry should be at index 100 */
    IndexedQueue *iq = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                              si, 5000000000ULL, 0, rt, rt);
    IndexedPoller *p = indexed_queue_create_poller(iq);
    /* Three messages at indices 100, 101, 102 */
    {
        int64_t polled_idx = -1;
        for (int64_t expected = 100; expected <= 102; expected++) {
            PollResult r = indexed_poller_poll(p, poll_handler, &polled_idx);
            TEST_ASSERT_EQUAL(POLL_ADVANCED, r);
            TEST_ASSERT_EQUAL_INT64(expected, polled_idx);
            polled_idx = -1;
        }
        /* Fourth poll should be EOS */
        PollResult r = indexed_poller_poll(p, poll_handler, &polled_idx);
        TEST_ASSERT_EQUAL(POLL_END_OF_STREAM, r);
    }

    indexed_poller_close(p);
    indexed_queue_close(iq);
    unlink(fpath);
}

void test_export_import_rebase_start_index(void)
{
    const char *msgs[] = {"a", "b", "c"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    int64_t si = indexed_reader_start_index(reader);
    int64_t ei = indexed_reader_last_index(reader);
    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(reader, out, si, ei, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);

    IndexedQueue *vq = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                              0, 5000000000ULL, 0, rt, rt);
    IndexedReader *vr = indexed_queue_create_reader(vq);
    TEST_ASSERT_EQUAL_INT64(0, indexed_reader_start_index(vr));
    indexed_reader_close(vr);
    indexed_queue_close(vq);

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
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
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
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
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

    char clean_cmd[1024];
    snprintf(clean_cmd, sizeof(clean_cmd), "rm -f %s_*", base);
    system(clean_cmd);
}

static int32_t map_to_zero(uint16_t aid, void *ctx)
{
    (void)aid; (void)ctx;
    return 0;
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
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
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
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
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

/* ── Phase 0: allowAppend — Normal import scenarios ──────────────────────── */

void test_import_to_new(void)
{
    const char *msgs[] = {"a", "b"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, 0, 1, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    unlink(fpath);
}

void test_import_rebase_append(void)
{
    const char *msgs[] = {"first", "second"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, 0, 1, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);

    /* Create destination queue with data first */
    cleanup_queue(COPY_Q);
    const char *existing[] = {"existing"};
    IndexedQueue *existing_q = create_queue(COPY_Q, existing, 1, 0);
    indexed_queue_close(existing_q);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("existing", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("first", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("second", loaded[2]);

    indexed_reader_close(reader);
    indexed_queue_close(src);
    unlink(fpath);
}

void test_import_preserve_append(void)
{
    const char *msgs[] = {"one"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, 0, 0, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);

    cleanup_queue(COPY_Q);
    const char *existing[] = {"existing"};
    IndexedQueue *existing_q = create_queue(COPY_Q, existing, 1, 0);
    indexed_queue_close(existing_q);

    FILE *in = fopen(fpath, "rb");
    /* PRESERVE + allow_append=1 on existing queue → EINVAL */
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, imported);

    indexed_reader_close(reader);
    indexed_queue_close(src);
    unlink(fpath);
}

void test_import_no_append(void)
{
    const char *msgs[] = {"x"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, 0, 0, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);

    cleanup_queue(COPY_Q);
    const char *existing[] = {"existing"};
    IndexedQueue *existing_q = create_queue(COPY_Q, existing, 1, 0);
    indexed_queue_close(existing_q);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);

    indexed_reader_close(reader);
    indexed_queue_close(src);
    unlink(fpath);
}

/* ── Phase 0: allowAppend — Normal copy scenarios ────────────────────────── */

void test_copy_to_new_rebase(void)
{
    const char *msgs[] = {"a", "b", "c"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    cleanup_queue(COPY_Q);
    int64_t n = queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 2, QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_copy_to_new_preserve(void)
{
    const char *msgs[] = {"x", "y"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 2, 10);
    IndexedReader *reader = indexed_queue_create_reader(src);

    /* Messages at indices 10, 11 */
    int64_t si = indexed_reader_start_index(reader);
    int64_t ei = indexed_reader_last_index(reader);

    cleanup_queue(COPY_Q);
    /* Copy with PRESERVE — dest starts at source start_index (10) */
    int64_t n = queue_transfer_copy(reader, TEST_DIR, COPY_Q, si, ei,
                                     QUEUE_TRANSFER_PRESERVE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(2, cnt);

    /* Verify index space is preserved */
    verify_index_space(COPY_Q, si, ei);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_copy_rebase_append(void)
{
    const char *msgs[] = {"new1", "new2"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    const char *existing[] = {"old"};
    IndexedQueue *dest = create_queue(COPY_Q, existing, 1, 0);
    indexed_queue_close(dest);

    int64_t n = queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 1, QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("old", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("new1", loaded[1]);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_copy_preserve_append(void)
{
    const char *msgs[] = {"m"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    const char *existing[] = {"old"};
    IndexedQueue *dest = create_queue(COPY_Q, existing, 1, 0);
    indexed_queue_close(dest);

    int64_t n = queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 0, QUEUE_TRANSFER_PRESERVE,
                                     NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, n);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_copy_no_append(void)
{
    const char *msgs[] = {"m"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    const char *existing[] = {"old"};
    IndexedQueue *dest = create_queue(COPY_Q, existing, 1, 0);
    indexed_queue_close(dest);

    int64_t n = queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 0, QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, n);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

/* ── Phase 0: allowAppend — Batch import scenarios ────────────────────────── */

void test_batch_import_to_new(void)
{
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1"}};
    int sizes[] = {2, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 1, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    unlink(fpath);
}

void test_batch_import_rebase_append(void)
{
    const char *batches[][3] = {{"new_b1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 0, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    const char *existing[][3] = {{"old_b1"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *dest = create_batch_queue(BQ_COPY, existing, existing_sizes, 1);
    indexed_batch_queue_close(dest);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    unlink(fpath);
}

void test_batch_import_preserve_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 0, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    IndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    indexed_batch_queue_close(dest);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, imported);
    unlink(fpath);
}

void test_batch_import_no_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 0, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    IndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    indexed_batch_queue_close(dest);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);
    unlink(fpath);
}

/* ── Phase 0: allowAppend — Batch copy scenarios ─────────────────────────── */

void test_batch_copy_to_new(void)
{
    const char *batches[][3] = {{"b1m1"}, {"b2m1"}};
    int sizes[] = {1, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(BQ_COPY);
    int64_t n = batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 1, BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_batch_copy_rebase_append(void)
{
    const char *batches[][3] = {{"new_b1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(BQ_COPY);
    const char *existing[][3] = {{"old_b1"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *dest = create_batch_queue(BQ_COPY, existing, existing_sizes, 1);
    indexed_batch_queue_close(dest);

    int64_t n = batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_batch_copy_preserve_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(BQ_COPY);
    IndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    indexed_batch_queue_close(dest);

    int64_t n = batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, BATCH_QUEUE_TRANSFER_PRESERVE,
                                           NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_batch_copy_no_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(BQ_COPY);
    IndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    indexed_batch_queue_close(dest);

    int64_t n = batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

/* ── Cross-type import: batch stream → normal (flatten) ──────────────────── */

static void batch_to_normal_export(int64_t *out_total)
{
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1"}};
    int sizes[] = {2, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    *out_total = batch_queue_transfer_export(reader, out, 0, 1, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_import_batch_stream_to_new(void)
{
    int64_t total;
    batch_to_normal_export(&total);
    TEST_ASSERT_EQUAL_INT64(2, total);

    cleanup_queue(CROSS_Q);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(CROSS_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    unlink(fpath);
}

void test_import_batch_stream_rebase_append(void)
{
    int64_t total;
    batch_to_normal_export(&total);

    cleanup_queue(CROSS_Q);
    const char *existing[] = {"existing"};
    IndexedQueue *eq = create_queue(CROSS_Q, existing, 1, 0);
    indexed_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(CROSS_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(4, cnt);
    TEST_ASSERT_EQUAL_STRING("existing", loaded[0]);
    unlink(fpath);
}

void test_import_batch_stream_preserve_append(void)
{
    int64_t total;
    batch_to_normal_export(&total);

    cleanup_queue(CROSS_Q);
    const char *existing[] = {"existing"};
    IndexedQueue *eq = create_queue(CROSS_Q, existing, 1, 0);
    indexed_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, imported);
    unlink(fpath);
}

void test_import_batch_stream_no_append(void)
{
    int64_t total;
    batch_to_normal_export(&total);

    cleanup_queue(CROSS_Q);
    const char *existing[] = {"existing"};
    IndexedQueue *eq = create_queue(CROSS_Q, existing, 1, 0);
    indexed_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);
    unlink(fpath);
}

/* ── Cross-type import: flat stream → batch (1-msg batches) ───────────────── */

static void flat_to_batch_export(int64_t *out_total)
{
    const char *msgs[] = {"m1", "m2", "m3"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    *out_total = queue_transfer_export(reader, out, 0, 2, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_import_flat_to_batch_to_new(void)
{
    int64_t total;
    flat_to_batch_export(&total);
    TEST_ASSERT_EQUAL_INT64(3, total);

    cleanup_queue(CROSS_Q);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);
    unlink(fpath);
}

void test_import_flat_to_batch_rebase_append(void)
{
    int64_t total;
    flat_to_batch_export(&total);

    cleanup_queue(CROSS_Q);
    const char *existing[][3] = {{"existing"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *eq = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    indexed_batch_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);
    unlink(fpath);
}

void test_import_flat_to_batch_preserve_append(void)
{
    int64_t total;
    flat_to_batch_export(&total);

    cleanup_queue(CROSS_Q);
    const char *existing[][3] = {{"existing"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *eq = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    indexed_batch_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, imported);
    unlink(fpath);
}

void test_import_flat_to_batch_no_append(void)
{
    int64_t total;
    flat_to_batch_export(&total);

    cleanup_queue(CROSS_Q);
    const char *existing[][3] = {{"existing"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *eq = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    indexed_batch_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);
    unlink(fpath);
}

/* ── Phase 1: Cross-type copy ────────────────────────────────────────────── */

void test_copy_batch_to_normal_flatten(void)
{
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1", "b2m2", "b2m3"}, {"b3m1"}};
    int sizes[] = {2, 3, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 3);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 2,
                                                      QUEUE_TRANSFER_REBASE,
                                                      NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(6, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    char loaded[10][100];
    int cnt = load_messages(CROSS_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(6, cnt);
    TEST_ASSERT_EQUAL_STRING("b1m1", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("b1m2", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("b2m1", loaded[2]);
    TEST_ASSERT_EQUAL_STRING("b2m2", loaded[3]);
    TEST_ASSERT_EQUAL_STRING("b2m3", loaded[4]);
    TEST_ASSERT_EQUAL_STRING("b3m1", loaded[5]);
}

void test_copy_normal_to_batch_1msg(void)
{
    const char *msgs[] = {"msg1", "msg2", "msg3", "msg4", "msg5"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 5, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 4,
                                                            BATCH_QUEUE_TRANSFER_REBASE,
                                                            NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(5, n);

    indexed_reader_close(reader);
    indexed_queue_close(src);

    /* Verify by reading back the batch queue */
    IndexedBatchQueue *q = indexed_batch_queue_create(TEST_DIR, CROSS_Q, 4096, 4, 1, 4096LL * 64,
                                                        0, 5000000000ULL, 0, rt, rt);
    IndexedBatchReader *br = indexed_batch_queue_create_reader(q);
    int64_t last = indexed_batch_reader_last_index(br);
    TEST_ASSERT_EQUAL_INT64(4, last);

    char buf[100];
    for (int64_t i = 0; i <= last; i++) {
        BatchReadingContext ctx;
        indexed_batch_reader_read(br, i, &ctx);
        TEST_ASSERT_EQUAL(BATCH_ENTRY_AVAILABLE, ctx.status);
        TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
        batch_iterator_init(&ctx.batch_iterator, ctx.batch_offset, ctx.message_count, 0);
        TEST_ASSERT(batch_iterator_advance(&ctx.batch_iterator));
        int32_t l = batch_iterator_current_length(&ctx.batch_iterator);
        memcpy(buf, batch_iterator_current_data(&ctx.batch_iterator), (size_t)l);
        buf[l - 1] = '\0';
        char expected[24];
        snprintf(expected, sizeof(expected), "msg%ld", (long)(i + 1));
        TEST_ASSERT_EQUAL_STRING(expected, buf);
    }
    indexed_batch_reader_close(br);
    indexed_batch_queue_close(q);
}

void test_copy_cross_type_roundtrip(void)
{
    const char *msgs[] = {"a", "b", "c"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    /* normal → batch (1-msg batches) */
    cleanup_queue(CROSS_Q);
    int64_t n1 = batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 2,
                                                             BATCH_QUEUE_TRANSFER_REBASE,
                                                             NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n1);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    /* batch → normal (flatten back) */
    IndexedBatchQueue *bq = indexed_batch_queue_create(TEST_DIR, CROSS_Q, 4096, 4, 1, 4096LL * 64,
                                                         0, 5000000000ULL, 0, rt, rt);
    IndexedBatchReader *br = indexed_batch_queue_create_reader(bq);

    cleanup_queue(COPY_Q);
    int64_t n2 = queue_transfer_copy_batch_to_normal(br, TEST_DIR, COPY_Q, 0, 2,
                                                       QUEUE_TRANSFER_REBASE,
                                                       NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n2);
    indexed_batch_reader_close(br);
    indexed_batch_queue_close(bq);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("a", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("b", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("c", loaded[2]);
}

void test_copy_batch_to_normal_append(void)
{
    const char *batches[][3] = {{"new_msg"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    const char *existing[] = {"old_msg"};
    IndexedQueue *dest = create_queue(CROSS_Q, existing, 1, 0);
    indexed_queue_close(dest);

    int64_t n = queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                      QUEUE_TRANSFER_REBASE,
                                                      NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    char loaded[10][100];
    int cnt = load_messages(CROSS_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(2, cnt);
    TEST_ASSERT_EQUAL_STRING("old_msg", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("new_msg", loaded[1]);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_copy_batch_to_normal_no_append(void)
{
    const char *batches[][3] = {{"m"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    const char *existing[] = {"old"};
    IndexedQueue *dest = create_queue(CROSS_Q, existing, 1, 0);
    indexed_queue_close(dest);

    int64_t n = queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                      QUEUE_TRANSFER_REBASE,
                                                      NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_copy_normal_to_batch_append(void)
{
    const char *msgs[] = {"new_msg"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    const char *existing[][3] = {{"old_batch"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *dest = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    indexed_batch_queue_close(dest);

    int64_t n = batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                             BATCH_QUEUE_TRANSFER_REBASE,
                                                             NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    /* Verify dest batch queue has both old and new batches */
    verify_batch_index_space(CROSS_Q, 0, 1);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_copy_normal_to_batch_preserve_append(void)
{
    const char *msgs[] = {"m"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    const char *existing[][3] = {{"old"}};
    int existing_sizes[] = {1};
    IndexedBatchQueue *dest = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    indexed_batch_queue_close(dest);

    int64_t n = batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                            BATCH_QUEUE_TRANSFER_PRESERVE,
                                                            NULL, NULL, NULL, rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, n);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

static int filter_first_msg_of_batch(int64_t batch_idx, int32_t msg_idx,
                                      int32_t msg_cnt, const void *data,
                                      int32_t length, void *ctx)
{
    (void)batch_idx; (void)msg_cnt; (void)data; (void)length; (void)ctx;
    return (msg_idx == 0) ? 1 : 0;
}

void test_copy_batch_to_normal_filter(void)
{
    /* 3 batches: 2, 3, and 1 messages each. Filter first msg of each batch. */
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1", "b2m2", "b2m3"}, {"b3m1"}};
    int sizes[] = {2, 3, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 3);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 2,
                                                      QUEUE_TRANSFER_REBASE,
                                                      filter_first_msg_of_batch, NULL, NULL,
                                                      rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    /* Only 3 messages should match (first msg of each batch) */
    TEST_ASSERT_EQUAL_INT64(3, n);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    char loaded[10][100];
    int cnt = load_messages(CROSS_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("b1m1", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("b2m1", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("b3m1", loaded[2]);
}

/* ── PRESERVE mode — comprehensive coverage ──────────────────────────────── */

void test_batch_import_preserve_to_new(void)
{
    const char *batches[][3] = {{"b1m1"}, {"b2m1"}};
    int sizes[] = {1, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 1, BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    verify_batch_index_space(CROSS_Q, 0, 1);
    unlink(fpath);
}

void test_import_batch_stream_preserve_to_new(void)
{
    const char *batches[][3] = {{"f1", "f2"}};
    int sizes[] = {2};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 0, BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    verify_index_space(CROSS_Q, 0, 1);
    unlink(fpath);
}

void test_import_flat_to_batch_preserve_to_new(void)
{
    const char *msgs[] = {"x", "y", "z"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, 0, 2, QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);
    verify_batch_index_space(CROSS_Q, 0, 2);
    unlink(fpath);
}

void test_batch_copy_preserve_to_new(void)
{
    const char *batches[][3] = {{"m1"}, {"m2"}};
    int sizes[] = {1, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = batch_queue_transfer_copy(reader, TEST_DIR, CROSS_Q, 0, 1, BATCH_QUEUE_TRANSFER_PRESERVE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);
    verify_batch_index_space(CROSS_Q, 0, 1);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_copy_batch_to_normal_preserve(void)
{
    const char *batches[][3] = {{"a1", "a2"}, {"b1"}};
    int sizes[] = {2, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 1,
                                                      QUEUE_TRANSFER_PRESERVE,
                                                      NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n);
    verify_index_space(CROSS_Q, 0, 2);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
}

void test_copy_normal_to_batch_preserve(void)
{
    const char *msgs[] = {"p1", "p2"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    IndexedReader *reader = indexed_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 1,
                                                            BATCH_QUEUE_TRANSFER_PRESERVE,
                                                            NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);
    verify_batch_index_space(CROSS_Q, 0, 1);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_batch_export_preserve(void)
{
    const char *batches[][3] = {{"m1"}};
    int sizes[] = {1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    int64_t n = batch_queue_transfer_export(reader, out, 0, 0, BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(1, n);

    /* Verify by importing back and checking index space */
    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    verify_batch_index_space(CROSS_Q, 0, 0);

    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);
    unlink(fpath);
}

void test_batch_export_import_preserve_roundtrip(void)
{
    const char *batches[][3] = {{"m1"}, {"m2"}};
    int sizes[] = {1, 1};
    IndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    IndexedBatchReader *reader = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(reader, out, 0, 1, BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(reader);
    indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    verify_batch_index_space(CROSS_Q, 0, 1);
    unlink(fpath);
}

/* ── Cross-type PRESERVE with non-zero start_index ──────────────────────── */

void test_copy_batch_to_normal_preserve_nonzero(void)
{
    /* Source batch queue with start_index=100, one batch at index 100 */
    cleanup_queue(BQ_QUEUE);
    mkdir("/tmp/mmap_test_bqtrans", 0775);
    IndexedBatchQueue *src = indexed_batch_queue_create(
        TEST_DIR, BQ_QUEUE, 4096, 4, 1, 4096LL * 64,
        100, 5000000000ULL, 0, rt, rt);
    IndexedBatchAppender *ba = indexed_batch_queue_create_appender(src);
    MutableBatch *mb = indexed_batch_appender_start_batch(ba);
    mutable_batch_append(mb, "hello", 6);
    int64_t off = mutable_batch_complete(mb);
    indexed_batch_appender_commit_batch(ba, off);
    indexed_batch_appender_close(ba);

    IndexedBatchReader *br = indexed_batch_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = queue_transfer_copy_batch_to_normal(
        br, TEST_DIR, CROSS_Q, 100, 100, QUEUE_TRANSFER_PRESERVE,
        NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    verify_index_space(CROSS_Q, 100, 100);

    indexed_batch_reader_close(br);
    indexed_batch_queue_close(src);
}

void test_copy_normal_to_batch_preserve_nonzero(void)
{
    /* Source normal queue with start_index=100, one message at index 100 */
    const char *msgs[] = {"world"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);

    cleanup_queue(CROSS_Q);
    int64_t n = batch_queue_transfer_copy_normal_to_batch(
        reader, TEST_DIR, CROSS_Q, 100, 100, BATCH_QUEUE_TRANSFER_PRESERVE,
        NULL, NULL, NULL, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    verify_batch_index_space(CROSS_Q, 100, 100);

    indexed_reader_close(reader);
    indexed_queue_close(src);
}

void test_import_batch_stream_preserve_nonzero(void)
{
    /* Source batch queue with start_index=100 */
    cleanup_queue(BQ_QUEUE);
    mkdir("/tmp/mmap_test_bqtrans", 0775);
    IndexedBatchQueue *src = indexed_batch_queue_create(
        TEST_DIR, BQ_QUEUE, 4096, 4, 1, 4096LL * 64,
        100, 5000000000ULL, 0, rt, rt);
    IndexedBatchAppender *ba = indexed_batch_queue_create_appender(src);
    MutableBatch *mb = indexed_batch_appender_start_batch(ba);
    mutable_batch_append(mb, "test", 5);
    int64_t batch_off = mutable_batch_complete(mb);
    indexed_batch_appender_commit_batch(ba, batch_off);
    indexed_batch_appender_close(ba);

    IndexedBatchReader *br = indexed_batch_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    batch_queue_transfer_export(br, out, 100, 100, BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_batch_reader_close(br);
    indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    verify_index_space(CROSS_Q, 100, 100);
    unlink(fpath);
}

void test_import_flat_to_batch_preserve_nonzero(void)
{
    /* Source normal queue with start_index=100 */
    const char *msgs[] = {"test"};
    IndexedQueue *src = create_queue(QUEUE, msgs, 1, 100);
    IndexedReader *reader = indexed_queue_create_reader(src);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(reader, out, 100, 100, QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(reader);
    indexed_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    verify_batch_index_space(CROSS_Q, 100, 100);
    unlink(fpath);
}

/* ── Batch appender ID collector ────────────────────────────────────────── */

typedef struct {
    uint8_t ids[256];
    int count;
} BatchIdCollector;

static NextMove batch_id_handler(int64_t idx, BatchReadingContext *ctx, void *data)
{
    (void)idx;
    BatchIdCollector *c = (BatchIdCollector *)data;
    if (c->count < 256) c->ids[c->count++] = ctx->appender_id;
    return NEXT_ADVANCE;
}

void test_batch_import_preserves_appender_ids(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    IndexedBatchQueue *q = indexed_batch_queue_create(
        TEST_DIR, QUEUE, 4096, 4, 1, 4096LL * 64,
        0, 5000000000ULL, 0, rt, rt);
    IndexedBatchAppender *a0 = indexed_batch_queue_create_appender(q);
    IndexedBatchAppender *a1 = indexed_batch_queue_create_appender(q);
    uint8_t id0 = indexed_batch_appender_id(a0);
    uint8_t id1 = indexed_batch_appender_id(a1);
    TEST_ASSERT(id0 != id1);

    MutableBatch *mb = indexed_batch_appender_start_batch(a0);
    mutable_batch_append(mb, "a0-msg", 7);
    indexed_batch_appender_commit_batch(a0, mutable_batch_complete(mb));

    mb = indexed_batch_appender_start_batch(a1);
    mutable_batch_append(mb, "a1-msg", 7);
    indexed_batch_appender_commit_batch(a1, mutable_batch_complete(mb));

    indexed_batch_appender_close(a0);
    indexed_batch_appender_close(a1);

    IndexedBatchReader *r = indexed_batch_queue_create_reader(q);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    int64_t n = batch_queue_transfer_export(r, out, 0, 1, BATCH_QUEUE_TRANSFER_REBASE,
                                             NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(2, n);
    indexed_batch_reader_close(r);
    indexed_batch_queue_close(q);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    n = batch_queue_transfer_import(in, TEST_DIR, COPY_Q, BATCH_QUEUE_TRANSFER_REBASE,
                                     rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, n);

    /* Verify via poller */
    IndexedBatchQueue *vq = indexed_batch_queue_create(
        TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
        0, 5000000000ULL, 0, rt, rt);
    IndexedBatchPoller *p = indexed_batch_queue_create_poller(vq);
    BatchIdCollector col = {{0}, 0};
    PollResult pr = indexed_batch_poller_poll(p, batch_id_handler, &col);
    TEST_ASSERT_EQUAL(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL(1, col.count);
    pr = indexed_batch_poller_poll(p, batch_id_handler, &col);
    TEST_ASSERT_EQUAL(POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL(2, col.count);
    /* Both IDs must appear; order depends on CAS timing */
    int seen0 = 0, seen1 = 0;
    for (int i = 0; i < 2; i++) {
        if (col.ids[i] == id0) seen0 = 1;
        if (col.ids[i] == id1) seen1 = 1;
    }
    TEST_ASSERT(seen0 && seen1);

    indexed_batch_poller_close(p);
    indexed_batch_queue_close(vq);
    unlink(fpath);
}

/* ── Appender ID preservation tests ──────────────────────────────────────── */

void test_import_preserves_appender_ids(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    IndexedQueue *q = indexed_queue_create(TEST_DIR, QUEUE, 4096, 4, 1, 4096LL * 64,
                                            0, 5000000000ULL, 0, rt, rt);
    IndexedAppender *a0 = indexed_queue_create_appender(q);
    IndexedAppender *a1 = indexed_queue_create_appender(q);
    uint8_t id0 = indexed_appender_id(a0);
    uint8_t id1 = indexed_appender_id(a1);
    TEST_ASSERT(id0 != id1);

    indexed_appender_append(a0, "from-a0", 8);
    indexed_appender_append(a1, "from-a1", 8);
    indexed_appender_append(a0, "from-a0-2", 10);
    indexed_appender_close(a0);
    indexed_appender_close(a1);

    IndexedReader *r = indexed_queue_create_reader(q);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    int64_t n = queue_transfer_export(r, out, 0, 2, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    indexed_reader_close(r);
    indexed_queue_close(q);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE,
                                              rt, rt, 0, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    /* Verify via reader: each entry has correct appender_id */
    {
        IndexedQueue *vq = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                                  0, 5000000000ULL, 0, rt, rt);
        IndexedReader *vr = indexed_queue_create_reader(vq);
        int64_t last = indexed_reader_last_index(vr);
        TEST_ASSERT_EQUAL_INT64(2, last); /* indices 0, 1, 2 */
        int seen0 = 0, seen1 = 0;
        for (int64_t i = indexed_reader_start_index(vr); i <= last; i++) {
            ReadingContext ctx;
            indexed_reader_read(vr, i, &ctx);
            TEST_ASSERT_EQUAL(ENTRY_AVAILABLE, ctx.status);
            if (ctx.appender_id == id0) seen0 = 1;
            if (ctx.appender_id == id1) seen1 = 1;
        }
        TEST_ASSERT(seen0 && seen1);
        indexed_reader_close(vr);
        indexed_queue_close(vq);
    }
    unlink(fpath);
}

void test_import_merge_via_mapper(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    IndexedQueue *q = indexed_queue_create(TEST_DIR, QUEUE, 4096, 4, 1, 4096LL * 64,
                                            0, 5000000000ULL, 0, rt, rt);
    IndexedAppender *a0 = indexed_queue_create_appender(q);
    IndexedAppender *a1 = indexed_queue_create_appender(q);
    indexed_appender_append(a0, "from-a0", 8);
    indexed_appender_append(a1, "from-a1", 8);
    indexed_appender_close(a0);
    indexed_appender_close(a1);

    IndexedReader *r = indexed_queue_create_reader(q);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(r, out, 0, 1, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(r);
    indexed_queue_close(q);

    /* Mapper that merges everything into appender 0 */
    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE,
                                              rt, rt, 0,
                                              map_to_zero, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);

    /* Verify all entries landed in appender 0 */
    IndexedQueue *vq = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                              0, 5000000000ULL, 0, rt, rt);
    IndexedReader *vr = indexed_queue_create_reader(vq);
    int64_t last = indexed_reader_last_index(vr);
    for (int64_t i = indexed_reader_start_index(vr); i <= last; i++) {
        ReadingContext ctx;
        indexed_reader_read(vr, i, &ctx);
        TEST_ASSERT_EQUAL(ENTRY_AVAILABLE, ctx.status);
        TEST_ASSERT_EQUAL_UINT8(0, ctx.appender_id);
    }
    indexed_reader_close(vr);
    indexed_queue_close(vq);
    unlink(fpath);
}

void test_import_auto_on_conflict(void)
{
    cleanup_queue(COPY_Q);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, COPY_Q);
    mkdir(path, 0775);

    /* Pre-create destination queue — keep appender ID 0 locked during import */
    IndexedQueue *dest = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                               0, 5000000000ULL, 0, rt, rt);
    IndexedAppender *existing = indexed_queue_create_appender_with_id(dest, 0);
    TEST_ASSERT_NOT_NULL(existing);

    /* Export a single-appender (aid=0) stream */
    cleanup_queue(QUEUE);
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);
    IndexedQueue *src = indexed_queue_create(TEST_DIR, QUEUE, 4096, 4, 1, 4096LL * 64,
                                              0, 5000000000ULL, 0, rt, rt);
    IndexedAppender *a = indexed_queue_create_appender(src);
    indexed_appender_append(a, "hello", 6);
    indexed_appender_close(a);

    IndexedReader *r = indexed_queue_create_reader(src);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(r, out, 0, 0, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(r);
    indexed_queue_close(src);

    /* Import with CONFLICT_AUTO — should auto-assign since ID 0 is in use */
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE,
                                              rt, rt, 1, NULL, NULL, CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);

    /* Verify the imported entry got a different appender ID (not 0, which is held) */
    IndexedQueue *vq = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                              0, 5000000000ULL, 0, rt, rt);
    IndexedReader *vr = indexed_queue_create_reader(vq);
    ReadingContext rctx;
    indexed_reader_read(vr, 0, &rctx);
    TEST_ASSERT_EQUAL(ENTRY_AVAILABLE, rctx.status);
    TEST_ASSERT(rctx.appender_id != 0);
    indexed_reader_close(vr);
    indexed_queue_close(vq);

    indexed_appender_close(existing);
    indexed_queue_close(dest);
    unlink(fpath);
}

void test_import_abort_on_conflict(void)
{
    cleanup_queue(COPY_Q);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, COPY_Q);
    mkdir(path, 0775);

    /* Pre-create destination queue — keep appender ID 0 locked during import */
    IndexedQueue *dest = indexed_queue_create(TEST_DIR, COPY_Q, 4096, 4, 1, 4096LL * 64,
                                               0, 5000000000ULL, 0, rt, rt);
    IndexedAppender *existing = indexed_queue_create_appender_with_id(dest, 0);
    TEST_ASSERT_NOT_NULL(existing);

    /* Export a single-appender (aid=0) stream */
    cleanup_queue(QUEUE);
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);
    IndexedQueue *src = indexed_queue_create(TEST_DIR, QUEUE, 4096, 4, 1, 4096LL * 64,
                                              0, 5000000000ULL, 0, rt, rt);
    IndexedAppender *a = indexed_queue_create_appender(src);
    indexed_appender_append(a, "hello", 6);
    indexed_appender_close(a);

    IndexedReader *r = indexed_queue_create_reader(src);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    queue_transfer_export(r, out, 0, 0, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    indexed_reader_close(r);
    indexed_queue_close(src);

    /* Import with CONFLICT_ABORT — should return MMAPQ_EIDCONFLICT since ID 0 is in use */
    FILE *in = fopen(fpath, "rb");
    int64_t imported = queue_transfer_import(in, TEST_DIR, COPY_Q, QUEUE_TRANSFER_REBASE,
                                              rt, rt, 1, NULL, NULL, CONFLICT_ABORT);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EIDCONFLICT, imported);

    indexed_appender_close(existing);
    indexed_queue_close(dest);
    unlink(fpath);
}

/* ── Main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();

    /* Existing tests (8) */
    RUN_TEST(test_export_import_roundtrip);
    RUN_TEST(test_export_import_preserve_start_index);
    RUN_TEST(test_poller_starts_at_preserved_index);
    RUN_TEST(test_export_import_rebase_start_index);
    RUN_TEST(test_export_import_fragment);
    RUN_TEST(test_export_empty_queue);
    RUN_TEST(test_export_chunked);
    RUN_TEST(test_filter_include);
    RUN_TEST(test_copy_queue);

    /* Phase 0 — Normal import allowAppend (4) */
    RUN_TEST(test_import_to_new);
    RUN_TEST(test_import_rebase_append);
    RUN_TEST(test_import_preserve_append);
    RUN_TEST(test_import_no_append);

    /* Phase 0 — Normal copy allowAppend (5) */
    RUN_TEST(test_copy_to_new_rebase);
    RUN_TEST(test_copy_to_new_preserve);
    RUN_TEST(test_copy_rebase_append);
    RUN_TEST(test_copy_preserve_append);
    RUN_TEST(test_copy_no_append);

    /* Phase 0 — Batch import allowAppend (4) */
    RUN_TEST(test_batch_import_to_new);
    RUN_TEST(test_batch_import_rebase_append);
    RUN_TEST(test_batch_import_preserve_append);
    RUN_TEST(test_batch_import_no_append);

    /* Phase 0 — Batch copy allowAppend (4) */
    RUN_TEST(test_batch_copy_to_new);
    RUN_TEST(test_batch_copy_rebase_append);
    RUN_TEST(test_batch_copy_preserve_append);
    RUN_TEST(test_batch_copy_no_append);

    /* Cross-type import — batch stream → normal (4) */
    RUN_TEST(test_import_batch_stream_to_new);
    RUN_TEST(test_import_batch_stream_rebase_append);
    RUN_TEST(test_import_batch_stream_preserve_append);
    RUN_TEST(test_import_batch_stream_no_append);

    /* Cross-type import — flat stream → batch (4) */
    RUN_TEST(test_import_flat_to_batch_to_new);
    RUN_TEST(test_import_flat_to_batch_rebase_append);
    RUN_TEST(test_import_flat_to_batch_preserve_append);
    RUN_TEST(test_import_flat_to_batch_no_append);

    /* PRESERVE mode — comprehensive (12) */
    RUN_TEST(test_copy_batch_to_normal_preserve_nonzero);
    RUN_TEST(test_copy_normal_to_batch_preserve_nonzero);
    RUN_TEST(test_import_batch_stream_preserve_nonzero);
    RUN_TEST(test_import_flat_to_batch_preserve_nonzero);
    RUN_TEST(test_batch_import_preserve_to_new);
    RUN_TEST(test_import_batch_stream_preserve_to_new);
    RUN_TEST(test_import_flat_to_batch_preserve_to_new);
    RUN_TEST(test_batch_copy_preserve_to_new);
    RUN_TEST(test_copy_batch_to_normal_preserve);
    RUN_TEST(test_copy_normal_to_batch_preserve);
    RUN_TEST(test_batch_export_preserve);
    RUN_TEST(test_batch_export_import_preserve_roundtrip);

    /* Phase 1 — Cross-type copy (8) */
    RUN_TEST(test_copy_batch_to_normal_flatten);
    RUN_TEST(test_copy_batch_to_normal_filter);
    RUN_TEST(test_copy_normal_to_batch_1msg);
    RUN_TEST(test_copy_cross_type_roundtrip);
    RUN_TEST(test_copy_batch_to_normal_append);
    RUN_TEST(test_copy_batch_to_normal_no_append);
    RUN_TEST(test_copy_normal_to_batch_append);
    RUN_TEST(test_copy_normal_to_batch_preserve_append);

    /* Appender ID preservation tests (4) */
    RUN_TEST(test_import_preserves_appender_ids);
    RUN_TEST(test_import_merge_via_mapper);
    RUN_TEST(test_import_auto_on_conflict);
    RUN_TEST(test_import_abort_on_conflict);

    return UNITY_END();
}
