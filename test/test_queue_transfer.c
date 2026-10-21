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
#include "internal/batch_payload_iterator_internal.h"
#include "internal/export_meta.h"

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

static MmapqRuntime *rt = NULL;

static void cleanup_queue(const char *name)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", TEST_DIR, name);
    int rc = system(cmd); (void)rc;
}

void setUp(void)
{
    int rc_rt = mmapq_runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
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

static MmapqIndexedQueue *create_queue(const char *name, const char **msgs, int n, int64_t start_index)
{
    cleanup_queue(name);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, name);
    mkdir(path, 0775);

    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(TEST_DIR, name, 32768, 16, 1, 32768LL * 8,
                                     start_index, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);

    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    for (int i = 0; i < n; i++)
        mmapq_indexed_appender_append(a, msgs[i], (int32_t)strlen(msgs[i]) + 1);
    mmapq_indexed_appender_close(a);
    return q;
}

static int load_messages(const char *name, char out[][100], int max)
{
    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(TEST_DIR, name, 32768, 16, 1, 32768LL * 8,
                                     0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    int count = 0;
    int64_t last = mmapq_indexed_reader_last_index(r);
    if (last < 0) { mmapq_indexed_reader_close(r); mmapq_indexed_queue_close(q); return 0; }
    for (int64_t i = mmapq_indexed_reader_start_index(r); i <= last && count < max; i++) {
        MmapqReadingContext ctx = {0};
        mmapq_indexed_reader_read(r, i, &ctx);
        if (ctx.status == MMAPQ_ENTRY_AVAILABLE) {
            memcpy(out[count], ctx.data, (size_t)ctx.length);
            out[count][ctx.length - 1] = '\0';
            count++;
        }
    }
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);
    return count;
}

static void verify_batch_index_space(const char *name, int64_t expected_start, int64_t expected_end)
{
    MmapqIndexedBatchQueue *vq = NULL;
    int rc_vq = mmapq_indexed_batch_queue_create(TEST_DIR, name, 32768, 16, 1, 32768LL * 8,
                                             0, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedBatchReader *vr = NULL;
    int rc_vr = mmapq_indexed_batch_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    int64_t si = mmapq_indexed_batch_reader_start_index(vr);
    int64_t li = mmapq_indexed_batch_reader_last_index(vr);
    TEST_ASSERT_EQUAL_INT64(expected_start, si);
    TEST_ASSERT_EQUAL_INT64(expected_end, li);

    MmapqBatchReadingContext vctx;
    mmapq_indexed_batch_reader_read(vr, expected_start, &vctx);
    TEST_ASSERT_EQUAL(MMAPQ_BATCH_ENTRY_AVAILABLE, vctx.status);

    if (expected_start > 0) {
        mmapq_indexed_batch_reader_read(vr, expected_start - 1, &vctx);
        TEST_ASSERT_EQUAL(MMAPQ_BATCH_ENTRY_ARCHIVED, vctx.status);
    }

    mmapq_indexed_batch_reader_close(vr);
    mmapq_indexed_batch_queue_close(vq);
}

static void verify_index_space(const char *name, int64_t expected_start, int64_t expected_end)
{
    MmapqIndexedQueue *vq = NULL;
    int rc_vq = mmapq_indexed_queue_create(TEST_DIR, name, 32768, 16, 1, 32768LL * 8,
                                      expected_start, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedReader *vr = NULL;
    int rc_vr = mmapq_indexed_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    TEST_ASSERT_EQUAL_INT64(expected_start, mmapq_indexed_reader_start_index(vr));
    TEST_ASSERT_EQUAL_INT64(expected_end, mmapq_indexed_reader_last_index(vr));

    MmapqReadingContext vctx;
    mmapq_indexed_reader_read(vr, expected_start, &vctx);
    TEST_ASSERT_EQUAL(MMAPQ_ENTRY_AVAILABLE, vctx.status);

    if (expected_start > 0) {
        mmapq_indexed_reader_read(vr, expected_start - 1, &vctx);
        TEST_ASSERT_EQUAL(MMAPQ_ENTRY_ARCHIVED, vctx.status);
    }

    mmapq_indexed_reader_close(vr);
    mmapq_indexed_queue_close(vq);
}

static MmapqIndexedBatchQueue *create_batch_queue(const char *name, const char *batches[][3],
                                               int batch_sizes[], int nb)
{
    cleanup_queue(name);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, name);
    mkdir(path, 0775);

    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(TEST_DIR, name, 32768, 16, 1, 32768LL * 8,
                                           0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedBatchAppender *a = NULL;
    int rc_a = mmapq_indexed_batch_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    for (int b = 0; b < nb; b++) {
        MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(a);
        for (int m = 0; m < batch_sizes[b]; m++) {
            mmapq_mutable_batch_append(mb, batches[b][m], (int32_t)strlen(batches[b][m]) + 1);
        }
        mmapq_mutable_batch_complete(mb);   /* CAND-49: complete publishes */
    }
    mmapq_indexed_batch_appender_close(a);
    return q;
}

/* ── Same-type transfer tests ────────────────────────────────────────────── */

void test_export_import_roundtrip(void)
{
    const char *msgs[] = {"msg1", "msg2", "msg3"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(reader, out, 0, 2, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    /* Messages are at indices 100, 101, 102 */
    int64_t actual_start = mmapq_indexed_reader_start_index(reader);
    TEST_ASSERT_EQUAL_INT64(100, actual_start);
    int64_t actual_end   = mmapq_indexed_reader_last_index(reader);
    TEST_ASSERT_EQUAL_INT64(102, actual_end);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    /* Export the range the queue actually has */
    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(reader, out, actual_start, actual_end,
                                       MMAPQ_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    /* Import with PRESERVE: new queue starts at index 100 */
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q,
                                              MMAPQ_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    /* Verify payloads */
    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);

    /* Verify index space is preserved */
    MmapqIndexedQueue *vq = NULL;
    int rc_vq = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                      actual_start, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedReader *vr = NULL;
    int rc_vr = mmapq_indexed_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    TEST_ASSERT_EQUAL_INT64(actual_start, mmapq_indexed_reader_start_index(vr));
    TEST_ASSERT_EQUAL_INT64(actual_end, mmapq_indexed_reader_last_index(vr));
    MmapqReadingContext vctx;
    mmapq_indexed_reader_read(vr, actual_start, &vctx);
    TEST_ASSERT_EQUAL(MMAPQ_ENTRY_AVAILABLE, vctx.status);
    mmapq_indexed_reader_read(vr, actual_start - 1, &vctx);
    TEST_ASSERT_EQUAL(MMAPQ_ENTRY_ARCHIVED, vctx.status);
    mmapq_indexed_reader_close(vr);
    mmapq_indexed_queue_close(vq);

    unlink(fpath);
}

static MmapqNextMove poll_handler(int64_t index, uint8_t aid, const void *data,
                              int32_t length, void *ctx)
{
    (void)aid; (void)data; (void)length;
    if (ctx) *(int64_t *)ctx = index;
    return MMAPQ_NEXT_ADVANCE;
}

void test_poller_starts_at_preserved_index(void)
{
    const char *msgs[] = {"a", "b", "c"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);
    int64_t si = mmapq_indexed_reader_start_index(reader);
    int64_t ei = mmapq_indexed_reader_last_index(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, si, ei, MMAPQ_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);

    /* Now poll the imported queue — first entry should be at index 100 */
    MmapqIndexedQueue *iq = NULL;
    int rc_iq = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                      si, 5000000000ULL, 0, rt, rt, &iq);
    TEST_ASSERT_EQUAL_INT(0, rc_iq);
    TEST_ASSERT_NOT_NULL(iq);
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_queue_create_poller(iq, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    /* Three messages at indices 100, 101, 102 */
    {
        int64_t polled_idx = -1;
        for (int64_t expected = 100; expected <= 102; expected++) {
            MmapqPollResult r = mmapq_indexed_poller_poll(p, poll_handler, &polled_idx);
            TEST_ASSERT_EQUAL(MMAPQ_POLL_ADVANCED, r);
            TEST_ASSERT_EQUAL_INT64(expected, polled_idx);
            polled_idx = -1;
        }
        /* Fourth poll should be EOS */
        MmapqPollResult r = mmapq_indexed_poller_poll(p, poll_handler, &polled_idx);
        TEST_ASSERT_EQUAL(MMAPQ_POLL_END_OF_STREAM, r);
    }

    mmapq_indexed_poller_close(p);
    mmapq_indexed_queue_close(iq);
    unlink(fpath);
}

void test_export_import_rebase_start_index(void)
{
    const char *msgs[] = {"a", "b", "c"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 100);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    int64_t si = mmapq_indexed_reader_start_index(reader);
    int64_t ei = mmapq_indexed_reader_last_index(reader);
    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(reader, out, si, ei, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);

    MmapqIndexedQueue *vq = NULL;
    int rc_vq = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                      0, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedReader *vr = NULL;
    int rc_vr = mmapq_indexed_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_reader_start_index(vr));
    mmapq_indexed_reader_close(vr);
    mmapq_indexed_queue_close(vq);

    unlink(fpath);
}

void test_export_import_fragment(void)
{
    const char *msgs[] = {"m1", "m2", "m3", "m4", "m5"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 5, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(reader, out, 1, 3, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *src = create_queue(QUEUE, NULL, 0, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(reader, out, 0, -1, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(0, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 10, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char base[512];
    snprintf(base, sizeof(base), "%s/%s_chunk", TEST_DIR, QUEUE);

    int64_t total = mmapq_queue_transfer_export_chunked(reader, base, 0, 9, 1, MMAPQ_QUEUE_TRANSFER_REBASE,
                                                    NULL, NULL, NULL);
    TEST_ASSERT_EQUAL_INT64(10, total);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    char clean_cmd[1024];
    snprintf(clean_cmd, sizeof(clean_cmd), "rm -f %s_*", base);
    int rc = system(clean_cmd); (void)rc;
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
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 5, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);

    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(reader, out, 0, 4, MMAPQ_QUEUE_TRANSFER_REBASE,
                                        filter_include_aaa, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(COPY_Q);
    int64_t n = mmapq_queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 2, MMAPQ_QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

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
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, 0, 1, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    unlink(fpath);
}

void test_import_rebase_append(void)
{
    const char *msgs[] = {"first", "second"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, 0, 1, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);

    /* Create destination queue with data first */
    cleanup_queue(COPY_Q);
    const char *existing[] = {"existing"};
    MmapqIndexedQueue *existing_q = create_queue(COPY_Q, existing, 1, 0);
    mmapq_indexed_queue_close(existing_q);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("existing", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("first", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("second", loaded[2]);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
    unlink(fpath);
}

void test_import_preserve_append(void)
{
    const char *msgs[] = {"one"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, 0, 0, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);

    cleanup_queue(COPY_Q);
    const char *existing[] = {"existing"};
    MmapqIndexedQueue *existing_q = create_queue(COPY_Q, existing, 1, 0);
    mmapq_indexed_queue_close(existing_q);

    FILE *in = fopen(fpath, "rb");
    /* PRESERVE + allow_append=1 on existing queue → EINVAL */
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, imported);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
    unlink(fpath);
}

void test_import_no_append(void)
{
    const char *msgs[] = {"x"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, 0, 0, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);

    cleanup_queue(COPY_Q);
    const char *existing[] = {"existing"};
    MmapqIndexedQueue *existing_q = create_queue(COPY_Q, existing, 1, 0);
    mmapq_indexed_queue_close(existing_q);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
    unlink(fpath);
}

/* ── Phase 0: allowAppend — Normal copy scenarios ────────────────────────── */

void test_copy_to_new_rebase(void)
{
    const char *msgs[] = {"a", "b", "c"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(COPY_Q);
    int64_t n = mmapq_queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 2, MMAPQ_QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_copy_to_new_preserve(void)
{
    const char *msgs[] = {"x", "y"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 2, 10);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    /* Messages at indices 10, 11 */
    int64_t si = mmapq_indexed_reader_start_index(reader);
    int64_t ei = mmapq_indexed_reader_last_index(reader);

    cleanup_queue(COPY_Q);
    /* Copy with PRESERVE — dest starts at source start_index (10) */
    int64_t n = mmapq_queue_transfer_copy(reader, TEST_DIR, COPY_Q, si, ei,
                                     MMAPQ_QUEUE_TRANSFER_PRESERVE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(2, cnt);

    /* Verify index space is preserved */
    verify_index_space(COPY_Q, si, ei);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_copy_rebase_append(void)
{
    const char *msgs[] = {"new1", "new2"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[] = {"old"};
    MmapqIndexedQueue *dest = create_queue(COPY_Q, existing, 1, 0);
    mmapq_indexed_queue_close(dest);

    int64_t n = mmapq_queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 1, MMAPQ_QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("old", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("new1", loaded[1]);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_copy_preserve_append(void)
{
    const char *msgs[] = {"m"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[] = {"old"};
    MmapqIndexedQueue *dest = create_queue(COPY_Q, existing, 1, 0);
    mmapq_indexed_queue_close(dest);

    int64_t n = mmapq_queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 0, MMAPQ_QUEUE_TRANSFER_PRESERVE,
                                     NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, n);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_copy_no_append(void)
{
    const char *msgs[] = {"m"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[] = {"old"};
    MmapqIndexedQueue *dest = create_queue(COPY_Q, existing, 1, 0);
    mmapq_indexed_queue_close(dest);

    int64_t n = mmapq_queue_transfer_copy(reader, TEST_DIR, COPY_Q, 0, 0, MMAPQ_QUEUE_TRANSFER_REBASE,
                                     NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, n);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

/* ── Phase 0: allowAppend — Batch import scenarios ────────────────────────── */

void test_batch_import_to_new(void)
{
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1"}};
    int sizes[] = {2, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    unlink(fpath);
}

void test_batch_import_rebase_append(void)
{
    const char *batches[][3] = {{"new_b1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    const char *existing[][3] = {{"old_b1"}};
    int existing_sizes[] = {1};
    MmapqIndexedBatchQueue *dest = create_batch_queue(BQ_COPY, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    unlink(fpath);
}

void test_batch_import_preserve_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    MmapqIndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, imported);
    unlink(fpath);
}

void test_batch_import_no_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(BQ_COPY);
    MmapqIndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, BQ_COPY, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);
    unlink(fpath);
}

/* ── Phase 0: allowAppend — Batch copy scenarios ─────────────────────────── */

void test_batch_copy_to_new(void)
{
    const char *batches[][3] = {{"b1m1"}, {"b2m1"}};
    int sizes[] = {1, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(BQ_COPY);
    int64_t n = mmapq_batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_batch_copy_rebase_append(void)
{
    const char *batches[][3] = {{"new_b1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(BQ_COPY);
    const char *existing[][3] = {{"old_b1"}};
    int existing_sizes[] = {1};
    MmapqIndexedBatchQueue *dest = create_batch_queue(BQ_COPY, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    int64_t n = mmapq_batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_batch_copy_preserve_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(BQ_COPY);
    MmapqIndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    int64_t n = mmapq_batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE,
                                           NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

/* CAND-51a F2 pin: a mid-batch source corruption is detected by the
 * read-only validation walk BEFORE the destination batch is started — the
 * destination lane is left untouched: no join completion event, and the
 * uncommitted walk lists zero batches. (Pre-fix the abandoned open batch
 * was join-completed to staged on reopen: join rc was 1 and the walk
 * listed the partial batch.) */
void test_batch_copy_source_corruption_mid_batch_leaves_nothing(void)
{
    const char *batches[][3] = {{"m-one", "m-two"}};
    int sizes[] = {2};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    mmapq_indexed_batch_queue_close(src);

    /* Corrupt the second message's length on disk. Batch framing:
     * @64 int32 count, then per message int32 len + data; msg2's length
     * sits at 64 + 4 + 4 + len1. */
    char payfile[512];
    snprintf(payfile, sizeof(payfile), "%s/%s/payload_0_0", TEST_DIR, BQ_QUEUE);
    FILE *f = fopen(payfile, "r+b");
    TEST_ASSERT_NOT_NULL(f);
    int32_t len1 = 0;
    TEST_ASSERT_EQUAL_INT(0, fseek(f, 68, SEEK_SET));
    TEST_ASSERT_EQUAL(1, fread(&len1, 4, 1, f));
    int64_t off = 72 + len1;
    int32_t bogus = 0x7FFFFFFF;
    TEST_ASSERT_EQUAL_INT(0, fseek(f, off, SEEK_SET));
    TEST_ASSERT_EQUAL(1, fwrite(&bogus, 4, 1, f));
    fclose(f);

    MmapqIndexedBatchReader *reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_reader_create(TEST_DIR, BQ_QUEUE, 4, 5000000000ULL, rt, &reader));
    cleanup_queue(BQ_COPY);
    int64_t n = mmapq_batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT, n);
    mmapq_indexed_batch_reader_close(reader);

    MmapqIndexedBatchQueue *dst = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(TEST_DIR, BQ_COPY, 32768, 16, 1, 32768LL * 8,
                                                          0, 5000000000ULL, 0, rt, rt, &dst));
    MmapqIndexedBatchAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender(dst, &a));
    int64_t joined = -1;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_join_completed_open_batch(a, &joined));
    MmapqUncommittedBatchIterator *uit = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_uncommitted_batches_checked(a, &uit));
    int walked = 0;
    while (1) {
        int64_t boff; int32_t count; MmapqBatchPayloadIterator *msgs;
        int nrc = mmapq_uncommitted_batch_next(uit, &boff, &count, &msgs);
        if (nrc == 0) break;
        TEST_ASSERT_GREATER_OR_EQUAL_INT(1, nrc);
        walked++;
    }
    TEST_ASSERT_EQUAL_INT(0, walked);
    mmapq_indexed_batch_appender_close(a);
    mmapq_indexed_batch_queue_close(dst);
}

void test_batch_copy_no_append(void)
{
    const char *batches[][3] = {{"b1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(BQ_COPY);
    MmapqIndexedBatchQueue *dest = create_batch_queue(BQ_COPY, batches, sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    int64_t n = mmapq_batch_queue_transfer_copy(reader, TEST_DIR, BQ_COPY, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

/* ── Cross-type import: batch stream → normal (flatten) ──────────────────── */

static void batch_to_normal_export(int64_t *out_total)
{
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1"}};
    int sizes[] = {2, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    *out_total = mmapq_batch_queue_transfer_export(reader, out, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
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
    int64_t imported = mmapq_queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *eq = create_queue(CROSS_Q, existing, 1, 0);
    mmapq_indexed_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *eq = create_queue(CROSS_Q, existing, 1, 0);
    mmapq_indexed_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, MMAPQ_QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedQueue *eq = create_queue(CROSS_Q, existing, 1, 0);
    mmapq_indexed_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);
    unlink(fpath);
}

/* ── Cross-type import: flat stream → batch (1-msg batches) ───────────────── */

static void flat_to_batch_export(int64_t *out_total)
{
    const char *msgs[] = {"m1", "m2", "m3"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    *out_total = mmapq_queue_transfer_export(reader, out, 0, 2, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
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
    int64_t imported = mmapq_batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedBatchQueue *eq = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedBatchQueue *eq = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedBatchQueue *eq = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(eq);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, imported);
    unlink(fpath);
}

/* ── Phase 1: Cross-type copy ────────────────────────────────────────────── */

void test_copy_batch_to_normal_flatten(void)
{
    const char *batches[][3] = {{"b1m1", "b1m2"}, {"b2m1", "b2m2", "b2m3"}, {"b3m1"}};
    int sizes[] = {2, 3, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 3);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 2,
                                                      MMAPQ_QUEUE_TRANSFER_REBASE,
                                                      NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(6, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

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
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 5, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 4,
                                                            MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                                            NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(5, n);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    /* Verify by reading back the batch queue */
    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(TEST_DIR, CROSS_Q, 32768, 16, 1, 32768LL * 8,
                                           0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedBatchReader *br = NULL;
    int rc_br = mmapq_indexed_batch_queue_create_reader(q, &br);
    TEST_ASSERT_EQUAL_INT(0, rc_br);
    TEST_ASSERT_NOT_NULL(br);
    int64_t last = mmapq_indexed_batch_reader_last_index(br);
    TEST_ASSERT_EQUAL_INT64(4, last);

    char buf[100];
    for (int64_t i = 0; i <= last; i++) {
        MmapqBatchReadingContext ctx = {0};
        mmapq_indexed_batch_reader_read(br, i, &ctx);
        TEST_ASSERT_EQUAL(MMAPQ_BATCH_ENTRY_AVAILABLE, ctx.status);
        TEST_ASSERT_EQUAL_INT(1, ctx.message_count);
        mmapq_batch_iterator_init(ctx.iterator, ctx.batch_offset, ctx.message_count, 0);
        TEST_ASSERT(mmapq_batch_iterator_advance(ctx.iterator));
        int32_t l = batch_iterator_current_length(ctx.iterator);
        memcpy(buf, batch_iterator_current_data(ctx.iterator), (size_t)l);
        buf[l - 1] = '\0';
        char expected[24];
        snprintf(expected, sizeof(expected), "msg%ld", (long)(i + 1));
        TEST_ASSERT_EQUAL_STRING(expected, buf);
    }
    mmapq_indexed_batch_reader_close(br);
    mmapq_indexed_batch_queue_close(q);
}

void test_copy_cross_type_roundtrip(void)
{
    const char *msgs[] = {"a", "b", "c"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    /* normal → batch (1-msg batches) */
    cleanup_queue(CROSS_Q);
    int64_t n1 = mmapq_batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 2,
                                                             MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                                             NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n1);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    /* batch → normal (flatten back) */
    MmapqIndexedBatchQueue *bq = NULL;
    int rc_bq = mmapq_indexed_batch_queue_create(TEST_DIR, CROSS_Q, 32768, 16, 1, 32768LL * 8,
                                            0, 5000000000ULL, 0, rt, rt, &bq);
    TEST_ASSERT_EQUAL_INT(0, rc_bq);
    TEST_ASSERT_NOT_NULL(bq);
    MmapqIndexedBatchReader *br = NULL;
    int rc_br = mmapq_indexed_batch_queue_create_reader(bq, &br);
    TEST_ASSERT_EQUAL_INT(0, rc_br);
    TEST_ASSERT_NOT_NULL(br);

    cleanup_queue(COPY_Q);
    int64_t n2 = mmapq_queue_transfer_copy_batch_to_normal(br, TEST_DIR, COPY_Q, 0, 2,
                                                       MMAPQ_QUEUE_TRANSFER_REBASE,
                                                       NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n2);
    mmapq_indexed_batch_reader_close(br);
    mmapq_indexed_batch_queue_close(bq);

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
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[] = {"old_msg"};
    MmapqIndexedQueue *dest = create_queue(CROSS_Q, existing, 1, 0);
    mmapq_indexed_queue_close(dest);

    int64_t n = mmapq_queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                      MMAPQ_QUEUE_TRANSFER_REBASE,
                                                      NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    char loaded[10][100];
    int cnt = load_messages(CROSS_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(2, cnt);
    TEST_ASSERT_EQUAL_STRING("old_msg", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("new_msg", loaded[1]);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_copy_batch_to_normal_no_append(void)
{
    const char *batches[][3] = {{"m"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[] = {"old"};
    MmapqIndexedQueue *dest = create_queue(CROSS_Q, existing, 1, 0);
    mmapq_indexed_queue_close(dest);

    int64_t n = mmapq_queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                      MMAPQ_QUEUE_TRANSFER_REBASE,
                                                      NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EEXIST, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_copy_normal_to_batch_append(void)
{
    const char *msgs[] = {"new_msg"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[][3] = {{"old_batch"}};
    int existing_sizes[] = {1};
    MmapqIndexedBatchQueue *dest = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    int64_t n = mmapq_batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                             MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                                             NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    /* Verify dest batch queue has both old and new batches */
    verify_batch_index_space(CROSS_Q, 0, 1);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_copy_normal_to_batch_preserve_append(void)
{
    const char *msgs[] = {"m"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    const char *existing[][3] = {{"old"}};
    int existing_sizes[] = {1};
    MmapqIndexedBatchQueue *dest = create_batch_queue(CROSS_Q, existing, existing_sizes, 1);
    mmapq_indexed_batch_queue_close(dest);

    int64_t n = mmapq_batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 0,
                                                            MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE,
                                                            NULL, NULL, NULL, rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, n);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
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
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 3);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 2,
                                                      MMAPQ_QUEUE_TRANSFER_REBASE,
                                                      filter_first_msg_of_batch, NULL, NULL,
                                                      rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    /* Only 3 messages should match (first msg of each batch) */
    TEST_ASSERT_EQUAL_INT64(3, n);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

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
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    verify_batch_index_space(CROSS_Q, 0, 1);
    unlink(fpath);
}

void test_import_batch_stream_preserve_to_new(void)
{
    const char *batches[][3] = {{"f1", "f2"}};
    int sizes[] = {2};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, MMAPQ_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);
    verify_index_space(CROSS_Q, 0, 1);
    unlink(fpath);
}

void test_import_flat_to_batch_preserve_to_new(void)
{
    const char *msgs[] = {"x", "y", "z"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, 0, 2, MMAPQ_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);
    verify_batch_index_space(CROSS_Q, 0, 2);
    unlink(fpath);
}

void test_batch_copy_preserve_to_new(void)
{
    const char *batches[][3] = {{"m1"}, {"m2"}};
    int sizes[] = {1, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_batch_queue_transfer_copy(reader, TEST_DIR, CROSS_Q, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE,
                                           NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);
    verify_batch_index_space(CROSS_Q, 0, 1);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_copy_batch_to_normal_preserve(void)
{
    const char *batches[][3] = {{"a1", "a2"}, {"b1"}};
    int sizes[] = {2, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_queue_transfer_copy_batch_to_normal(reader, TEST_DIR, CROSS_Q, 0, 1,
                                                      MMAPQ_QUEUE_TRANSFER_PRESERVE,
                                                      NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(3, n);
    verify_index_space(CROSS_Q, 0, 2);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_copy_normal_to_batch_preserve(void)
{
    const char *msgs[] = {"p1", "p2"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_batch_queue_transfer_copy_normal_to_batch(reader, TEST_DIR, CROSS_Q, 0, 1,
                                                            MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE,
                                                            NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(2, n);
    verify_batch_index_space(CROSS_Q, 0, 1);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_batch_export_preserve(void)
{
    const char *batches[][3] = {{"m1"}};
    int sizes[] = {1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(1, n);

    /* Verify by importing back and checking index space */
    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    verify_batch_index_space(CROSS_Q, 0, 0);

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
    unlink(fpath);
}

void test_batch_export_import_preserve_roundtrip(void)
{
    const char *batches[][3] = {{"m1"}, {"m2"}};
    int sizes[] = {1, 1};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 2);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(reader, out, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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
    MmapqIndexedBatchQueue *src = NULL;
    int rc_src = mmapq_indexed_batch_queue_create(
        TEST_DIR, BQ_QUEUE, 32768, 16, 1, 32768LL * 8,
        100, 5000000000ULL, 0, rt, rt, &src);
    TEST_ASSERT_EQUAL_INT(0, rc_src);
    TEST_ASSERT_NOT_NULL(src);
    MmapqIndexedBatchAppender *ba = NULL;
    int rc_ba = mmapq_indexed_batch_queue_create_appender(src, &ba);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(ba);
    MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(ba);
    mmapq_mutable_batch_append(mb, "hello", 6);
    mmapq_mutable_batch_complete(mb);   /* CAND-49: complete publishes */
    mmapq_indexed_batch_appender_close(ba);

    MmapqIndexedBatchReader *br = NULL;
    int rc_br = mmapq_indexed_batch_queue_create_reader(src, &br);
    TEST_ASSERT_EQUAL_INT(0, rc_br);
    TEST_ASSERT_NOT_NULL(br);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_queue_transfer_copy_batch_to_normal(
        br, TEST_DIR, CROSS_Q, 100, 100, MMAPQ_QUEUE_TRANSFER_PRESERVE,
        NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    verify_index_space(CROSS_Q, 100, 100);

    mmapq_indexed_batch_reader_close(br);
    mmapq_indexed_batch_queue_close(src);
}

void test_copy_normal_to_batch_preserve_nonzero(void)
{
    /* Source normal queue with start_index=100, one message at index 100 */
    const char *msgs[] = {"world"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 100);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    cleanup_queue(CROSS_Q);
    int64_t n = mmapq_batch_queue_transfer_copy_normal_to_batch(
        reader, TEST_DIR, CROSS_Q, 100, 100, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE,
        NULL, NULL, NULL, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT64(1, n);

    verify_batch_index_space(CROSS_Q, 100, 100);

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_import_batch_stream_preserve_nonzero(void)
{
    /* Source batch queue with start_index=100 */
    cleanup_queue(BQ_QUEUE);
    mkdir("/tmp/mmap_test_bqtrans", 0775);
    MmapqIndexedBatchQueue *src = NULL;
    int rc_src = mmapq_indexed_batch_queue_create(
        TEST_DIR, BQ_QUEUE, 32768, 16, 1, 32768LL * 8,
        100, 5000000000ULL, 0, rt, rt, &src);
    TEST_ASSERT_EQUAL_INT(0, rc_src);
    TEST_ASSERT_NOT_NULL(src);
    MmapqIndexedBatchAppender *ba = NULL;
    int rc_ba = mmapq_indexed_batch_queue_create_appender(src, &ba);
    TEST_ASSERT_EQUAL_INT(0, rc_ba);
    TEST_ASSERT_NOT_NULL(ba);
    MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(ba);
    mmapq_mutable_batch_append(mb, "test", 5);
    mmapq_mutable_batch_complete(mb);   /* CAND-49: complete publishes */
    mmapq_indexed_batch_appender_close(ba);

    MmapqIndexedBatchReader *br = NULL;
    int rc_br = mmapq_indexed_batch_queue_create_reader(src, &br);
    TEST_ASSERT_EQUAL_INT(0, rc_br);
    TEST_ASSERT_NOT_NULL(br);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, BQ_QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_batch_queue_transfer_export(br, out, 100, 100, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_batch_reader_close(br);
    mmapq_indexed_batch_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import_batch_stream(in, TEST_DIR, CROSS_Q, MMAPQ_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    verify_index_space(CROSS_Q, 100, 100);
    unlink(fpath);
}

void test_import_flat_to_batch_preserve_nonzero(void)
{
    /* Source normal queue with start_index=100 */
    const char *msgs[] = {"test"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 1, 100);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(reader, out, 100, 100, MMAPQ_QUEUE_TRANSFER_PRESERVE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    cleanup_queue(CROSS_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_batch_queue_transfer_import_flat_stream(in, TEST_DIR, CROSS_Q, MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
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

static MmapqNextMove batch_id_handler(int64_t idx, MmapqBatchReadingContext *ctx, void *data)
{
    (void)idx;
    BatchIdCollector *c = (BatchIdCollector *)data;
    if (c->count < 256) c->ids[c->count++] = ctx->appender_id;
    return MMAPQ_NEXT_ADVANCE;
}

void test_batch_import_preserves_appender_ids(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(
        TEST_DIR, QUEUE, 32768, 16, 1, 32768LL * 8,
        0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedBatchAppender *a0 = NULL;
    int rc_a0 = mmapq_indexed_batch_queue_create_appender(q, &a0);
    TEST_ASSERT_EQUAL_INT(0, rc_a0);
    TEST_ASSERT_NOT_NULL(a0);
    MmapqIndexedBatchAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_batch_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    uint8_t id0 = mmapq_indexed_batch_appender_id(a0);
    uint8_t id1 = mmapq_indexed_batch_appender_id(a1);
    TEST_ASSERT(id0 != id1);

    MmapqMutableBatch *mb = mmapq_indexed_batch_appender_start_batch(a0);
    mmapq_mutable_batch_append(mb, "a0-msg", 7);
    mmapq_mutable_batch_complete(mb);   /* CAND-49: complete publishes */

    mb = mmapq_indexed_batch_appender_start_batch(a1);
    mmapq_mutable_batch_append(mb, "a1-msg", 7);
    mmapq_mutable_batch_complete(mb);   /* CAND-49: complete publishes */

    mmapq_indexed_batch_appender_close(a0);
    mmapq_indexed_batch_appender_close(a1);

    MmapqIndexedBatchReader *r = NULL;
    int rc_r = mmapq_indexed_batch_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_batch_queue_transfer_export(r, out, 0, 1, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                             NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(2, n);
    mmapq_indexed_batch_reader_close(r);
    mmapq_indexed_batch_queue_close(q);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    n = mmapq_batch_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                     rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, n);

    /* Verify via poller */
    MmapqIndexedBatchQueue *vq = NULL;
    int rc_vq = mmapq_indexed_batch_queue_create(
        TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
        0, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedBatchPoller *p = NULL;
    int rc_p = mmapq_indexed_batch_queue_create_poller(vq, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);
    BatchIdCollector col = {{0}, 0};
    MmapqPollResult pr = mmapq_indexed_batch_poller_poll(p, batch_id_handler, &col);
    TEST_ASSERT_EQUAL(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL(1, col.count);
    pr = mmapq_indexed_batch_poller_poll(p, batch_id_handler, &col);
    TEST_ASSERT_EQUAL(MMAPQ_POLL_ADVANCED, pr);
    TEST_ASSERT_EQUAL(2, col.count);
    /* Both IDs must appear; order depends on CAS timing */
    int seen0 = 0, seen1 = 0;
    for (int i = 0; i < 2; i++) {
        if (col.ids[i] == id0) seen0 = 1;
        if (col.ids[i] == id1) seen1 = 1;
    }
    TEST_ASSERT(seen0 && seen1);

    mmapq_indexed_batch_poller_close(p);
    mmapq_indexed_batch_queue_close(vq);
    unlink(fpath);
}

/* ── Appender ID preservation tests ──────────────────────────────────────── */

void test_import_preserves_appender_ids(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(TEST_DIR, QUEUE, 32768, 16, 1, 32768LL * 8,
                                     0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a0 = NULL;
    int rc_a0 = mmapq_indexed_queue_create_appender(q, &a0);
    TEST_ASSERT_EQUAL_INT(0, rc_a0);
    TEST_ASSERT_NOT_NULL(a0);
    MmapqIndexedAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    uint8_t id0 = mmapq_indexed_appender_id(a0);
    uint8_t id1 = mmapq_indexed_appender_id(a1);
    TEST_ASSERT(id0 != id1);

    mmapq_indexed_appender_append(a0, "from-a0", 8);
    mmapq_indexed_appender_append(a1, "from-a1", 8);
    mmapq_indexed_appender_append(a0, "from-a0-2", 10);
    mmapq_indexed_appender_close(a0);
    mmapq_indexed_appender_close(a1);

    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    int64_t n = mmapq_queue_transfer_export(r, out, 0, 2, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);

    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE,
                                              rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    /* Verify via reader: each entry has correct appender_id */
    {
        MmapqIndexedQueue *vq = NULL;
        int rc_vq = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                           0, 5000000000ULL, 0, rt, rt, &vq);
        TEST_ASSERT_EQUAL_INT(0, rc_vq);
        TEST_ASSERT_NOT_NULL(vq);
        MmapqIndexedReader *vr = NULL;
        int rc_vr = mmapq_indexed_queue_create_reader(vq, &vr);
        TEST_ASSERT_EQUAL_INT(0, rc_vr);
        TEST_ASSERT_NOT_NULL(vr);
        int64_t last = mmapq_indexed_reader_last_index(vr);
        TEST_ASSERT_EQUAL_INT64(2, last); /* indices 0, 1, 2 */
        int seen0 = 0, seen1 = 0;
        for (int64_t i = mmapq_indexed_reader_start_index(vr); i <= last; i++) {
        MmapqReadingContext ctx = {0};
            mmapq_indexed_reader_read(vr, i, &ctx);
            TEST_ASSERT_EQUAL(MMAPQ_ENTRY_AVAILABLE, ctx.status);
            if (ctx.appender_id == id0) seen0 = 1;
            if (ctx.appender_id == id1) seen1 = 1;
        }
        TEST_ASSERT(seen0 && seen1);
        mmapq_indexed_reader_close(vr);
        mmapq_indexed_queue_close(vq);
    }
    unlink(fpath);
}

void test_import_merge_via_mapper(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(TEST_DIR, QUEUE, 32768, 16, 1, 32768LL * 8,
                                     0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a0 = NULL;
    int rc_a0 = mmapq_indexed_queue_create_appender(q, &a0);
    TEST_ASSERT_EQUAL_INT(0, rc_a0);
    TEST_ASSERT_NOT_NULL(a0);
    MmapqIndexedAppender *a1 = NULL;
    int rc_a1 = mmapq_indexed_queue_create_appender(q, &a1);
    TEST_ASSERT_EQUAL_INT(0, rc_a1);
    TEST_ASSERT_NOT_NULL(a1);
    mmapq_indexed_appender_append(a0, "from-a0", 8);
    mmapq_indexed_appender_append(a1, "from-a1", 8);
    mmapq_indexed_appender_append(a0, "from-a0-2", 10);
    mmapq_indexed_appender_close(a0);
    mmapq_indexed_appender_close(a1);

    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(r, out, 0, 1, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);

    /* Mapper that merges everything into appender 0 */
    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE,
                                              rt, rt, 0,
                                              map_to_zero, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(2, imported);

    /* Verify all entries landed in appender 0 */
    MmapqIndexedQueue *vq = NULL;
    int rc_vq = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                      0, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedReader *vr = NULL;
    int rc_vr = mmapq_indexed_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    int64_t last = mmapq_indexed_reader_last_index(vr);
    for (int64_t i = mmapq_indexed_reader_start_index(vr); i <= last; i++) {
        MmapqReadingContext ctx = {0};
        mmapq_indexed_reader_read(vr, i, &ctx);
        TEST_ASSERT_EQUAL(MMAPQ_ENTRY_AVAILABLE, ctx.status);
        TEST_ASSERT_EQUAL_UINT8(0, ctx.appender_id);
    }
    mmapq_indexed_reader_close(vr);
    mmapq_indexed_queue_close(vq);
    unlink(fpath);
}

void test_import_auto_on_conflict(void)
{
    cleanup_queue(COPY_Q);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, COPY_Q);
    mkdir(path, 0775);

    /* Pre-create destination queue — keep appender ID 0 locked during import */
    MmapqIndexedQueue *dest = NULL;
    int rc_dest = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                        0, 5000000000ULL, 0, rt, rt, &dest);
    TEST_ASSERT_EQUAL_INT(0, rc_dest);
    TEST_ASSERT_NOT_NULL(dest);
    MmapqIndexedAppender *existing = NULL;
    int rc_existing = mmapq_indexed_queue_create_appender_with_id(dest, 0, &existing);
    TEST_ASSERT_EQUAL_INT(0, rc_existing);
    TEST_ASSERT_NOT_NULL(existing);

    /* Export a single-appender (aid=0) stream */
    cleanup_queue(QUEUE);
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);
    MmapqIndexedQueue *src = NULL;
    int rc_src = mmapq_indexed_queue_create(TEST_DIR, QUEUE, 32768, 16, 1, 32768LL * 8,
                                       0, 5000000000ULL, 0, rt, rt, &src);
    TEST_ASSERT_EQUAL_INT(0, rc_src);
    TEST_ASSERT_NOT_NULL(src);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(src, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "hello", 6);
    mmapq_indexed_appender_close(a);

    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(src, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(r, out, 0, 0, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(src);

    /* Import with MMAPQ_CONFLICT_AUTO — should auto-assign since ID 0 is in use */
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE,
                                              rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);

    /* Verify the imported entry got a different appender ID (not 0, which is held) */
    MmapqIndexedQueue *vq = NULL;
    int rc_vq = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                      0, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedReader *vr = NULL;
    int rc_vr = mmapq_indexed_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    MmapqReadingContext rctx;
    mmapq_indexed_reader_read(vr, 0, &rctx);
    TEST_ASSERT_EQUAL(MMAPQ_ENTRY_AVAILABLE, rctx.status);
    TEST_ASSERT(rctx.appender_id != 0);
    mmapq_indexed_reader_close(vr);
    mmapq_indexed_queue_close(vq);

    mmapq_indexed_appender_close(existing);
    mmapq_indexed_queue_close(dest);
    unlink(fpath);
}

void test_import_abort_on_conflict(void)
{
    cleanup_queue(COPY_Q);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, COPY_Q);
    mkdir(path, 0775);

    /* Pre-create destination queue — keep appender ID 0 locked during import */
    MmapqIndexedQueue *dest = NULL;
    int rc_dest = mmapq_indexed_queue_create(TEST_DIR, COPY_Q, 32768, 16, 1, 32768LL * 8,
                                        0, 5000000000ULL, 0, rt, rt, &dest);
    TEST_ASSERT_EQUAL_INT(0, rc_dest);
    TEST_ASSERT_NOT_NULL(dest);
    MmapqIndexedAppender *existing = NULL;
    int rc_existing = mmapq_indexed_queue_create_appender_with_id(dest, 0, &existing);
    TEST_ASSERT_EQUAL_INT(0, rc_existing);
    TEST_ASSERT_NOT_NULL(existing);

    /* Export a single-appender (aid=0) stream */
    cleanup_queue(QUEUE);
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);
    MmapqIndexedQueue *src = NULL;
    int rc_src = mmapq_indexed_queue_create(TEST_DIR, QUEUE, 32768, 16, 1, 32768LL * 8,
                                       0, 5000000000ULL, 0, rt, rt, &src);
    TEST_ASSERT_EQUAL_INT(0, rc_src);
    TEST_ASSERT_NOT_NULL(src);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(src, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "hello", 6);
    mmapq_indexed_appender_close(a);

    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(src, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(r, out, 0, 0, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(src);

    /* Import with MMAPQ_CONFLICT_ABORT — should return MMAPQ_EIDCONFLICT since ID 0 is in use */
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE,
                                              rt, rt, 1, NULL, NULL, MMAPQ_CONFLICT_ABORT);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EIDCONFLICT, imported);

    mmapq_indexed_appender_close(existing);
    mmapq_indexed_queue_close(dest);
    unlink(fpath);
}

static int32_t map_to_out_of_range(uint16_t aid, void *ctx)
{
    (void)aid; (void)ctx;
    return -2; /* outside 0-255 range — should auto-assign */
}

void test_import_mapper_invalid_id_falls_back_to_auto(void)
{
    cleanup_queue(QUEUE);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_DIR, QUEUE);
    mkdir(path, 0775);

    MmapqIndexedQueue *q = NULL;
    int rc_q = mmapq_indexed_queue_create(TEST_DIR, QUEUE, 32768, 16, 1, 32768LL * 8,
                                     0, 5000000000ULL, 0, rt, rt, &q);
    TEST_ASSERT_EQUAL_INT(0, rc_q);
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a = NULL;
    int rc_a = mmapq_indexed_queue_create_appender(q, &a);
    TEST_ASSERT_EQUAL_INT(0, rc_a);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "hello", 6);
    mmapq_indexed_appender_close(a);

    MmapqIndexedReader *r = NULL;
    int rc_r = mmapq_indexed_queue_create_reader(q, &r);
    TEST_ASSERT_EQUAL_INT(0, rc_r);
    TEST_ASSERT_NOT_NULL(r);
    char fpath[512];
    snprintf(fpath, sizeof(fpath), "%s/%s_export.bin", TEST_DIR, QUEUE);
    FILE *out = fopen(fpath, "wb");
    mmapq_queue_transfer_export(r, out, 0, 0, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
    fclose(out);
    mmapq_indexed_reader_close(r);
    mmapq_indexed_queue_close(q);

    /* Mapper returns 255 (out of range) — should auto-assign, not fail */
    cleanup_queue(COPY_Q);
    FILE *in = fopen(fpath, "rb");
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE,
                                               rt, rt, 0,
                                               map_to_out_of_range, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    unlink(fpath);
}

/* ── C-09: export stream header reserved bytes ─────────────────────────────
 * The 30 reserved bytes (offsets 34-63) of the 64-byte ExportMeta header must
 * be zero on the wire.  The primary pin is the direct encoder unit test; the
 * fmemopen tests are integration coverage whose pre-fix failure depends on
 * stack-frame geometry (recorded, not counted as fail-first evidence).  The
 * byte-identity checks pin export determinism only — identical garbage would
 * pass them on buggy code.  The import pins guard backward compatibility:
 * streams written before the fix carry garbage reserved bytes and must stay
 * importable (the decoder ignores reserved; import must never validate it). */

/* Direct unit pin on the encoder — deterministic in both build configs. */
void test_export_meta_encode_zeroes_reserved(void)
{
    ExportMeta meta;
    export_meta_init(&meta);
    meta.schema_version = 1;
    meta.region_size    = 32768;
    meta.start_index    = 7;
    meta.max_file_size  = 32768LL * 8;
    meta.total_entries  = 3;
    meta.archivable     = 0;
    meta.queue_type     = 0;

    uint8_t buf[EXPORT_META_SIZE];
    memset(buf, 0xAA, sizeof(buf));
    export_meta_encode(&meta, buf);
    for (int i = EXPORT_META_PAYLOAD_START; i < EXPORT_META_SIZE; i++)
        TEST_ASSERT_EQUAL_UINT8(0, buf[i]);
}

static volatile uint8_t dirty_sink;

/* Fill 4 KiB of stack with 0xAA so a subsequent uninitialized stack buffer
 * likely lands on dirtied bytes.  noinline + the volatile sink read defeat
 * dead-store elimination under -O3. */
__attribute__((noinline)) static void dirty_stack_4k(void)
{
    uint8_t buf[4096];
    memset(buf, 0xAA, sizeof(buf));
    for (size_t i = 0; i < sizeof(buf); i += 512)
        dirty_sink = buf[i];
}

static void assert_header_reserved_zero(const char *buf)
{
    for (int i = EXPORT_META_PAYLOAD_START; i < EXPORT_META_SIZE; i++)
        TEST_ASSERT_EQUAL_UINT8(0, (uint8_t)buf[i]);
}

void test_export_stream_header_reserved_bytes_zero(void)
{
    const char *msgs[] = {"r1", "r2"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 2, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    /* header + 2 entries of (uint16 appender_id + int32 length + 3 bytes) */
    const size_t stream_len = EXPORT_META_SIZE + 2 * (sizeof(uint16_t) + sizeof(int32_t) + 3);
    static char buf1[4096], buf2[4096];
    memset(buf1, 0, sizeof(buf1));
    memset(buf2, 0, sizeof(buf2));

    dirty_stack_4k();
    FILE *out = fmemopen(buf1, sizeof(buf1), "wb");
    TEST_ASSERT_NOT_NULL(out);
    int64_t n = mmapq_queue_transfer_export(reader, out, 0, 1, MMAPQ_QUEUE_TRANSFER_REBASE,
                                            NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(2, n);
    assert_header_reserved_zero(buf1);

    dirty_stack_4k();
    out = fmemopen(buf2, sizeof(buf2), "wb");
    TEST_ASSERT_NOT_NULL(out);
    n = mmapq_queue_transfer_export(reader, out, 0, 1, MMAPQ_QUEUE_TRANSFER_REBASE,
                                    NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(2, n);
    TEST_ASSERT_EQUAL_INT(0, memcmp(buf1, buf2, stream_len));

    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);
}

void test_batch_export_stream_header_reserved_bytes_zero(void)
{
    const char *batches[][3] = {{"b1", "b2", NULL}};
    int sizes[] = {2};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    /* header + (uint16 appender_id + int32 count) + 2 x (int32 length + 3 bytes) */
    const size_t stream_len = EXPORT_META_SIZE + sizeof(uint16_t) + sizeof(int32_t)
                            + 2 * (sizeof(int32_t) + 3);
    static char buf1[4096], buf2[4096];
    memset(buf1, 0, sizeof(buf1));
    memset(buf2, 0, sizeof(buf2));

    dirty_stack_4k();
    FILE *out = fmemopen(buf1, sizeof(buf1), "wb");
    TEST_ASSERT_NOT_NULL(out);
    int64_t n = mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                                  NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(1, n);
    assert_header_reserved_zero(buf1);

    dirty_stack_4k();
    out = fmemopen(buf2, sizeof(buf2), "wb");
    TEST_ASSERT_NOT_NULL(out);
    n = mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                          NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(1, n);
    TEST_ASSERT_EQUAL_INT(0, memcmp(buf1, buf2, stream_len));

    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);
}

void test_import_accepts_nonzero_reserved_bytes(void)
{
    const char *msgs[] = {"c1", "c2", "c3"};
    MmapqIndexedQueue *src = create_queue(QUEUE, msgs, 3, 0);
    MmapqIndexedReader *reader = NULL;
    int rc_reader = mmapq_indexed_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    static char buf[4096];
    memset(buf, 0, sizeof(buf));
    FILE *out = fmemopen(buf, sizeof(buf), "wb");
    TEST_ASSERT_NOT_NULL(out);
    int64_t n = mmapq_queue_transfer_export(reader, out, 0, 2, MMAPQ_QUEUE_TRANSFER_REBASE,
                                            NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(3, n);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_queue_close(src);

    /* Simulate a stream written by a pre-fix library: garbage in reserved. */
    memset(buf + EXPORT_META_PAYLOAD_START, 0xAA, EXPORT_META_PAYLOAD_SIZE);

    cleanup_queue(COPY_Q);
    FILE *in = fmemopen(buf, sizeof(buf), "rb");
    TEST_ASSERT_NOT_NULL(in);
    int64_t imported = mmapq_queue_transfer_import(in, TEST_DIR, COPY_Q, MMAPQ_QUEUE_TRANSFER_REBASE,
                                                   rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(3, imported);

    char loaded[10][100];
    int cnt = load_messages(COPY_Q, loaded, 10);
    TEST_ASSERT_EQUAL_INT(3, cnt);
    TEST_ASSERT_EQUAL_STRING("c1", loaded[0]);
    TEST_ASSERT_EQUAL_STRING("c2", loaded[1]);
    TEST_ASSERT_EQUAL_STRING("c3", loaded[2]);
}

void test_batch_import_accepts_nonzero_reserved_bytes(void)
{
    const char *batches[][3] = {{"x1", "x2", NULL}};
    int sizes[] = {2};
    MmapqIndexedBatchQueue *src = create_batch_queue(BQ_QUEUE, batches, sizes, 1);
    MmapqIndexedBatchReader *reader = NULL;
    int rc_reader = mmapq_indexed_batch_queue_create_reader(src, &reader);
    TEST_ASSERT_EQUAL_INT(0, rc_reader);
    TEST_ASSERT_NOT_NULL(reader);

    static char buf[4096];
    memset(buf, 0, sizeof(buf));
    FILE *out = fmemopen(buf, sizeof(buf), "wb");
    TEST_ASSERT_NOT_NULL(out);
    int64_t n = mmapq_batch_queue_transfer_export(reader, out, 0, 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                                  NULL, NULL, NULL);
    fclose(out);
    TEST_ASSERT_EQUAL_INT64(1, n);
    mmapq_indexed_batch_reader_close(reader);
    mmapq_indexed_batch_queue_close(src);

    /* Simulate a stream written by a pre-fix library: garbage in reserved. */
    memset(buf + EXPORT_META_PAYLOAD_START, 0xAA, EXPORT_META_PAYLOAD_SIZE);

    cleanup_queue(BQ_COPY);
    FILE *in = fmemopen(buf, sizeof(buf), "rb");
    TEST_ASSERT_NOT_NULL(in);
    int64_t imported = mmapq_batch_queue_transfer_import(in, TEST_DIR, BQ_COPY,
                                                         MMAPQ_BATCH_QUEUE_TRANSFER_REBASE,
                                                         rt, rt, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fclose(in);
    TEST_ASSERT_EQUAL_INT64(1, imported);
    verify_batch_index_space(BQ_COPY, 0, 0);

    /* Read back the imported payloads, not just the index space: reserved-byte
       garbage must not disturb the batch stream parse. */
    MmapqIndexedBatchQueue *vq = NULL;
    int rc_vq = mmapq_indexed_batch_queue_create(TEST_DIR, BQ_COPY, 32768, 16, 1, 32768LL * 8,
                                                 0, 5000000000ULL, 0, rt, rt, &vq);
    TEST_ASSERT_EQUAL_INT(0, rc_vq);
    TEST_ASSERT_NOT_NULL(vq);
    MmapqIndexedBatchReader *vr = NULL;
    int rc_vr = mmapq_indexed_batch_queue_create_reader(vq, &vr);
    TEST_ASSERT_EQUAL_INT(0, rc_vr);
    TEST_ASSERT_NOT_NULL(vr);
    MmapqBatchReadingContext vctx = {0};
    mmapq_indexed_batch_reader_read(vr, 0, &vctx);
    TEST_ASSERT_EQUAL(MMAPQ_BATCH_ENTRY_AVAILABLE, vctx.status);
    TEST_ASSERT_EQUAL_INT(2, vctx.message_count);
    mmapq_batch_iterator_init(vctx.iterator, vctx.batch_offset, vctx.message_count, 0);
    const char *expected[] = {"x1", "x2"};
    char msgbuf[100];
    int seen = 0;
    while (mmapq_batch_iterator_advance(vctx.iterator)) {
        TEST_ASSERT_LESS_THAN(2, seen);
        int32_t l = batch_iterator_current_length(vctx.iterator);
        memcpy(msgbuf, batch_iterator_current_data(vctx.iterator), (size_t)l);
        msgbuf[l - 1] = '\0';
        TEST_ASSERT_EQUAL_STRING(expected[seen], msgbuf);
        seen++;
    }
    TEST_ASSERT_EQUAL_INT(2, seen);
    TEST_ASSERT_EQUAL_INT(0, vctx.iterator->last_error);
    mmapq_indexed_batch_reader_close(vr);
    mmapq_indexed_batch_queue_close(vq);
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
    RUN_TEST(test_batch_copy_source_corruption_mid_batch_leaves_nothing);

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
    RUN_TEST(test_import_mapper_invalid_id_falls_back_to_auto);

    /* C-09 — export header reserved bytes (5) */
    RUN_TEST(test_export_meta_encode_zeroes_reserved);
    RUN_TEST(test_export_stream_header_reserved_bytes_zero);
    RUN_TEST(test_batch_export_stream_header_reserved_bytes_zero);
    RUN_TEST(test_import_accepts_nonzero_reserved_bytes);
    RUN_TEST(test_batch_import_accepts_nonzero_reserved_bytes);

    return UNITY_END();
}
