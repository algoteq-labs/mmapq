#include "unity.h"
#include "indexed_archiver.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "internal/queue_meta.h"
#include "internal/appender_meta.h"
#include "internal/header_codec.h"
#include "internal/payload_iterator.h"
#include "internal/uncommitted_payload_iterator.h"
#include "runtime.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

static const char    *DIR      = "/tmp";
static const char    *QUEUE    = "mmap_test_arch";
static const int32_t  REGION_SZ = 4096 * 16;
static const int64_t  MAX_FILE  = (int64_t)4096 * 16 * 64;
static const int      RING_SZ   = 4;
static const uint64_t TIMEOUT   = 5000000000ULL;

static MmapqRuntime *rt = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

/* Create queue with archivable=1 by writing meta before opening appender */
static MmapqIndexedAppender *create_archivable_appender(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);

    QueueMeta *m = NULL;
    int qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        REGION_SZ, MAX_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a = NULL;
    int rc = mmapq_indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    if (rc != 0) return NULL;
    return a;
}

static MmapqNextMove poll_advance(int64_t idx, uint8_t aid,
                              const void *data, int32_t len, void *ctx)
{
    (void)idx; (void)aid; (void)data; (void)len;
    if (ctx) (*(int *)ctx)++;
    return MMAPQ_NEXT_ADVANCE;
}

void setUp(void)
{
    cleanup();
    int rc_rt = mmapq_runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);
}

void tearDown(void)
{
    cleanup();
}

/* ── Helper: ensure queue directory exists ───────────────────────────────── */

static void ensure_queue_dir(void)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", DIR, QUEUE);
    mkdir(path, 0775);
}

/* ── New tests for archivable parameter ───────────────────────────────────── */

void test_archivable_roundtrip(void)
{
    ensure_queue_dir();
    /* Create archivable queue via public factory */
    MmapqIndexedAppender *a = NULL;
    int rc = mmapq_indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 1, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_close(a);

    /* Verify byte 24 == 1 */
    char path[512];
    snprintf(path, sizeof(path), "/tmp/%s/meta", QUEUE);
    uint8_t byte24 = 0xFF;
    FILE *f = fopen(path, "rb");
    if (f) {
        fseek(f, 24, SEEK_SET);
        TEST_ASSERT_EQUAL_INT(1, (int)fread(&byte24, 1, 1, f));
        fclose(f);
    }
    TEST_ASSERT_EQUAL_INT(1, byte24);

    /* Verify archiver reports archivable */
    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_archivable(arch));
    mmapq_indexed_archiver_close(arch);

    /* Reopen with archivable=0 — disk wins, still archivable */
    a = NULL;
    rc = mmapq_indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_close(a);

    arch = NULL;
    int rc_arch2 = mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch2);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_TRUE(mmapq_indexed_archiver_archivable(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_default_not_archivable(void)
{
    ensure_queue_dir();
    MmapqIndexedAppender *a = NULL;
    int rc = mmapq_indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_close(a);

    char path[512];
    snprintf(path, sizeof(path), "/tmp/%s/meta", QUEUE);
    uint8_t byte24 = 0xFF;
    FILE *f = fopen(path, "rb");
    if (f) {
        fseek(f, 24, SEEK_SET);
        TEST_ASSERT_EQUAL_INT(1, (int)fread(&byte24, 1, 1, f));
        fclose(f);
    }
    TEST_ASSERT_EQUAL_INT(0, byte24);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_FALSE(mmapq_indexed_archiver_archivable(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_archive_up_to_index_rejects_non_archivable(void)
{
    ensure_queue_dir();
    MmapqIndexedAppender *a = NULL;
    int rc = mmapq_indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_append(a, "msg", 3);
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    int rc2 = mmapq_indexed_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc2);
    mmapq_indexed_archiver_close(arch);
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_archiver_start_index_initially_zero(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    mmapq_indexed_appender_append(a, "hello", 5);
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_archiver_start_index(arch));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_archivable(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_archiver_archive_forward_updates_start_index(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    for (int i = 0; i < 5; i++) {
        char msg[16];
        snprintf(msg, sizeof(msg), "msg%d", i);
        mmapq_indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_archiver_start_index(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_archiver_poller_returns_archived_below_start_index(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    for (int i = 0; i < 5; i++) {
        char msg[16];
        snprintf(msg, sizeof(msg), "m%d", i);
        mmapq_indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    mmapq_indexed_appender_close(a);

    /* Create poller BEFORE archiving — cursor starts at 0 */
    MmapqIndexedPoller *p = NULL;
    int rc_p = mmapq_indexed_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Verify entry 0 is readable before archive */
    int count = 0;
    MmapqPollResult r = mmapq_indexed_poller_poll(p, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT(1, count);

    /* Reset cursor to 0 */
    mmapq_indexed_poller_move_to_index(p, 0);

    /* Archive entries 0..2 — now start_index = 3 */
    MmapqIndexedArchiver *arch = NULL;
    int rc_arch3 = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch3);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_archiver_archive_up_to_index(arch, 3);
    mmapq_indexed_archiver_close(arch);

    /* Poller cursor is at 0 which is now below start_index=3 — returns MMAPQ_POLL_ARCHIVED */
    r = mmapq_indexed_poller_poll(p, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ARCHIVED, r);
    TEST_ASSERT_EQUAL_INT(1, count); /* count unchanged — handler not called */

    mmapq_indexed_poller_close(p);

    /* New poller starts at start_index=3 automatically */
    MmapqIndexedPoller *p2 = NULL;
    int rc_p2 = mmapq_indexed_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &p2);
    TEST_ASSERT_EQUAL_INT(0, rc_p2);
    TEST_ASSERT_NOT_NULL(p2);
    count = 0;
    r = mmapq_indexed_poller_poll(p2, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT(1, count); /* entry 3 */

    r = mmapq_indexed_poller_poll(p2, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT(2, count); /* entry 4 */

    r = mmapq_indexed_poller_poll(p2, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(MMAPQ_POLL_END_OF_STREAM, r);

    mmapq_indexed_poller_close(p2);
}

void test_archiver_start_offset_advances_after_archive(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    uint8_t appender_id = mmapq_indexed_appender_id(a);
    for (int i = 0; i < 3; i++) {
        char msg[16];
        snprintf(msg, sizeof(msg), "m%d", i);
        mmapq_indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    int64_t before = 0;
    int ok = mmapq_indexed_archiver_start_offset(arch, appender_id, &before);
    TEST_ASSERT_EQUAL_INT(1, ok);

    mmapq_indexed_archiver_archive_up_to_index(arch, 2);

    int64_t after = 0;
    ok = mmapq_indexed_archiver_start_offset(arch, appender_id, &after);
    TEST_ASSERT_EQUAL_INT(1, ok);
    TEST_ASSERT_TRUE(after > before);
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_archiver_start_index(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_archiver_archivable_files_none_at_start_zero(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    mmapq_indexed_appender_append(a, "x", 1);
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    char *paths[64];
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    mmapq_indexed_archiver_close(arch);
}

void test_archiver_backward_unarchive(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    for (int i = 0; i < 4; i++) {
        char msg[16];
        snprintf(msg, sizeof(msg), "b%d", i);
        mmapq_indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_archiver_start_index(arch));

    /* Un-archive back to 1 — files still exist */
    mmapq_indexed_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_archiver_start_index(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_archiver_noop_when_index_equals_start(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    mmapq_indexed_appender_append(a, "x", 1);
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc_arch = mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    mmapq_indexed_archiver_archive_up_to_index(arch, 0); /* no-op */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_archiver_start_index(arch));
    mmapq_indexed_archiver_close(arch);
}

void test_archive_forward_empty_queue(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    int rc = mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(arch);
    int rc2 = mmapq_indexed_archiver_archive_up_to_index(arch, 5);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc2);
    mmapq_indexed_archiver_close(arch);
}

static void overwrite_queue_bytes(const char *name, int64_t offset,
                                  const void *data, size_t size)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/%s", DIR, QUEUE, name);
    FILE *f = fopen(path, "r+b");
    TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQUAL_INT(0, fseek(f, (long)offset, SEEK_SET));
    TEST_ASSERT_EQUAL_INT64(size, fwrite(data, 1, size, f));
    TEST_ASSERT_EQUAL_INT(0, fclose(f));
}

static void check_archive_rejects_corrupt_payload(int32_t length,
                                                 int short_prefix,
                                                 int other_appender)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_id(a));
    if (other_appender) {
        TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "valid", 5));
        MmapqIndexedAppender *b = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
            REGION_SZ, RING_SZ, MAX_FILE, TIMEOUT, 1, rt, 0, &b));
        mmapq_indexed_appender_close(a);
        a = b;
        TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_appender_id(a));
    }
    TEST_ASSERT_EQUAL_INT64(other_appender,
        mmapq_indexed_appender_append(a, "first", 5));
    TEST_ASSERT_EQUAL_INT64(other_appender + 1,
        mmapq_indexed_appender_append(a, "second", 6));
    mmapq_indexed_appender_close(a);

    if (short_prefix) {
        uint64_t header = header_encode((uint8_t)other_appender, REGION_SZ - 2);
        overwrite_queue_bytes("header_0", header_position(other_appender),
                              &header, sizeof(header));
    } else {
        overwrite_queue_bytes(other_appender ? "payload_1_0" : "payload_0_0",
                              INITIAL_PAYLOAD_POSITION, &length, sizeof(length));
    }

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    int64_t before[2], after[2];
    for (int id = 0; id <= other_appender; id++)
        TEST_ASSERT_EQUAL_INT(1,
            mmapq_indexed_archiver_start_offset(arch, (uint8_t)id, &before[id]));
    int rc = mmapq_indexed_archiver_archive_up_to_index(arch, other_appender + 1);
    int64_t start = mmapq_indexed_archiver_start_index(arch);
    for (int id = 0; id <= other_appender; id++)
        TEST_ASSERT_EQUAL_INT(1,
            mmapq_indexed_archiver_start_offset(arch, (uint8_t)id, &after[id]));
    char *paths[64];
    int count = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    for (int i = 0; i < count; i++) free(paths[i]);
    mmapq_indexed_archiver_close(arch);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_EQUAL_INT64(0, start);
    for (int id = 0; id <= other_appender; id++)
        TEST_ASSERT_EQUAL_INT64(before[id], after[id]);
    TEST_ASSERT_EQUAL_INT(0, count);

    MmapqIndexedReader *reader = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &reader));
    MmapqReadingContext ctx;
    mmapq_indexed_reader_read(reader, other_appender + 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(6, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("second", ctx.data, 6);
    mmapq_indexed_reader_close(reader);
}

void test_archiver_rejects_oversized_payload_without_archiving_file(void)
{
    check_archive_rejects_corrupt_payload(1073741824, 0, 0);
}

void test_archiver_corruption_preserves_all_appender_offsets(void)
{
    check_archive_rejects_corrupt_payload(1073741824, 0, 1);
}

void test_archiver_rejects_payload_one_byte_past_region(void)
{
    check_archive_rejects_corrupt_payload(
        REGION_SZ - INITIAL_PAYLOAD_POSITION - PAYLOAD_LENGTH_SIZE + 1, 0, 0);
}

void test_archiver_rejects_truncated_length_prefix(void)
{
    check_archive_rejects_corrupt_payload(0, 1, 0);
}

void test_archiver_accepts_payload_ending_at_region_boundary(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);
    int32_t length = REGION_SZ - INITIAL_PAYLOAD_POSITION - PAYLOAD_LENGTH_SIZE;
    char *payload = malloc((size_t)length);
    TEST_ASSERT_NOT_NULL(payload);
    memset(payload, 'x', (size_t)length);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, payload, length));
    free(payload);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, "second", 6));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_archiver_start_index(arch));
    int64_t offset;
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_start_offset(arch, 0, &offset));
    TEST_ASSERT_EQUAL_INT64(REGION_SZ, offset);
    char *paths[64];
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archivable_files(arch, paths, 64));
    mmapq_indexed_archiver_close(arch);
}

/* ── A5 / CAND-22 regression: archiving + uncommitted iterator ─────────────
 *
 * archive_up_to_index() advances appender_meta_start_offset past entries
 * whose HEADERS remain readable (only fully-covered header files are
 * deleted). scan_committed_ranges() walks headers from index 0 and re-inits
 * the appender-owned inner iterator with each committed header's payload
 * position. An archived entry's offset is below start_of_stream; pre-fix,
 * mmapqi_payload_iterator_init() free()d the appender-owned iterator on such
 * an offset and the next line advanced the freed object (ASan
 * heap-use-after-free at payload_iterator.c has_next; SIGSEGV under
 * -O2 -DNDEBUG), and mmapq_indexed_appender_close() double-freed it.
 *
 * Geometry: 4 × 4-byte messages at payload offsets 64, 72, 80, 88;
 * archiving 2 advances start_offset to exactly 80.
 */

void test_uncommitted_iterator_after_archive_no_use_after_free(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);

    for (int i = 0; i < 4; i++) {
        char buf[16];
        snprintf(buf, sizeof buf, "msg%d", i);
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_appender_append(a, buf, (int32_t)strlen(buf)));
    }

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, 2));

    /* start_offset must actually have advanced, or the test proves nothing. */
    int64_t sos = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_archiver_start_offset(arch, 0, &sos));
    TEST_ASSERT_EQUAL_INT64(80, sos);
    mmapq_indexed_archiver_close(arch);

    /* Pre-fix this reads freed memory inside scan_committed_ranges. */
    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);
    /* All four messages are committed: nothing uncommitted remains. */
    TEST_ASSERT_FALSE(mmapqi_uncommitted_iterator_advance(it));

    /* A second call must not compound the damage. */
    it = mmapq_indexed_appender_uncommitted_iterator(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);
    TEST_ASSERT_FALSE(mmapqi_uncommitted_iterator_advance(it));

    /* Pre-fix this double-frees uncommitted_inner. */
    mmapq_indexed_appender_close(a);
}

/* Second route to the same defect: rewind() re-enters init() with the stored
 * offsets and re-runs the scan against the advanced start_offset. A guard at
 * the factory cannot reach this path — the offending offset comes from the
 * header stream, not from the caller's arguments. Pins rewind as a supported
 * operation after archiving (behavioural contract). */
void test_uncommitted_iterator_rewind_after_archive(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);

    for (int i = 0; i < 4; i++) {
        char buf[16];
        snprintf(buf, sizeof buf, "msg%d", i);
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_appender_append(a, buf, (int32_t)strlen(buf)));
    }

    MmapqUncommittedPayloadIterator *u =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(u);

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    mmapq_indexed_archiver_close(arch);

    /* Pre-fix this re-runs the scan and reads freed memory. Post-fix the
     * re-scan succeeds against the advanced start_offset. */
    TEST_ASSERT_EQUAL_INT(0, mmapqi_uncommitted_iterator_rewind(u));
    TEST_ASSERT_FALSE(mmapqi_uncommitted_iterator_advance(u));

    mmapq_indexed_appender_close(a);
}

/* The archived-entry skip must not be over-broad: an uncommitted message
 * appended after archiving must still be found exactly once. */
void test_uncommitted_iterator_skips_archived_entries(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);

    for (int i = 0; i < 4; i++) {
        char buf[16];
        snprintf(buf, sizeof buf, "msg%d", i);
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_appender_append(a, buf, (int32_t)strlen(buf)));
    }

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    mmapq_indexed_archiver_close(arch);

    /* One uncommitted message after the committed, partly-archived prefix. */
    TEST_ASSERT_EQUAL_INT64(96,
        mmapq_indexed_appender_append_uncommitted(a, "late", 4));

    MmapqUncommittedPayloadIterator *it =
        mmapq_indexed_appender_uncommitted_iterator(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    int found = 0;
    while (mmapqi_uncommitted_iterator_advance(it)) {
        found++;
        Buffer *buf = mmapqi_uncommitted_iterator_current(it);
        TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
        TEST_ASSERT_EQUAL_MEMORY("late", buf->data, 4);
    }
    TEST_ASSERT_EQUAL_INT(1, found);

    mmapq_indexed_appender_close(a);
}

/* Ownership pin — the acceptance gate for the A5 contract fix. Route pins
 * pass on a skip-only partial fix; this one fails on it. Rejection must
 * return MMAPQ_EINVAL AND leave the caller-owned iterator usable: a valid
 * re-init afterwards is the discriminating line (a free-on-reject init
 * turns the re-init into a use-after-free). */
void test_payload_iterator_init_rejects_out_of_range(void)
{
    MmapqIndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);

    for (int i = 0; i < 4; i++) {
        char buf[16];
        snprintf(buf, sizeof buf, "msg%d", i);
        TEST_ASSERT_EQUAL_INT64(i,
            mmapq_indexed_appender_append(a, buf, (int32_t)strlen(buf)));
    }

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch));
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    mmapq_indexed_archiver_close(arch);

    MmapqPayloadIterator *it = mmapq_indexed_appender_iterator(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET);
    TEST_ASSERT_NOT_NULL(it);

    /* start_of_stream is now 80; offset 64 is archived. Reject. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL,
        mmapqi_payload_iterator_init(it, 64, PAYLOAD_ITERATOR_NOT_SET));

    /* The iterator was NOT freed: a valid re-init succeeds and walks the
     * first unarchived message. */
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_payload_iterator_init(
            it, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_TRUE(mmapqi_payload_iterator_advance(it));
    Buffer *buf = mmapqi_payload_iterator_current(it);
    TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
    TEST_ASSERT_EQUAL_MEMORY("msg2", buf->data, 4);

    mmapq_indexed_appender_close(a);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_archiver_start_index_initially_zero);
    RUN_TEST(test_archiver_archive_forward_updates_start_index);
    RUN_TEST(test_archiver_poller_returns_archived_below_start_index);
    RUN_TEST(test_archiver_start_offset_advances_after_archive);
    RUN_TEST(test_archiver_archivable_files_none_at_start_zero);
    RUN_TEST(test_archiver_backward_unarchive);
    RUN_TEST(test_archiver_noop_when_index_equals_start);
    RUN_TEST(test_archivable_roundtrip);
    RUN_TEST(test_default_not_archivable);
    RUN_TEST(test_archive_up_to_index_rejects_non_archivable);
    RUN_TEST(test_archive_forward_empty_queue);
    RUN_TEST(test_archiver_rejects_oversized_payload_without_archiving_file);
    RUN_TEST(test_archiver_corruption_preserves_all_appender_offsets);
    RUN_TEST(test_archiver_rejects_payload_one_byte_past_region);
    RUN_TEST(test_archiver_rejects_truncated_length_prefix);
    RUN_TEST(test_archiver_accepts_payload_ending_at_region_boundary);
    RUN_TEST(test_uncommitted_iterator_after_archive_no_use_after_free);
    RUN_TEST(test_uncommitted_iterator_rewind_after_archive);
    RUN_TEST(test_uncommitted_iterator_skips_archived_entries);
    RUN_TEST(test_payload_iterator_init_rejects_out_of_range);
    return UNITY_END();
}
