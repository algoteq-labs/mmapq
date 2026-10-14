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
#include "internal/queue_files.h"
#include "internal/test_hooks.h"
#include "runtime.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>

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

/* CAND-28 (owner review, 2nd round): the set records ANY window of
 * FILE_INDEX_SET_MAX consecutive file numbers, whatever order they arrive
 * in — resequenced commits make payload file order non-monotonic. set()
 * returns 1 when stored, 0 when the window genuinely exceeds the span;
 * first()/next() return absolute indices. The previous silent drop (and
 * the intermediate absolute indexing) let a backward undo skip or refuse
 * existence validation for files >= 4096. */
void test_file_index_set_overflow_returns_zero(void)
{
    FileIndexSet s;

    /* Ascending window: stores until the span exceeds FILE_INDEX_SET_MAX */
    file_index_set_clear(&s);
    TEST_ASSERT_EQUAL_INT(1, file_index_set_set(&s, 10000));
    TEST_ASSERT_EQUAL_INT(1, file_index_set_set(&s, 10001));
    TEST_ASSERT_EQUAL_INT(0, file_index_set_set(&s, 10000 + FILE_INDEX_SET_MAX));
    TEST_ASSERT_EQUAL_INT(1, file_index_set_test(&s, 10000));
    TEST_ASSERT_EQUAL_INT(1, file_index_set_test(&s, 10001));
    TEST_ASSERT_EQUAL_INT64(10000, mmapqi_file_index_set_first(&s));
    TEST_ASSERT_EQUAL_INT64(10001, mmapqi_file_index_set_next(&s, 10000));
    TEST_ASSERT_EQUAL_INT64(-1, mmapqi_file_index_set_next(&s, 10001));

    /* Out-of-order recording: the set rebases downward when the widened
     * window still fits (10000..10001), and first() tracks the new base */
    file_index_set_clear(&s);
    TEST_ASSERT_EQUAL_INT(1, file_index_set_set(&s, 10001));
    TEST_ASSERT_EQUAL_INT(1, file_index_set_set(&s, 10000));
    TEST_ASSERT_EQUAL_INT64(10000, mmapqi_file_index_set_first(&s));
    TEST_ASSERT_EQUAL_INT(1, file_index_set_test(&s, 10000));
    TEST_ASSERT_EQUAL_INT(1, file_index_set_test(&s, 10001));
    TEST_ASSERT_EQUAL_INT(0, file_index_set_set(&s, 14096)); /* 10000..14096 = 4097 files */

    /* Downward rebase refused when the window genuinely exceeds the span */
    file_index_set_clear(&s);
    TEST_ASSERT_EQUAL_INT(1, file_index_set_set(&s, 10001));
    TEST_ASSERT_EQUAL_INT(0, file_index_set_set(&s, 5905));  /* 5905..10001 = 4097 files */
}

/* CAND-28 owner-review pin: a one-index undo whose file numbers are >=
 * FILE_INDEX_SET_MAX must succeed — the FileIndexSet records a SPAN
 * relative to the first file in the undo range, so a narrow undo high in
 * the file numbering is servable. With 4 KiB files (512 headers each),
 * index 2^21 sits in header file 4096; the absolute-indexed set refused
 * this undo with EINVAL (owner probe: the same undo returned 0 on the D3
 * tree). */
void test_archiver_undo_one_index_at_header_file_4096(void)
{
    const int64_t S = (int64_t)512 * 4096;  /* first index of header file 4096 */

    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    for (int64_t i = 0; i < S + 2; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "u", 1));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, S + 2));
    TEST_ASSERT_EQUAL_INT64(S + 2, mmapq_indexed_archiver_start_index(arch));

    /* One-index undo at header file 4096 — absolute-indexed set: EINVAL. */
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, S + 1));
    TEST_ASSERT_EQUAL_INT64(S + 1, mmapq_indexed_archiver_start_index(arch));
    mmapq_indexed_archiver_close(arch);
}

/* CAND-28 owner-review pin (2nd round): append_uncommitted + commit is the
 * documented UDP-resequencing flow and commit() has no ordering check, so
 * per-appender payload positions do NOT ascend with index. A narrow undo
 * must still succeed: the FileIndexSet records any window of
 * FILE_INDEX_SET_MAX consecutive file numbers regardless of arrival order.
 * Region = file = 4 KiB: A (4000 B) fills payload file 0 to offset 4068,
 * so B (100 B) rolls to file 1. Commit B -> idx 0, then A -> idx 1;
 * forward(2), undo(0): the payload set records file 1 first, then file 0
 * below the base. Owner probe: 0 on the D3 tree and the first CAND-28
 * tree; ECORRUPT on the relative-base tree without downward rebase.
 * (No batch twin: the batch appender owns a single in-flight batch per
 * appender, so it cannot commit offsets out of order.) */
void test_archiver_undo_with_resequenced_commits(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, &a));
    TEST_ASSERT_NOT_NULL(a);

    char payload_a[4000];
    memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];
    memset(payload_b, 'B', sizeof(payload_b));

    int64_t off_a = mmapq_indexed_appender_append_uncommitted(
        a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(
        a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(4096, off_b);  /* rolled to payload file 1 */

    /* Resequenced commit: B first, then A */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_archiver_start_index(arch));

    /* Undo over both entries — payload file order is 1 then 0 */
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, 0));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_archiver_start_index(arch));
    mmapq_indexed_archiver_close(arch);

    /* Both entries read back correctly */
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &r));
    TEST_ASSERT_NOT_NULL(r);
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT((int)sizeof(payload_b), ctx.length);
    TEST_ASSERT_EQUAL_MEMORY(payload_b, ctx.data, sizeof(payload_b));
    mmapq_indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT((int)sizeof(payload_a), ctx.length);
    TEST_ASSERT_EQUAL_MEMORY(payload_a, ctx.data, sizeof(payload_a));
    mmapq_indexed_reader_close(r);
}

/* ── CAND-30 pins (archiving vs resequenced/uncommitted payloads) ────────── */
/* Geometry for all CAND-30 pins: 4 KiB regions, 4 KiB files, archivable —
 * matches the reviewer probes (S1/S2/T1/T2/P12). */

/* Mirrors the appender's padding rule at region == file == 4096. */
static int64_t record_end(int64_t pos, int32_t len)
{
    int64_t cap = 4096 - (pos % 4096);
    if (cap < (int64_t)(PAYLOAD_LENGTH_SIZE + len)) pos += cap;
    return pos + PAYLOAD_LENGTH_SIZE + len;
}

static MmapqIndexedAppender *tiny_archivable_appender(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    return a;
}

static MmapqIndexedArchiver *tiny_archiver(void)
{
    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    return arch;
}

static int64_t arch_start_offset(MmapqIndexedArchiver *arch, uint8_t aid)
{
    int64_t off = -1;
    TEST_ASSERT_EQUAL_INT(1,
        mmapq_indexed_archiver_start_offset(arch, aid, &off));
    return off;
}

static int list_contains(char **paths, int n, const char *suffix)
{
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], suffix)) return 1;
    return 0;
}

static void free_paths(char **paths, int n)
{
    for (int i = 0; i < n; i++) free(paths[i]);
}

/* P1 / S1: resequenced commits — a live payload below the boundary must keep
 * its file off the archivable list. Pre-fix: payload_0_0 listed, read(1)
 * fails after deleting it (owner probe). */
void test_archiver_resequenced_live_payload_not_listed(void)
{
    char payload_a[4000]; memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));
    char payload_c[50];   memset(payload_c, 'C', sizeof(payload_c));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(4096, off_b);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(a, payload_c, (int32_t)sizeof(payload_c)));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));

    /* Boundary must not pass A's live payload at 64 (pre-fix: 4200) */
    TEST_ASSERT_LESS_OR_EQUAL_INT64(INITIAL_PAYLOAD_POSITION,
        arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    TEST_ASSERT_FALSE(list_contains(paths, n, "payload_0_0"));
    for (int i = 0; i < n; i++) unlink(paths[i]);
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &r));
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT((int)sizeof(payload_a), ctx.length);
    TEST_ASSERT_EQUAL_MEMORY(payload_a, ctx.data, sizeof(payload_a));
    mmapq_indexed_reader_close(r);
}

/* P2 / S2: an uncommitted payload overtaken by archiving can never become
 * live — commit returns EINVAL and nothing is published (pre-fix: commits
 * as idx 1; its file was already listed). */
void test_commit_overtaken_payload_rejected(void)
{
    char payload_a[4000]; memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, payload_b, (int32_t)sizeof(payload_b)));

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    mmapq_indexed_archiver_close(arch);

    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, mmapq_indexed_appender_commit(a, off_a));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &r));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM,
        mmapq_indexed_reader_entry_status(r, 1));
    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
}

/* P3: undo lowers the boundary to the lowest restored payload and never
 * raises it. Pre-fix: undo takes the FIRST restored position and blind-stores
 * it — here raising 4068 (post-forward) to 4096. */
void test_archiver_undo_never_raises_start_offset(void)
{
    char payload_a[4000]; memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(4096, off_b);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    int64_t after_forward = arch_start_offset(arch, 0);
    /* Exact value: the candidate is the end of the MAXIMUM-POSITION archived
     * entry (B ends at 4200), not the highest-index one (A ends at 4068) —
     * this is what catches mutation M1. */
    TEST_ASSERT_EQUAL_INT64(4200, after_forward);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 0));
    int64_t after_undo = arch_start_offset(arch, 0);
    TEST_ASSERT_LESS_OR_EQUAL_INT64(after_forward, after_undo);
    TEST_ASSERT_LESS_OR_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, after_undo);
    mmapq_indexed_archiver_close(arch);
}

/* P4 / T2: a joining appender with no live entry must walk forward from
 * start_offset, not write at it — bytes there may belong to a dead payload.
 * a0 abandons an uncommitted payload and has no committed entry; a1 commits
 * idx 0 so archiving can run. Pre-fix: rejoin 1 writes at start_offset over
 * A's length prefix; rejoin 2 fails ECORRUPT. */
void test_appender_rejoin_over_abandoned_payload(void)
{
    char payload_a[3000]; memset(payload_a, 'A', sizeof(payload_a));

    MmapqIndexedAppender *a0 = NULL;
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, 0, &a0));
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a0, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    /* abandoned — never committed */
    mmapq_indexed_appender_close(a0);

    MmapqIndexedAppender *a1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 1, 0, &a1));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a1, "B-live", 6));
    mmapq_indexed_appender_close(a1);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    mmapq_indexed_archiver_close(arch);

    /* Rejoin a0: no committed entry anywhere — must walk from start_offset */
    MmapqIndexedAppender *r1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, 0, &r1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(r1, "Zshort", 6));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(r1, "W", 1));
    mmapq_indexed_appender_close(r1);

    /* Second rejoin must recover cleanly (pre-fix: ECORRUPT) */
    MmapqIndexedAppender *r2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, 0, &r2));
    mmapq_indexed_appender_close(r2);

    MmapqIndexedReader *rd = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &rd));
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(rd, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(6, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("Zshort", ctx.data, 6);
    mmapq_indexed_reader_read(rd, 2, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT(1, ctx.length);
    TEST_ASSERT_EQUAL_MEMORY("W", ctx.data, 1);
    mmapq_indexed_reader_close(rd);
}

/* P5 (guard): appender creation on a fresh queue succeeds — the join walk's
 * pre-wrap distinguishes "no file yet" from corruption (mutation M3). */
void test_appender_join_fresh_queue(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "first", 5));
    mmapq_indexed_appender_close(a);
}

/* P6 (guard): in-order commits — the header-based boundary equals the old
 * highest-index computation exactly. Multi-file so rolling is exercised. */
void test_archiver_in_order_boundary_unchanged(void)
{
    char msg[1000]; memset(msg, 'm', sizeof(msg));
    MmapqIndexedAppender *a = tiny_archivable_appender();
    for (int i = 0; i < 10; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, msg, (int32_t)sizeof(msg)));
    mmapq_indexed_appender_close(a);

    int64_t expected = INITIAL_PAYLOAD_POSITION;
    for (int i = 0; i < 10; i++) expected = record_end(expected, (int32_t)sizeof(msg));

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 10));
    TEST_ASSERT_EQUAL_INT64(expected, arch_start_offset(arch, 0));
    mmapq_indexed_archiver_close(arch);
}

/* P8 / T1 (guard): a dead (never committed) tail must not block reclamation —
 * every payload file below the last entry's file stays listable. */
void test_archiver_crash_tail_reclamation(void)
{
    char msg[1000]; memset(msg, 'x', sizeof(msg));
    MmapqIndexedAppender *a = tiny_archivable_appender();
    for (int i = 0; i < 5; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, msg, (int32_t)sizeof(msg)));
    int64_t off_x = mmapq_indexed_appender_append_uncommitted(a, msg, (int32_t)sizeof(msg));
    TEST_ASSERT_GREATER_THAN_INT64(0, off_x);
    /* crash tail — never committed */
    mmapq_indexed_appender_close(a);

    /* Restart: the join walks past the dead payload */
    MmapqIndexedAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, &a2));
    for (int i = 5; i < 25; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a2, msg, (int32_t)sizeof(msg)));
    mmapq_indexed_appender_close(a2);

    int64_t expected = INITIAL_PAYLOAD_POSITION;
    for (int i = 0; i < 26; i++) expected = record_end(expected, (int32_t)sizeof(msg));

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 25));
    TEST_ASSERT_EQUAL_INT64(expected, arch_start_offset(arch, 0));

    int64_t files_below = expected / 4096;
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(2, files_below);
    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    /* Every payload file below the boundary's file is listed, including the
     * one holding the dead payload (file 1) */
    for (int64_t f = 0; f < files_below; f++) {
        char suffix[64];
        snprintf(suffix, sizeof(suffix), "payload_0_%lld", (long long)f);
        TEST_ASSERT_TRUE(list_contains(paths, n, suffix));
    }
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);
}

/* P12 / P12-case: the join's backward header scan must be bounded at
 * start_index — an archived header may point into a payload file that was
 * legitimately deleted. a0's only entry (ends exactly at the file end) is
 * archived, its payload file deleted, a1 stays live. Pre-fix: rejoin reads
 * the archived header, walks from the deleted payload → ECORRUPT. */
void test_appender_rejoin_after_payload_file_deleted(void)
{
    char big[4028]; memset(big, 'Z', sizeof(big));
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a0 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, 0, &a0));
    /* [64, 64+4+4028) = [64, 4096) — ends exactly at the file end */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a0, big, (int32_t)sizeof(big)));
    mmapq_indexed_appender_close(a0);

    MmapqIndexedAppender *a1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 1, 0, &a1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a1, "live", 4));
    mmapq_indexed_appender_close(a1);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(4096, arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_0_0"));
    for (int i = 0; i < n; i++) unlink(paths[i]);
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);

    /* Rejoin a0: its only entry is archived and its payload file is gone.
     * The bounded scan finds no live entry; the join walks from
     * start_offset = 4096 — the end of the stream. Pre-fix: ECORRUPT. */
    MmapqIndexedAppender *r1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        4096, 4, 4096, TIMEOUT, 0, rt, 0, 0, &r1));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(r1, "back", 4));
    mmapq_indexed_appender_close(r1);
}

/* P13: the commit fast path must be keyed on the last APPENDED offset —
 * "offset > last committed" skips the check for X and strands it. */
void test_commit_fast_path_keyed_on_last_appended(void)
{
    char msg[1000]; memset(msg, 'k', sizeof(msg));
    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, msg, (int32_t)sizeof(msg));
    int64_t off_x = mmapq_indexed_appender_append_uncommitted(a, msg, (int32_t)sizeof(msg));
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(a, msg, (int32_t)sizeof(msg));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    TEST_ASSERT_GREATER_THAN_INT64(off_x, off_b);   /* positions ascend: A < X < B */

    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));  /* fast */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));  /* slow */

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    /* Boundary = end of B, above X */
    TEST_ASSERT_GREATER_THAN_INT64(off_x, arch_start_offset(arch, 0));
    mmapq_indexed_archiver_close(arch);

    /* X was overtaken by archiving — committing it must be refused */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, mmapq_indexed_appender_commit(a, off_x));

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &r));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_END_OF_STREAM,
        mmapq_indexed_reader_entry_status(r, 2));
    mmapq_indexed_reader_close(r);
    mmapq_indexed_appender_close(a);
}

/* P14: the archive lock — an archive and an archivable_files listing both
 * block while another holder owns archive.lock exclusively. Pre-fix: no lock
 * exists, so both run immediately. */
typedef struct {
    MmapqIndexedArchiver *arch;
    int                  mode;   /* 0 = archive, 1 = list */
    _Atomic int          done;
    int                  rc;
} LockProbe;

static void *lock_probe_main(void *arg)
{
    LockProbe *p = arg;
    if (p->mode == 0) {
        p->rc = mmapq_indexed_archiver_archive_up_to_index(p->arch, 1);
    } else {
        char *paths[8] = {0};
        int n = mmapq_indexed_archiver_archivable_files(p->arch, paths, 8);
        for (int i = 0; i < n; i++) free(paths[i]);
        p->rc = n;
    }
    atomic_store_explicit(&p->done, 1, memory_order_release);
    return NULL;
}

void test_archive_lock_excludes_concurrent_archive(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, "m0", 2));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, "m1", 2));
    mmapq_indexed_appender_close(a);

    char lockpath[512];
    snprintf(lockpath, sizeof(lockpath), "%s/%s/archive.lock", DIR, QUEUE);
    int fd = open(lockpath, O_RDWR | O_CREAT, 0644);
    TEST_ASSERT_GREATER_OR_EQUAL(0, fd);
    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_EX));

    for (int mode = 0; mode <= 1; mode++) {
        MmapqIndexedArchiver *arch = tiny_archiver();
        LockProbe probe = { .arch = arch, .mode = mode, .done = 0, .rc = -999 };
        pthread_t t;
        TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, lock_probe_main, &probe));

        struct timespec ts = { .tv_sec = 0, .tv_nsec = 200000000 };
        nanosleep(&ts, NULL);
        TEST_ASSERT_EQUAL_INT(0,
            atomic_load_explicit(&probe.done, memory_order_acquire));

        TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_UN));
        TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
        TEST_ASSERT_EQUAL_INT(1, atomic_load_explicit(&probe.done, memory_order_acquire));
        TEST_ASSERT_GREATER_OR_EQUAL(0, probe.rc);
        mmapq_indexed_archiver_close(arch);

        if (mode == 0) {
            /* re-take the exclusive lock for the second probe */
            TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_EX));
        }
    }
    close(fd);
}

/* P11 / S4: resequencing appender vs archiving thread. Soundness note: the
 * invariant is checked by the ARCHIVER thread after each archive returns
 * (a commit-side immediate check can observe the transient between the
 * archiver's publish and its lowering pass — a false positive). The design's
 * guarantee: after archive returns, start_offset ≤ every payload whose commit
 * returned before the archive started. */
/* STRIDE 5 (odd multiple of the pair size): archives land with n-1 on a
 * B-index (even), so pre-fix the boundary passes the not-yet-archived A —
 * the S4 violation. An even stride would always land after an A-index and
 * mask it. */
#define P11_PAIRS  1000
#define P11_STRIDE 5

typedef struct {
    int64_t idx;
    int64_t pos;
} P11Rec;

static P11Rec      p11_recs[2 * P11_PAIRS];
static _Atomic long p11_rec_count;
static _Atomic int  p11_appender_done;

typedef struct {
    _Atomic int violations;
    _Atomic int archives;
    int       archive_errors;
} P11Checker;

static void *p11_appender_main(void *arg)
{
    int *rejects = arg;
    char msg[200]; memset(msg, 'u', sizeof(msg));
    MmapqIndexedAppender *a = NULL;
    if (mmapq_indexed_appender_create(DIR, QUEUE, 4096, 4, 4096, TIMEOUT,
                                      0, rt, 0, &a) != 0) {
        atomic_store(&p11_appender_done, 1);
        return NULL;
    }
    for (int k = 0; k < P11_PAIRS; k++) {
        int64_t pa = mmapq_indexed_appender_append_uncommitted(a, msg, (int32_t)sizeof(msg));
        int64_t pb = mmapq_indexed_appender_append_uncommitted(a, msg, (int32_t)sizeof(msg));
        int64_t ib = mmapq_indexed_appender_commit(a, pb);
        if (ib >= 0) {
            long s = atomic_load_explicit(&p11_rec_count, memory_order_relaxed);
            p11_recs[s].idx = ib; p11_recs[s].pos = pb;
            atomic_store_explicit(&p11_rec_count, s + 1, memory_order_release);
        } else if (ib == MMAPQ_EINVAL) {
            (*rejects)++;
        }
        int64_t ia = mmapq_indexed_appender_commit(a, pa);
        if (ia >= 0) {
            long s = atomic_load_explicit(&p11_rec_count, memory_order_relaxed);
            p11_recs[s].idx = ia; p11_recs[s].pos = pa;
            atomic_store_explicit(&p11_rec_count, s + 1, memory_order_release);
        } else if (ia == MMAPQ_EINVAL) {
            (*rejects)++;
        }
    }
    mmapq_indexed_appender_close(a);
    atomic_store_explicit(&p11_appender_done, 1, memory_order_release);
    return NULL;
}

static void *p11_archiver_main(void *arg)
{
    P11Checker *ck = arg;
    MmapqIndexedArchiver *arch = NULL;
    if (mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch) != 0)
        return NULL;
    int64_t next_n = 0;
    while (!atomic_load_explicit(&p11_appender_done, memory_order_acquire)
           || next_n < atomic_load_explicit(&p11_rec_count, memory_order_acquire)) {
        long s = atomic_load_explicit(&p11_rec_count, memory_order_acquire);
        if (s >= next_n + P11_STRIDE) {
            next_n += P11_STRIDE;
            int rc = mmapq_indexed_archiver_archive_up_to_index(arch, next_n);
            if (rc != 0) { ck->archive_errors++; continue; }
            atomic_fetch_add(&ck->archives, 1);
            int64_t si = mmapq_indexed_archiver_start_index(arch);
            int64_t so = arch_start_offset(arch, 0);
            for (long i = 0; i < s; i++) {
                if (p11_recs[i].idx >= si && p11_recs[i].pos < so)
                    atomic_fetch_add(&ck->violations, 1);
            }
        } else {
            sched_yield();
        }
    }
    mmapq_indexed_archiver_close(arch);
    return NULL;
}

void test_commit_archive_race_stress(void)
{
    /* tiny archivable queue */
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        4096, 4096, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    atomic_store(&p11_rec_count, 0);
    atomic_store(&p11_appender_done, 0);
    int rejects = 0;
    P11Checker ck = { .violations = 0, .archives = 0, .archive_errors = 0 };

    pthread_t ta, tc;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&ta, NULL, p11_appender_main, &rejects));
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&tc, NULL, p11_archiver_main, &ck));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(ta, NULL));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(tc, NULL));

    TEST_ASSERT_EQUAL_INT(0, ck.archive_errors);
    TEST_ASSERT_GREATER_OR_EQUAL(100, atomic_load(&ck.archives));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&ck.violations));
}

/* P9: a commit racing an archive — held between the pre-check and the CAS
 * by the test hook while a full archive publishes a higher boundary. The
 * re-check must lower the boundary below the committed payload; its file
 * must stay off the archivable list. Pre-fix: no re-check — payload_0_0 is
 * listed. (Hook-compiled builds only; the release-loop build is a trivial
 * pass — the scenario cannot be set up deterministically without the hook.) */
#ifdef MMAPQ_TEST_HOOKS
static _Atomic int p9_held, p9_release;

static void p9_hold_hook(void)
{
    atomic_store_explicit(&p9_held, 1, memory_order_release);
    /* Bounded: a keying regression (M9) routes the commit past the hook —
     * the main side times out and fails, but this thread must not spin
     * forever and hang the suite. */
    for (int spins = 0; spins < 5000000; spins++) {
        if (atomic_load_explicit(&p9_release, memory_order_acquire)) return;
        sched_yield();
    }
}

typedef struct {
    MmapqIndexedAppender *a;
    int64_t              offset;
    int64_t              rc;
} P9Commit;

static void *p9_commit_main(void *arg)
{
    P9Commit *c = arg;
    c->rc = mmapq_indexed_appender_commit(c->a, c->offset);
    return NULL;
}
#endif

void test_commit_racing_archive_lowers_boundary(void)
{
#ifdef MMAPQ_TEST_HOOKS
    char payload_p[4000]; memset(payload_p, 'P', sizeof(payload_p));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_p = mmapq_indexed_appender_append_uncommitted(a, payload_p, (int32_t)sizeof(payload_p));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_p);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, payload_b, (int32_t)sizeof(payload_b)));

    atomic_store(&p9_held, 0);
    atomic_store(&p9_release, 0);
    mmapq_test_hook_set_commit_pre(p9_hold_hook);

    P9Commit c = { .a = a, .offset = off_p, .rc = -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, p9_commit_main, &c));
    /* Bounded wait: if the hook never fires (e.g. a keying regression
     * bypasses the slow path), release and join, then fail — never hang */
    int waited_ms = 0;
    while (!atomic_load_explicit(&p9_held, memory_order_acquire)) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
        if (++waited_ms > 5000) {
            atomic_store_explicit(&p9_release, 1, memory_order_release);
            pthread_join(t, NULL);
            mmapq_test_hook_set_commit_pre(NULL);
            TEST_FAIL_MESSAGE("commit hook never fired (fast-path keying regression?)");
        }
    }

    /* Full archive while the commit is held: publishes end of B; the live
     * pass sees no idx 1 (the commit has not CASed yet) */
    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_GREATER_THAN_INT64(off_p, arch_start_offset(arch, 0));

    atomic_store_explicit(&p9_release, 1, memory_order_release);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    mmapq_test_hook_set_commit_pre(NULL);

    TEST_ASSERT_EQUAL_INT64(1, c.rc);
    /* The re-check lowered the boundary below P */
    TEST_ASSERT_LESS_OR_EQUAL_INT64(off_p, arch_start_offset(arch, 0));
    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    TEST_ASSERT_FALSE(list_contains(paths, n, "payload_0_0"));
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* P15: a live-pass read failure after publish must restore every published
 * start_offset to its pre-call value, keep the new start_index, and return
 * the error — never leave the boundary too high. */
#ifdef MMAPQ_TEST_HOOKS
static int p15_inject_timeout(void) { return MMAPQ_ETIMEDOUT; }
#endif

void test_archiver_live_pass_failure_restores_boundary(void)
{
#ifdef MMAPQ_TEST_HOOKS
    char payload_a[4000]; memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));
    char payload_c[50];   memset(payload_c, 'C', sizeof(payload_c));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, payload_a, (int32_t)sizeof(payload_a));
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(a, payload_c, (int32_t)sizeof(payload_c)));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = tiny_archiver();
    mmapq_test_hook_set_archiver_live_pass(p15_inject_timeout);
    int rc = mmapq_indexed_archiver_archive_up_to_index(arch, 1);
    mmapq_test_hook_set_archiver_live_pass(NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    /* The logical archive stands; only reclamation does not advance */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_archiver_start_index(arch));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    TEST_ASSERT_FALSE(list_contains(paths, n, "payload_0_0"));
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
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
    RUN_TEST(test_file_index_set_overflow_returns_zero);
    RUN_TEST(test_archiver_undo_one_index_at_header_file_4096);
    RUN_TEST(test_archiver_undo_with_resequenced_commits);

    /* CAND-30 pins */
    RUN_TEST(test_archiver_resequenced_live_payload_not_listed);
    RUN_TEST(test_commit_overtaken_payload_rejected);
    RUN_TEST(test_archiver_undo_never_raises_start_offset);
    RUN_TEST(test_appender_rejoin_over_abandoned_payload);
    RUN_TEST(test_appender_join_fresh_queue);
    RUN_TEST(test_archiver_in_order_boundary_unchanged);
    RUN_TEST(test_archiver_crash_tail_reclamation);
    RUN_TEST(test_appender_rejoin_after_payload_file_deleted);
    RUN_TEST(test_commit_fast_path_keyed_on_last_appended);
    RUN_TEST(test_archive_lock_excludes_concurrent_archive);
    RUN_TEST(test_commit_archive_race_stress);
    RUN_TEST(test_commit_racing_archive_lowers_boundary);
    RUN_TEST(test_archiver_live_pass_failure_restores_boundary);
    return UNITY_END();
}
