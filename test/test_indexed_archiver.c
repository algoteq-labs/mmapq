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
#include "internal/file_mapper.h"
#include "internal/region_ring.h"
#include "internal/region_ring_accessor.h"
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
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>
#include <poll.h>
#include <signal.h>

static const char    *DIR      = "/tmp";
static const char    *QUEUE    = "mmap_test_arch";
static const int32_t  REGION_SZ = 4096 * 16;
static const int64_t  MAX_FILE  = (int64_t)4096 * 16 * 64;
static const int      RING_SZ   = 4;
static const uint64_t TIMEOUT   = 5000000000ULL;

/* Tiny archivable geometry for the CAND-28/30/35/36/37 pins: 32 KiB
 * regions, 32 KiB files — one 32 KB header block per file, the smallest
 * legal geometry since CAND-38 (the pre-rule probes used 4 KiB). A big
 * payload fills file 0 leaving 28 bytes (a long padding is written), so
 * the next payload rolls to file 1. */
#define TINY_REGION   32768
#define TINY_FILE     32768
#define TINY_BIG_LEN  (TINY_FILE - INITIAL_PAYLOAD_POSITION - PAYLOAD_LENGTH_SIZE - 28)
/* Exact layout at the tiny geometry: A @64 ends at TINY_A_END, B rolls to
 * file 1 (@TINY_B_START) and ends at TINY_B_END, Z (50 B) ends TINY_Z_END. */
#define TINY_A_END    ((int64_t)TINY_FILE - 28)                          /* 32740 */
#define TINY_B_START  ((int64_t)TINY_FILE)                               /* 32768 */
#define TINY_B_END    ((int64_t)TINY_FILE + PAYLOAD_LENGTH_SIZE + 100)   /* 32872 */
#define TINY_Z_END    (TINY_B_END + PAYLOAD_LENGTH_SIZE + 50)            /* 32926 */


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
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_uncommitted_iterator_checked(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET, &it));
    TEST_ASSERT_NOT_NULL(it);
    /* All four messages are committed: nothing uncommitted remains.
     * (D8: advance is three-way — a negative error is truthy; compare
     * against 0/1 explicitly, here and below.) */
    TEST_ASSERT_EQUAL_INT(0, mmapqi_uncommitted_iterator_advance(it));

    /* A second call must not compound the damage. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_uncommitted_iterator_checked(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET, &it));
    TEST_ASSERT_NOT_NULL(it);
    TEST_ASSERT_EQUAL_INT(0, mmapqi_uncommitted_iterator_advance(it));

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

    MmapqUncommittedPayloadIterator *u = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_uncommitted_iterator_checked(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET, &u));
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
    TEST_ASSERT_EQUAL_INT(0, mmapqi_uncommitted_iterator_advance(u));

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

    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_uncommitted_iterator_checked(
        a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET, &it));
    TEST_ASSERT_NOT_NULL(it);

    int found = 0;
    while (mmapqi_uncommitted_iterator_advance(it) == 1) {
        found++;
        Buffer *buf = mmapqi_uncommitted_iterator_current(it);
        TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
        TEST_ASSERT_EQUAL_MEMORY("late", buf->data, 4);
    }
    TEST_ASSERT_EQUAL_INT(1, found);

    mmapq_indexed_appender_close(a);
}

/* ── CAND-49 §7.12: D-G pins (the archive line) ────────────────────────────
 * Each pin names the path it guards and cites doc/design/archive-boundaries.md;
 * every pin asserts the EXACT yielded set (offsets + terminal next == 0) —
 * the completeness direction included, because TLC cannot catch an iterator
 * that yields too little (the model's refill set is the definition).
 * Geometry: tiny archivable (32 KiB regions/files), 4-byte payloads, stride
 * 8 — message k at 64 + 8k. Fail-first evidence (standalone, both configs,
 * e5f07b7): audit/claude-CAND-49/transcript-ff49-e5f07b7.txt. */

static MmapqIndexedAppender *tiny_archivable_appender(void);
static MmapqIndexedArchiver *tiny_archiver(void);

static void dg_msg(char *buf, int k) { snprintf(buf, 8, "m%dxx", k); }
static int64_t dg_stage(MmapqIndexedAppender *a, int k)
{
    char b[8]; dg_msg(b, k);
    return mmapq_indexed_appender_append_uncommitted(a, b, 4);
}
static int64_t dg_commit_at(MmapqIndexedAppender *a, int k)
{
    char b[8]; dg_msg(b, k);
    return mmapq_indexed_appender_append(a, b, 4);
}
static void dg_assert_yields(MmapqIndexedAppender *a, int64_t start,
                             const int64_t *want, int n)
{
    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_uncommitted_iterator_checked(
        a, start, PAYLOAD_ITERATOR_NOT_SET, &it));
    TEST_ASSERT_NOT_NULL(it);
    for (int i = 0; i < n; i++) {
        const void *data = NULL; int32_t len = -1; int64_t off = -1;
        TEST_ASSERT_EQUAL_INT(1, mmapq_uncommitted_iterator_next(it, &data, &len, &off));
        TEST_ASSERT_EQUAL_INT64(want[i], off);
        TEST_ASSERT_EQUAL_INT32(4, len);
        char b[8]; dg_msg(b, (int)((want[i] - INITIAL_PAYLOAD_POSITION) / 8));
        TEST_ASSERT_EQUAL_MEMORY(b, data, 4);
    }
    const void *data = (const void *)1; int32_t len = -1; int64_t off = 0;
    TEST_ASSERT_EQUAL_INT(0, mmapq_uncommitted_iterator_next(it, &data, &len, &off));
    TEST_ASSERT_NULL(data);
    TEST_ASSERT_EQUAL_INT64(MMAPQ_PAYLOAD_ITERATOR_NOT_SET, off);
}
static int64_t dg_meta_field(int32_t field_offset)
{
    AppenderMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_appender_meta_open_ro(DIR, QUEUE, 0, &m));
    int64_t v;
    if (field_offset == APPENDER_META_ARCHIVE_OFFSET_OFFSET)
        v = appender_meta_archive_offset(m);
    else
        v = appender_meta_start_offset(m);
    mmapqi_appender_meta_close(m);
    return v;
}
static void dg_pwrite_meta(int32_t field_offset, int64_t val)
{
    char meta[512];
    snprintf(meta, sizeof meta, "%s/%s/appender_meta_0", DIR, QUEUE);
    int fd = open(meta, O_WRONLY);
    TEST_ASSERT_GREATER_THAN_INT(0, fd);
    TEST_ASSERT_EQUAL_INT(8, (int)pwrite(fd, &val, 8, field_offset));
    close(fd);
}

/* D-G path 1 (race): the iterator pre-fix read start_offset before si,
 * pairing the new si with the old boundary. Constructed deterministically:
 * archive, then pwrite start_offset back to the pre-archive value — si
 * published, the lane's boundary store "pending". With the §3.9 read order
 * (acquire si first, then the archive line) the snapshot is consistent:
 * yields exactly u2 above the line. */
void test_uncommitted_dg_race_window(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, dg_commit_at(a, 0));   /* c0 @64 */
    TEST_ASSERT_EQUAL_INT64(1, dg_commit_at(a, 1));   /* c1 @72 */
    TEST_ASSERT_EQUAL_INT64(80, dg_stage(a, 2));      /* u2 @80 */
    MmapqIndexedArchiver *ar = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 2));
    mmapq_indexed_archiver_close(ar);
    TEST_ASSERT_EQUAL_INT64(80, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));
    dg_pwrite_meta(APPENDER_META_START_OFFSET_OFFSET, INITIAL_PAYLOAD_POSITION);

    const int64_t want[] = { 80 };
    dg_assert_yields(a, PAYLOAD_ITERATOR_NOT_SET, want, 1);
    mmapq_indexed_appender_close(a);
}

/* D-G path 2 (commit re-check min, steady state): a slow commit's CAND-30
 * atomic-min lowers start_offset below its own just-archived header. The
 * line does not follow: yields exactly u1; the chopped committed u0 is never
 * yielded. Fail-first: pre-fix the walk from start_offset yields u0. */
#ifdef MMAPQ_TEST_HOOKS
static int dg_inject_timeout(void) { return MMAPQ_ETIMEDOUT; }  /* live-pass injection */
static _Atomic int dg_fired, dg_release;
static void dg_hold_hook(void)
{
    atomic_store_explicit(&dg_fired, 1, memory_order_release);
    for (long s = 0; s < 50000000; s++) {   /* bounded — never hang the suite */
        if (atomic_load_explicit(&dg_release, memory_order_acquire)) return;
        sched_yield();
    }
}
typedef struct { MmapqIndexedAppender *a; int64_t off; int64_t rc; } DgCommit;
static void *dg_commit_main(void *p)
{
    DgCommit *j = p;
    j->rc = mmapq_indexed_appender_commit(j->a, j->off);
    return NULL;
}
static void dg_park_commit(MmapqIndexedAppender *a, int64_t off, int which_hook,
                           DgCommit *j, pthread_t *t)
{
    atomic_store(&dg_fired, 0); atomic_store(&dg_release, 0);
    if (which_hook == 0) mmapq_test_hook_set_commit_pre(dg_hold_hook);
    else                 mmapq_test_hook_set_commit_post_cas(dg_hold_hook);
    j->a = a; j->off = off; j->rc = -999;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(t, NULL, dg_commit_main, j));
    int waited = 0;
    while (!atomic_load_explicit(&dg_fired, memory_order_acquire)) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
        if (++waited > 5000) {
            atomic_store_explicit(&dg_release, 1, memory_order_release);
            pthread_join(*t, NULL);
            mmapq_test_hook_set_commit_pre(NULL);
            mmapq_test_hook_set_commit_post_cas(NULL);
            TEST_FAIL_MESSAGE("commit hook never fired (slow-path routing regression?)");
        }
    }
}
static void dg_release_commit(pthread_t t)
{
    atomic_store_explicit(&dg_release, 1, memory_order_release);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    mmapq_test_hook_set_commit_pre(NULL);
    mmapq_test_hook_set_commit_post_cas(NULL);
}
#endif

void test_uncommitted_dg_commit_min_below_line(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(64, dg_stage(a, 0));    /* u0 @64 */
    TEST_ASSERT_EQUAL_INT64(72, dg_stage(a, 1));    /* u1 @72 */

    /* Slow-path commit(u0) parked between the CAS and the re-check. */
    DgCommit j; pthread_t t;
    dg_park_commit(a, 64, 1, &j, &t);
    MmapqIndexedArchiver *ar = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 1));
    mmapq_indexed_archiver_close(ar);
    dg_release_commit(t);
    TEST_ASSERT_EQUAL_INT64(0, j.rc);

    /* The re-check lowered start_offset below u0's archived header; the
     * archive line stands. */
    TEST_ASSERT_EQUAL_INT64(64, dg_meta_field(APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(72, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    const int64_t want[] = { 72 };
    dg_assert_yields(a, PAYLOAD_ITERATOR_NOT_SET, want, 1);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, 72));
    mmapq_indexed_appender_close(a);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* D-G path 3 (rollback): a live-pass read failure rolls the boundaries back
 * while si stands; the archive line is never rolled back. Yields exactly u2
 * above the line — the chopped committed c0 is never yielded. */
void test_uncommitted_dg_rollback_below_line(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, dg_commit_at(a, 0));   /* c0 @64 */
    TEST_ASSERT_EQUAL_INT64(1, dg_commit_at(a, 1));   /* c1 @72 */
    TEST_ASSERT_EQUAL_INT64(80, dg_stage(a, 2));      /* u2 @80 */

    MmapqIndexedArchiver *ar = tiny_archiver();
    mmapq_test_hook_set_archiver_live_pass(dg_inject_timeout);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT,
        mmapq_indexed_archiver_archive_up_to_index(ar, 1));
    mmapq_test_hook_set_archiver_live_pass(NULL);

    /* The logical archive stands; the boundary rolled back; the line stood. */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_archiver_start_index(ar));
    mmapq_indexed_archiver_close(ar);
    TEST_ASSERT_EQUAL_INT64(64, dg_meta_field(APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(72, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    const int64_t want[] = { 80 };
    dg_assert_yields(a, PAYLOAD_ITERATOR_NOT_SET, want, 1);
    mmapq_indexed_appender_close(a);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* D-G path 4 (undo): undo lowers the boundaries and restores headers; the
 * archive line is never lowered, so the restored-committed positions below
 * the line are never yielded and the chopped uncommitted u1 stays chopped. */
void test_uncommitted_dg_undo_keeps_line(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(64, dg_stage(a, 0));
    TEST_ASSERT_EQUAL_INT64(72, dg_stage(a, 1));
    TEST_ASSERT_EQUAL_INT64(80, dg_stage(a, 2));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, 64));  /* c0 */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, 80));  /* c2 */

    MmapqIndexedArchiver *ar = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 2));
    TEST_ASSERT_EQUAL_INT64(88, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 0));  /* undo */
    /* The undo lowered start_offset; the line is unchanged (exact value). */
    TEST_ASSERT_EQUAL_INT64(64, dg_meta_field(APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(88, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));
    mmapq_indexed_archiver_close(ar);

    dg_assert_yields(a, PAYLOAD_ITERATOR_NOT_SET, NULL, 0);
    mmapq_indexed_appender_close(a);
}

/* D-G completeness direction: out-of-order commits across an archive
 * boundary (commit the higher m1, archive, then the lower m0's parked commit
 * lands and its re-check lowers start_offset), then a fresh staged u2. The
 * iterator yields exactly the genuinely uncommitted message above the line —
 * nothing chopped, nothing missing. Dies if the line over-approximates. */
void test_uncommitted_dg_out_of_order_completeness(void)
{
#ifdef MMAPQ_TEST_HOOKS
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(64, dg_stage(a, 0));      /* m0 @64 */
    TEST_ASSERT_EQUAL_INT64(72, dg_stage(a, 1));      /* m1 @72 */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, 72));  /* m1 first */

    DgCommit j; pthread_t t;
    dg_park_commit(a, 64, 0, &j, &t);                 /* m0 parked pre-CAS */
    MmapqIndexedArchiver *ar = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 1));
    mmapq_indexed_archiver_close(ar);
    dg_release_commit(t);
    TEST_ASSERT_EQUAL_INT64(1, j.rc);

    TEST_ASSERT_EQUAL_INT64(80, dg_stage(a, 2));      /* u2 @80, above the line */
    TEST_ASSERT_EQUAL_INT64(80, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    const int64_t want[] = { 80 };
    dg_assert_yields(a, PAYLOAD_ITERATOR_NOT_SET, want, 1);
    mmapq_indexed_appender_close(a);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* §3.9 zero-line ruling: a zero archive line (< INITIAL_PAYLOAD_POSITION)
 * simulates a corrupt appender meta; pre-0.2.0 queues are refused at open
 * with MMAPQ_ESCHEMA — the factory refuses with exactly MMAPQ_ECORRUPT
 * (CAND-32 range-check precedent). */
void test_uncommitted_zero_archive_offset_refused(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, dg_commit_at(a, 0));
    TEST_ASSERT_EQUAL_INT64(72, dg_stage(a, 1));

    dg_pwrite_meta(APPENDER_META_ARCHIVE_OFFSET_OFFSET, 0);

    MmapqUncommittedPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT,
        mmapq_indexed_appender_uncommitted_iterator_checked(
            a, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET, &it));
    TEST_ASSERT_NULL(it);
    mmapq_indexed_appender_close(a);
}

/* §3.9 clamp rule: a caller start_offset >= start-of-stream but below the
 * archive line clamps up to the line — the yielded set is identical to the
 * no-argument call. State via archive + undo (start_offset lowered by the
 * undo, the line standing). */
void test_uncommitted_dg_caller_below_line_clamps(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(64, dg_stage(a, 0));
    TEST_ASSERT_EQUAL_INT64(72, dg_stage(a, 1));
    TEST_ASSERT_EQUAL_INT64(80, dg_stage(a, 2));
    TEST_ASSERT_EQUAL_INT64(88, dg_stage(a, 3));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, 64));  /* c0 */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, 80));  /* c2 */

    MmapqIndexedArchiver *ar = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 2));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 0));  /* undo */
    mmapq_indexed_archiver_close(ar);
    /* start_offset 64 (lowered by the undo), line 88 (standing). */
    TEST_ASSERT_EQUAL_INT64(64, dg_meta_field(APPENDER_META_START_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(88, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));

    const int64_t want[] = { 88 };
    dg_assert_yields(a, PAYLOAD_ITERATOR_NOT_SET, want, 1);   /* no-arg */
    dg_assert_yields(a, INITIAL_PAYLOAD_POSITION, want, 1);   /* clamps up */
    mmapq_indexed_appender_close(a);
}

/* §3.9 max rule (mutant M18): after an undo, a shorter forward archive
 * computes a candidate below the current line — the max-store keeps the
 * line; a plain store would lower it and reopen D-G. */
void test_archive_offset_not_lowered_by_shorter_forward(void)
{
    MmapqIndexedAppender *a = tiny_archivable_appender();
    for (int i = 0; i < 4; i++)
        TEST_ASSERT_EQUAL_INT64(i, dg_commit_at(a, i));   /* c0..c3 @64..88 */

    MmapqIndexedArchiver *ar = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 3));
    TEST_ASSERT_EQUAL_INT64(88, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 0));  /* undo */
    /* Shorter forward: candidate = end of c1 = 80 < 88. */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(ar, 2));
    TEST_ASSERT_EQUAL_INT64(88, dg_meta_field(APPENDER_META_ARCHIVE_OFFSET_OFFSET));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_archiver_start_index(ar));
    mmapq_indexed_archiver_close(ar);
    mmapq_indexed_appender_close(a);
}

/* ── D5 payload access (CAND-50 item 5): a caller-owned payload iterator
 * over the live queue's payload files — the re-scope target for the pin
 * that used the removed appender factory. The bounds callbacks answer
 * fixed, test-known positions (the pin controls the on-disk state). */
typedef struct { int64_t sos; int64_t eos; } D5Bounds;
static int64_t d5_sos(void *ctx) { return ((D5Bounds *)ctx)->sos; }
static int64_t d5_eos(void *ctx) { return ((D5Bounds *)ctx)->eos; }

typedef struct {
    FileMapper     *fm;
    RegionRing     *ring;
    RegionAccessor *acc;
} D5PayloadAccess;

static void d5_open_payload_access(D5PayloadAccess *px)
{
    char prefix[512];
    snprintf(prefix, sizeof prefix, "%s/%s/payload_0", DIR, QUEUE);
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_file_mapper_rolled_create(prefix, MAX_FILE, REGION_SZ,
            mmapqi_file_init_zeros, NULL, MAP_MODE_READ_WRITE, 6, 1, &px->fm));
    TEST_ASSERT_NOT_NULL(px->fm);
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_region_ring_async_create(RING_SZ, REGION_SZ, px->fm,
            TIMEOUT, rt, &px->ring));
    TEST_ASSERT_NOT_NULL(px->ring);
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_region_ring_accessor_create(px->ring, REGION_SZ, 1, 0.5,
            &px->acc));
    TEST_ASSERT_NOT_NULL(px->acc);
}

static void d5_close_payload_access(D5PayloadAccess *px)
{
    /* the async ring and its mapper are runtime-managed — the house test
     * idiom destroys the accessor only (test_payload_walk.c precedent;
     * the process exits reclaims the rest) */
    mmapqi_region_ring_accessor_destroy(px->acc);
}

/* Ownership pin — the acceptance gate for the A5 contract fix. Route pins
 * pass on a skip-only partial fix; this one fails on it. Rejection must
 * return MMAPQ_EINVAL AND leave the caller-owned iterator usable: a valid
 * re-init afterwards is the discriminating line (a free-on-reject init
 * turns the re-init into a use-after-free).
 * CAND-50 item 5: re-scoped from the removed public appender factory to
 * the internal entry points over a test-side accessor on the live queue's
 * payload files (the D5 helper). */
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

    D5PayloadAccess px;
    d5_open_payload_access(&px);
    /* start_of_stream is now 80 (messages at 64/72/80/88, end 96) */
    D5Bounds bounds = { 80, INITIAL_PAYLOAD_POSITION + 4 * 8 };
    MmapqPayloadIterator *it = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_payload_iterator_create(QUEUE, px.acc, d5_sos, d5_eos,
                                       &bounds, 0, &it));
    TEST_ASSERT_NOT_NULL(it);

    /* offset 64 is archived. Reject. */
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL,
        mmapqi_payload_iterator_init(it, 64, PAYLOAD_ITERATOR_NOT_SET));

    /* The iterator was NOT freed: a valid re-init succeeds and walks the
     * first unarchived message. */
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_payload_iterator_init(
            it, PAYLOAD_ITERATOR_NOT_SET, PAYLOAD_ITERATOR_NOT_SET));
    TEST_ASSERT_EQUAL_INT(1, mmapqi_payload_iterator_advance(it));
    Buffer *buf = mmapqi_payload_iterator_current(it);
    TEST_ASSERT_EQUAL_INT32(4, buf->capacity);
    TEST_ASSERT_EQUAL_MEMORY("msg2", buf->data, 4);

    d5_close_payload_access(&px);
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
 * the file numbering is servable. With 32 KiB files (4096 headers each),
 * header file 4096 starts at index 4096*4096; the queue is created with
 * start_index = S so two appends reach it (CAND-38 migration: the pre-rule
 * 4 KiB-file version appended all S+2 entries — the boundary being pinned
 * is the FILE NUMBER, not the append count). The absolute-indexed set
 * refused this undo with EINVAL (owner probe: the same undo returned 0 on
 * the D3 tree). */
void test_archiver_undo_one_index_at_header_file_4096(void)
{
    const int64_t S = (int64_t)4096 * 4096;  /* first index of header file 4096 */

    /* Boundary reach: the undo's file number really is >= FILE_INDEX_SET_MAX */
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(FILE_INDEX_SET_MAX,
        (S + 1) / (TINY_FILE / HEADER_SIZE));

    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, S,
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, S, &a));
    TEST_ASSERT_NOT_NULL(a);
    for (int64_t i = S; i < S + 2; i++)
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
 * Region = file = 32 KiB: A fills payload file 0 leaving 28 bytes (long
 * padding), so B (100 B) rolls to file 1. Commit B -> idx 0, then A -> idx 1;
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
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, &a));
    TEST_ASSERT_NOT_NULL(a);

    char payload_a[TINY_BIG_LEN];
    memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];
    memset(payload_b, 'B', sizeof(payload_b));

    int64_t off_a = mmapq_indexed_appender_append_uncommitted(
        a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(
        a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(TINY_B_START, off_b);  /* rolled to payload file 1 */

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
/* Geometry for all CAND-30 pins: 32 KiB regions, 32 KiB files (one 32 KB
 * header block per file — the smallest legal geometry since CAND-38; the
 * pre-rule probes used 4 KiB), archivable — structure matches the reviewer
 * probes (S1/S2/T1/T2/P12): a big payload fills file 0 leaving 28 bytes (a
 * long padding is written), so the next payload rolls to file 1. */
/* Mirrors the appender's padding rule at region == file == TINY_FILE. */
static int64_t record_end(int64_t pos, int32_t len)
{
    int64_t cap = TINY_FILE - (pos % TINY_FILE);
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
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, &a));
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
    char payload_a[TINY_BIG_LEN]; memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));
    char payload_c[50];   memset(payload_c, 'C', sizeof(payload_c));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(TINY_B_START, off_b);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(a, payload_c, (int32_t)sizeof(payload_c)));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));

    /* Boundary must not pass A's live payload at 64 (pre-fix: TINY_B_END) */
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
    char payload_a[TINY_BIG_LEN]; memset(payload_a, 'A', sizeof(payload_a));
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
 * it — here raising TINY_A_END (post-forward) to TINY_B_START. */
void test_archiver_undo_never_raises_start_offset(void)
{
    char payload_a[TINY_BIG_LEN]; memset(payload_a, 'A', sizeof(payload_a));
    char payload_b[100];  memset(payload_b, 'B', sizeof(payload_b));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    int64_t off_b = mmapq_indexed_appender_append_uncommitted(a, payload_b, (int32_t)sizeof(payload_b));
    TEST_ASSERT_EQUAL_INT64(TINY_B_START, off_b);
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_commit(a, off_b));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_commit(a, off_a));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    int64_t after_forward = arch_start_offset(arch, 0);
    /* Exact value: the candidate is the end of the MAXIMUM-POSITION archived
     * entry (B ends at TINY_B_END), not the highest-index one (A ends at
     * TINY_A_END) — this is what catches mutation M1. */
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, after_forward);

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
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &a0));
    int64_t off_a = mmapq_indexed_appender_append_uncommitted(a0, payload_a, (int32_t)sizeof(payload_a));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_a);
    /* abandoned — never committed */
    mmapq_indexed_appender_close(a0);

    MmapqIndexedAppender *a1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 1, 0, &a1));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a1, "B-live", 6));
    mmapq_indexed_appender_close(a1);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    mmapq_indexed_archiver_close(arch);

    /* Rejoin a0: no committed entry anywhere — must walk from start_offset */
    MmapqIndexedAppender *r1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &r1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(r1, "Zshort", 6));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(r1, "W", 1));
    mmapq_indexed_appender_close(r1);

    /* Second rejoin must recover cleanly (pre-fix: ECORRUPT) */
    MmapqIndexedAppender *r2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &r2));
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
    /* 4000 B x 10 = 40 KB > 32 KiB file: rolling stays exercised (CAND-38
     * migration: was 1000 B at 4 KiB files). */
    char msg[4000]; memset(msg, 'm', sizeof(msg));
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

    /* Restart: the join walks past the dead payload. 5 + 1 + 65 = 71 records
     * of ~1 KB ≈ 71 KB: two full 32 KiB files below the boundary (CAND-38
     * migration: was 26 records at 4 KiB files). */
    MmapqIndexedAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, &a2));
    for (int i = 5; i < 70; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a2, msg, (int32_t)sizeof(msg)));
    mmapq_indexed_appender_close(a2);

    int64_t expected = INITIAL_PAYLOAD_POSITION;
    for (int i = 0; i < 71; i++) expected = record_end(expected, (int32_t)sizeof(msg));

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 70));
    TEST_ASSERT_EQUAL_INT64(expected, arch_start_offset(arch, 0));

    int64_t files_below = expected / TINY_FILE;
    TEST_ASSERT_GREATER_OR_EQUAL_INT64(2, files_below);
    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    /* Every payload file below the boundary's file is listed, including the
     * one holding the dead payload (file 0) */
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
    /* Sized to end exactly at the file end: [64, TINY_FILE) (CAND-38
     * migration: was 4028 at 4 KiB files). */
    char big[TINY_FILE - INITIAL_PAYLOAD_POSITION - PAYLOAD_LENGTH_SIZE];
    memset(big, 'Z', sizeof(big));
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a0 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &a0));
    /* [64, 64+4+sizeof(big)) = [64, TINY_FILE) — ends exactly at the file end */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a0, big, (int32_t)sizeof(big)));
    mmapq_indexed_appender_close(a0);

    MmapqIndexedAppender *a1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 1, 0, &a1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a1, "live", 4));
    mmapq_indexed_appender_close(a1);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(TINY_FILE, arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_0_0"));
    for (int i = 0; i < n; i++) unlink(paths[i]);
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);

    /* Rejoin a0: its only entry is archived and its payload file is gone.
     * The bounded scan finds no live entry; the join walks from
     * start_offset = TINY_FILE — the end of the stream. Pre-fix: ECORRUPT. */
    MmapqIndexedAppender *r1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &r1));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(r1, "back", 4));
    mmapq_indexed_appender_close(r1);
}

/* DOC-01 W5 (ledger X7): the rejoin walk never re-creates a reclaimed
 * file — the plain case (no racing commit; test_c54_hole1_single covers
 * the racing one).  Reclaim via the library call, close, rejoin, append:
 * the reclaimed path stays absent throughout.  Evidence class was N in
 * the ledger; passes pre/post the comment fix (the contradiction was
 * documentary — stated, not fail-first). */
void test_doc01_rejoin_after_reclaim_recreates_nothing(void)
{
    /* Sized to end exactly at the file end: [64, TINY_FILE). */
    char big[TINY_FILE - INITIAL_PAYLOAD_POSITION - PAYLOAD_LENGTH_SIZE];
    memset(big, 'Z', sizeof(big));
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a0 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &a0));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a0, big, (int32_t)sizeof(big)));
    mmapq_indexed_appender_close(a0);

    MmapqIndexedAppender *a1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 1, 0, &a1));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a1, "live", 4));
    mmapq_indexed_appender_close(a1);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(TINY_FILE, arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(0, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_0_0"));
    /* Reclaim through the library call (re-list + unlink in one hold). */
    TEST_ASSERT_EQUAL_INT(n, mmapq_indexed_archiver_reclaim_files(arch, paths, n));
    char reclaimed[560];
    snprintf(reclaimed, sizeof(reclaimed), "%s/payload_0_0", dir_path);
    TEST_ASSERT_EQUAL_INT(-1, access(reclaimed, F_OK));
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);

    /* Rejoin: lane 0 has no live entry; the join anchors at
     * max(start_offset, archive_offset) = TINY_FILE — inside file 1. */
    MmapqIndexedAppender *r1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &r1));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(r1, "back", 4));
    mmapq_indexed_appender_close(r1);

    /* The pin: the reclaimed file was not re-created by the join walk
     * or the append. */
    TEST_ASSERT_EQUAL_INT(-1, access(reclaimed, F_OK));
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
    if (mmapq_indexed_appender_create(DIR, QUEUE, TINY_REGION, 4, TINY_FILE, TIMEOUT,
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
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
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
    char payload_p[TINY_BIG_LEN]; memset(payload_p, 'P', sizeof(payload_p));
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
    char payload_a[TINY_BIG_LEN]; memset(payload_a, 'A', sizeof(payload_a));
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

/* CAND-37: a slow-path commit's re-check min landing between the archiver's
 * start_offset snapshot and its publish must not be erased by the publish.
 * Pre-fix the publish is a plain store: X (uncommitted at 64) commits racing
 * archive(2); its min lowers TINY_B_END -> 64 between the snapshot (old =
 * TINY_B_END) and the publish; the store of TINY_Z_END erases it; the live
 * pass's injected timeout then rolls back to old = TINY_B_END, leaving X
 * live below the boundary and payload_0_0 listed. Post-fix the publish is a
 * CAS from the snapshot: it fails against 64, the lowering stands, and the
 * rollback mins 64 with TINY_B_END.
 * (Hook-compiled builds only; the release-loop build is a trivial pass — the
 * scenario cannot be set up deterministically without the hooks.) */
#ifdef MMAPQ_TEST_HOOKS
static _Atomic int c37_pre_fired, c37_pre_timed_out, c37_pre_go;
static _Atomic int c37_min_fired, c37_min_timed_out;
static _Atomic int c37_pub_fired, c37_pub_timed_out;
static _Atomic int c37_snapshot_taken, c37_min_done;

static void c37_spin_until(_Atomic int *flag, _Atomic int *timed_out)
{
    /* Bounded: a hook routing regression must fail the pin via the
     * fired/timed_out assertions, never hang the suite. */
    for (int spins = 0; spins < 5000000; spins++) {
        if (atomic_load_explicit(flag, memory_order_acquire)) return;
        sched_yield();
    }
    atomic_store_explicit(timed_out, 1, memory_order_release);
}

static void c37_commit_pre_hook(void)
{
    atomic_store_explicit(&c37_pre_fired, 1, memory_order_release);
    c37_spin_until(&c37_pre_go, &c37_pre_timed_out);
}

static void c37_commit_min_pre_hook(void)
{
    atomic_store_explicit(&c37_min_fired, 1, memory_order_release);
    c37_spin_until(&c37_snapshot_taken, &c37_min_timed_out);
}

static void c37_publish_pre_hook(void)
{
    atomic_store_explicit(&c37_pub_fired, 1, memory_order_release);
    atomic_store_explicit(&c37_snapshot_taken, 1, memory_order_release);
    c37_spin_until(&c37_min_done, &c37_pub_timed_out);
}

static int c37_inject_timeout(void) { return MMAPQ_ETIMEDOUT; }

typedef struct {
    MmapqIndexedAppender *a;
    int64_t              offset;
    int64_t              rc;
} C37Commit;

static void *c37_commit_main(void *arg)
{
    C37Commit *c = arg;
    c->rc = mmapq_indexed_appender_commit(c->a, c->offset);
    atomic_store_explicit(&c37_min_done, 1, memory_order_release);
    return NULL;
}

/* Bounded main-side wait; 0 on timeout (caller disarms, joins, then fails). */
static int c37_wait(_Atomic int *flag)
{
    for (int ms = 0; ms < 5000; ms++) {
        if (atomic_load_explicit(flag, memory_order_acquire)) return 1;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
    }
    return 0;
}
#endif

void test_archive_publish_cas_survives_racing_commit_min(void)
{
#ifdef MMAPQ_TEST_HOOKS
    char payload_x[TINY_BIG_LEN]; memset(payload_x, 'X', sizeof(payload_x));
    char payload_y[100];  memset(payload_y, 'Y', sizeof(payload_y));
    char payload_z[50];   memset(payload_z, 'Z', sizeof(payload_z));

    MmapqIndexedAppender *a = tiny_archivable_appender();
    int64_t off_x = mmapq_indexed_appender_append_uncommitted(a, payload_x, (int32_t)sizeof(payload_x));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, off_x);
    int64_t end_x = record_end(off_x, (int32_t)sizeof(payload_x));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, payload_y, (int32_t)sizeof(payload_y)));
    int64_t end_y = record_end(end_x, (int32_t)sizeof(payload_y));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, end_y);  /* rolls to file 1 */
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, payload_z, (int32_t)sizeof(payload_z)));
    int64_t end_z = record_end(end_y, (int32_t)sizeof(payload_z));
    TEST_ASSERT_EQUAL_INT64(TINY_Z_END, end_z);

    atomic_store(&c37_pre_fired, 0);  atomic_store(&c37_pre_timed_out, 0);
    atomic_store(&c37_pre_go, 0);
    atomic_store(&c37_min_fired, 0);  atomic_store(&c37_min_timed_out, 0);
    atomic_store(&c37_pub_fired, 0);  atomic_store(&c37_pub_timed_out, 0);
    atomic_store(&c37_snapshot_taken, 0); atomic_store(&c37_min_done, 0);

    /* The archiver is created before the commit thread starts, and every
     * result observed while the thread is held is captured into locals and
     * asserted only after release+join+disarm — a Unity longjmp with the
     * thread alive would let it write c.rc into a dead stack frame. */
    MmapqIndexedArchiver *arch = tiny_archiver();

    /* 1. Slow-path commit of X, held after the pre-check, before the CAS */
    mmapq_test_hook_set_commit_pre(c37_commit_pre_hook);
    C37Commit c = { .a = a, .offset = off_x, .rc = -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, c37_commit_main, &c));
    if (!c37_wait(&c37_pre_fired)) {
        atomic_store(&c37_pre_go, 1);
        pthread_join(t, NULL);
        mmapq_test_hook_set_commit_pre(NULL);
        mmapq_indexed_archiver_close(arch);
        TEST_FAIL_MESSAGE("commit_pre never fired (slow-path routing regression?)");
    }

    /* 2. Full archive to 1 while the commit is held: publishes end of Y;
     *    the live pass sees Z at TINY_B_END (not below) and no index 2 yet */
    int rc1 = mmapq_indexed_archiver_archive_up_to_index(arch, 1);
    int64_t so1 = -1;
    int so1_rc = mmapq_indexed_archiver_start_offset(arch, 0, &so1);
    if (rc1 != 0 || so1_rc != 1 || so1 != end_y) {
        atomic_store_explicit(&c37_pre_go, 1, memory_order_release);
        pthread_join(t, NULL);
        mmapq_test_hook_set_commit_pre(NULL);
        mmapq_indexed_archiver_close(arch);
        TEST_ASSERT_EQUAL_INT(0, rc1);
        TEST_ASSERT_EQUAL_INT(1, so1_rc);
        TEST_ASSERT_EQUAL_INT64(end_y, so1);
    }

    /* 3. Release the commit: CAS at index 2, fence, re-check (64 <
     *    TINY_B_END), then hold it at commit_min_pre, before the lowering
     *    min */
    mmapq_test_hook_set_commit_min_pre(c37_commit_min_pre_hook);
    atomic_store_explicit(&c37_pre_go, 1, memory_order_release);
    if (!c37_wait(&c37_min_fired)) {
        atomic_store(&c37_snapshot_taken, 1);
        pthread_join(t, NULL);
        mmapq_test_hook_set_commit_min_pre(NULL);
        mmapq_test_hook_set_commit_pre(NULL);
        mmapq_indexed_archiver_close(arch);
        TEST_FAIL_MESSAGE("commit_min_pre never fired (re-check branch regression?)");
    }

    /* 4. Archive to 2 with the commit parked: snapshot old = TINY_B_END,
     *    then park the archiver at publish_pre. The commit's min lands while
     *    the archiver is parked; the publish must not erase it. The live
     *    pass's first read is failed by injection to force the rollback. */
    mmapq_test_hook_set_archiver_publish_pre(c37_publish_pre_hook);
    mmapq_test_hook_set_archiver_live_pass(c37_inject_timeout);
    int rc = mmapq_indexed_archiver_archive_up_to_index(arch, 2);
    mmapq_test_hook_set_archiver_live_pass(NULL);
    mmapq_test_hook_set_archiver_publish_pre(NULL);
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    mmapq_test_hook_set_commit_min_pre(NULL);
    mmapq_test_hook_set_commit_pre(NULL);

    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_archiver_start_index(arch));
    TEST_ASSERT_EQUAL_INT64(2, c.rc);
    /* Hook-liveness: the choreography, not luck, produced the interleaving */
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c37_pre_fired), "commit_pre never fired");
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c37_min_fired), "commit_min_pre never fired");
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c37_pub_fired), "publish_pre never fired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&c37_pre_timed_out), "commit_pre spin expired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&c37_min_timed_out), "commit_min_pre spin expired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&c37_pub_timed_out), "publish_pre spin expired");
    /* The racing commit's lowering survives the publish and the rollback:
     * X at 64 is live, the boundary is 64, file 0 is not listable.
     * Pre-fix: TINY_B_END here and payload_0_0 listed. */
    TEST_ASSERT_EQUAL_INT64(off_x, arch_start_offset(arch, 0));
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

/* ── CAND-36 pins: reclaim_files (re-list + unlink under archive.lock SH) ── */

static int queue_file_present(const char *name)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s/%s", DIR, QUEUE, name);
    return access(path, F_OK) == 0;
}

/* strdup the listing entry containing suffix; caller frees. */
static char *mark_of(char **paths, int n, const char *suffix)
{
    for (int i = 0; i < n; i++)
        if (strstr(paths[i], suffix)) return strdup(paths[i]);
    return NULL;
}

/* Single-lane geometry: A TINY_BIG_LEN @64 (idx 0, ends TINY_A_END), B 100
 * @TINY_B_START (idx 1, rolls, ends TINY_B_END). archive(2) -> start_offset
 * TINY_B_END -> payload_0_0 listed. */
static void single_lane_geometry(void)
{
    char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, sm, (int32_t)sizeof(sm)));
    mmapq_indexed_appender_close(a);
    TEST_ASSERT_EQUAL_INT64(TINY_A_END, record_end(INITIAL_PAYLOAD_POSITION, TINY_BIG_LEN));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, record_end(TINY_B_START, 100));
}

/* B2 geometry (reviewer probe): two appenders, appends a0:big, a1:big,
 * a0:100, a1:100 -> indices 0-3. Big payloads @64 end TINY_A_END; small
 * payloads roll to @TINY_B_START end TINY_B_END. archive(4) ->
 * TINY_B_END/TINY_B_END, payload_0_0 and payload_1_0 listed. */
static void two_lane_geometry(void)
{
    char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
    char sm[100];   memset(sm, 's', sizeof(sm));

    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a0 = NULL, *a1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 0, 0, &a0));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create_with_id(DIR, QUEUE,
        TINY_REGION, 4, TINY_FILE, TIMEOUT, 0, rt, 1, 0, &a1));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a0, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a1, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(a0, sm, (int32_t)sizeof(sm)));
    TEST_ASSERT_EQUAL_INT64(3, mmapq_indexed_appender_append(a1, sm, (int32_t)sizeof(sm)));
    mmapq_indexed_appender_close(a0);
    mmapq_indexed_appender_close(a1);

    TEST_ASSERT_EQUAL_INT64(TINY_A_END, record_end(INITIAL_PAYLOAD_POSITION, TINY_BIG_LEN));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, record_end(TINY_B_START, 100));
}

/* Bounded main-side wait; 0 on timeout (caller disarms, joins, then fails). */
static int c36_wait(_Atomic int *flag)
{
    for (int ms = 0; ms < 5000; ms++) {
        if (atomic_load_explicit(flag, memory_order_acquire)) return 1;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
        nanosleep(&ts, NULL);
    }
    return 0;
}

typedef struct {
    MmapqIndexedArchiver *arch;
    char                **marks;
    int                   n_marks;
    _Atomic int           done;
    int                   rc;
} C36Reclaim;

static void *c36_reclaim_main(void *arg)
{
    C36Reclaim *p = arg;
    p->rc = mmapq_indexed_archiver_reclaim_files(p->arch, p->marks, p->n_marks);
    atomic_store_explicit(&p->done, 1, memory_order_release);
    return NULL;
}

/* P1: reclaim deletes exactly the marks that a fresh complete listing still
 * reports — the unmarked lane's file survives and stays listed; a repeat
 * reclaim with the same marks is a no-op. */
void test_reclaim_deletes_only_marked_still_archivable(void)
{
    two_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 4));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, arch_start_offset(arch, 0));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, arch_start_offset(arch, 1));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(2, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_0_0"));
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_1_0"));

    char *marks[1] = { mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_reclaim_files(arch, marks, 1));

    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));
    TEST_ASSERT_TRUE(queue_file_present("payload_1_0"));
    memset(paths, 0, sizeof(paths));
    n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_1_0"));
    free_paths(paths, n);

    /* same marks again: the re-list no longer lists the gone file */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_reclaim_files(arch, marks, 1));

    /* live files intact */
    TEST_ASSERT_TRUE(queue_file_present("payload_0_1"));
    TEST_ASSERT_TRUE(queue_file_present("payload_1_1"));

    free(marks[0]);
    mmapq_indexed_archiver_close(arch);
}


/* CAND-56 (walkthrough D5): print_summary must report the payload files
 * that survive a reclamation of the low-numbered ones — the listing loop
 * must skip missing file indices, not stop at the first gap. */
void test_c56_print_summary_lists_surviving_payload_files(void)
{
    char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
    MmapqIndexedAppender *a = tiny_archivable_appender();
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(a, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(1, mmapq_indexed_appender_append(a, big, (int32_t)sizeof(big)));
    TEST_ASSERT_EQUAL_INT64(2, mmapq_indexed_appender_append(a, big, (int32_t)sizeof(big)));
    mmapq_indexed_appender_close(a);

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));

    /* boundary = end of index 1's payload at 65444: only payload_0_0 is
     * wholly below it and listed; payload_0_1 (to 65536) straddles. */
    char *paths[16] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 16);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_0_0"));
    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_reclaim_files(arch, paths, n));
    free_paths(paths, n);

    TEST_ASSERT_FALSE(queue_file_present("payload_0_0"));
    TEST_ASSERT_TRUE(queue_file_present("payload_0_1"));
    TEST_ASSERT_TRUE(queue_file_present("payload_0_2"));

    char sb[8192];
    int w = mmapq_indexed_archiver_print_summary(arch, sb, (int)sizeof sb);
    TEST_ASSERT_GREATER_THAN(0, w);
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(sb, "payload_0_1"),
        "summary must list surviving payload_0_1");
    TEST_ASSERT_NOT_NULL_MESSAGE(strstr(sb, "payload_0_2"),
        "summary must list surviving payload_0_2");
    TEST_ASSERT_NULL_MESSAGE(strstr(sb, "payload_0_0"),
        "summary must not list the reclaimed payload_0_0");
    mmapq_indexed_archiver_close(arch);
}

/* P2: a mark that an undo restored drops out of the re-list and is NOT
 * deleted — the re-list, not the mark, decides. */
void test_reclaim_relist_excludes_undo_restored_files(void)
{
    single_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, arch_start_offset(arch, 0));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 0));

    /* start_index is back at 0: the re-list is empty, the mark drops out */
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_reclaim_files(arch, marks, 1));
    TEST_ASSERT_EQUAL_INT(0, access(marks[0], F_OK));

    free(marks[0]);
    mmapq_indexed_archiver_close(arch);
}

/* P4: reclaim takes archive.lock SHARED — it proceeds while another holder
 * keeps a shared flock (an EX implementation would block; P14 shape). */
void test_reclaim_lock_is_shared(void)
{
    single_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    free_paths(paths, n);

    char lockpath[512];
    snprintf(lockpath, sizeof(lockpath), "%s/%s/archive.lock", DIR, QUEUE);
    int fd = open(lockpath, O_RDWR);
    TEST_ASSERT_GREATER_OR_EQUAL(0, fd);
    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_SH));

    C36Reclaim p = { .arch = arch, .marks = marks, .n_marks = 1,
                     .done = 0, .rc = -999 };
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, c36_reclaim_main, &p));

    if (!c36_wait(&p.done)) {
        /* unblock the EX-blocked thread BEFORE joining, or the suite hangs */
        flock(fd, LOCK_UN);
        close(fd);
        pthread_join(t, NULL);
        free(marks[0]);
        mmapq_indexed_archiver_close(arch);
        TEST_FAIL_MESSAGE("reclaim blocked behind a shared hold (lock taken EX?)");
    }
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_UN));
    close(fd);

    TEST_ASSERT_EQUAL_INT(1, p.rc);
    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));

    free(marks[0]);
    mmapq_indexed_archiver_close(arch);
}

/* P6 (B1): the re-list is COMPLETE — it grows past the mark count. The
 * listing enumerates lane 0 first, so a listing truncated at the mark
 * capacity returns payload_0_0 and the payload_1_0 mark never matches. */
void test_reclaim_relists_beyond_mark_capacity(void)
{
    two_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 4));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(2, n);

    char *marks[1] = { mark_of(paths, n, "payload_1_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    free_paths(paths, n);

    TEST_ASSERT_EQUAL_INT(1, mmapq_indexed_archiver_reclaim_files(arch, marks, 1));
    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));
    TEST_ASSERT_TRUE(queue_file_present("payload_0_0"));

    memset(paths, 0, sizeof(paths));
    n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_0_0"));
    free_paths(paths, n);

    free(marks[0]);
    mmapq_indexed_archiver_close(arch);
}

#ifdef MMAPQ_TEST_HOOKS
static _Atomic int c36_hook_fired, c36_hook_timed_out, c36_hook_release;

static void c36_hold_hook(void)
{
    /* Bounded: a hook routing regression must fail the pin via the
     * fired/timed_out assertions, never hang the suite. */
    atomic_store_explicit(&c36_hook_fired, 1, memory_order_release);
    for (int spins = 0; spins < 5000000; spins++) {
        if (atomic_load_explicit(&c36_hook_release, memory_order_acquire)) return;
        sched_yield();
    }
    atomic_store_explicit(&c36_hook_timed_out, 1, memory_order_release);
}

typedef struct {
    MmapqIndexedArchiver *arch;
    int64_t              index;
    _Atomic int          done;
    int                  rc;
} C36Undo;

static void *c36_undo_main(void *arg)
{
    C36Undo *p = arg;
    p->rc = mmapq_indexed_archiver_archive_up_to_index(p->arch, p->index);
    atomic_store_explicit(&p->done, 1, memory_order_release);
    return NULL;
}
#endif

/* P3: the unlink happens INSIDE the SH hold — an undo started while a
 * reclaim is parked between its re-list and its unlink blocks on EX, then
 * fails its existence check (MMAPQ_ECORRUPT) because the file is gone.
 * Mutant M1 (lock released before the unlink) lets the undo through. */
void test_reclaim_unlink_excludes_concurrent_undo(void)
{
#ifdef MMAPQ_TEST_HOOKS
    single_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    free_paths(paths, n);

    /* A second archiver instance for the undo — archivers are
     * thread-confined. Both are created before any thread starts, and every
     * result observed while a thread is held is captured into locals and
     * asserted only after release+join+disarm (a Unity longjmp with a
     * thread alive would orphan it with a pointer to a dead stack frame). */
    MmapqIndexedArchiver *arch2 = tiny_archiver();

    atomic_store(&c36_hook_fired, 0);
    atomic_store(&c36_hook_timed_out, 0);
    atomic_store(&c36_hook_release, 0);
    mmapq_test_hook_set_reclaim_pre_unlink(c36_hold_hook);

    C36Reclaim r = { .arch = arch, .marks = marks, .n_marks = 1,
                     .done = 0, .rc = -999 };
    pthread_t t1;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t1, NULL, c36_reclaim_main, &r));
    if (!c36_wait(&c36_hook_fired)) {
        atomic_store(&c36_hook_release, 1);
        pthread_join(t1, NULL);
        mmapq_test_hook_set_reclaim_pre_unlink(NULL);
        free(marks[0]);
        mmapq_indexed_archiver_close(arch);
        mmapq_indexed_archiver_close(arch2);
        TEST_FAIL_MESSAGE("reclaim_pre_unlink never fired (hook routing regression?)");
    }

    /* The reclaim is parked inside its SH hold; the undo must block on EX.
     * Everything observed while a thread is alive is captured into locals
     * and asserted only after release+join+disarm (F1). */
    C36Undo u = { .arch = arch2, .index = 0, .done = 0, .rc = -999 };
    pthread_t t2;
    int create2_rc = pthread_create(&t2, NULL, c36_undo_main, &u);

    int undo_done_during_hold = -1;
    if (create2_rc == 0) {
        /* short bounded window for the negative check — the 5 s bound is
         * for things that must happen */
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 200000000 };
        nanosleep(&ts, NULL);
        undo_done_during_hold = atomic_load_explicit(&u.done, memory_order_acquire);
    }

    atomic_store_explicit(&c36_hook_release, 1, memory_order_release);
    int join1_rc = pthread_join(t1, NULL);
    int join2_rc = 0;
    if (create2_rc == 0) {
        c36_wait(&u.done);
        join2_rc = pthread_join(t2, NULL);
    }
    mmapq_test_hook_set_reclaim_pre_unlink(NULL);

    TEST_ASSERT_EQUAL_INT(0, create2_rc);
    TEST_ASSERT_EQUAL_INT(0, join1_rc);
    TEST_ASSERT_EQUAL_INT(0, join2_rc);
    TEST_ASSERT_FALSE_MESSAGE(undo_done_during_hold,
        "undo completed while the reclaim held archive.lock SH");
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c36_hook_fired),
        "reclaim_pre_unlink never fired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&c36_hook_timed_out),
        "reclaim_pre_unlink spin expired");
    TEST_ASSERT_EQUAL_INT(1, r.rc);
    /* the undo's existence check fails: the restored file was reclaimed */
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, u.rc);
    TEST_ASSERT_EQUAL_INT(-1, access(marks[0], F_OK));

    free(marks[0]);
    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_archiver_close(arch2);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* P5: a file already unlinked by a concurrent reclaimer (SH admits them)
 * is tolerated — ENOENT is neither counted nor an error. */
void test_reclaim_tolerates_concurrent_unlink(void)
{
#ifdef MMAPQ_TEST_HOOKS
    single_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 2));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    char *marks[1] = { mark_of(paths, n, "payload_0_0") };
    TEST_ASSERT_NOT_NULL(marks[0]);
    free_paths(paths, n);

    atomic_store(&c36_hook_fired, 0);
    atomic_store(&c36_hook_timed_out, 0);
    atomic_store(&c36_hook_release, 0);
    mmapq_test_hook_set_reclaim_pre_unlink(c36_hold_hook);

    C36Reclaim r = { .arch = arch, .marks = marks, .n_marks = 1,
                     .done = 0, .rc = -999 };
    pthread_t t1;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t1, NULL, c36_reclaim_main, &r));
    if (!c36_wait(&c36_hook_fired)) {
        atomic_store(&c36_hook_release, 1);
        pthread_join(t1, NULL);
        mmapq_test_hook_set_reclaim_pre_unlink(NULL);
        free(marks[0]);
        mmapq_indexed_archiver_close(arch);
        TEST_FAIL_MESSAGE("reclaim_pre_unlink never fired (hook routing regression?)");
    }

    /* the reclaim is parked after its re-list; a concurrent reclaimer
     * (or hand-deleter) unlinks the mark first. Captured, asserted after
     * join+disarm (F1). */
    int unl_rc = unlink(marks[0]);

    atomic_store_explicit(&c36_hook_release, 1, memory_order_release);
    int join_rc = pthread_join(t1, NULL);
    mmapq_test_hook_set_reclaim_pre_unlink(NULL);

    TEST_ASSERT_EQUAL_INT(0, unl_rc);
    TEST_ASSERT_EQUAL_INT(0, join_rc);

    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c36_hook_fired),
        "reclaim_pre_unlink never fired");
    TEST_ASSERT_FALSE_MESSAGE(atomic_load(&c36_hook_timed_out),
        "reclaim_pre_unlink spin expired");
    TEST_ASSERT_EQUAL_INT(0, r.rc);   /* ENOENT tolerated, not counted */

    free(marks[0]);
    mmapq_indexed_archiver_close(arch);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* ── CAND-35 pins: the listing caps each lane's deletable boundary at the ───
 * lowest live payload visible in the headers. A slow-path commit killed
 * between its header CAS and its boundary re-check strands start_offset
 * above a live payload forever; the listing must not trust start_offset
 * alone. */

/* P2: a mapping timeout inside the listing's live-header scan must
 * propagate as MMAPQ_ETIMEDOUT — the checked read is load-bearing (the
 * plain read would collapse the timeout to end-of-stream and silently
 * skip the cap). */
#ifdef MMAPQ_TEST_HOOKS
static _Atomic int c35_scan_fired;

static int c35_listing_inject(void)
{
    atomic_store_explicit(&c35_scan_fired, 1, memory_order_release);
    return 1;   /* inject WRAP_MAP_TIMED_OUT at the wrap-result level */
}
#endif

void test_c35_listing_scan_timeout_propagates(void)
{
#ifdef MMAPQ_TEST_HOOKS
    /* A @64 idx 0 (ends TINY_A_END), B @TINY_B_START idx 1 (ends TINY_B_END) */
    single_lane_geometry();

    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(TINY_A_END, arch_start_offset(arch, 0));

    /* Control: the scan runs (B at TINY_B_START floors the lane; cap
     * min(TINY_A_END, TINY_B_START)) and the honest listing stays empty */
    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    free_paths(paths, n);

    atomic_store(&c35_scan_fired, 0);
    mmapq_test_hook_set_archiver_listing_scan(c35_listing_inject);
    n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    mmapq_test_hook_set_archiver_listing_scan(NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, n);
    TEST_ASSERT_TRUE_MESSAGE(atomic_load(&c35_scan_fired),
        "listing scan hook never fired");
    mmapq_indexed_archiver_close(arch);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
}

/* P1/P3 fork machinery (B2): the suite setUp acquires the runtime
 * singleton, whose mapper thread does not exist in a forked child — the
 * child uses a NULL runtime (sync ring) for ALL library calls (reviewer
 * probe: inherited rt hangs; NULL finishes in 0.00 s). The parent
 * pre-creates the queue meta BEFORE the fork (no runtime needed) and
 * keeps the suite rt for itself. Every parent wait is bounded: poll()
 * with a 5 000 ms bound on the rendezvous pipe, a WNOHANG waitpid loop —
 * on expiry the child is killed and reaped BEFORE failing. The child
 * carries alarm() as a backstop, never returns to Unity, asserts
 * nothing. Excluded under TSan: the parent is multi-threaded at fork
 * (the singleton's thread), so die_after_fork would kill the child
 * (test_runtime.c precedent). */
#if defined(MMAPQ_TEST_HOOKS) && !defined(__SANITIZE_THREAD__)
static int c35_c2p[2], c35_p2c[2];


static void c35_child_pre_hook(void)
{
    /* pre-check passed — rendezvous with the parent, then wait for go */
    (void)!write(c35_c2p[1], "P", 1);
    char b;
    (void)!read(c35_p2c[0], &b, 1);
}

static void c35_child_post_cas_hook(void)
{
    /* the literal kill -9 between the header CAS and the fence/re-check */
    raise(SIGKILL);
}

/* Pre-create the archivable queue meta (parent side, before fork). */
static void c35_precreate_meta(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        TINY_REGION, TINY_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
}

/* poll-bounded read of one rendezvous byte; 0 on timeout. */
static int c35_read_byte_bounded(int fd)
{
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    if (poll(&pfd, 1, 5000) <= 0) return 0;
    char b;
    return read(fd, &b, 1) == 1;
}

/* Bounded waitpid (WNOHANG loop, 5 000 ms); 0 and *status set on reaped
 * exit, -1 on expiry (caller kills, reaps, then fails). */
static int c35_wait_child_bounded(pid_t pid, int *status)
{
    for (int ms = 0; ms < 5000; ms += 10) {
        if (waitpid(pid, status, WNOHANG) == pid) return 0;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 10000000 };
        nanosleep(&ts, NULL);
    }
    return -1;
}
#endif

/* P1: kill -9 between a resequenced commit's CAS and its re-check strands
 * start_offset (TINY_B_END) above the live entry (A at 64). The capped
 * listing must return nothing. Fail-first: pre-fix lists payload_0_0
 * (n == 1). */
void test_c35_listing_cap_covers_crashed_commit(void)
{
#if defined(MMAPQ_TEST_HOOKS) && !defined(__SANITIZE_THREAD__)
    char payload_a[TINY_BIG_LEN]; memset(payload_a, 'A', sizeof(payload_a));

    c35_precreate_meta();
    TEST_ASSERT_EQUAL_INT(0, pipe(c35_c2p));
    TEST_ASSERT_EQUAL_INT(0, pipe(c35_p2c));

    pid_t pid = fork();
    TEST_ASSERT_NOT_EQUAL(-1, pid);

    if (pid == 0) {
        /* Child: the appender that crashes mid-commit. NULL runtime (B2). */
        alarm(15);
        close(c35_c2p[0]);
        close(c35_p2c[1]);
        MmapqIndexedAppender *a = NULL;
        if (mmapq_indexed_appender_create(DIR, QUEUE, TINY_REGION, 4, TINY_FILE,
                TIMEOUT, 0, NULL, 0, &a) != 0) _exit(2);
        char a_pay[TINY_BIG_LEN]; memset(a_pay, 'A', sizeof(a_pay));
        char b_pay[100];  memset(b_pay, 'B', sizeof(b_pay));
        if (mmapq_indexed_appender_append_uncommitted(a, a_pay,
                (int32_t)sizeof(a_pay)) != INITIAL_PAYLOAD_POSITION) _exit(3);
        if (mmapq_indexed_appender_append_uncommitted(a, b_pay,
                (int32_t)sizeof(b_pay)) != TINY_B_START) _exit(4);
        if (mmapq_indexed_appender_commit(a, TINY_B_START) != 0) _exit(5);
        mmapq_test_hook_set_commit_pre(c35_child_pre_hook);
        mmapq_test_hook_set_commit_post_cas(c35_child_post_cas_hook);
        /* slow path: pre-check passes (start_offset 64), parks, CASes the
         * header at index 1, dies before the fence + re-check + min */
        mmapq_indexed_appender_commit(a, INITIAL_PAYLOAD_POSITION);
        _exit(6);
    }

    close(c35_c2p[1]);
    close(c35_p2c[0]);

    if (!c35_read_byte_bounded(c35_c2p[0])) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        TEST_FAIL_MESSAGE("child never reached the commit pre-check");
    }

    /* Archive while the child is parked between the pre-check and the CAS:
     * publishes end of B (TINY_B_END); the live pass sees no index 1. */
    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 1));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, arch_start_offset(arch, 0));

    TEST_ASSERT_EQUAL_INT(1, (int)write(c35_p2c[1], "G", 1));
    int wst = 0;
    if (c35_wait_child_bounded(pid, &wst) != 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        mmapq_indexed_archiver_close(arch);
        TEST_FAIL_MESSAGE("child did not die at the post-CAS hook");
    }
    TEST_ASSERT_TRUE(WIFSIGNALED(wst));
    TEST_ASSERT_EQUAL_INT(SIGKILL, WTERMSIG(wst));

    /* The stranded state: start_offset TINY_B_END above live A at 64. */
    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    TEST_ASSERT_FALSE(list_contains(paths, n, "payload_0_0"));
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);

    /* Non-vacuity: the stranded entry really is live and readable */
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &r));
    MmapqReadingContext ctx = {0};
    mmapq_indexed_reader_read(r, 1, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    TEST_ASSERT_EQUAL_INT((int)sizeof(payload_a), ctx.length);
    TEST_ASSERT_EQUAL_MEMORY(payload_a, ctx.data, sizeof(payload_a));
    mmapq_indexed_reader_close(r);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS; fork excluded under TSan");
#endif
}

/* P3: the cap is PER LANE — lane 1 (no live entries) keeps its honestly
 * archivable file listed while the stranded lane 0's file drops out.
 * Fail-first: pre-fix lists BOTH payload_0_0 and payload_1_0 (n == 2). */
void test_c35_listing_cap_is_per_lane(void)
{
#if defined(MMAPQ_TEST_HOOKS) && !defined(__SANITIZE_THREAD__)
    c35_precreate_meta();
    TEST_ASSERT_EQUAL_INT(0, pipe(c35_c2p));
    TEST_ASSERT_EQUAL_INT(0, pipe(c35_p2c));

    pid_t pid = fork();
    TEST_ASSERT_NOT_EQUAL(-1, pid);

    if (pid == 0) {
        /* Child: both appenders, NULL runtime (B2). */
        alarm(15);
        close(c35_c2p[0]);
        close(c35_p2c[1]);
        MmapqIndexedAppender *a0 = NULL, *a1 = NULL;
        if (mmapq_indexed_appender_create_with_id(DIR, QUEUE, TINY_REGION, 4,
                TINY_FILE, TIMEOUT, 0, NULL, 0, 0, &a0) != 0) _exit(2);
        if (mmapq_indexed_appender_create_with_id(DIR, QUEUE, TINY_REGION, 4,
                TINY_FILE, TIMEOUT, 0, NULL, 1, 0, &a1) != 0) _exit(3);
        char big[TINY_BIG_LEN]; memset(big, 'G', sizeof(big));
        char sm[100];   memset(sm, 's', sizeof(sm));
        /* lane 1: two committed entries, indices 0 and 1 */
        if (mmapq_indexed_appender_append(a1, big, (int32_t)sizeof(big)) != 0) _exit(4);
        if (mmapq_indexed_appender_append(a1, sm, (int32_t)sizeof(sm)) != 1) _exit(5);
        /* lane 0: A0 uncommitted @64, B0 uncommitted @TINY_B_START,
         * commit B0 -> 2 */
        if (mmapq_indexed_appender_append_uncommitted(a0, big,
                (int32_t)sizeof(big)) != INITIAL_PAYLOAD_POSITION) _exit(6);
        if (mmapq_indexed_appender_append_uncommitted(a0, sm,
                (int32_t)sizeof(sm)) != TINY_B_START) _exit(7);
        if (mmapq_indexed_appender_commit(a0, TINY_B_START) != 2) _exit(8);
        mmapq_test_hook_set_commit_pre(c35_child_pre_hook);
        mmapq_test_hook_set_commit_post_cas(c35_child_post_cas_hook);
        /* commit A0: parks, CASes the header at index 3, dies pre-fence */
        mmapq_indexed_appender_commit(a0, INITIAL_PAYLOAD_POSITION);
        _exit(9);
    }

    close(c35_c2p[1]);
    close(c35_p2c[0]);

    if (!c35_read_byte_bounded(c35_c2p[0])) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        TEST_FAIL_MESSAGE("child never reached the commit pre-check");
    }

    /* Archive indices 0-2 while the child is parked: lane boundaries
     * TINY_B_END (end of Y1) / TINY_B_END (end of B0); the live pass sees
     * no index 3. */
    MmapqIndexedArchiver *arch = tiny_archiver();
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_archiver_archive_up_to_index(arch, 3));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, arch_start_offset(arch, 0));
    TEST_ASSERT_EQUAL_INT64(TINY_B_END, arch_start_offset(arch, 1));

    TEST_ASSERT_EQUAL_INT(1, (int)write(c35_p2c[1], "G", 1));
    int wst = 0;
    if (c35_wait_child_bounded(pid, &wst) != 0) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        mmapq_indexed_archiver_close(arch);
        TEST_FAIL_MESSAGE("child did not die at the post-CAS hook");
    }
    TEST_ASSERT_TRUE(WIFSIGNALED(wst));
    TEST_ASSERT_EQUAL_INT(SIGKILL, WTERMSIG(wst));

    /* Lane 0 capped at 64 (live A0); lane 1 has no live entries -> uncapped */
    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(1, n);
    TEST_ASSERT_TRUE(list_contains(paths, n, "payload_1_0"));
    TEST_ASSERT_FALSE(list_contains(paths, n, "payload_0_0"));
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS; fork excluded under TSan");
#endif
}

/* ── CAND-38 pin P2: positive reclaim guard at aligned geometry ────────────
 * 32 KB regions, one 32 KB header block per file: archiving past one header
 * file and reclaiming must lose nothing, and the rejoined appender continues
 * at N. Mirrors the reviewer probe's aligned rows (hdrloss.c M=32768/98304:
 * 0 unreadable). The misaligned rows are the pre-fix data-loss demonstration
 * (992/2112 live entries unreadable at 8 KB files); post-rule they are
 * refused at create (P1), so they are recorded from the probe, not pinned. */
static void c38_reclaim_loses_nothing(int64_t max_file)
{
    const int32_t region = 32768;
    const int64_t hif    = max_file / HEADER_SIZE;  /* headers per file */
    const int64_t N      = 2 * hif + 100;
    const int64_t upto   = hif + 50;                /* past one header file */

    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(DIR, QUEUE, 0,
        region, max_file, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        region, 16, max_file, TIMEOUT, 0, rt, 0, &a));
    TEST_ASSERT_NOT_NULL(a);
    for (int64_t i = 0; i < N; i++)
        TEST_ASSERT_EQUAL_INT64(i, mmapq_indexed_appender_append(a, "cccc", 4));

    MmapqIndexedArchiver *arch = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_create(DIR, QUEUE, 4, rt, &arch));
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_archiver_archive_up_to_index(arch, upto));

    char *paths[64] = {0};
    int n = mmapq_indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_GREATER_OR_EQUAL(1, n);
    int hdr = 0;
    for (int i = 0; i < n; i++) if (strstr(paths[i], "/header_")) hdr++;
    TEST_ASSERT_GREATER_OR_EQUAL(1, hdr);   /* a header file really listed */
    TEST_ASSERT_EQUAL_INT(n, mmapq_indexed_archiver_reclaim_files(arch, paths, n));
    free_paths(paths, n);
    mmapq_indexed_archiver_close(arch);
    mmapq_indexed_appender_close(a);

    /* Fresh reader over the whole live range: nothing lost. */
    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapq_indexed_reader_create(DIR, QUEUE, 4, TIMEOUT, NULL, &r));
    TEST_ASSERT_NOT_NULL(r);
    for (int64_t i = upto; i < N; i++) {
        MmapqReadingContext ctx = {0};
        mmapq_indexed_reader_read(r, i, &ctx);
        TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, ctx.status);
    }
    mmapq_indexed_reader_close(r);

    /* Rejoin: the next append is index N — no committed index reused. */
    MmapqIndexedAppender *a2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_appender_create(DIR, QUEUE,
        region, 16, max_file, TIMEOUT, 0, rt, 0, &a2));
    TEST_ASSERT_NOT_NULL(a2);
    TEST_ASSERT_EQUAL_INT64(N, mmapq_indexed_appender_append(a2, "next", 4));
    mmapq_indexed_appender_close(a2);
}

void test_c38_reclaim_aligned_32k_file_loses_nothing(void)
{
    c38_reclaim_loses_nothing(32768);
}

void test_c38_reclaim_aligned_96k_file_loses_nothing(void)
{
    c38_reclaim_loses_nothing(98304);
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
    RUN_TEST(test_uncommitted_dg_race_window);
    RUN_TEST(test_uncommitted_dg_commit_min_below_line);
    RUN_TEST(test_uncommitted_dg_rollback_below_line);
    RUN_TEST(test_uncommitted_dg_undo_keeps_line);
    RUN_TEST(test_uncommitted_dg_out_of_order_completeness);
    RUN_TEST(test_uncommitted_zero_archive_offset_refused);
    RUN_TEST(test_uncommitted_dg_caller_below_line_clamps);
    RUN_TEST(test_archive_offset_not_lowered_by_shorter_forward);
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
    RUN_TEST(test_doc01_rejoin_after_reclaim_recreates_nothing);
    RUN_TEST(test_commit_fast_path_keyed_on_last_appended);
    RUN_TEST(test_archive_lock_excludes_concurrent_archive);
    RUN_TEST(test_commit_archive_race_stress);
    RUN_TEST(test_commit_racing_archive_lowers_boundary);
    RUN_TEST(test_archiver_live_pass_failure_restores_boundary);
    RUN_TEST(test_archive_publish_cas_survives_racing_commit_min);
    RUN_TEST(test_reclaim_deletes_only_marked_still_archivable);
    RUN_TEST(test_c56_print_summary_lists_surviving_payload_files);
    RUN_TEST(test_reclaim_relist_excludes_undo_restored_files);
    RUN_TEST(test_reclaim_unlink_excludes_concurrent_undo);
    RUN_TEST(test_reclaim_lock_is_shared);
    RUN_TEST(test_reclaim_tolerates_concurrent_unlink);
    RUN_TEST(test_reclaim_relists_beyond_mark_capacity);

    /* CAND-35 pins */
    RUN_TEST(test_c35_listing_scan_timeout_propagates);
    RUN_TEST(test_c35_listing_cap_covers_crashed_commit);
    RUN_TEST(test_c35_listing_cap_is_per_lane);

    /* CAND-38 pins */
    RUN_TEST(test_c38_reclaim_aligned_32k_file_loses_nothing);
    RUN_TEST(test_c38_reclaim_aligned_96k_file_loses_nothing);
    return UNITY_END();
}
