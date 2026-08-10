#include "unity.h"
#include "indexed_archiver.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "queue_meta.h"
#include "appender_meta.h"
#include "header_codec.h"
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

static Runtime *rt = NULL;

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/%s", DIR, QUEUE);
    int rc = system(cmd); (void)rc;
}

/* Create queue with archivable=1 by writing meta before opening appender */
static IndexedAppender *create_archivable_appender(void)
{
    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", DIR, QUEUE);
    mkdir(dir_path, 0775);

    QueueMeta *m = NULL;
    int qmerr = queue_meta_open_rw(DIR, QUEUE, 0,
        REGION_SZ, MAX_FILE, /* archivable= */ 1, QUEUE_TYPE_NORMAL, &m);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_NOT_NULL(m);
    queue_meta_close(m);

    IndexedAppender *a = NULL;
    int rc = indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    if (rc != 0) return NULL;
    return a;
}

static NextMove poll_advance(int64_t idx, uint8_t aid,
                              const void *data, int32_t len, void *ctx)
{
    (void)idx; (void)aid; (void)data; (void)len;
    if (ctx) (*(int *)ctx)++;
    return NEXT_ADVANCE;
}

void setUp(void)
{
    cleanup();
    int rc_rt = runtime_region_mapper(&rt);
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
    IndexedAppender *a = NULL;
    int rc = indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 1, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_close(a);

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
    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_TRUE(indexed_archiver_archivable(arch));
    indexed_archiver_close(arch);

    /* Reopen with archivable=0 — disk wins, still archivable */
    a = NULL;
    rc = indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_close(a);

    arch = NULL;
    int rc_arch2 = indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch2);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_TRUE(indexed_archiver_archivable(arch));
    indexed_archiver_close(arch);
}

void test_default_not_archivable(void)
{
    ensure_queue_dir();
    IndexedAppender *a = NULL;
    int rc = indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_close(a);

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

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_FALSE(indexed_archiver_archivable(arch));
    indexed_archiver_close(arch);
}

void test_archive_up_to_index_rejects_non_archivable(void)
{
    ensure_queue_dir();
    IndexedAppender *a = NULL;
    int rc = indexed_appender_create(DIR, QUEUE, REGION_SZ, RING_SZ,
        MAX_FILE, TIMEOUT, 0, rt, 0, &a);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_append(a, "msg", 3);
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    int rc2 = indexed_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc2);
    indexed_archiver_close(arch);
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_archiver_start_index_initially_zero(void)
{
    IndexedAppender *a = create_archivable_appender();
    indexed_appender_append(a, "hello", 5);
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    TEST_ASSERT_EQUAL_INT64(0, indexed_archiver_start_index(arch));
    TEST_ASSERT_EQUAL_INT(1, indexed_archiver_archivable(arch));
    indexed_archiver_close(arch);
}

void test_archiver_archive_forward_updates_start_index(void)
{
    IndexedAppender *a = create_archivable_appender();
    for (int i = 0; i < 5; i++) {
        char msg[16];
        snprintf(msg, sizeof(msg), "msg%d", i);
        indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    indexed_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, indexed_archiver_start_index(arch));
    indexed_archiver_close(arch);
}

void test_archiver_poller_returns_archived_below_start_index(void)
{
    IndexedAppender *a = create_archivable_appender();
    for (int i = 0; i < 5; i++) {
        char msg[8];
        snprintf(msg, sizeof(msg), "m%d", i);
        indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    indexed_appender_close(a);

    /* Create poller BEFORE archiving — cursor starts at 0 */
    IndexedPoller *p = NULL;
    int rc_p = indexed_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &p);
    TEST_ASSERT_EQUAL_INT(0, rc_p);
    TEST_ASSERT_NOT_NULL(p);

    /* Verify entry 0 is readable before archive */
    int count = 0;
    PollResult r = indexed_poller_poll(p, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT(1, count);

    /* Reset cursor to 0 */
    indexed_poller_move_to_index(p, 0);

    /* Archive entries 0..2 — now start_index = 3 */
    IndexedArchiver *arch = NULL;
    int rc_arch3 = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch3);
    TEST_ASSERT_NOT_NULL(arch);
    indexed_archiver_archive_up_to_index(arch, 3);
    indexed_archiver_close(arch);

    /* Poller cursor is at 0 which is now below start_index=3 — returns POLL_ARCHIVED */
    r = indexed_poller_poll(p, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(POLL_ARCHIVED, r);
    TEST_ASSERT_EQUAL_INT(1, count); /* count unchanged — handler not called */

    indexed_poller_close(p);

    /* New poller starts at start_index=3 automatically */
    IndexedPoller *p2 = NULL;
    int rc_p2 = indexed_poller_create(DIR, QUEUE, RING_SZ, TIMEOUT, rt, &p2);
    TEST_ASSERT_EQUAL_INT(0, rc_p2);
    TEST_ASSERT_NOT_NULL(p2);
    count = 0;
    r = indexed_poller_poll(p2, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT(1, count); /* entry 3 */

    r = indexed_poller_poll(p2, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(POLL_ADVANCED, r);
    TEST_ASSERT_EQUAL_INT(2, count); /* entry 4 */

    r = indexed_poller_poll(p2, poll_advance, &count);
    TEST_ASSERT_EQUAL_INT(POLL_END_OF_STREAM, r);

    indexed_poller_close(p2);
}

void test_archiver_start_offset_advances_after_archive(void)
{
    IndexedAppender *a = create_archivable_appender();
    uint8_t appender_id = indexed_appender_id(a);
    for (int i = 0; i < 3; i++) {
        char msg[8];
        snprintf(msg, sizeof(msg), "m%d", i);
        indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    int64_t before = 0;
    int ok = indexed_archiver_start_offset(arch, appender_id, &before);
    TEST_ASSERT_EQUAL_INT(1, ok);

    indexed_archiver_archive_up_to_index(arch, 2);

    int64_t after = 0;
    ok = indexed_archiver_start_offset(arch, appender_id, &after);
    TEST_ASSERT_EQUAL_INT(1, ok);
    TEST_ASSERT_TRUE(after > before);
    TEST_ASSERT_EQUAL_INT64(2, indexed_archiver_start_index(arch));
    indexed_archiver_close(arch);
}

void test_archiver_archivable_files_none_at_start_zero(void)
{
    IndexedAppender *a = create_archivable_appender();
    indexed_appender_append(a, "x", 1);
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    char *paths[64];
    int n = indexed_archiver_archivable_files(arch, paths, 64);
    TEST_ASSERT_EQUAL_INT(0, n);
    indexed_archiver_close(arch);
}

void test_archiver_backward_unarchive(void)
{
    IndexedAppender *a = create_archivable_appender();
    for (int i = 0; i < 4; i++) {
        char msg[8];
        snprintf(msg, sizeof(msg), "b%d", i);
        indexed_appender_append(a, msg, (int32_t)strlen(msg));
    }
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    indexed_archiver_archive_up_to_index(arch, 3);
    TEST_ASSERT_EQUAL_INT64(3, indexed_archiver_start_index(arch));

    /* Un-archive back to 1 — files still exist */
    indexed_archiver_archive_up_to_index(arch, 1);
    TEST_ASSERT_EQUAL_INT64(1, indexed_archiver_start_index(arch));
    indexed_archiver_close(arch);
}

void test_archiver_noop_when_index_equals_start(void)
{
    IndexedAppender *a = create_archivable_appender();
    indexed_appender_append(a, "x", 1);
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc_arch = indexed_archiver_create(DIR, QUEUE, 4, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc_arch);
    TEST_ASSERT_NOT_NULL(arch);
    indexed_archiver_archive_up_to_index(arch, 0); /* no-op */
    TEST_ASSERT_EQUAL_INT64(0, indexed_archiver_start_index(arch));
    indexed_archiver_close(arch);
}

void test_archive_forward_empty_queue(void)
{
    IndexedAppender *a = create_archivable_appender();
    TEST_ASSERT_NOT_NULL(a);
    indexed_appender_close(a);

    IndexedArchiver *arch = NULL;
    int rc = indexed_archiver_create(DIR, QUEUE, RING_SZ, rt, &arch);
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(arch);
    int rc2 = indexed_archiver_archive_up_to_index(arch, 5);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc2);
    indexed_archiver_close(arch);
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
    return UNITY_END();
}
