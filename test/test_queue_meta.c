#include "unity.h"
#include "internal/queue_meta.h"
#include "internal/test_hooks.h"

#include <unistd.h>
#include <stdio.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/file.h>
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
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, 32768 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
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
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 65536, 65536 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT32(65536, queue_meta_region_size(meta));
}

void test_open_rw_stores_max_file_size(void)
{
    int64_t mfs = 32768LL * 512;
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, mfs, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT64(mfs, queue_meta_max_file_size(meta));
}

void test_open_rw_schema_version(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, 32768 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
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
    int qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, 32768 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    mmapqi_queue_meta_close(meta);
    meta = NULL;

    QueueMeta *ro = NULL;
    int roerr = mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro);
    TEST_ASSERT_EQUAL_INT(0, roerr);
    TEST_ASSERT_NOT_NULL(ro);
    TEST_ASSERT_EQUAL_INT32(32768, queue_meta_region_size(ro));
    mmapqi_queue_meta_close(ro);
}

void test_start_index_zero_non_archivable(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, 32768 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT64(0, queue_meta_start_index(meta));
}

void test_archivable_false(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, 32768 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_EQUAL_INT(0, queue_meta_archivable(meta));
}

void test_reopen_rw_preserves_values(void)
{
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 32768, 32768 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    mmapqi_queue_meta_close(meta);

    /* Re-open with different params — existing values should be preserved */
    qmerr = mmapqi_queue_meta_open_rw(DIR, QUEUE, 0, 65536, 65536 * 256, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    /* File already existed — region_size from file is 32768, not 65536 */
    TEST_ASSERT_EQUAL_INT32(32768, queue_meta_region_size(meta));
}

/* ── D2: RW size guard + meta_validate geometry checks ───────────────────── */

/* max_file_size 98304 = 3 x 32 KB = 8 x 12288 is divisible by the corrupt
 * region_size 12288 (3 x 4096) used below, so each corrupt value trips
 * exactly one geometry check. (CAND-38 migration: was 49152 = 12 x 4096,
 * now a multiple of the 32 KB header block.) */
#define D2_MAX_FILE ((int64_t)98304)

static void create_valid_meta(void)
{
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);
}

static void pwrite_meta_field(const void *buf, size_t len, int32_t offset)
{
    char path[256];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    int fd = open(path, O_WRONLY);
    TEST_ASSERT_TRUE(fd >= 0);
    TEST_ASSERT_EQUAL_INT((int)len, (int)pwrite(fd, buf, len, offset));
    close(fd);
}

static void assert_ro_open_rc(int expected)
{
    QueueMeta *ro = NULL;
    TEST_ASSERT_EQUAL_INT(expected,
        mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro));
    TEST_ASSERT_NULL(ro);
}

/* 1. CAND-15: a truncated meta is writable but permanently unreadable —
 *    RW open must refuse it the same way RO does. */
void test_rw_meta_open_rejects_truncated_meta(void)
{
    create_valid_meta();

    char path[256];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    TEST_ASSERT_EQUAL_INT(0, truncate(path, 32));

    QueueMeta *m = NULL;
    int rc = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &m);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOTREADY, rc);
    TEST_ASSERT_NULL(m);
}

/* 2. region_size positive, divides max_file_size, but not a power of two */
void test_open_ro_corrupt_region_size_not_pow2(void)
{
    create_valid_meta();
    int32_t bad = 12288;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_REGION_SIZE_OFFSET);
    assert_ro_open_rc(MMAPQ_ECORRUPT);
}

/* 3. region_size zero (zero passes the pow2 bitmask — only the <= 0 check
 *    catches it, and it must run before the % to avoid a divide-by-zero) */
void test_open_ro_corrupt_region_size_zero(void)
{
    create_valid_meta();
    int32_t bad = 0;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_REGION_SIZE_OFFSET);
    assert_ro_open_rc(MMAPQ_ECORRUPT);
}

/* 4. max_file_size positive but not a multiple of region_size */
void test_open_ro_corrupt_max_file_size_not_multiple(void)
{
    create_valid_meta();
    int64_t bad = D2_MAX_FILE + 8;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_MAX_FILE_SIZE_OFFSET);
    assert_ro_open_rc(MMAPQ_ECORRUPT);
}

/* 5. max_file_size zero */
void test_open_ro_corrupt_max_file_size_zero(void)
{
    create_valid_meta();
    int64_t bad = 0;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_MAX_FILE_SIZE_OFFSET);
    assert_ro_open_rc(MMAPQ_ECORRUPT);
}

/* 6. The geometry checks are shared code; pin the RW path once, which also
 *    pins that open_rw propagates meta_validate's actual return code. */
void test_open_rw_corrupt_region_size_not_pow2(void)
{
    create_valid_meta();
    int32_t bad = 12288;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_REGION_SIZE_OFFSET);

    QueueMeta *m = NULL;
    int rc = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &m);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(m);
}

/* 7. Caller-supplied impossible geometry is a caller error, not corruption */
void test_open_rw_caller_region_size_not_pow2_eINVAL(void)
{
    qmerr = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 12288, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, qmerr);
    TEST_ASSERT_NULL(meta);
}

/* 8. Same for max_file_size */
void test_open_rw_caller_max_file_size_not_multiple_eINVAL(void)
{
    qmerr = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE + 8, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, qmerr);
    TEST_ASSERT_NULL(meta);
}

/* 9. Control: valid geometry still opens on both paths */
void test_open_valid_geometry_control(void)
{
    create_valid_meta();

    QueueMeta *ro = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro));
    TEST_ASSERT_NOT_NULL(ro);
    mmapqi_queue_meta_close(ro);

    qmerr = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &meta);
    TEST_ASSERT_EQUAL_INT(0, qmerr);
    TEST_ASSERT_NOT_NULL(meta);
}

/* 10. The RO open passes a non-NULL initialiser (mmapqi_file_init_none)
 *     precisely so that fixed_size_region_create takes the creator's
 *     flock(LOCK_EX): a reader must wait for initialisation to finish and
 *     can never map a partially written meta. Pin the blocking behavior;
 *     timing-based, but safe-direction only (the thread flags started
 *     before opening, so a slow machine cannot fake a pass). */
typedef struct {
    _Atomic int started;
    _Atomic int done;
    int         rc;   /* written by the thread, read after pthread_join */
} RoOpenResult;

static void *ro_open_thread(void *arg)
{
    RoOpenResult *r = (RoOpenResult *)arg;
    atomic_store(&r->started, 1);
    QueueMeta *ro = NULL;
    r->rc = mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro);
    if (ro) mmapqi_queue_meta_close(ro);
    atomic_store(&r->done, 1);
    return NULL;
}

void test_open_ro_waits_for_creator_lock(void)
{
    create_valid_meta();

    char path[256];
    snprintf(path, sizeof(path), "%s/%s/meta", DIR, QUEUE);
    int fd = open(path, O_RDWR);
    TEST_ASSERT_TRUE(fd >= 0);
    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_EX));

    RoOpenResult res = {0, 0, -1};
    pthread_t t;
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&t, NULL, ro_open_thread, &res));
    /* spin until the thread is about to open — atomics, so -O3 -flto
     * cannot hoist these into infinite loops */
    while (!atomic_load(&res.started)) { }
    usleep(50 * 1000);
    TEST_ASSERT_FALSE(atomic_load(&res.done));   /* must be blocked on our lock */

    TEST_ASSERT_EQUAL_INT(0, flock(fd, LOCK_UN));
    TEST_ASSERT_EQUAL_INT(0, pthread_join(t, NULL));
    TEST_ASSERT_EQUAL_INT(0, res.rc);
    close(fd);
}

/* ── D3: meta_validate start_index bounds ────────────────────────────────── */

/* An on-disk start_index beyond MMAPQI_MAX_INDEX is corruption: at such
 * indices header positions alias already-written slots (index 2^61 maps to
 * position 0). Pre-fix this opened rc=0 and reads aliased index 0's data. */
void test_open_ro_corrupt_start_index_above_max(void)
{
    create_valid_meta();
    int64_t bad = (int64_t)1 << 61;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_START_INDEX_OFFSET);
    assert_ro_open_rc(MMAPQ_ECORRUPT);
}

/* A negative on-disk start_index is corruption the same way. */
void test_open_ro_corrupt_start_index_negative(void)
{
    create_valid_meta();
    int64_t bad = -4096;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_START_INDEX_OFFSET);
    assert_ro_open_rc(MMAPQ_ECORRUPT);
}

/* Pin that open_rw propagates meta_validate's start_index return code
 * (the checks are shared code; pinned once, mirroring D2 pin 6). */
void test_open_rw_corrupt_start_index_above_max(void)
{
    create_valid_meta();
    int64_t bad = (int64_t)1 << 61;
    pwrite_meta_field(&bad, sizeof(bad), QUEUE_META_START_INDEX_OFFSET);

    QueueMeta *m = NULL;
    int rc = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &m);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ECORRUPT, rc);
    TEST_ASSERT_NULL(m);
}

/* Boundary control: the largest legal start_index still opens, both paths.
 * D3_MAX_INDEX literal must match MMAPQI_MAX_INDEX in header_codec.h. */
#define D3_MAX_INDEX (((int64_t)1 << 56) - 1)
void test_open_start_index_max_boundary_control(void)
{
    QueueMeta *m = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(
        DIR, QUEUE, D3_MAX_INDEX, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &m));
    TEST_ASSERT_NOT_NULL(m);
    mmapqi_queue_meta_close(m);

    QueueMeta *ro = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro));
    TEST_ASSERT_NOT_NULL(ro);
    TEST_ASSERT_EQUAL_INT64(D3_MAX_INDEX, queue_meta_start_index(ro));
    mmapqi_queue_meta_close(ro);
}

/* ── CAND-38 pin P3: pre-rule on-disk geometry is ESCHEMA, not ECORRUPT ────
 * A meta written before the geometry rule (region 4096, or 16384 with a
 * file that is not a multiple of the 32 KB header block) describes a queue
 * this build refuses to run — rejected-by-compatibility, not damaged:
 * MMAPQ_ESCHEMA on reopen, both open paths. */

void test_c38_open_ro_prerule_region_eschema(void)
{
    create_valid_meta();
    int32_t old = 4096;
    pwrite_meta_field(&old, sizeof(old), QUEUE_META_REGION_SIZE_OFFSET);
    /* D2_MAX_FILE stays a multiple of 4096, so ONLY the geometry rule
     * fires — the ECORRUPT impossible-value checks all pass. */
    assert_ro_open_rc(MMAPQ_ESCHEMA);

    /* The RW path with legal caller geometry refuses the same disk meta
     * (disk values win; meta_validate's code propagates). */
    QueueMeta *m = NULL;
    int rc = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &m);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ESCHEMA, rc);
    TEST_ASSERT_NULL(m);
}

void test_c38_open_ro_prerule_file_geometry_eschema(void)
{
    create_valid_meta();
    /* region 16384 (power of two, < 32 KB) + file 49152: file % region == 0
     * but neither the region rule nor the header-block file rule holds. */
    int32_t old_region = 16384;
    int64_t old_file   = 49152;
    pwrite_meta_field(&old_region, sizeof(old_region), QUEUE_META_REGION_SIZE_OFFSET);
    pwrite_meta_field(&old_file,   sizeof(old_file),   QUEUE_META_MAX_FILE_SIZE_OFFSET);
    assert_ro_open_rc(MMAPQ_ESCHEMA);
}

void test_c38_open_current_geometry_control(void)
{
    /* Valid post-rule geometry opens on both paths. */
    create_valid_meta();

    QueueMeta *ro = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro));
    TEST_ASSERT_NOT_NULL(ro);
    mmapqi_queue_meta_close(ro);

    TEST_ASSERT_EQUAL_INT(0, mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, D2_MAX_FILE, 0, QUEUE_TYPE_NORMAL, &meta));
    TEST_ASSERT_NOT_NULL(meta);
}

/* ── CAND-38 pin P4: the page-size half of the rule uses the RUNTIME page ──
 * size. region_size 32768 is page-aligned on 4/16 KB-page hosts but not on
 * a 64 KB-page host; the seam simulates both. A queue is portable only to
 * hosts whose page size divides its region_size: reopening a 32 KB-region
 * meta under a simulated 64 KB page is ESCHEMA (platform mismatch, not
 * corruption). Caller-argument refusal is EINVAL. The hook is disarmed
 * BEFORE every assertion so a longjmp cannot leak an armed override. */

void test_c38_page_size_seam(void)
{
#ifdef MMAPQ_TEST_HOOKS
    /* Simulated 64 KB-page host: region 32768 is not a page multiple. */
    mmapq_test_hook_set_page_size(65536);
    int rc64 = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, 98304, 0, QUEUE_TYPE_NORMAL, &meta);
    mmapq_test_hook_set_page_size(0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc64);
    TEST_ASSERT_NULL(meta);

    /* Simulated 16 KB and 4 KB pages: accepted (32768 % 16384 == 0). */
    mmapq_test_hook_set_page_size(16384);
    int rc16 = mmapqi_queue_meta_open_rw(
        DIR, QUEUE, 0, 32768, 98304, 0, QUEUE_TYPE_NORMAL, &meta);
    mmapq_test_hook_set_page_size(0);
    TEST_ASSERT_EQUAL_INT(0, rc16);
    TEST_ASSERT_NOT_NULL(meta);
    mmapqi_queue_meta_close(meta); meta = NULL;

    /* meta_validate path: the 32 KB-region meta on disk, reopened under a
     * simulated 64 KB page, is a platform mismatch — ESCHEMA. */
    mmapq_test_hook_set_page_size(65536);
    QueueMeta *ro = NULL;
    int rcro = mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro);
    mmapq_test_hook_set_page_size(0);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ESCHEMA, rcro);
    TEST_ASSERT_NULL(ro);

    /* And still opens with the real (4 KB) page size once disarmed. */
    ro = NULL;
    TEST_ASSERT_EQUAL_INT(0,
        mmapqi_queue_meta_open_ro(DIR, QUEUE, QUEUE_TYPE_NORMAL, &ro));
    TEST_ASSERT_NOT_NULL(ro);
    mmapqi_queue_meta_close(ro);
#else
    TEST_IGNORE_MESSAGE("needs MMAPQ_TEST_HOOKS (normal-loop build only)");
#endif
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
    RUN_TEST(test_rw_meta_open_rejects_truncated_meta);
    RUN_TEST(test_open_ro_corrupt_region_size_not_pow2);
    RUN_TEST(test_open_ro_corrupt_region_size_zero);
    RUN_TEST(test_open_ro_corrupt_max_file_size_not_multiple);
    RUN_TEST(test_open_ro_corrupt_max_file_size_zero);
    RUN_TEST(test_open_rw_corrupt_region_size_not_pow2);
    RUN_TEST(test_open_rw_caller_region_size_not_pow2_eINVAL);
    RUN_TEST(test_open_rw_caller_max_file_size_not_multiple_eINVAL);
    RUN_TEST(test_open_valid_geometry_control);
    RUN_TEST(test_open_ro_waits_for_creator_lock);
    RUN_TEST(test_open_ro_corrupt_start_index_above_max);
    RUN_TEST(test_open_ro_corrupt_start_index_negative);
    RUN_TEST(test_open_rw_corrupt_start_index_above_max);
    RUN_TEST(test_open_start_index_max_boundary_control);
    RUN_TEST(test_c38_open_ro_prerule_region_eschema);
    RUN_TEST(test_c38_open_ro_prerule_file_geometry_eschema);
    RUN_TEST(test_c38_open_current_geometry_control);
    RUN_TEST(test_c38_page_size_seam);
    return UNITY_END();
}
