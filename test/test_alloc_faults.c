/*
 * test_alloc_faults.c — allocation-failure fault injection (D1: CAND-13,
 * CAND-14, and the queue_region_accessor mapper-ownership contract).
 *
 * Built with -Wl,--wrap=calloc (see scripts/test.sh): every calloc in the
 * binary routes through __wrap_calloc, which fails the fail_at-th calloc
 * after arming. Leak/UAF evidence is LSan/ASan's at process exit — the
 * assertions here pin the error returns; run under `test.sh --asan` for the
 * memory evidence. Counters are atomic: harmless single-threaded in these
 * pins, required for the cross-thread pins sharing this file later (D1b).
 *
 * Ordinal map (empirically mapped with a scratch sweep at D1a fail-first;
 * geometry region 65536, ring 4, runtime NULL; rc -9 = MMAPQ_ENOMEM):
 *
 *   rolled_create:          #1 RolledImpl   #2 cache   #3 FileMapper
 *   create_rw / create_ro:  #1 acc          #2-4  header rolled_create
 *                           #5-15 header sync_create:
 *                               #5      regions array — nothing created yet
 *                               #6-13   region impl+struct pairs (the in-loop
 *                                       destroy cleans these)
 *                               #14-15  SyncRingImpl / RegionRing — the D1b
 *                                       leak window; deliberately NOT pinned
 *                                       in D1a (leaks until D1b by design)
 *                           #16-17  header accessor_create (impl, acc)
 *                           RW only, payload: #18-20 rolled_create,
 *                               #21-31 sync_create (same windows; #30-31 D1b),
 *                               #32-33 accessor_create
 *   payload() lazy path (armed after a successful create_ro):
 *                           #1-3 rolled_create   #4-14 sync_create (same
 *                           windows; #13-14 D1b)   #15-16 accessor_create
 */
#include "unity.h"
#include "internal/queue_region_accessor.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"
#include "mmap_platform.h"

#include <stdatomic.h>
#include <stddef.h>

extern void *__real_calloc(size_t, size_t);

static _Atomic long armed, calloc_count, fail_at;

void *__wrap_calloc(size_t n, size_t sz)
{
    if (atomic_load(&armed)) {
        long c = atomic_fetch_add(&calloc_count, 1) + 1;
        if (c == atomic_load(&fail_at)) return NULL;
    }
    return __real_calloc(n, sz);
}

static void arm(long fa)
{
    atomic_store(&calloc_count, 0);
    atomic_store(&fail_at, fa);
    atomic_store(&armed, 1);
}

static void disarm(void) { atomic_store(&armed, 0); }

#define DIR         "/tmp/mmapq_test_alloc_faults"
#define REGION_SIZE 65536
#define RING_SIZE   4
#define MAX_FILE    262144
#define TIMEOUT_NS  5000000000ULL

void setUp(void)    {}
void tearDown(void) {}

/* ── CAND-13: rolled_create OOM paths must free file_prefix ─────────────── */

static int rolled_create_armed(long ordinal, FileMapper **fm)
{
    *fm = NULL;
    arm(ordinal);
    int rc = mmapqi_file_mapper_rolled_create(
        DIR "/header", MAX_FILE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_ONLY, 6, 1, fm);
    disarm();
    return rc;
}

void test_rolled_fail_impl_calloc_clean_control(void)
{
    /* #1 = RolledImpl calloc: nothing allocated yet — clean before and after */
    FileMapper *fm;
    int rc = rolled_create_armed(1, &fm);
    if (fm) mmapqi_file_mapper_destroy(fm);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(fm);
}

void test_rolled_fail_cache_calloc_frees_prefix(void)
{
    /* #2 = cache calloc (file_mapper_rolled.c:276): :277 used to leak the prefix */
    FileMapper *fm;
    int rc = rolled_create_armed(2, &fm);
    if (fm) mmapqi_file_mapper_destroy(fm);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(fm);
}

void test_rolled_fail_mapper_calloc_frees_prefix(void)
{
    /* #3 = FileMapper calloc (:279): :280 used to leak the prefix */
    FileMapper *fm;
    int rc = rolled_create_armed(3, &fm);
    if (fm) mmapqi_file_mapper_destroy(fm);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(fm);
}

/* ── Ownership contract: on ANY make_ring_accessor failure the mapper has ──
 * ── been destroyed and the caller must not touch it ─────────────────────── */

static int create_rw_armed(long ordinal, QueueRegionAccessor **acc)
{
    *acc = NULL;
    arm(ordinal);
    int rc = mmapqi_queue_region_accessor_create_rw(
        DIR, "q", REGION_SIZE, RING_SIZE, 1, MAX_FILE, TIMEOUT_NS, NULL, 0, acc);
    disarm();
    return rc;
}

static int create_ro_armed(long ordinal, QueueRegionAccessor **acc)
{
    *acc = NULL;
    arm(ordinal);
    int rc = mmapqi_queue_region_accessor_create_ro(
        DIR, "q", REGION_SIZE, RING_SIZE, 1, MAX_FILE, TIMEOUT_NS, NULL, acc);
    disarm();
    return rc;
}

void test_create_rw_header_ring_fail_no_leak(void)
{
    /* #5 = header sync_create regions array: pre-fix the caller's
     * file_mapper_close leaked the never-attached mapper chain (326 B / 4) */
    QueueRegionAccessor *acc;
    int rc = create_rw_armed(5, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

void test_create_rw_header_accessor_fail_no_uaf(void)
{
    /* #16 = header accessor_create impl calloc: ring_create has already
     * destroyed the mapper — pre-fix the caller's file_mapper_close was a
     * use-after-free (ASan abort) */
    QueueRegionAccessor *acc;
    int rc = create_rw_armed(16, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

void test_create_ro_header_ring_fail_no_leak(void)
{
    /* #5, RO path (:222) — same leak sub-case as RW */
    QueueRegionAccessor *acc;
    int rc = create_ro_armed(5, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

void test_create_ro_header_accessor_fail_no_uaf(void)
{
    /* #16, RO path (:222) — same UAF sub-case as RW */
    QueueRegionAccessor *acc;
    int rc = create_ro_armed(16, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

void test_create_rw_payload_ring_fail_no_leak(void)
{
    /* #21 = payload sync_create regions array: pre-fix close(acc) skipped the
     * unattached pay_mapper (occupied still 0) — 329 B / 4 leak */
    QueueRegionAccessor *acc;
    int rc = create_rw_armed(21, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

/* ── CAND-14: lazy payload path ──────────────────────────────────────────── */

static QueueRegionAccessor *new_ro_accessor(void)
{
    QueueRegionAccessor *acc = NULL;
    int rc = create_ro_armed(-1, &acc);   /* disarmed-value: never fails */
    TEST_ASSERT_EQUAL_INT(0, rc);
    TEST_ASSERT_NOT_NULL(acc);
    return acc;
}

void test_lazy_payload_ring_fail_no_leak(void)
{
    /* lazy #4 = payload sync_create regions array: pre-fix `return NULL`
     * leaked the pay_mapper chain (329 B / 4) — CAND-14 proper */
    QueueRegionAccessor *acc = new_ro_accessor();
    arm(4);
    RegionAccessor *p = mmapqi_queue_region_accessor_payload(acc, 0);
    disarm();
    mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_NULL(p);
}

void test_lazy_payload_accessor_fail_clean_control(void)
{
    /* lazy #15 = payload accessor_create impl calloc: ring_create destroyed
     * the mapper and the caller never touched it — clean before and after */
    QueueRegionAccessor *acc = new_ro_accessor();
    arm(15);
    RegionAccessor *p = mmapqi_queue_region_accessor_payload(acc, 0);
    disarm();
    mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_NULL(p);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_rolled_fail_impl_calloc_clean_control);
    RUN_TEST(test_rolled_fail_cache_calloc_frees_prefix);
    RUN_TEST(test_rolled_fail_mapper_calloc_frees_prefix);
    RUN_TEST(test_create_rw_header_ring_fail_no_leak);
    RUN_TEST(test_create_rw_header_accessor_fail_no_uaf);
    RUN_TEST(test_create_ro_header_ring_fail_no_leak);
    RUN_TEST(test_create_ro_header_accessor_fail_no_uaf);
    RUN_TEST(test_create_rw_payload_ring_fail_no_leak);
    RUN_TEST(test_lazy_payload_ring_fail_no_leak);
    RUN_TEST(test_lazy_payload_accessor_fail_clean_control);
    return UNITY_END();
}
