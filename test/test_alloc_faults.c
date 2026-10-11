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
 *   async_create (direct, live runtime; all 15 callocs on the main thread —
 *   the runtime thread never callocs in this no-files geometry, trace-verified
 *   3/3 runs):
 *                           #1 regions array     #2-9 region impl+struct pairs
 *                           #10 mapping_impl     #11-13 register internals
 *                               (ExecList node + items + free-list node —
 *                               CAND-26 made register/unregister propagate
 *                               these; pins below)
 *                           #14 AsyncRingImpl (size 32)   #15 RegionRing (32)
 * The post-registration pins use a size-32 predicate (impl = 1st match, ring =
 * 2nd), stable across D1b's register-last reorder which renumbers ordinals:
 * pre-fix #14/#15 follow registration (5/5 aborts recorded: ASan
 * heap-use-after-free in async_mapping_execute on the runtime thread, or
 * SEGV); post-fix the same structs are allocated before registration.
 */
#include "unity.h"
#include "internal/queue_region_accessor.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"
#include "internal/region_ring.h"
#include "internal/queue_meta.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_reader.h"
#include "mmap_platform.h"
#include "runtime.h"

#include <stdatomic.h>
#include <stddef.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void *__real_calloc(size_t, size_t);

static _Atomic long   armed, calloc_count, fail_at;
static _Atomic size_t fail_size;   /* 0 = ordinal mode */
static _Atomic long   fail_nth;    /* ordinal mode: which calloc;
                                    * size mode: which match of fail_size */
static _Atomic long   match_count;

void *__wrap_calloc(size_t n, size_t sz)
{
    if (atomic_load(&armed)) {
        if (atomic_load(&fail_size) != 0) {
            if (sz == atomic_load(&fail_size)) {
                long m = atomic_fetch_add(&match_count, 1) + 1;
                if (m == atomic_load(&fail_nth)) return NULL;
            }
        } else {
            long c = atomic_fetch_add(&calloc_count, 1) + 1;
            if (c == atomic_load(&fail_at)) return NULL;
        }
    }
    return __real_calloc(n, sz);
}

static void arm(long fa)
{
    atomic_store(&fail_size, 0);
    atomic_store(&calloc_count, 0);
    atomic_store(&fail_at, fa);
    atomic_store(&armed, 1);
}

static void arm_size(size_t sz, long nth)
{
    atomic_store(&match_count, 0);
    atomic_store(&fail_nth, nth);
    atomic_store(&fail_size, sz);
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

/* ── D1b: region_ring create paths ───────────────────────────────────────── */

void test_sync_ring_impl_calloc_fail_no_leak(void)
{
    /* create_rw (runtime NULL) #14 = SyncRingImpl calloc (region_ring.c:63):
     * pre-fix freed the regions array without the 4 created region objects
     * (902 B / 12 with the mapper chain) */
    QueueRegionAccessor *acc;
    int rc = create_rw_armed(14, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

void test_sync_ring_ring_calloc_fail_no_leak(void)
{
    /* #15 = RegionRing calloc (:68): same leak shape as #14 pre-fix */
    QueueRegionAccessor *acc;
    int rc = create_rw_armed(15, &acc);
    if (acc) mmapqi_queue_region_accessor_close(acc);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(acc);
}

static MmapqRuntime *new_runtime(void)
{
    MmapqIdleStrategy *idle = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_idle_strategy_busy_spin_create(&idle));
    TEST_ASSERT_NOT_NULL(idle);
    MmapqRuntime *rt = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_runtime_create(
        "alloc-faults", idle, MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS,
        NULL, NULL, &rt));
    TEST_ASSERT_NOT_NULL(rt);
    return rt;
}

void test_async_ring_create_closed_runtime_no_leak(void)
{
    /* Registration failure without fault injection: register returns 0 on a
     * closed runtime (runtime.c:477-479). Pre-fix the regions array, the 4
     * region objects, and the caller-owned mapper all leaked (999 B / 13). */
    MmapqRuntime *rt = new_runtime();
    mmapq_runtime_close(rt);

    FileMapper *fm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_rolled_create(
        DIR "/payload_0", MAX_FILE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_WRITE, 6, 1, &fm));
    TEST_ASSERT_NOT_NULL(fm);

    RegionRing *ring = NULL;
    int rc = mmapqi_region_ring_async_create(RING_SIZE, REGION_SIZE, fm,
                                             TIMEOUT_NS, rt, &ring);
    /* fm consumed by the create on failure (contract) — never touched */
    mmapq_runtime_destroy(rt);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, rc);
    TEST_ASSERT_NULL(ring);
}

static int async_create_armed_size(size_t sz, long nth, MmapqRuntime *rt,
                                   RegionRing **ring)
{
    FileMapper *fm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_rolled_create(
        DIR "/payload_0", MAX_FILE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_WRITE, 6, 1, &fm));
    TEST_ASSERT_NOT_NULL(fm);
    *ring = NULL;
    arm_size(sz, nth);
    int rc = mmapqi_region_ring_async_create(RING_SIZE, REGION_SIZE, fm,
                                             TIMEOUT_NS, rt, ring);
    disarm();
    return rc;
}

void test_async_ring_impl_calloc_fail_no_uaf(void)
{
    /* First size-32 calloc in async_create = AsyncRingImpl. Pre-fix it sat
     * AFTER mmapq_runtime_register: the failure freed mapping_impl while the
     * runtime thread held it — 5/5 aborts (ASan heap-use-after-free in
     * async_mapping_execute region_ring.c:103 on T1, or SEGV). Post-fix it
     * precedes registration: plain ENOMEM, no thread involvement. */
    MmapqRuntime *rt = new_runtime();
    RegionRing *ring;
    int rc = async_create_armed_size(32, 1, rt, &ring);
    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
    if (ring) mmapqi_region_ring_destroy(ring);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(ring);
}

void test_async_ring_ring_calloc_fail_no_uaf(void)
{
    /* Second size-32 calloc = RegionRing — same pre-fix UAF window. */
    MmapqRuntime *rt = new_runtime();
    RegionRing *ring;
    int rc = async_create_armed_size(32, 2, rt, &ring);
    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
    if (ring) mmapqi_region_ring_destroy(ring);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENOMEM, rc);
    TEST_ASSERT_NULL(ring);
}

/* ── D1c: consumption is total — validation returns destroy the mapper ───── */

void test_reader_open_corrupt_meta_region_size_no_leak(void)
{
    /* A corrupt meta region_size (positive, divides max_file_size, but not a
     * power of two) passes meta_validate and rolled_create, then EINVALs at
     * ring creation — reached from disk via the reader's lazy open
     * (indexed_reader.c reader_init). Pre-fix the header mapper chain leaked
     * on every open attempt (LSan: 4 allocations). */
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/q && mkdir -p %s", DIR, DIR);
    if (system(cmd) != 0) { /* best-effort */ }

    /* meta is written on the appender path, not by bare queue create */
    MmapqIndexedQueue *q = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(
        DIR, "q", REGION_SIZE, RING_SIZE, 1, MAX_FILE, 0,
        TIMEOUT_NS, 0, NULL, NULL, &q));
    TEST_ASSERT_NOT_NULL(q);
    MmapqIndexedAppender *a = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender(q, &a));
    int32_t payload = 42;
    TEST_ASSERT_TRUE(mmapq_indexed_appender_append(a, &payload, sizeof(payload)) >= 0);
    mmapq_indexed_appender_close(a);
    mmapq_indexed_queue_close(q);

    char meta_path[256];
    snprintf(meta_path, sizeof(meta_path), "%s/q/meta", DIR);
    int fd = open(meta_path, O_RDWR);
    TEST_ASSERT_TRUE(fd >= 0);
    int32_t bad_region_size = 12288;   /* 3 × 4096: divides 49152, not pow2 */
    int64_t bad_max_file    = 49152;
    TEST_ASSERT_EQUAL(4, pwrite(fd, &bad_region_size, 4, QUEUE_META_REGION_SIZE_OFFSET));
    TEST_ASSERT_EQUAL(8, pwrite(fd, &bad_max_file, 8, QUEUE_META_MAX_FILE_SIZE_OFFSET));
    close(fd);

    MmapqIndexedReader *r = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_reader_create(
        DIR, "q", RING_SIZE, TIMEOUT_NS, NULL, &r));
    TEST_ASSERT_NOT_NULL(r);

    MmapqReadingContext ctx;
    memset(&ctx, 0, sizeof(ctx));
    mmapq_indexed_reader_read(r, 0, &ctx);
    TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_ERROR, ctx.status);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, mmapq_indexed_reader_last_error(r));
    mmapq_indexed_reader_close(r);

    if (system(cmd) != 0) { /* best-effort */ }
}

/* ── CAND-26: register/unregister propagate allocation failure ───────────── */

typedef struct {
    MmapqExecutable  exec;
    MmapqRuntime    *rt;             /* unregister pins only */
    MmapqExecutable *target;         /* unregister pins only */
    long             fail_ordinal;   /* unregister pins only */
    _Atomic long     exec_count;
    _Atomic int      unreg_rc;
    _Atomic int      unreg_done;
    _Atomic int      close_called;
    _Atomic int      go;             /* unreg_execute gate */
} ProbeExec;

static int probe_execute(MmapqExecutable *self)
{
    ProbeExec *p = (ProbeExec *)self->ctx;
    atomic_fetch_add(&p->exec_count, 1);
    return 0;
}

static void probe_close(MmapqExecutable *self)
{
    ProbeExec *p = (ProbeExec *)self->ctx;
    atomic_store(&p->close_called, 1);
}

static void probe_init(ProbeExec *p)
{
    memset(p, 0, sizeof(*p));
    p->exec.execute = probe_execute;
    p->exec.close   = probe_close;
    p->exec.ctx     = p;
    p->unreg_rc     = -2;
}

static void spin_until_count(ProbeExec *p, long target)
{
    uint64_t deadline = mmapq_monotonic_nanos() + 2000000000ULL;
    while (atomic_load(&p->exec_count) < target
           && mmapq_monotonic_nanos() < deadline) mmapq_spin_hint();
}

static void spin_until_done(ProbeExec *p)
{
    uint64_t deadline = mmapq_monotonic_nanos() + 2000000000ULL;
    while (!atomic_load(&p->unreg_done)
           && mmapq_monotonic_nanos() < deadline) mmapq_spin_hint();
}

static void register_pin(long ordinal)
{
    /* Any calloc failure inside register must return 0 with the installed
     * list untouched. Pre-fix: node/items failure returned 1 with the whole
     * list dropped (or crashed: items memcpy through NULL); free-node
     * failure returned 1 and freed the old list from under the thread. */
    MmapqRuntime *rt = new_runtime();
    ProbeExec p1; probe_init(&p1);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &p1.exec));
    spin_until_count(&p1, 1);
    TEST_ASSERT_TRUE(atomic_load(&p1.exec_count) >= 1);

    ProbeExec p2; probe_init(&p2);
    arm(ordinal);
    int rc = mmapq_runtime_register(rt, &p2.exec);
    disarm();
    TEST_ASSERT_EQUAL_INT(0, rc);

    /* the old list is intact: p1 keeps executing; p2 never ran */
    long n = atomic_load(&p1.exec_count);
    spin_until_count(&p1, n + 1);
    TEST_ASSERT_TRUE(atomic_load(&p1.exec_count) > n);
    TEST_ASSERT_EQUAL(0, atomic_load(&p2.exec_count));

    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
}

void test_register_node_calloc_fail_returns_zero(void)      { register_pin(1); }
void test_register_items_calloc_fail_returns_zero(void)     { register_pin(2); }
void test_register_freenode_calloc_fail_returns_zero(void)  { register_pin(3); }

/* Runs ON the runtime thread (the unregister contract) and unregisters the
 * target with the fault armed. Gated on `go` so the main thread can first
 * place a trailing probe behind u in the list (see unregister_pin). */
static int unreg_execute(MmapqExecutable *self)
{
    ProbeExec *p = (ProbeExec *)self->ctx;
    if (!atomic_load(&p->go) || atomic_load(&p->unreg_done)) return 0;
    arm(p->fail_ordinal);
    int rc = mmapq_runtime_unregister(p->rt, p->target);
    disarm();
    atomic_store(&p->unreg_rc, rc);
    atomic_store(&p->unreg_done, 1);
    return 0;
}

static void assert_count_frozen(ProbeExec *p)
{
    long n = atomic_load(&p->exec_count);
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 100000000 };  /* 100 ms */
    nanosleep(&ts, NULL);
    TEST_ASSERT_EQUAL(n, atomic_load(&p->exec_count));
}

static void unregister_pin(long ordinal)
{
    /* unregister never fails (CAND-26): on allocation failure it replaces
     * the target's slot with the no-op executable in place and returns 1 —
     * callers' assumptions hold, nothing upstream changes.
     * p4 trails u in the list and u is gated on `go` until p4 has executed:
     * the iteration that fires u then continues into p4's slot, so the
     * pre-fix free-node fallback (freeing the list mid-iteration) is always
     * dereferenced — without the trailing probe this pin was flaky pre-fix
     * (pass or crash, timing-dependent). */
    MmapqRuntime *rt = new_runtime();
    ProbeExec p1; probe_init(&p1);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &p1.exec));
    spin_until_count(&p1, 1);
    TEST_ASSERT_TRUE(atomic_load(&p1.exec_count) >= 1);

    ProbeExec u; probe_init(&u);
    u.exec.execute = unreg_execute;
    u.rt = rt; u.target = &p1.exec; u.fail_ordinal = ordinal;
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &u.exec));

    ProbeExec p4; probe_init(&p4);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &p4.exec));
    spin_until_count(&p4, 1);
    TEST_ASSERT_TRUE(atomic_load(&p4.exec_count) >= 1);
    atomic_store(&u.go, 1);

    spin_until_done(&u);
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&u.unreg_done));
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&u.unreg_rc));   /* never fails */

    /* the target's slot was no-op'd: p1's count freezes; p4 keeps running */
    assert_count_frozen(&p1);
    long n4 = atomic_load(&p4.exec_count);
    spin_until_count(&p4, n4 + 1);
    TEST_ASSERT_TRUE(atomic_load(&p4.exec_count) > n4);

    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
}

void test_unregister_node_calloc_fail_noops_slot(void)      { unregister_pin(1); }
void test_unregister_items_calloc_fail_noops_slot(void)     { unregister_pin(2); }
void test_unregister_freenode_calloc_fail_noops_slot(void)  { unregister_pin(3); }

/* ── CAND-26: one pin per unregister call site ───────────────────────────── */

/* Loop site (runtime.c work<0 path): the thread closes the executable, then
 * unregisters it — with the allocation fault armed. */
static int dying_execute(MmapqExecutable *self)
{
    ProbeExec *p = (ProbeExec *)self->ctx;
    atomic_fetch_add(&p->exec_count, 1);
    arm(1);   /* the unregister's remove-node calloc fails */
    return -1;
}

void test_loop_unregister_alloc_fail_noops_slot(void)
{
    MmapqRuntime *rt = new_runtime();
    ProbeExec d; probe_init(&d);
    d.exec.execute = dying_execute;   /* close via probe_close (flag) */
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &d.exec));

    uint64_t deadline = mmapq_monotonic_nanos() + 2000000000ULL;
    while (!atomic_load(&d.close_called)
           && mmapq_monotonic_nanos() < deadline) mmapq_spin_hint();
    disarm();
    TEST_ASSERT_EQUAL_INT(1, atomic_load(&d.close_called));

    /* the failed unregister no-op'd the slot: a still-registered (and
     * re-executing) closed executable is the failure mode — count must
     * freeze. (Without this the pin passed vacuously on a fallback
     * mutation — close() is called before the unregister it guards.) */
    assert_count_frozen(&d);

    /* the failed unregister fell back to the in-place no-op: the runtime
     * closes cleanly (no hang, no crash, no leak) */
    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
}

/* waiting_deregister site (region_ring.c): async ring close registers a
 * deregistration executable that unregisters the mapping exec and itself. */
void test_waiting_deregister_alloc_fail_close_completes(void)
{
    MmapqRuntime *rt = new_runtime();
    FileMapper *fm = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_rolled_create(
        DIR "/payload_9", MAX_FILE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_WRITE, 6, 1, &fm));
    RegionRing *ring = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_region_ring_async_create(
        RING_SIZE, REGION_SIZE, fm, TIMEOUT_NS, rt, &ring));
    TEST_ASSERT_NOT_NULL(ring);

    /* close path: the main thread registers wait_impl (3 callocs: ExecList
     * node, items, FreeNode); the runtime thread's first unregister (the
     * mapping exec) follows — fail its remove-node calloc (#4) */
    arm(4);
    mmapqi_region_ring_destroy(ring);   /* must complete, not hang */
    disarm();

    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
}

/* unregister_and_wait site (runtime.c): the helper's registration failure is
 * transient — the function retries until it succeeds (or the runtime closes).
 * One injected failure → one retry → completion. */
void test_unregister_and_wait_alloc_fail_retries(void)
{
    MmapqRuntime *rt = new_runtime();
    ProbeExec p1; probe_init(&p1);
    TEST_ASSERT_EQUAL_INT(1, mmapq_runtime_register(rt, &p1.exec));
    spin_until_count(&p1, 1);

    arm(1);   /* helper register's ExecList node calloc fails once */
    mmapq_runtime_unregister_and_wait(rt, &p1.exec);
    disarm();

    /* returned (no hang) and p1 is unregistered: count frozen */
    assert_count_frozen(&p1);

    mmapq_runtime_close(rt);
    mmapq_runtime_destroy(rt);
}

/* ── D1d: consumption is total even on caller-error EINVAL ───────────────── */

void test_ring_create_caller_error_eINVAL_consumes_mapper(void)
{
    /* out == NULL (sync) and runtime == NULL (async) are unreachable from
     * in-tree callers, but the contract says the mapper is consumed on ANY
     * failure — these pin the literal reading. */
    FileMapper *fm1 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_rolled_create(
        DIR "/h1", MAX_FILE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_ONLY, 6, 1, &fm1));
    int rc1 = mmapqi_region_ring_sync_create(RING_SIZE, REGION_SIZE, fm1,
                                             TIMEOUT_NS, NULL);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc1);

    FileMapper *fm2 = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapqi_file_mapper_rolled_create(
        DIR "/h2", MAX_FILE, REGION_SIZE,
        mmapqi_file_init_none, NULL, MAP_MODE_READ_ONLY, 6, 1, &fm2));
    RegionRing *ring = NULL;
    int rc2 = mmapqi_region_ring_async_create(RING_SIZE, REGION_SIZE, fm2,
                                              TIMEOUT_NS, NULL, &ring);
    TEST_ASSERT_EQUAL_INT(MMAPQ_EINVAL, rc2);
    TEST_ASSERT_NULL(ring);
    /* fm1/fm2 destroyed by the creates — LSan verifies */
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
    RUN_TEST(test_sync_ring_impl_calloc_fail_no_leak);
    RUN_TEST(test_sync_ring_ring_calloc_fail_no_leak);
    RUN_TEST(test_async_ring_create_closed_runtime_no_leak);
    RUN_TEST(test_async_ring_impl_calloc_fail_no_uaf);
    RUN_TEST(test_async_ring_ring_calloc_fail_no_uaf);
    RUN_TEST(test_reader_open_corrupt_meta_region_size_no_leak);
    RUN_TEST(test_register_node_calloc_fail_returns_zero);
    RUN_TEST(test_register_items_calloc_fail_returns_zero);
    RUN_TEST(test_register_freenode_calloc_fail_returns_zero);
    RUN_TEST(test_unregister_node_calloc_fail_noops_slot);
    RUN_TEST(test_unregister_items_calloc_fail_noops_slot);
    RUN_TEST(test_unregister_freenode_calloc_fail_noops_slot);
    RUN_TEST(test_loop_unregister_alloc_fail_noops_slot);
    RUN_TEST(test_waiting_deregister_alloc_fail_close_completes);
    RUN_TEST(test_unregister_and_wait_alloc_fail_retries);
    RUN_TEST(test_ring_create_caller_error_eINVAL_consumes_mapper);
    return UNITY_END();
}
