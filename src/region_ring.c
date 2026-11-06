#include "internal/region_ring.h"
#include "internal/runtime_internal.h"
#include "runtime.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <stdatomic.h>
#include <stdio.h>

/* ═══════════════════════════════════════════════════════════════════════════
 * Sync RegionRing
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    FileMapper *file_mapper;
} SyncRingImpl;

static void sync_ring_close(RegionRing *self)
{
    MMAP_ASSERT(self != NULL, "sync_ring_close: self is NULL");

    SyncRingImpl *impl = (SyncRingImpl *)self->impl;

    for (int i = 0; i < self->ring_size; i++) {
        if (self->regions[i] != NULL) {
            region_close(self->regions[i]);
        }
    }

    file_mapper_close(impl->file_mapper);
}

/* Destroy count regions and the array itself (allocation-failure teardown).
 * Regions only reference the mapper; destroying them never touches it. */
static void destroy_regions(Region **regions, int count)
{
    for (int i = 0; i < count; i++) mmapqi_region_destroy(regions[i]);
    free(regions);
}

int mmapqi_region_ring_sync_create(
    int         ring_size,
    int32_t     region_size,
    FileMapper *file_mapper,
    uint64_t    timeout_ns,
    RegionRing **out)
{
    if (out == NULL) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    *out = NULL;
    if (file_mapper == NULL) return MMAPQ_EINVAL;
    /* A non-NULL mapper is consumed on every path: owned by the ring on
     * success, destroyed here on failure — the caller must never touch it
     * after this call. Caller-error only: a corrupt meta region_size is
     * rejected by meta_validate with MMAPQ_ECORRUPT before reaching here. */
    if (ring_size <= 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    if ((ring_size & (ring_size - 1)) != 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    if (region_size <= 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    if ((region_size & (region_size - 1)) != 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }

    Region **regions = calloc((size_t)ring_size, sizeof(Region *));
    if (regions == NULL) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_ENOMEM; }

    for (int i = 0; i < ring_size; i++) {
        int rc = mmapqi_region_sync_create(
            file_mapper,
            region_size,
            timeout_ns,
            REGION_MAX_FILE_NOT_FOUND_ERRORS,
            REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
            &regions[i]);
        if (rc != 0) {
            destroy_regions(regions, i);
            mmapqi_file_mapper_destroy(file_mapper);
            return rc;
        }
    }

    SyncRingImpl *impl = calloc(1, sizeof(SyncRingImpl));
    if (impl == NULL) {
        destroy_regions(regions, ring_size);
        mmapqi_file_mapper_destroy(file_mapper);
        return MMAPQ_ENOMEM;
    }
    impl->file_mapper = file_mapper;

    RegionRing *ring = calloc(1, sizeof(RegionRing));
    if (ring == NULL) {
        free(impl);
        destroy_regions(regions, ring_size);
        mmapqi_file_mapper_destroy(file_mapper);
        return MMAPQ_ENOMEM;
    }
    ring->close     = sync_ring_close;
    ring->regions   = (Region **)regions;
    ring->ring_size = ring_size;
    ring->is_async  = 0;
    ring->impl      = impl;

    *out = ring;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Async RegionRing
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * AsyncMappingExecutable — drives the async region state machines.
 *
 * Registered with the MmapqRuntime. On each execute() call, polls every region
 * in the ring for pending map/unmap requests. Returns work count so the
 * MmapqRuntime's idle strategy can back off when no regions need mapping.
 */
typedef struct {
    Region    **regions;
    int         ring_size;
    MmapqExecutable  exec;  /* embedded — registered with MmapqRuntime */
} AsyncMappingImpl;

static int async_mapping_execute(MmapqExecutable *self)
{
    AsyncMappingImpl *impl = (AsyncMappingImpl *)self->ctx;
    int work = 0;
    for (int i = 0; i < impl->ring_size; i++) {
        work += mmapqi_region_async_process_request(impl->regions[i]);
    }
    return work;
}

static void async_mapping_close(MmapqExecutable *self)
{
    /* MmapqRuntime is shutting down — unmanage all regions so app thread
     * fails fast rather than spinning indefinitely waiting for mapping */
    AsyncMappingImpl *impl = (AsyncMappingImpl *)self->ctx;
    for (int i = 0; i < impl->ring_size; i++) {
        mmapqi_region_async_unmanage(impl->regions[i]);
    }
}

/* ── Async ring close ────────────────────────────────────────────────────── */

/* Floor for the deregistration wait in async_ring_close. The wait's bound
 * is a liveness bound on the runtime thread, and the floor only has to
 * cover a HEALTHY thread reaching the deregistration executable — at most
 * one executable step (a region map), milliseconds — so 1 s is generous by
 * orders of magnitude while keeping a wedged runtime's close at about 1 s
 * per ring (CAND-24's bound stays meaningful). */
#define MMAPQI_RING_CLOSE_WAIT_FLOOR_NS UINT64_C(1000000000)   /* 1 s */

typedef struct {
    FileMapper       *file_mapper;
    MmapqRuntime          *runtime;
    AsyncMappingImpl *mapping_impl;
    atomic_int        closed;
    int               mapper_detached;  /* set/read on the closing thread only */
    uint64_t          timeout_ns;       /* mapping/deregister wait budget, from create */
} AsyncRingImpl;

/**
 * Deregister + signal completion — used by async ring close to know
 * when the mapper task has been safely removed from the runtime.
 */
typedef struct {
    MmapqRuntime          *runtime;
    MmapqExecutable       *target;
    void             *target_ctx_to_free;
    atomic_int        done;    /* set to 1 after unregistration completes */
    MmapqExecutable        self_exec;
} WaitingDeregisterImpl;

static int waiting_deregister_execute(MmapqExecutable *self)
{
    WaitingDeregisterImpl *impl = (WaitingDeregisterImpl *)self->ctx;
    /* Must be called from runtime thread — safe to call unregister */
    mmapq_runtime_unregister(impl->runtime, impl->target);
    mmapq_runtime_unregister(impl->runtime, &impl->self_exec);
    if (impl->target_ctx_to_free) {
        free(impl->target_ctx_to_free);
    }
    atomic_store_explicit(&impl->done, 1, memory_order_release);
    /* Note: impl itself is stack/heap owned by the waiter — don't free here */
    return 1;
}

static void waiting_deregister_close(MmapqExecutable *self)
{
    WaitingDeregisterImpl *impl = (WaitingDeregisterImpl *)self->ctx;
    if (impl->target_ctx_to_free) {
        free(impl->target_ctx_to_free);
    }
    atomic_store_explicit(&impl->done, 1, memory_order_release);
}

static void async_ring_close(RegionRing *self)
{
    MMAP_ASSERT(self != NULL, "async_ring_close: self is NULL");

    AsyncRingImpl *impl = (AsyncRingImpl *)self->impl;

    if (!atomic_compare_exchange_strong(&impl->closed, &(int){0}, 1)) {
        return;
    }

    /*
     * Close order is critical:
     *
     * Step 1: Deregister the AsyncMappingExecutable from the runtime and
     *         WAIT for the runtime thread to confirm it is done.
     *
     *         This MUST happen first. After this point the runtime thread
     *         will never call mmapqi_region_async_process_request() on our regions
     *         again. Only then is it safe for region_close() to self-drive
     *         process_request() without racing against the mapper thread.
     *
     * Step 2: Close all regions. With the mapper thread stopped, region_close()
     *         can safely self-drive any pending UNMAP_REQUESTED transitions.
     *
     * Step 3: Close the file mapper — all regions are unmapped.
     *
     * WaitingDeregisterImpl is heap-allocated (CAND-24): on the deadline
     * path below the registered deregistration executable may still run
     * on the live runtime thread AFTER this function returns — it writes
     * wait_impl->done and frees mapping_impl via target_ctx_to_free. A
     * stack wait_impl would be gone by then; the deadline fix would
     * introduce the very use-after-free class it is filed against.
     */
    WaitingDeregisterImpl *wait_impl = calloc(1, sizeof *wait_impl);
    if (wait_impl == NULL) {
        /*
         * Allocation failure on a close path must not hang: the dereg
         * executable was never registered and mapping_impl stays
         * registered on a possibly-live thread — take the leak path
         * exactly like a deadline expiry (Steps 2-3 skipped, destroy
         * skips the region/file-mapper teardown via mapper_detached).
         */
        impl->mapper_detached = 1;
        return;
    }
    wait_impl->runtime            = impl->runtime;
    wait_impl->target             = &impl->mapping_impl->exec;
    wait_impl->target_ctx_to_free = impl->mapping_impl;
    atomic_store(&wait_impl->done, 0);
    wait_impl->self_exec.execute = waiting_deregister_execute;
    wait_impl->self_exec.close   = waiting_deregister_close;
    wait_impl->self_exec.ctx     = wait_impl;

    if (mmapq_runtime_register(impl->runtime, &wait_impl->self_exec)) {
        /*
         * The deadline below is a liveness bound on the runtime thread,
         * not a mapping-latency budget. impl->timeout_ns is the queue's
         * MAPPING timeout — a per-map latency budget that is legitimately
         * 0 (mapping is opportunistic: timeout 0 means "fail this poll
         * fast", not "the runtime thread is wedged"). Bounding the
         * DEREGISTRATION wait by it made close give up after ~1000 spins
         * on a perfectly healthy runtime thread that was merely still
         * completing an in-flight map, then leak the mapper state and
         * skip Steps 2-3 (mappings and fds left open). Floor the wait at
         * MMAPQI_RING_CLOSE_WAIT_FLOOR_NS: close must stay bounded (never
         * hang on a wedged thread) but must not give up before a healthy
         * thread has reached the deregistration executable.
         */
        uint64_t wait_budget_ns = impl->timeout_ns;
        if (wait_budget_ns < MMAPQI_RING_CLOSE_WAIT_FLOOR_NS) {
            wait_budget_ns = MMAPQI_RING_CLOSE_WAIT_FLOOR_NS;
        }
        uint64_t spins = 0;
        uint64_t start = mmapq_monotonic_nanos();
        for (;;) {
            if (atomic_load_explicit(&wait_impl->done, memory_order_acquire))
                break;
            mmapq_spin_hint();
            /*
             * Yield periodically: on a 1-vCPU/oversubscribed host a pure
             * busy-spin starves the very mapper thread we are waiting for
             * (13a pattern, await_mapped). A well-provisioned host never
             * reaches the threshold.
             */
            if (++spins >= 1000) {
                spins = 0;
                sched_yield();
                if (mmapq_monotonic_nanos() - start >= wait_budget_ns) {
                    /*
                     * CAND-24: the runtime thread is wedged or slow (the
                     * opposite ordering of C-02) — stop waiting. The
                     * registered dereg executable may still run on the
                     * live thread, so wait_impl is LEAKED, never freed
                     * here, and Steps 2-3 are skipped via mapper_detached
                     * exactly like the detached-runtime path below.
                     * Bounded leak on a degenerate path.
                     */
                    impl->mapper_detached = 1;
                    fprintf(stderr,
                        "mmapq region_ring: deregister wait timed out; leaking mapper state (runtime thread may still be live)\n");
                    return;
                }
            }
        }
        /* Mapper thread has confirmed it is done. mapping_impl is freed by
         * waiting_deregister_execute via target_ctx_to_free. */
        free(wait_impl);
    } else if (mmapqi_runtime_thread_detached(impl->runtime)) {
        /*
         * The runtime is closed AND its thread was detached after a join
         * timeout (C-02): the mapper thread may still be live, managing
         * our regions through mapping_impl. Freeing mapping_impl or
         * closing the regions/file mapper (Steps 2-3) would race it.
         * Leak instead: skip Steps 2-3, and mmapqi_region_ring_destroy
         * skips the region and file-mapper teardown on this path.
         * Bounded leak on a degenerate path; the thread exits once its
         * executable returns, or at process exit.
         */
        free(wait_impl);   /* never registered — the dereg exec cannot run */
        impl->mapper_detached = 1;
        return;
    } else if (!mmapq_runtime_is_closed(impl->runtime)) {
        /*
         * Register failed on ALLOCATION, not on a closed runtime (CAND-24;
         * the branch below once assumed a closed runtime was register's
         * only failure mode — stale since register's ENOMEM returns).
         * The dereg executable was never registered and the mapping
         * executable is still registered on the LIVE runtime thread:
         * freeing mapping_impl or closing the regions/file mapper would
         * be a use-after-free it can still reach. Take the leak path
         * exactly like a deadline expiry. Bounded leak on a degenerate
         * path.
         */
        free(wait_impl);   /* never registered — as above */
        impl->mapper_detached = 1;
        return;
    } else {
        /*
         * The runtime is already closed (the detached and alloc-failure
         * cases are handled above), so the dereg executable will never
         * run and waiting on it would spin forever (audit C-01). Reached
         * sequentially with a JOINED runtime thread, this means
         * mmapq_runtime_close() has returned after joining it, and the
         * shutdown drain has already run async_mapping_close() on our
         * mapping executable: no mapper can be running and the regions are
         * already UNMANAGED, so Steps 2-3 below are exactly as safe as on
         * the normal path. Free mapping_impl here, mirroring
         * waiting_deregister_execute's target_ctx_to_free cleanup.
         * Closing the runtime concurrently with a participant from another
         * thread remains a contract violation.
         */
        free(impl->mapping_impl);
        free(wait_impl);   /* never registered — as above */
    }
    impl->mapping_impl = NULL;

    /* Step 2: close regions — mapper thread is gone, self-drive is safe */
    for (int i = 0; i < self->ring_size; i++) {
        if (self->regions[i] != NULL) {
            region_close(self->regions[i]);
        }
    }

    /* Step 3: close file mapper — all regions are now unmapped */
    file_mapper_close(impl->file_mapper);
}

int mmapqi_region_ring_async_create(
    int         ring_size,
    int32_t     region_size,
    FileMapper *file_mapper,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    RegionRing **out)
{
    if (out == NULL) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    *out = NULL;
    if (file_mapper == NULL) return MMAPQ_EINVAL;
    if (runtime     == NULL) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    /* Consumed on every path, as in sync_create. */
    if (ring_size <= 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    if ((ring_size & (ring_size - 1)) != 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    if (region_size <= 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }
    if ((region_size & (region_size - 1)) != 0) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_EINVAL; }

    /*
     * Allocate everything first; register last. Registration is the
     * publication point: nothing may fail after it, and everything the
     * runtime thread can reach through mapping_impl — the regions array
     * and its region objects — must be fully initialized before
     * mmapq_runtime_register. Every failure path below also destroys the
     * mapper (consumed on any failure; the caller must not touch it after
     * this call).
     */
    Region **regions = calloc((size_t)ring_size, sizeof(Region *));
    if (regions == NULL) { mmapqi_file_mapper_destroy(file_mapper); return MMAPQ_ENOMEM; }

    for (int i = 0; i < ring_size; i++) {
        int rc = mmapqi_region_async_create(
            file_mapper,
            region_size,
            timeout_ns,
            REGION_MAX_FILE_NOT_FOUND_ERRORS,
            REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
            &regions[i]);
        if (rc != 0) {
            destroy_regions(regions, i);
            mmapqi_file_mapper_destroy(file_mapper);
            return rc;
        }
    }

    AsyncMappingImpl *mapping_impl = calloc(1, sizeof(AsyncMappingImpl));
    if (mapping_impl == NULL) {
        destroy_regions(regions, ring_size);
        mmapqi_file_mapper_destroy(file_mapper);
        return MMAPQ_ENOMEM;
    }
    mapping_impl->regions   = regions;
    mapping_impl->ring_size = ring_size;
    mapping_impl->exec.execute = async_mapping_execute;
    mapping_impl->exec.close   = async_mapping_close;
    mapping_impl->exec.ctx     = mapping_impl;

    AsyncRingImpl *impl = calloc(1, sizeof(AsyncRingImpl));
    if (impl == NULL) {
        free(mapping_impl);
        destroy_regions(regions, ring_size);
        mmapqi_file_mapper_destroy(file_mapper);
        return MMAPQ_ENOMEM;
    }
    impl->file_mapper   = file_mapper;
    impl->runtime       = runtime;
    impl->mapping_impl  = mapping_impl;
    impl->timeout_ns    = timeout_ns;
    atomic_store(&impl->closed, 0);

    RegionRing *ring = calloc(1, sizeof(RegionRing));
    if (ring == NULL) {
        free(impl); free(mapping_impl);
        destroy_regions(regions, ring_size);
        mmapqi_file_mapper_destroy(file_mapper);
        return MMAPQ_ENOMEM;
    }
    ring->close     = async_ring_close;
    ring->regions   = regions;
    ring->ring_size = ring_size;
    ring->is_async  = 1;
    ring->impl      = impl;

    /* The one failure possible after all allocations: tear down everything. */
    if (!mmapq_runtime_register(runtime, &mapping_impl->exec)) {
        free(impl); free(ring); free(mapping_impl);
        destroy_regions(regions, ring_size);
        mmapqi_file_mapper_destroy(file_mapper);
        return MMAPQ_ETIMEDOUT;
    }

    *out = ring;
    return 0;
}

/* ── Destroy ─────────────────────────────────────────────────────────────── */

void mmapqi_region_ring_destroy(RegionRing *ring)
{
    if (ring == NULL) return;

    /*
     * close() shuts down the async mapping executable (waiting for
     * deregistration) and calls file_mapper_close() on the ring's mapper.
     * We then call mmapqi_file_mapper_destroy() to free the mapper's memory.
     *
     * Ownership rule: FileMapper ownership is transferred to RegionRing
     * at region_ring_*_create(). The caller must not close or destroy
     * the FileMapper after passing it to RegionRing — on success the ring
     * owns it, and on any create failure the create has already destroyed
     * it. Consumption is total.
     */
    region_ring_close(ring);

    if (ring->is_async
        && ((AsyncRingImpl *)ring->impl)->mapper_detached) {
        /*
         * The runtime thread was detached before this ring closed
         * (async_ring_close above): it may still be managing the regions
         * through the leaked mapping_impl, and each region reaches the
         * file mapper. Leak the regions, the regions array, and the file
         * mapper with it — freeing them would be a use-after-free on the
         * live thread. Only impl/ring itself are safe to free; nothing
         * the detached thread can reach points at them.
         */
        free(ring->impl);
        ring->impl = NULL;
        free(ring);
        return;
    }

    for (int i = 0; i < ring->ring_size; i++) {
        if (ring->regions[i] != NULL) {
            mmapqi_region_destroy(ring->regions[i]);
            ring->regions[i] = NULL;
        }
    }
    free(ring->regions);
    ring->regions = NULL;

    if (ring->is_async) {
        AsyncRingImpl *impl = (AsyncRingImpl *)ring->impl;
        /*
         * mapping_impl was freed by region_ring_close — by the
         * deregistration executable on the runtime thread, or directly if
         * the runtime was already closed with its thread joined. (If the
         * thread was detached instead, the mapper_detached early return
         * above leaked it along with the regions.)
         * file_mapper is fully owned by this ring — destroy it now.
         */
        if (impl->file_mapper != NULL) {
            mmapqi_file_mapper_destroy(impl->file_mapper);
            impl->file_mapper = NULL;
        }
        free(impl);
    } else {
        SyncRingImpl *impl = (SyncRingImpl *)ring->impl;
        if (impl->file_mapper != NULL) {
            mmapqi_file_mapper_destroy(impl->file_mapper);
            impl->file_mapper = NULL;
        }
        free(impl);
    }

    ring->impl = NULL;
    free(ring);
}
