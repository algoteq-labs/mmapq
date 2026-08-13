#include "internal/region_ring.h"
#include "runtime.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <stdatomic.h>

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

int mmapqi_region_ring_sync_create(
    int         ring_size,
    int32_t     region_size,
    FileMapper *file_mapper,
    uint64_t    timeout_ns,
    RegionRing **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_mapper == NULL) return MMAPQ_EINVAL;
    if (ring_size <= 0) return MMAPQ_EINVAL;
    if ((ring_size & (ring_size - 1)) != 0) return MMAPQ_EINVAL;
    if (region_size <= 0) return MMAPQ_EINVAL;
    if ((region_size & (region_size - 1)) != 0) return MMAPQ_EINVAL;

    Region **regions = calloc((size_t)ring_size, sizeof(Region *));
    if (regions == NULL) return MMAPQ_ENOMEM;

    for (int i = 0; i < ring_size; i++) {
        int rc = mmapqi_region_sync_create(
            file_mapper,
            region_size,
            timeout_ns,
            REGION_MAX_FILE_NOT_FOUND_ERRORS,
            REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
            &regions[i]);
        if (rc != 0) {
            for (int j = 0; j < i; j++) mmapqi_region_destroy(regions[j]);
            free(regions);
            return rc;
        }
    }

    SyncRingImpl *impl = calloc(1, sizeof(SyncRingImpl));
    if (impl == NULL) { free(regions); return MMAPQ_ENOMEM; }
    impl->file_mapper = file_mapper;

    RegionRing *ring = calloc(1, sizeof(RegionRing));
    if (ring == NULL) { free(impl); free(regions); return MMAPQ_ENOMEM; }
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

typedef struct {
    FileMapper       *file_mapper;
    MmapqRuntime          *runtime;
    AsyncMappingImpl *mapping_impl;
    atomic_int        closed;
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
     * WaitingDeregisterImpl is stack-allocated — safe because we spin until
     * done==1 before leaving this scope.
     */
    WaitingDeregisterImpl wait_impl = {
        .runtime            = impl->runtime,
        .target             = &impl->mapping_impl->exec,
        .target_ctx_to_free = impl->mapping_impl,
    };
    atomic_store(&wait_impl.done, 0);
    wait_impl.self_exec.execute = waiting_deregister_execute;
    wait_impl.self_exec.close   = waiting_deregister_close;
    wait_impl.self_exec.ctx     = &wait_impl;

    mmapq_runtime_register(impl->runtime, &wait_impl.self_exec);

    while (!atomic_load_explicit(&wait_impl.done, memory_order_acquire)) {
        mmapq_spin_hint();
    }
    /* Mapper thread has confirmed it is done. mapping_impl is freed by
     * waiting_deregister_execute via target_ctx_to_free. */
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
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_mapper == NULL) return MMAPQ_EINVAL;
    if (runtime     == NULL) return MMAPQ_EINVAL;
    if (ring_size <= 0) return MMAPQ_EINVAL;
    if ((ring_size & (ring_size - 1)) != 0) return MMAPQ_EINVAL;
    if (region_size <= 0) return MMAPQ_EINVAL;
    if ((region_size & (region_size - 1)) != 0) return MMAPQ_EINVAL;

    /* Create regions */
    Region **regions = calloc((size_t)ring_size, sizeof(Region *));
    if (regions == NULL) return MMAPQ_ENOMEM;

    for (int i = 0; i < ring_size; i++) {
        int rc = mmapqi_region_async_create(
            file_mapper,
            region_size,
            timeout_ns,
            REGION_MAX_FILE_NOT_FOUND_ERRORS,
            REGION_MAX_BEYOND_FILE_SIZE_ERRORS,
            &regions[i]);
        if (rc != 0) {
            for (int j = 0; j < i; j++) mmapqi_region_destroy(regions[j]);
            free(regions);
            return rc;
        }
    }

    /* Create and register the AsyncMappingExecutable */
    AsyncMappingImpl *mapping_impl = calloc(1, sizeof(AsyncMappingImpl));
    if (mapping_impl == NULL) { free(regions); return MMAPQ_ENOMEM; }
    mapping_impl->regions   = regions;
    mapping_impl->ring_size = ring_size;
    mapping_impl->exec.execute = async_mapping_execute;
    mapping_impl->exec.close   = async_mapping_close;
    mapping_impl->exec.ctx     = mapping_impl;

    if (!mmapq_runtime_register(runtime, &mapping_impl->exec)) {
        free(mapping_impl);
        return MMAPQ_ETIMEDOUT;
    }

    /* Create async ring impl */
    AsyncRingImpl *impl = calloc(1, sizeof(AsyncRingImpl));
    if (impl == NULL) { free(mapping_impl); return MMAPQ_ENOMEM; }
    impl->file_mapper   = file_mapper;
    impl->runtime       = runtime;
    impl->mapping_impl  = mapping_impl;
    atomic_store(&impl->closed, 0);

    RegionRing *ring = calloc(1, sizeof(RegionRing));
    if (ring == NULL) { free(mapping_impl); free(impl); free(regions); return MMAPQ_ENOMEM; }
    ring->close     = async_ring_close;
    ring->regions   = regions;
    ring->ring_size = ring_size;
    ring->is_async  = 1;
    ring->impl      = impl;

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
     * the FileMapper after passing it to RegionRing.
     */
    region_ring_close(ring);

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
         * mapping_impl is freed by the DeregisterExecutable on the runtime
         * thread after unregistration (already done by region_ring_close).
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
