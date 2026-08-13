#ifndef MMAPQ_RUNTIME_H
#define MMAPQ_RUNTIME_H

#include <stdint.h>
#include <stddef.h>

/**
 * MmapqRuntime — a dedicated poll thread executing registered Executables.
 *
 * Mirrors Java IPCRuntime / IIPCRuntime exactly.
 *
 * A single MmapqRuntime thread iterates its list of Executables in a tight loop,
 * calling execute() on each in turn, accumulating work counts, and applying
 * an MmapqIdleStrategy at the end of each full iteration when no work was done.
 *
 * Threading contract (identical to Java):
 *   - register()   may be called from any thread.
 *   - unregister() MUST be called from the MmapqRuntime thread only.
 *     Cross-thread unregistration must be performed by registering a
 *     DeregisterExecutable that calls unregister() from within the poll loop.
 *     Enforced with MMAP_ASSERT.
 *
 * MmapqExecutable list modification:
 *   The internal list is a copy-on-write array. Structural modifications
 *   (register/unregister) produce a new array. The poll loop always
 *   iterates the current snapshot — modifications are safe from within
 *   the MmapqRuntime thread and visible on the next iteration.
 *
 * Shutdown:
 *   close() signals the MmapqRuntime thread to stop, waits up to close_timeout_ns
 *   for it to drain, then calls close() on all remaining Executables.
 */

/* ── MmapqExecutable ──────────────────────────────────────────────────────────── */

/**
 * MmapqExecutable — a unit of work polled by the MmapqRuntime.
 *
 * execute():
 *   Called once per MmapqRuntime iteration.
 *   Returns work done (0 = idle, >0 = useful work performed).
 *   Must not block. Must not call unregister() on a different MmapqExecutable.
 *   May call unregister(self) to deregister itself from within execute().
 *
 * close():
 *   Called when the MmapqRuntime is shutting down, or when execute() throws.
 *   Must release all resources. Will not be called again after this.
 *   May be called from the MmapqRuntime thread or the closing thread.
 *
 * ctx: opaque caller-supplied context pointer passed to both functions.
 */
typedef struct MmapqExecutable {
    int  (*execute)(struct MmapqExecutable *self);
    void (*close)  (struct MmapqExecutable *self);
    void *ctx;
} MmapqExecutable;

/* ── MmapqIdleStrategy ────────────────────────────────────────────────────────── */

/**
 * MmapqIdleStrategy — applied at the end of each MmapqRuntime iteration when idle.
 *
 * idle():
 *   Called with work_done = total work from the last iteration.
 *   work_done == 0 → no useful work, apply backoff.
 *   work_done  > 0 → reset to full speed.
 *
 * close(): release any resources held by the strategy.
 * ctx:     opaque state for the strategy implementation.
 */
typedef struct MmapqIdleStrategy {
    void (*idle) (struct MmapqIdleStrategy *self, int work_done);
    void (*close)(struct MmapqIdleStrategy *self);
    void *ctx;
} MmapqIdleStrategy;

/* Built-in idle strategies */

/**
 * Busy spin — never yields. Lowest latency, 100% CPU.
 * Appropriate for ultra-low-latency region mapping threads.
 * out: on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_idle_strategy_busy_spin_create(MmapqIdleStrategy **out);

/**
 * Yield — calls sched_yield() when idle.
 * Lower CPU than busy spin, slightly higher latency.
 * out: on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_idle_strategy_yield_create(MmapqIdleStrategy **out);

/**
 * Sleep — calls nanosleep() with increasing duration when idle.
 * min_sleep_ns: initial sleep duration
 * max_sleep_ns: maximum sleep duration (doubles each idle cycle)
 * out:          on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_idle_strategy_sleep_create(uint64_t min_sleep_ns, uint64_t max_sleep_ns, MmapqIdleStrategy **out);

/**
 * Backoff — progressive backoff: spin → yield → sleep.
 * Mirrors Java MmapqIdleStrategy.BACKOFF behaviour.
 * spin_count:   iterations before switching to yield
 * yield_count:  yield iterations before switching to sleep
 * max_sleep_ns: maximum sleep duration
 * out:          on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_idle_strategy_backoff_create(
    int      spin_count,
    int      yield_count,
    uint64_t max_sleep_ns,
    MmapqIdleStrategy **out);

void mmapq_idle_strategy_destroy(MmapqIdleStrategy *strategy);

/* ── MmapqRuntime ─────────────────────────────────────────────────────────────── */

typedef struct MmapqRuntime MmapqRuntime;

/**
 * Create a new MmapqRuntime.
 *
 * name:             thread name (appears in ps/top, max 15 chars on Linux)
 * idle:             idle strategy (MmapqRuntime takes ownership)
 * close_timeout_ns: how long close() waits for the thread to drain
 * on_close:         optional callback invoked after the MmapqRuntime thread stops
 *                   (may be NULL). Called from the thread calling close().
 *                   on_close_ctx: context passed to on_close.
 *
 * The MmapqRuntime thread starts immediately on creation.
 *
 * out: on success, set to the new MmapqRuntime (refcount 1); the caller owns
 *      one reference and must release it with mmapq_runtime_destroy().
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_runtime_create(
    const char   *name,
    MmapqIdleStrategy *idle,
    uint64_t      close_timeout_ns,
    void        (*on_close)(MmapqRuntime *rt, void *ctx),
    void         *on_close_ctx,
    MmapqRuntime     **out);

/**
 * Register an MmapqExecutable with the MmapqRuntime.
 *
 * May be called from any thread.
 * Returns 1 on success, 0 if the MmapqRuntime is already closed.
 *
 * The MmapqExecutable must remain valid until close() is called on it.
 * The MmapqRuntime does NOT take ownership of the MmapqExecutable pointer —
 * the caller manages the MmapqExecutable's lifetime.
 */
int mmapq_runtime_register(MmapqRuntime *rt, MmapqExecutable *exec);

/**
 * Unregister an MmapqExecutable from the MmapqRuntime.
 *
 * MUST be called from the MmapqRuntime thread only (MMAP_ASSERT enforced).
 * Cross-thread unregistration: register a DeregisterExecutable instead.
 *
 * Returns 1 on success, 0 if not found or MmapqRuntime is closed.
 */
int mmapq_runtime_unregister(MmapqRuntime *rt, MmapqExecutable *exec);

/**
 * mmapq_runtime_unregister_and_wait — safely unregister from any thread.
 *
 * May be called from any thread, including the calling thread.
 * Blocks until the runtime thread has finished executing the target
 * MmapqExecutable for the last time and removed it from the poll list.
 * After this returns, the caller may safely free exec and its ctx.
 *
 * Implementation: registers a small helper MmapqExecutable that performs the
 * unregistration from the runtime thread (satisfying the runtime-thread-only
 * constraint of mmapq_runtime_unregister), then signals completion via an atomic.
 * The calling thread spins with mmapq_spin_hint until the signal is set.
 *
 * This is the safe public API for cross-thread lifecycle management.
 * Prefer this over raw mmapq_runtime_unregister() for any external caller.
 *
 * Aborts if rt is closed or exec is not registered.
 */
void mmapq_runtime_unregister_and_wait(MmapqRuntime *rt, MmapqExecutable *exec);

/**
 * Pin the background thread of this MmapqRuntime to a specific CPU core.
 *
 * May be called from any thread. If called before the MmapqRuntime thread
 * has started, the affinity will be applied as soon as it starts.
 *
 * Returns 0 on success, or an error code.
 */
int mmapq_runtime_set_affinity(MmapqRuntime *rt, int cpu_core);

/**
 * Returns 1 if the MmapqRuntime is closed, 0 otherwise.
 */
int mmapq_runtime_is_closed(const MmapqRuntime *rt);

/**
 * Close the MmapqRuntime.
 *
 * Signals the MmapqRuntime thread to stop. Waits up to close_timeout_ns
 * for it to finish. Calls close() on all remaining Executables.
 * Calls on_close callback if provided.
 *
 * Safe to call from any thread. Idempotent.
 */
void mmapq_runtime_close(MmapqRuntime *rt);

/**
 * Clone a MmapqRuntime, incrementing its internal reference count.
 * The caller must call mmapq_runtime_destroy() to release the reference.
 * Safe to call from any thread.
 * out: on success, set to the same runtime pointer (refcount incremented)
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_runtime_clone(MmapqRuntime *rt, MmapqRuntime **out);

/**
 * Destroy the MmapqRuntime and free all memory.
 * Calls mmapq_runtime_close() if not already closed.
 * If the MmapqRuntime was cloned, only decrements the reference count;
 * the underlying MmapqRuntime is freed when the last reference is released.
 * After destroy(), the pointer must not be used.
 */
void mmapq_runtime_destroy(MmapqRuntime *rt);

/* ── DeregisterExecutable — cross-thread unregistration helper ───────────── */

/**
 * Create a one-shot MmapqExecutable that, when executed on the MmapqRuntime thread,
 * unregisters target from the MmapqRuntime and then unregisters itself.
 *
 * This is the correct pattern for cross-thread unregistration:
 *
 *   MmapqExecutable *dereg = mmapq_deregister_executable_create(runtime, target);
 *   mmapq_runtime_register(runtime, dereg);
 *   // On the next MmapqRuntime iteration, target and dereg are both removed.
 *
 * The DeregisterExecutable frees itself after execution.
 *
 * out: on success, set to the new executable
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_deregister_executable_create(
    MmapqRuntime *runtime, MmapqExecutable *target, MmapqExecutable **out);

/**
 * Like mmapq_deregister_executable_create() but also frees ctx_to_free after
 * unregistration. Used when the target's owning struct must be freed only
 * after the runtime thread has stopped executing it.
 *
 * out: on success, set to the new executable
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_deregister_executable_create_with_free(
    MmapqRuntime *runtime, MmapqExecutable *target, void *ctx_to_free, MmapqExecutable **out);

/* ── Default MmapqRuntime configuration ──────────────────────────────────────── */
#define MMAPQ_RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS  (10ULL * 1000000000ULL)  /* 10 seconds */

/* ── Singleton MmapqRuntime registry ──────────────────────────────────────────── */

/**
 * Well-known MmapqRuntime IDs — mirrors Java IPCRuntimes constants.
 */
#define MMAPQ_RUNTIME_ID_REGION_MAPPER       0
#define MMAPQ_RUNTIME_ID_SHARED_MEM_NOTIFIER 1
#define MMAPQ_RUNTIME_ID_METRICS_PROCESSOR   2

/**
 * Get or create a singleton MmapqRuntime by ID.
 *
 * If a MmapqRuntime with runtime_id already exists, returns it.
 * Otherwise creates a new one with the given name and idle strategy.
 *
 * When the MmapqRuntime is closed, it removes itself from the registry.
 * Thread-safe — uses a mutex for singleton creation.
 *
 * out: on success, set to the runtime (clone of the singleton; the caller
 *      owns one reference and must release it with mmapq_runtime_destroy()).
 *      If an existing singleton was found, the caller-supplied idle
 *      strategy is destroyed (never used).
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_runtime_get_instance(
    int           runtime_id,
    const char   *name,
    MmapqIdleStrategy *idle,
    uint64_t      close_timeout_ns,
    MmapqRuntime     **out);

/**
 * Get the singleton region mapper MmapqRuntime.
 * Creates it with backoff idle strategy if it does not exist.
 * out: on success, set to the region mapper runtime (caller-owned ref)
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_runtime_region_mapper(MmapqRuntime **out);

/**
 * Get the singleton metrics processor MmapqRuntime.
 * Creates it with backoff idle strategy if it does not exist.
 * out: on success, set to the metrics processor runtime (caller-owned ref)
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_runtime_metrics_processor(MmapqRuntime **out);

#endif /* MMAPQ_RUNTIME_H */
