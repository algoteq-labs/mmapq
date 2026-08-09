#ifndef RUNTIME_H
#define RUNTIME_H

#include <stdint.h>
#include <stddef.h>

/**
 * Runtime — a dedicated poll thread executing registered Executables.
 *
 * Mirrors Java IPCRuntime / IIPCRuntime exactly.
 *
 * A single Runtime thread iterates its list of Executables in a tight loop,
 * calling execute() on each in turn, accumulating work counts, and applying
 * an IdleStrategy at the end of each full iteration when no work was done.
 *
 * Threading contract (identical to Java):
 *   - register()   may be called from any thread.
 *   - unregister() MUST be called from the Runtime thread only.
 *     Cross-thread unregistration must be performed by registering a
 *     DeregisterExecutable that calls unregister() from within the poll loop.
 *     Enforced with MMAP_ASSERT.
 *
 * Executable list modification:
 *   The internal list is a copy-on-write array. Structural modifications
 *   (register/unregister) produce a new array. The poll loop always
 *   iterates the current snapshot — modifications are safe from within
 *   the Runtime thread and visible on the next iteration.
 *
 * Shutdown:
 *   close() signals the Runtime thread to stop, waits up to close_timeout_ns
 *   for it to drain, then calls close() on all remaining Executables.
 */

/* ── Executable ──────────────────────────────────────────────────────────── */

/**
 * Executable — a unit of work polled by the Runtime.
 *
 * execute():
 *   Called once per Runtime iteration.
 *   Returns work done (0 = idle, >0 = useful work performed).
 *   Must not block. Must not call unregister() on a different Executable.
 *   May call unregister(self) to deregister itself from within execute().
 *
 * close():
 *   Called when the Runtime is shutting down, or when execute() throws.
 *   Must release all resources. Will not be called again after this.
 *   May be called from the Runtime thread or the closing thread.
 *
 * ctx: opaque caller-supplied context pointer passed to both functions.
 */
typedef struct Executable {
    int  (*execute)(struct Executable *self);
    void (*close)  (struct Executable *self);
    void *ctx;
} Executable;

/* ── IdleStrategy ────────────────────────────────────────────────────────── */

/**
 * IdleStrategy — applied at the end of each Runtime iteration when idle.
 *
 * idle():
 *   Called with work_done = total work from the last iteration.
 *   work_done == 0 → no useful work, apply backoff.
 *   work_done  > 0 → reset to full speed.
 *
 * close(): release any resources held by the strategy.
 * ctx:     opaque state for the strategy implementation.
 */
typedef struct IdleStrategy {
    void (*idle) (struct IdleStrategy *self, int work_done);
    void (*close)(struct IdleStrategy *self);
    void *ctx;
} IdleStrategy;

/* Built-in idle strategies */

/**
 * Busy spin — never yields. Lowest latency, 100% CPU.
 * Appropriate for ultra-low-latency region mapping threads.
 * out: on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int idle_strategy_busy_spin_create(IdleStrategy **out);

/**
 * Yield — calls sched_yield() when idle.
 * Lower CPU than busy spin, slightly higher latency.
 * out: on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int idle_strategy_yield_create(IdleStrategy **out);

/**
 * Sleep — calls nanosleep() with increasing duration when idle.
 * min_sleep_ns: initial sleep duration
 * max_sleep_ns: maximum sleep duration (doubles each idle cycle)
 * out:          on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int idle_strategy_sleep_create(uint64_t min_sleep_ns, uint64_t max_sleep_ns, IdleStrategy **out);

/**
 * Backoff — progressive backoff: spin → yield → sleep.
 * Mirrors Java IdleStrategy.BACKOFF behaviour.
 * spin_count:   iterations before switching to yield
 * yield_count:  yield iterations before switching to sleep
 * max_sleep_ns: maximum sleep duration
 * out:          on success, set to the new idle strategy
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int idle_strategy_backoff_create(
    int      spin_count,
    int      yield_count,
    uint64_t max_sleep_ns,
    IdleStrategy **out);

void idle_strategy_destroy(IdleStrategy *strategy);

/* ── Runtime ─────────────────────────────────────────────────────────────── */

typedef struct Runtime Runtime;

/**
 * Create a new Runtime.
 *
 * name:             thread name (appears in ps/top, max 15 chars on Linux)
 * idle:             idle strategy (Runtime takes ownership)
 * close_timeout_ns: how long close() waits for the thread to drain
 * on_close:         optional callback invoked after the Runtime thread stops
 *                   (may be NULL). Called from the thread calling close().
 *                   on_close_ctx: context passed to on_close.
 *
 * The Runtime thread starts immediately on creation.
 *
 * out: on success, set to the new Runtime (refcount 1); the caller owns
 *      one reference and must release it with runtime_destroy().
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int runtime_create(
    const char   *name,
    IdleStrategy *idle,
    uint64_t      close_timeout_ns,
    void        (*on_close)(Runtime *rt, void *ctx),
    void         *on_close_ctx,
    Runtime     **out);

/**
 * Register an Executable with the Runtime.
 *
 * May be called from any thread.
 * Returns 1 on success, 0 if the Runtime is already closed.
 *
 * The Executable must remain valid until close() is called on it.
 * The Runtime does NOT take ownership of the Executable pointer —
 * the caller manages the Executable's lifetime.
 */
int runtime_register(Runtime *rt, Executable *exec);

/**
 * Unregister an Executable from the Runtime.
 *
 * MUST be called from the Runtime thread only (MMAP_ASSERT enforced).
 * Cross-thread unregistration: register a DeregisterExecutable instead.
 *
 * Returns 1 on success, 0 if not found or Runtime is closed.
 */
int runtime_unregister(Runtime *rt, Executable *exec);

/**
 * runtime_unregister_and_wait — safely unregister from any thread.
 *
 * May be called from any thread, including the calling thread.
 * Blocks until the runtime thread has finished executing the target
 * Executable for the last time and removed it from the poll list.
 * After this returns, the caller may safely free exec and its ctx.
 *
 * Implementation: registers a small helper Executable that performs the
 * unregistration from the runtime thread (satisfying the runtime-thread-only
 * constraint of runtime_unregister), then signals completion via an atomic.
 * The calling thread spins with spin_hint until the signal is set.
 *
 * This is the safe public API for cross-thread lifecycle management.
 * Prefer this over raw runtime_unregister() for any external caller.
 *
 * Aborts if rt is closed or exec is not registered.
 */
void runtime_unregister_and_wait(Runtime *rt, Executable *exec);

/**
 * Pin the background thread of this Runtime to a specific CPU core.
 *
 * May be called from any thread. If called before the Runtime thread
 * has started, the affinity will be applied as soon as it starts.
 *
 * Returns 0 on success, or an error code.
 */
int runtime_set_affinity(Runtime *rt, int cpu_core);

/**
 * Returns 1 if the Runtime is closed, 0 otherwise.
 */
int runtime_is_closed(const Runtime *rt);

/**
 * Close the Runtime.
 *
 * Signals the Runtime thread to stop. Waits up to close_timeout_ns
 * for it to finish. Calls close() on all remaining Executables.
 * Calls on_close callback if provided.
 *
 * Safe to call from any thread. Idempotent.
 */
void runtime_close(Runtime *rt);

/**
 * Clone a Runtime, incrementing its internal reference count.
 * The caller must call runtime_destroy() to release the reference.
 * Safe to call from any thread.
 * out: on success, set to the same runtime pointer (refcount incremented)
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int runtime_clone(Runtime *rt, Runtime **out);

/**
 * Destroy the Runtime and free all memory.
 * Calls runtime_close() if not already closed.
 * If the Runtime was cloned, only decrements the reference count;
 * the underlying Runtime is freed when the last reference is released.
 * After destroy(), the pointer must not be used.
 */
void runtime_destroy(Runtime *rt);

/* ── DeregisterExecutable — cross-thread unregistration helper ───────────── */

/**
 * Create a one-shot Executable that, when executed on the Runtime thread,
 * unregisters target from the Runtime and then unregisters itself.
 *
 * This is the correct pattern for cross-thread unregistration:
 *
 *   Executable *dereg = deregister_executable_create(runtime, target);
 *   runtime_register(runtime, dereg);
 *   // On the next Runtime iteration, target and dereg are both removed.
 *
 * The DeregisterExecutable frees itself after execution.
 *
 * out: on success, set to the new executable
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int deregister_executable_create(
    Runtime *runtime, Executable *target, Executable **out);

/**
 * Like deregister_executable_create() but also frees ctx_to_free after
 * unregistration. Used when the target's owning struct must be freed only
 * after the runtime thread has stopped executing it.
 *
 * out: on success, set to the new executable
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int deregister_executable_create_with_free(
    Runtime *runtime, Executable *target, void *ctx_to_free, Executable **out);

/* ── Default Runtime configuration ──────────────────────────────────────── */
#define RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS  (10ULL * 1000000000ULL)  /* 10 seconds */

/* ── Singleton Runtime registry ──────────────────────────────────────────── */

/**
 * Well-known Runtime IDs — mirrors Java IPCRuntimes constants.
 */
#define RUNTIME_ID_REGION_MAPPER       0
#define RUNTIME_ID_SHARED_MEM_NOTIFIER 1
#define RUNTIME_ID_METRICS_PROCESSOR   2

/**
 * Get or create a singleton Runtime by ID.
 *
 * If a Runtime with runtime_id already exists, returns it.
 * Otherwise creates a new one with the given name and idle strategy.
 *
 * When the Runtime is closed, it removes itself from the registry.
 * Thread-safe — uses a mutex for singleton creation.
 *
 * out: on success, set to the runtime (clone of the singleton; the caller
 *      owns one reference and must release it with runtime_destroy()).
 *      If an existing singleton was found, the caller-supplied idle
 *      strategy is destroyed (never used).
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int runtime_get_instance(
    int           runtime_id,
    const char   *name,
    IdleStrategy *idle,
    uint64_t      close_timeout_ns,
    Runtime     **out);

/**
 * Get the singleton region mapper Runtime.
 * Creates it with backoff idle strategy if it does not exist.
 * out: on success, set to the region mapper runtime (caller-owned ref)
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int runtime_region_mapper(Runtime **out);

/**
 * Get the singleton metrics processor Runtime.
 * Creates it with backoff idle strategy if it does not exist.
 * out: on success, set to the metrics processor runtime (caller-owned ref)
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int runtime_metrics_processor(Runtime **out);

#endif /* RUNTIME_H */
