#include "runtime.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>
#include <time.h>

/* ═══════════════════════════════════════════════════════════════════════════
 * IdleStrategy implementations
 * ═══════════════════════════════════════════════════════════════════════════ */

/* ── Busy spin ───────────────────────────────────────────────────────────── */

static void busy_spin_idle(IdleStrategy *self, int work_done)
{
    (void)self;
    (void)work_done;
    spin_hint();
}

static void idle_noop_close(IdleStrategy *self) { (void)self; }

IdleStrategy *idle_strategy_busy_spin_create(void)
{
    IdleStrategy *s = calloc(1, sizeof(IdleStrategy));
    MMAP_ASSERT(s != NULL, "idle_strategy_busy_spin_create: out of memory");
    s->idle  = busy_spin_idle;
    s->close = idle_noop_close;
    s->ctx   = NULL;
    return s;
}

/* ── Yield ───────────────────────────────────────────────────────────────── */

static void yield_idle(IdleStrategy *self, int work_done)
{
    (void)self;
    if (work_done == 0) {
        sched_yield();
    }
}

IdleStrategy *idle_strategy_yield_create(void)
{
    IdleStrategy *s = calloc(1, sizeof(IdleStrategy));
    MMAP_ASSERT(s != NULL, "idle_strategy_yield_create: out of memory");
    s->idle  = yield_idle;
    s->close = idle_noop_close;
    s->ctx   = NULL;
    return s;
}

/* ── Sleep ───────────────────────────────────────────────────────────────── */

typedef struct {
    uint64_t min_sleep_ns;
    uint64_t max_sleep_ns;
    uint64_t current_sleep_ns;
} SleepCtx;

static void sleep_idle(IdleStrategy *self, int work_done)
{
    SleepCtx *ctx = (SleepCtx *)self->ctx;
    if (work_done > 0) {
        ctx->current_sleep_ns = ctx->min_sleep_ns;
        return;
    }
    struct timespec ts = {
        .tv_sec  = (time_t)(ctx->current_sleep_ns / UINT64_C(1000000000)),
        .tv_nsec = (long)  (ctx->current_sleep_ns % UINT64_C(1000000000)),
    };
    nanosleep(&ts, NULL);

    ctx->current_sleep_ns *= 2;
    if (ctx->current_sleep_ns > ctx->max_sleep_ns) {
        ctx->current_sleep_ns = ctx->max_sleep_ns;
    }
}

static void sleep_close(IdleStrategy *self)
{
    free(self->ctx);
}

IdleStrategy *idle_strategy_sleep_create(uint64_t min_sleep_ns, uint64_t max_sleep_ns)
{
    MMAP_ASSERT(min_sleep_ns > 0,              "idle_strategy_sleep_create: min_sleep_ns must be > 0");
    MMAP_ASSERT(max_sleep_ns >= min_sleep_ns,  "idle_strategy_sleep_create: max >= min required");

    SleepCtx *ctx = calloc(1, sizeof(SleepCtx));
    MMAP_ASSERT(ctx != NULL, "idle_strategy_sleep_create: out of memory");
    ctx->min_sleep_ns     = min_sleep_ns;
    ctx->max_sleep_ns     = max_sleep_ns;
    ctx->current_sleep_ns = min_sleep_ns;

    IdleStrategy *s = calloc(1, sizeof(IdleStrategy));
    MMAP_ASSERT(s != NULL, "idle_strategy_sleep_create: out of memory");
    s->idle  = sleep_idle;
    s->close = sleep_close;
    s->ctx   = ctx;
    return s;
}

/* ── Backoff — spin → yield → sleep ─────────────────────────────────────── */

typedef struct {
    int      spin_count;
    int      yield_count;
    uint64_t max_sleep_ns;
    int      spin_counter;
    int      yield_counter;
    uint64_t sleep_ns;
} BackoffCtx;

static void backoff_idle(IdleStrategy *self, int work_done)
{
    BackoffCtx *ctx = (BackoffCtx *)self->ctx;
    if (work_done > 0) {
        ctx->spin_counter  = 0;
        ctx->yield_counter = 0;
        ctx->sleep_ns      = UINT64_C(1000);  /* reset to 1µs */
        return;
    }

    if (ctx->spin_counter < ctx->spin_count) {
        ctx->spin_counter++;
        spin_hint();
        return;
    }
    if (ctx->yield_counter < ctx->yield_count) {
        ctx->yield_counter++;
        sched_yield();
        return;
    }

    /* Sleep phase */
    struct timespec ts = {
        .tv_sec  = (time_t)(ctx->sleep_ns / UINT64_C(1000000000)),
        .tv_nsec = (long)  (ctx->sleep_ns % UINT64_C(1000000000)),
    };
    nanosleep(&ts, NULL);

    ctx->sleep_ns *= 2;
    if (ctx->sleep_ns > ctx->max_sleep_ns) {
        ctx->sleep_ns = ctx->max_sleep_ns;
    }
}

static void backoff_close(IdleStrategy *self)
{
    free(self->ctx);
}

IdleStrategy *idle_strategy_backoff_create(
    int      spin_count,
    int      yield_count,
    uint64_t max_sleep_ns)
{
    BackoffCtx *ctx = calloc(1, sizeof(BackoffCtx));
    MMAP_ASSERT(ctx != NULL, "idle_strategy_backoff_create: out of memory");
    ctx->spin_count   = spin_count;
    ctx->yield_count  = yield_count;
    ctx->max_sleep_ns = max_sleep_ns;
    ctx->spin_counter = 0;
    ctx->yield_counter= 0;
    ctx->sleep_ns     = UINT64_C(1000);

    IdleStrategy *s = calloc(1, sizeof(IdleStrategy));
    MMAP_ASSERT(s != NULL, "idle_strategy_backoff_create: out of memory");
    s->idle  = backoff_idle;
    s->close = backoff_close;
    s->ctx   = ctx;
    return s;
}

void idle_strategy_destroy(IdleStrategy *strategy)
{
    if (strategy == NULL) return;
    if (strategy->close) strategy->close(strategy);
    free(strategy);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Copy-on-write executable list
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * ExecList — immutable snapshot of registered executables.
 *
 * The runtime thread always iterates the current snapshot.
 * register()/unregister() produce a new snapshot atomically.
 * The runtime thread loads the current snapshot pointer at the start
 * of each iteration — it sees the latest registration without locking.
 */
typedef struct ExecList {
    Executable **items;
    int          count;
    int          capacity;
} ExecList;

static ExecList *exec_list_create(int capacity)
{
    ExecList *list = calloc(1, sizeof(ExecList));
    MMAP_ASSERT(list != NULL, "exec_list_create: out of memory");
    list->items    = calloc((size_t)capacity, sizeof(Executable *));
    list->count    = 0;
    list->capacity = capacity;
    return list;
}

static ExecList *exec_list_add(ExecList *old, Executable *exec)
{
    int new_cap = old->capacity + 8;
    ExecList *next = calloc(1, sizeof(ExecList));
    MMAP_ASSERT(next != NULL, "exec_list_add: out of memory");
    next->items    = calloc((size_t)new_cap, sizeof(Executable *));
    next->count    = old->count + 1;
    next->capacity = new_cap;
    memcpy(next->items, old->items, (size_t)old->count * sizeof(Executable *));
    next->items[old->count] = exec;
    return next;
}

static ExecList *exec_list_remove(ExecList *old, Executable *exec)
{
    ExecList *next = calloc(1, sizeof(ExecList));
    MMAP_ASSERT(next != NULL, "exec_list_remove: out of memory");
    next->items    = calloc((size_t)old->capacity, sizeof(Executable *));
    next->count    = 0;
    next->capacity = old->capacity;
    for (int i = 0; i < old->count; i++) {
        if (old->items[i] != exec) {
            next->items[next->count++] = old->items[i];
        }
    }
    return next;
}

static void exec_list_free(ExecList *list)
{
    if (list == NULL) return;
    free(list->items);
    free(list);
}

/* ── Deferred free list ──────────────────────────────────────────────────── */
/*
 * FreeNode — a node in the singly-linked list of ExecLists waiting to be
 * freed by the runtime thread. Application threads push onto the head;
 * the runtime thread atomically swaps the whole list and frees it.
 */
typedef struct FreeNode {
    ExecList        *list;
    struct FreeNode *next;
} FreeNode;



/* ═══════════════════════════════════════════════════════════════════════════
 * Runtime
 * ═══════════════════════════════════════════════════════════════════════════ */

struct Runtime {
    char         name[16];
    IdleStrategy *idle;
    uint64_t      close_timeout_ns;
    void        (*on_close)(Runtime *rt, void *ctx);
    void         *on_close_ctx;

    _Atomic(ExecList *) exec_list;
    atomic_int          closed;

    pthread_t    thread;
    pthread_mutex_t register_mutex;

    pthread_t    runtime_thread_id;
    atomic_int   thread_started;

    pthread_mutex_t close_mutex;
    pthread_cond_t  close_cond;
    atomic_int      thread_done;

    /*
     * free_list: atomic singly-linked list of ExecLists to free.
     * Application and runtime threads push onto the head lock-free.
     * The runtime thread atomically swaps the head to NULL and frees
     * the entire chain at the top of each iteration — after it has
     * finished iterating the previous list snapshot.
     */
    _Atomic(FreeNode *) free_list;
};

/* ── Deferred free helpers ───────────────────────────────────────────────── */

static void push_to_free(Runtime *rt, ExecList *old)
{
    if (old == NULL) return;
    FreeNode *node = calloc(1, sizeof(FreeNode));
    if (node == NULL) { exec_list_free(old); return; }
    node->list = old;
    FreeNode *head;
    do {
        head = atomic_load_explicit(&rt->free_list, memory_order_relaxed);
        node->next = head;
    } while (!atomic_compare_exchange_weak_explicit(
                 &rt->free_list, &head, node,
                 memory_order_release, memory_order_relaxed));
}

static void drain_free_list(Runtime *rt)
{
    FreeNode *head = atomic_exchange_explicit(&rt->free_list, NULL, memory_order_acquire);
    while (head != NULL) {
        FreeNode *next = head->next;
        exec_list_free(head->list);
        free(head);
        head = next;
    }
}

/* ── Poll thread ─────────────────────────────────────────────────────────── */

static void *runtime_thread_fn(void *arg)
{
    Runtime *rt = (Runtime *)arg;

    /*
     * Set thread name from within the thread itself.
     * On macOS, pthread_setname_np only sets the calling thread's name,
     * so we must call it here. On Linux, thread_set_name() from the
     * creator thread is sufficient, but calling it here too is harmless.
     */
#if defined(__APPLE__)
    pthread_setname_np(rt->name);
#endif

    atomic_store(&rt->thread_started, 1);
    rt->runtime_thread_id = pthread_self();

    while (!atomic_load(&rt->closed)) {
        /* Drain superseded ExecLists queued by register/unregister calls.
         * Safe: the previous iteration has finished executing all items in
         * the old list snapshot, so no thread is iterating those items now. */
        drain_free_list(rt);

        /*
         * Load the current list snapshot at the top of each iteration.
         * execute() calls may modify the list (via runtime_unregister),
         * so we capture count and items upfront and iterate the snapshot.
         * After execute() the list pointer in rt->exec_list may have changed,
         * but our local 'list' pointer is still valid — runtime_unregister
         * pushes the old list onto free_list, which drain_free_list() will
         * collect at the top of the NEXT iteration, after we are done with it.
         *
         * INVARIANT: runtime_unregister() must only be called at the top
         * level of execute(), never while we are mid-iteration below.
         * The deregister executable satisfies this — it calls unregister
         * and returns immediately, so no nested iteration occurs.
         */
        ExecList *list      = atomic_load_explicit(&rt->exec_list, memory_order_acquire);
        int       count     = list->count;
        int       work_count = 0;

        for (int i = 0; i < count; i++) {
            Executable *exec = list->items[i];
            int work = exec->execute(exec);
            if (work < 0) {
                exec->close(exec);
                runtime_unregister(rt, exec);
                break;  /* list changed — restart on next outer iteration */
            }
            work_count += work;
        }

        rt->idle->idle(rt->idle, work_count);
    }

    /* Drain: close all remaining executables */
    ExecList *list = atomic_load_explicit(&rt->exec_list, memory_order_acquire);
    for (int i = 0; i < list->count; i++) {
        list->items[i]->close(list->items[i]);
    }

    pthread_mutex_lock(&rt->close_mutex);
    atomic_store(&rt->thread_done, 1);
    pthread_cond_signal(&rt->close_cond);
    pthread_mutex_unlock(&rt->close_mutex);

    return NULL;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

Runtime *runtime_create(
    const char   *name,
    IdleStrategy *idle,
    uint64_t      close_timeout_ns,
    void        (*on_close)(Runtime *rt, void *ctx),
    void         *on_close_ctx)
{
    MMAP_ASSERT(name != NULL, "runtime_create: name is NULL");
    MMAP_ASSERT(idle != NULL, "runtime_create: idle is NULL");

    Runtime *rt = calloc(1, sizeof(Runtime));
    MMAP_ASSERT(rt != NULL, "runtime_create: out of memory");

    strncpy(rt->name, name, sizeof(rt->name) - 1);
    rt->idle             = idle;
    rt->close_timeout_ns = close_timeout_ns;
    rt->on_close         = on_close;
    rt->on_close_ctx     = on_close_ctx;

    atomic_store(&rt->closed,       0);
    atomic_store(&rt->thread_done,  0);
    atomic_store(&rt->thread_started, 0);
    atomic_store(&rt->free_list, NULL);

    ExecList *initial = exec_list_create(8);
    atomic_store_explicit(&rt->exec_list, initial, memory_order_relaxed);

    pthread_mutex_init(&rt->register_mutex, NULL);
    pthread_mutex_init(&rt->close_mutex,    NULL);
    pthread_cond_init (&rt->close_cond,     NULL);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&rt->thread, &attr, runtime_thread_fn, rt);
    pthread_attr_destroy(&attr);

    /* Set thread name for debugger visibility (macOS: thread sets its own name) */
    thread_set_name(rt->thread, rt->name);

    /* Wait for thread to start so runtime_thread_id is set */
    while (!atomic_load(&rt->thread_started)) {
        spin_hint();
    }

    return rt;
}

/* ── register ────────────────────────────────────────────────────────────── */

int runtime_register(Runtime *rt, Executable *exec)
{
    MMAP_ASSERT(rt   != NULL, "runtime_register: rt is NULL");
    MMAP_ASSERT(exec != NULL, "runtime_register: exec is NULL");

    if (atomic_load(&rt->closed)) {
        return 0;
    }

    pthread_mutex_lock(&rt->register_mutex);

    ExecList *old  = atomic_load_explicit(&rt->exec_list, memory_order_acquire);
    ExecList *next = exec_list_add(old, exec);
    atomic_store_explicit(&rt->exec_list, next, memory_order_release);

    /*
     * Push old onto the runtime thread's free_list. The runtime thread drains
     * free_list at the top of each iteration, after finishing the previous
     * iteration, guaranteeing old is no longer being iterated when freed.
     */
    push_to_free(rt, old);

    pthread_mutex_unlock(&rt->register_mutex);
    return 1;
}

/* ── unregister — MUST be called from Runtime thread ─────────────────────── */

int runtime_unregister(Runtime *rt, Executable *exec)
{
    MMAP_ASSERT(rt   != NULL, "runtime_unregister: rt is NULL");
    MMAP_ASSERT(exec != NULL, "runtime_unregister: exec is NULL");

    /* Threading contract enforcement */
    MMAP_ASSERT(pthread_equal(pthread_self(), rt->runtime_thread_id),
        "runtime_unregister: must be called from the Runtime thread. "
        "Use deregister_executable_create() for cross-thread unregistration.");

    if (atomic_load(&rt->closed)) {
        return 0;
    }

    pthread_mutex_lock(&rt->register_mutex);

    ExecList *old  = atomic_load_explicit(&rt->exec_list, memory_order_acquire);
    ExecList *next = exec_list_remove(old, exec);
    atomic_store_explicit(&rt->exec_list, next, memory_order_release);

    /*
     * Push old onto free_list. Called from the runtime thread — we have
     * already broken out of the iteration loop (work < 0), so old is no
     * longer being iterated. The runtime thread drains free_list at the
     * top of its next outer iteration.
     */
    push_to_free(rt, old);

    pthread_mutex_unlock(&rt->register_mutex);
    return 1;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Runtime affinity helper
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    int        cpu_core;
    Executable self_exec;
} AffinityImpl;

static int affinity_execute(Executable *self)
{
    AffinityImpl *impl = (AffinityImpl *)self->ctx;
    thread_set_affinity(impl->cpu_core);
    free(impl);
    return -1; /* unregister self after one-shot */
}

static void affinity_close(Executable *self)
{
    (void)self;
}

int runtime_set_affinity(Runtime *rt, int cpu_core)
{
    MMAP_ASSERT(rt != NULL, "runtime_set_affinity: rt is NULL");

    AffinityImpl *impl = calloc(1, sizeof(AffinityImpl));
    if (impl == NULL) return -1;

    impl->cpu_core          = cpu_core;
    impl->self_exec.execute = affinity_execute;
    impl->self_exec.close   = affinity_close;
    impl->self_exec.ctx     = impl;

    if (!runtime_register(rt, &impl->self_exec)) {
        free(impl);
        return -1;
    }

    return 0;
}

/* ── close ───────────────────────────────────────────────────────────────── */

int runtime_is_closed(const Runtime *rt)
{
    MMAP_ASSERT(rt != NULL, "runtime_is_closed: rt is NULL");
    return atomic_load(&rt->closed);
}

void runtime_close(Runtime *rt)
{
    MMAP_ASSERT(rt != NULL, "runtime_close: rt is NULL");

    if (!atomic_compare_exchange_strong(&rt->closed,
                                         &(int){0}, 1)) {
        return;  /* already closed */
    }

    /* Wait for runtime thread to finish */
    pthread_mutex_lock(&rt->close_mutex);
    uint64_t deadline = monotonic_nanos() + rt->close_timeout_ns;

    while (!atomic_load(&rt->thread_done)) {
        uint64_t now = monotonic_nanos();
        if (now >= deadline) {
            fprintf(stderr, "runtime_close: timeout waiting for thread '%s' to stop\n",
                    rt->name);
            break;
        }
        uint64_t remaining_ns = deadline - now;
        struct timespec ts = {
            .tv_sec  = (time_t)(remaining_ns / UINT64_C(1000000000)),
            .tv_nsec = (long)  (remaining_ns % UINT64_C(1000000000)),
        };
        pthread_cond_timedwait(&rt->close_cond, &rt->close_mutex, &ts);
    }
    pthread_mutex_unlock(&rt->close_mutex);

    pthread_join(rt->thread, NULL);

    if (rt->on_close != NULL) {
        rt->on_close(rt, rt->on_close_ctx);
    }
}

void runtime_destroy(Runtime *rt)
{
    if (rt == NULL) return;
    runtime_close(rt);

    ExecList *list = atomic_load(&rt->exec_list);
    exec_list_free(list);
    drain_free_list(rt);

    idle_strategy_destroy(rt->idle);

    pthread_mutex_destroy(&rt->register_mutex);
    pthread_mutex_destroy(&rt->close_mutex);
    pthread_cond_destroy (&rt->close_cond);

    free(rt);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * DeregisterExecutable — cross-thread unregistration helper
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    Runtime    *runtime;
    Executable *target;
    void       *target_ctx_to_free;  /* if non-NULL, freed after unregistration */
    Executable  self_exec;
} DeregisterImpl;

static int deregister_execute(Executable *self)
{
    DeregisterImpl *impl = (DeregisterImpl *)self->ctx;
    runtime_unregister(impl->runtime, impl->target);
    runtime_unregister(impl->runtime, &impl->self_exec);
    if (impl->target_ctx_to_free) {
        free(impl->target_ctx_to_free);
    }
    free(impl);
    return 1;
}

static void deregister_close(Executable *self)
{
    DeregisterImpl *impl = (DeregisterImpl *)self->ctx;
    if (impl->target_ctx_to_free) {
        free(impl->target_ctx_to_free);
    }
    free(impl);
}

Executable *deregister_executable_create(Runtime *runtime, Executable *target)
{
    return deregister_executable_create_with_free(runtime, target, NULL);
}

Executable *deregister_executable_create_with_free(
    Runtime *runtime, Executable *target, void *ctx_to_free)
{
    MMAP_ASSERT(runtime != NULL, "deregister_executable_create: runtime is NULL");
    MMAP_ASSERT(target  != NULL, "deregister_executable_create: target is NULL");

    DeregisterImpl *impl = calloc(1, sizeof(DeregisterImpl));
    MMAP_ASSERT(impl != NULL, "deregister_executable_create: out of memory");

    impl->runtime              = runtime;
    impl->target               = target;
    impl->target_ctx_to_free   = ctx_to_free;
    impl->self_exec.execute    = deregister_execute;
    impl->self_exec.close      = deregister_close;
    impl->self_exec.ctx        = impl;

    return &impl->self_exec;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * runtime_unregister_and_wait — safe cross-thread unregistration
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    Runtime    *runtime;
    Executable *target;
    atomic_int  done;     /* set to 1 by runtime thread after unregistration */
    Executable  self_exec;
} UnregisterWaitImpl;

static int unregister_wait_execute(Executable *self)
{
    UnregisterWaitImpl *impl = (UnregisterWaitImpl *)self->ctx;

    /* Unregister the target — we are on the runtime thread so this is safe */
    runtime_unregister(impl->runtime, impl->target);

    /* Unregister ourselves */
    runtime_unregister(impl->runtime, &impl->self_exec);

    /* Signal the waiting caller thread */
    atomic_store_explicit(&impl->done, 1, memory_order_release);

    return 1;
}

static void unregister_wait_close(Executable *self)
{
    (void)self;  /* impl is stack/caller-owned — do not free */
}

void runtime_unregister_and_wait(Runtime *rt, Executable *exec)
{
    MMAP_ASSERT(rt   != NULL, "runtime_unregister_and_wait: rt is NULL");
    MMAP_ASSERT(exec != NULL, "runtime_unregister_and_wait: exec is NULL");
    MMAP_ASSERT(!runtime_is_closed(rt),
        "runtime_unregister_and_wait: runtime is already closed");

    /*
     * Stack-allocate the helper — it must stay valid until done=1.
     * The runtime thread sets done=1 as its last act before returning from
     * unregister_wait_execute, so by the time this function returns the
     * helper is no longer referenced by the runtime.
     */
    UnregisterWaitImpl impl;
    impl.runtime              = rt;
    impl.target               = exec;
    atomic_store(&impl.done,  0);
    impl.self_exec.execute    = unregister_wait_execute;
    impl.self_exec.close      = unregister_wait_close;
    impl.self_exec.ctx        = &impl;

    /* Register the helper — it will run on the runtime thread */
    int ok = runtime_register(rt, &impl.self_exec);
    (void)ok;
    MMAP_ASSERT(ok, "runtime_unregister_and_wait: failed to register helper");

    /* Spin until the runtime thread signals completion */
    while (!atomic_load_explicit(&impl.done, memory_order_acquire)) {
        spin_hint();
    }

    /* impl is stack-allocated — no free needed */
}

#define MAX_RUNTIME_INSTANCES 16

typedef struct {
    int      runtime_id;
    Runtime *runtime;
} RuntimeEntry;

static RuntimeEntry   registry[MAX_RUNTIME_INSTANCES];
static int            registry_count = 0;
static pthread_mutex_t registry_mutex = PTHREAD_MUTEX_INITIALIZER;

static void registry_on_close(Runtime *rt, void *ctx)
{
    int *runtime_id = (int *)ctx;
    pthread_mutex_lock(&registry_mutex);
    for (int i = 0; i < registry_count; i++) {
        if (registry[i].runtime_id == *runtime_id && registry[i].runtime == rt) {
            /* Compact */
            registry[i] = registry[--registry_count];
            break;
        }
    }
    pthread_mutex_unlock(&registry_mutex);
    free(runtime_id);
}

Runtime *runtime_get_instance(
    int           runtime_id,
    const char   *name,
    IdleStrategy *idle,
    uint64_t      close_timeout_ns)
{
    pthread_mutex_lock(&registry_mutex);

    for (int i = 0; i < registry_count; i++) {
        if (registry[i].runtime_id == runtime_id) {
            Runtime *existing = registry[i].runtime;
            pthread_mutex_unlock(&registry_mutex);
            /* Caller provided idle but won't be used — destroy it */
            idle_strategy_destroy(idle);
            return existing;
        }
    }

    MMAP_ASSERT(registry_count < MAX_RUNTIME_INSTANCES,
        "runtime_get_instance: maximum runtime instances reached");

    int *id_copy = malloc(sizeof(int));
    MMAP_ASSERT(id_copy != NULL, "runtime_get_instance: out of memory");
    *id_copy = runtime_id;

    Runtime *rt = runtime_create(name, idle, close_timeout_ns,
                                  registry_on_close, id_copy);

    registry[registry_count].runtime_id = runtime_id;
    registry[registry_count].runtime    = rt;
    registry_count++;

    pthread_mutex_unlock(&registry_mutex);
    return rt;
}

Runtime *runtime_region_mapper(void)
{
    return runtime_get_instance(
        RUNTIME_ID_REGION_MAPPER,
        "region-mapper",
        idle_strategy_backoff_create(100, 10, UINT64_C(1000000)),
        RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS);
}

Runtime *runtime_metrics_processor(void)
{
    return runtime_get_instance(
        RUNTIME_ID_METRICS_PROCESSOR,
        "metrics-proc",
        idle_strategy_backoff_create(100, 100, UINT64_C(1000000)),
        RUNTIME_DEFAULT_CLOSE_TIMEOUT_NS);
}
