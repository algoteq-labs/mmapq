#ifndef MMAP_PLATFORM_H
#define MMAP_PLATFORM_H

/*
 * Return codes used across the queue API.
 *
 *   >= 0  success (index, count, etc.)
 *    -1   mapping timeout or I/O error (transient — retry)
 *    -2   invalid argument (NULL pointer, zero/negative length, etc.)
 */
#define MMAPQ_ETIMEDOUT  (-1)   /* mapping timeout or I/O — transient, retry */
#define MMAPQ_EINVAL     (-2)   /* invalid argument (NULL, zero length, etc.) */
#define MMAPQ_ECLOSED    (-3)   /* operation on closed handle */
#define MMAPQ_ECORRUPT   (-4)   /* on-disk corruption detected */
#define MMAPQ_ESCHEMA    (-5)   /* schema version or queue type mismatch */
#define MMAPQ_EEXIST     (-6)   /* destination queue already exists */
#define MMAPQ_ENOENT     (-7)   /* file or queue does not exist */
#define MMAPQ_EIDCONFLICT (-8)  /* requested appender ID already in use */

#if defined(__linux__) && !defined(_GNU_SOURCE)
#  define _GNU_SOURCE
#endif

/**
 * Platform detection and CPU hints.
 *
 * Supports Linux and macOS (including Apple Silicon / ARM64).
 *
 * _GNU_SOURCE is defined above on Linux to expose pthread_setname_np,
 * pthread_setaffinity_np and CPU_SET macros.
 *
 * CROSS-PROCESS ATOMICS:
 *   The queue uses lock-free atomics over MAP_SHARED mmap regions.
 *   This requires naturally-aligned lock-free operations, which are
 *   guaranteed on:
 *     Linux x86-64  (LOCK CMPXCHG)
 *     Linux ARM64   (LDAXR/STLXR, CASAL)
 *     macOS ARM64   (same as Linux ARM64 — same hardware)
 *     macOS x86-64  (Rosetta 2, same as x86-64)
 */

#if !defined(__linux__) && !defined(__APPLE__)
#  error "mmapq supports Linux and macOS only"
#endif

#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>

/**
 * huge_pages_hint — advise the kernel to use huge pages for a mapped region.
 *
 * Linux: uses madvise with MADV_HUGEPAGE. Requires Transparent Huge Pages (THP)
 *        to be enabled in the kernel (typically 'madvise' or 'always' mode).
 * macOS: no-op (not supported via standard madvise).
 */
static inline void huge_pages_hint(void *addr, size_t length)
{
#if defined(__linux__) && defined(MADV_HUGEPAGE)
    madvise(addr, length, MADV_HUGEPAGE);
#else
    (void)addr;
    (void)length;
#endif
}

/**
 * spin_hint — yield the CPU pipeline during a spin-wait loop.
 *
 * x86_64: PAUSE instruction.
 * ARM64:  YIELD instruction (same semantics, different mnemonic).
 */
static inline void spin_hint(void)
{
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#else
    __asm__ volatile("" ::: "memory");
#endif
}

/**
 * monotonic_nanos — current time in nanoseconds from a monotonic clock.
 *
 * Linux: CLOCK_MONOTONIC — uses VDSO for sub-100ns overhead.
 * macOS: CLOCK_MONOTONIC.
 */
static inline uint64_t monotonic_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

/**
 * memory_barrier — full compiler and hardware memory barrier.
 */
static inline void memory_barrier(void)
{
    __asm__ volatile("" ::: "memory");
}

/**
 * page_size — OS memory page size in bytes (cached after first call).
 *
 * Linux: typically 4096 bytes.
 * macOS ARM64 (M1/M2/M3): typically 16384 bytes.
 * All mmap region sizes must be multiples of this value.
 */
static inline long page_size(void)
{
    static long cached = 0;
    if (__builtin_expect(cached == 0, 0)) {
        cached = sysconf(_SC_PAGESIZE);
    }
    return cached;
}

/**
 * thread_set_name — set the name of a pthread (for debugger visibility).
 *
 * Linux: pthread_setname_np(thread, name) — sets another thread's name.
 *        Requires _GNU_SOURCE (set by build system on Linux).
 * macOS: pthread_setname_np(name) — sets only the CALLING thread's name.
 *        Must be called from within the thread itself; see runtime_thread_fn.
 */
static inline void thread_set_name(pthread_t thread, const char *name)
{
#if defined(__linux__)
    pthread_setname_np(thread, name);
#elif defined(__APPLE__)
    (void)thread;
    (void)name;
    /* macOS: thread sets its own name at startup inside runtime_thread_fn */
#endif
}

/**
 * thread_set_affinity_of — pin a specific thread to a specific CPU core.
 *
 * Linux: uses pthread_setaffinity_np.
 * macOS: no-op (not supported via standard API).
 */
static inline int thread_set_affinity_of(pthread_t thread, int cpu_core)
{
#if defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_core, &cpuset);
    return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
#else
    (void)thread;
    (void)cpu_core;
    return 0;
#endif
}

/**
 * thread_set_affinity — pin the current thread to a specific CPU core.
 */
static inline int thread_set_affinity(int cpu_core)
{
    return thread_set_affinity_of(pthread_self(), cpu_core);
}

#endif /* MMAP_PLATFORM_H */
