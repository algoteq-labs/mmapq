#ifndef MMAPQ_MMAP_PLATFORM_H
#define MMAPQ_MMAP_PLATFORM_H

/*
 * Return codes used across the queue API.
 *
 *   >= 0  success (index, count, etc.)
 *    -1   MMAPQ_ETIMEDOUT   — mapping timeout or I/O (transient, retry)
 *    -2   MMAPQ_EINVAL      — invalid argument (NULL, zero length, etc.)
 *    -3   MMAPQ_ECLOSED     — operation on closed handle
 *    -4   MMAPQ_ECORRUPT    — on-disk corruption detected
 *    -5   MMAPQ_ESCHEMA     — schema version or queue type mismatch, or
 *                             on-disk geometry not supported by this build/host
 *                             (CAND-38: pre-rule geometry or a page-size mismatch)
 *    -6   MMAPQ_EEXIST      — destination queue already exists
 *    -7   MMAPQ_ENOENT      — file or queue does not exist
 *    -8   MMAPQ_EIDCONFLICT — requested appender ID already in use
 *    -9   MMAPQ_ENOMEM      — internal allocation failure (back off, do not retry hot-loop)
 *   -10   MMAPQ_EIO         — I/O error (metadata file write failure, retry)
 *   -11   MMAPQ_ENOTREADY   — meta file exists but not yet fully initialised (transient, retry)
 *   -12   MMAPQ_ECALLBACK   — appender-ID mapper returned MMAPQ_APPENDER_ID_ABORT; transfer stopped after cleanup
 */
#define MMAPQ_ETIMEDOUT  (-1)   /* mapping timeout or I/O — transient, retry */
#define MMAPQ_EINVAL     (-2)   /* invalid argument (NULL, zero length, etc.) */
#define MMAPQ_ECLOSED    (-3)   /* operation on closed handle */
#define MMAPQ_ECORRUPT   (-4)   /* on-disk corruption detected */
#define MMAPQ_ESCHEMA    (-5)   /* schema version or queue type mismatch, or on-disk geometry not supported by this build/host */
#define MMAPQ_EEXIST     (-6)   /* destination queue already exists */
#define MMAPQ_ENOENT     (-7)   /* file or queue does not exist */
#define MMAPQ_EIDCONFLICT (-8)  /* requested appender ID already in use */
#define MMAPQ_ENOMEM     (-9)   /* internal allocation failure (calloc, strdup, etc.) — back off, do not retry hot-loop */
#define MMAPQ_EIO        (-10)  /* I/O error — metadata file write failure, retry */
#define MMAPQ_ENOTREADY  (-11)  /* meta file exists but not yet fully initialised — transient, retry */
#define MMAPQ_ECALLBACK  (-12)  /* transfer aborted by appender-ID mapper callback */

/* ── Error string ──────────────────────────────────────────────────────────── */

/**
 * mmapq_strerror — return a human-readable string for an MMAPQ_* error code.
 *
 * rc: a negative MMAPQ_* error code (e.g. MMAPQ_EINVAL = -2)
 * Returns: a statically allocated string. Never returns NULL.
 *          Returns "unknown error" for unrecognised codes.
 */
const char *mmapq_strerror(int rc);

/* ── Version ───────────────────────────────────────────────────────────────── */

#define MMAPQ_VERSION_MAJOR 0
#define MMAPQ_VERSION_MINOR 2
#define MMAPQ_VERSION_PATCH 0

#define MMAPQ_VERSION_STR "0.2.0"

/**
 * mmapq_version — return the library version string.
 *
 * Returns: a statically allocated version string (e.g. "0.2.0").
 * Never returns NULL. Safe to call at any time.
 */
static inline const char *mmapq_version(void) {
    return MMAPQ_VERSION_STR;
}

#if defined(__linux__) && !defined(_GNU_SOURCE)
#  error "mmapq requires -D_GNU_SOURCE on Linux — add -D_GNU_SOURCE to your CFLAGS"
#endif

/**
 * Platform detection and CPU hints.
 *
 * Supports Linux (x86-64 and ARM64).
 *
 * mmapq requires -D_GNU_SOURCE on Linux to expose pthread_setname_np,
 * pthread_setaffinity_np and CPU_SET macros.
 *
 * CROSS-PROCESS ATOMICS:
 *   The queue uses lock-free atomics over MAP_SHARED mmap regions.
 *   This requires naturally-aligned lock-free operations, which are
 *   guaranteed on:
 *     Linux x86-64  (LOCK CMPXCHG)
 *     Linux ARM64   (LDAXR/STLXR, CASAL)
 */

#if !defined(__linux__)
#  error "mmapq supports Linux only"
#endif

#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>

/**
 * mmapq_spin_hint — yield the CPU pipeline during a spin-wait loop.
 *
 * x86_64: PAUSE instruction.
 * ARM64:  YIELD instruction (same semantics, different mnemonic).
 */
static inline void mmapq_spin_hint(void)
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
 * mmapq_monotonic_nanos — current time in nanoseconds from a monotonic clock.
 *
 * Linux: CLOCK_MONOTONIC — uses VDSO for sub-100ns overhead.
 */
static inline uint64_t mmapq_monotonic_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

/**
 * mmapq_thread_set_name — set the name of a pthread (for debugger visibility).
 *
 * Linux: pthread_setname_np(thread, name).
 *        Requires _GNU_SOURCE on the compiler command line.
 */
static inline void mmapq_thread_set_name(pthread_t thread, const char *name)
{
    pthread_setname_np(thread, name);
}

/**
 * mmapq_thread_set_affinity_of — pin a specific thread to a specific CPU core.
 *
 * Linux: uses pthread_setaffinity_np.
 * Non-Linux: no-op (not supported via standard API).
 */
static inline int mmapq_thread_set_affinity_of(pthread_t thread, int cpu_core)
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
 * mmapq_thread_set_affinity — pin the current thread to a specific CPU core.
 */
static inline int mmapq_thread_set_affinity(int cpu_core)
{
    return mmapq_thread_set_affinity_of(pthread_self(), cpu_core);
}

#endif /* MMAPQ_MMAP_PLATFORM_H */
