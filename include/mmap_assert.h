#ifndef MMAP_ASSERT_H
#define MMAP_ASSERT_H

#include <stdio.h>
#include <stdlib.h>

/**
 * MMAP_ASSERT — fail-fast assertion with message.
 *
 * In debug builds: prints file, line, condition, and message to stderr, then aborts.
 * In release builds (NDEBUG defined): compiles to nothing — zero overhead.
 *
 * Philosophy: a violated invariant is always a bug. Silent graceful degradation
 * in a low-latency shared memory library prolongs the agony. Crash immediately
 * at the point of violation so the cause is unambiguous.
 */
#ifdef NDEBUG
    #define MMAP_ASSERT(cond, msg) ((void)0)
#else
    #define MMAP_ASSERT(cond, msg)                                          \
        do {                                                                 \
            if (!(cond)) {                                                   \
                fprintf(stderr,                                              \
                    "\nASSERTION FAILED: %s\n"                              \
                    "  message : %s\n"                                       \
                    "  file    : %s\n"                                       \
                    "  line    : %d\n",                                      \
                    #cond, (msg), __FILE__, __LINE__);                       \
                abort();                                                     \
            }                                                                \
        } while (0)
#endif

#endif /* MMAP_ASSERT_H */
