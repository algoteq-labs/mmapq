#ifndef PLATFORM_INTERNAL_H
#define PLATFORM_INTERNAL_H

#include <stddef.h>
#include <unistd.h>
#include <sys/mman.h>

/**
 * huge_pages_hint — advise the kernel to use huge pages for a mapped region.
 *
 * Linux: uses madvise with MADV_HUGEPAGE. Requires Transparent Huge Pages (THP)
 *        to be enabled in the kernel (typically 'madvise' or 'always' mode).
 * Non-Linux: no-op (not supported via standard madvise).
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
 * page_size — OS memory page size in bytes (cached after first call).
 *
 * Linux: typically 4096 bytes.
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
 * mmapqi_page_size — runtime page size seen by the queue geometry rule
 * (CAND-38): region_size must be a multiple of this so region mmap offsets
 * stay page-aligned on 16/64 KB-page kernels. Defined in platform.c as a
 * real function so the MMAPQ_TEST_HOOKS override (test_hooks.h) is a single
 * process-global. Generic region layers use page_size() above — the rule
 * is a queue geometry rule, not a mapper rule.
 */
long mmapqi_page_size(void);

#endif /* PLATFORM_INTERNAL_H */
