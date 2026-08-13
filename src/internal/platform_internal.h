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

#endif /* PLATFORM_INTERNAL_H */
