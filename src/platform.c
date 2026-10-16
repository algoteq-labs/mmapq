#include "internal/platform_internal.h"
#include "internal/test_hooks.h"
#include "mmap_platform.h"

/* ── Page size seam for the CAND-38 geometry rule ──────────────────────────
 * mmapqi_page_size() backs the queue geometry checks (region_size must be a
 * multiple of the runtime page size). It is a real function — not the
 * static inline page_size() above — so the MMAPQ_TEST_HOOKS override is a
 * single process-global: a static inline would give each translation unit
 * its own copy and the seam could not reach all check sites (queue create,
 * batch queue create, meta open_rw + meta_validate). The override exists to
 * pin 16/64 KB-page host behaviour on a 4 KB-page host. */
#ifdef MMAPQ_TEST_HOOKS
static long page_size_override;   /* 0 = disarmed */

void mmapq_test_hook_set_page_size(long ps)
{
    page_size_override = ps;
}
#endif

long mmapqi_page_size(void)
{
#ifdef MMAPQ_TEST_HOOKS
    if (page_size_override != 0) return page_size_override;
#endif
    /* If sysconf ever failed, page_size() would return -1 and
     * region_size % -1 == 0 would pass: the page check fails open (no
     * division by zero). Accepted — sysconf(_SC_PAGESIZE) cannot fail on
     * Linux (CAND-38 review note). */
    return page_size();
}
