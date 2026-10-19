#include "internal/committed_ranges.h"
#include "internal/mmap_assert.h"

#include <stdlib.h>
#include <string.h>

/* ── Range array helpers (CAND-49 §3.1 — extracted from
 *    uncommitted_payload_iterator.c, shared by both walkers) ────────────── */

#define RANGE_INITIAL_CAPACITY 64

/* Growth keeps realloc doubling, moved VERBATIM from the pre-extraction
 * ranges_ensure_capacity. CAND-39 is deliberately NOT fixed here: on realloc
 * failure the old buffer leaks and r->v is NULL while count/cap still
 * describe it — the next append writes through NULL. Frozen scope;
 * see fix_plan CAND-39. */
static void ranges_ensure_capacity(CommittedRanges *r)
{
    if (r->count < r->cap) return;

    int new_cap = r->cap == 0
        ? RANGE_INITIAL_CAPACITY
        : r->cap * 2;

    r->v = realloc(r->v, (size_t)new_cap * sizeof(CommittedRange));
    if (r->v == NULL) return;
    r->cap = new_cap;
}

void mmapqi_ranges_append(CommittedRanges *r, int64_t start, int64_t end)
{
    MMAP_ASSERT(start < end, "mmapqi_ranges_append: start must be < end");

    ranges_ensure_capacity(r);
    r->v[r->count].start = start;
    r->v[r->count].end   = end;
    r->count++;
}

void mmapqi_ranges_clear(CommittedRanges *r)
{
    r->count = 0;
}

static int range_start_cmp(const void *pa, const void *pb)
{
    int64_t a = ((const CommittedRange *)pa)->start;
    int64_t b = ((const CommittedRange *)pb)->start;
    return (a > b) - (a < b);
}

void mmapqi_ranges_sort_merge(CommittedRanges *r)
{
    if (r->count == 0) return;

    qsort(r->v, (size_t)r->count, sizeof(CommittedRange), range_start_cmp);

    /* One merge pass: fuse overlapping (start <= running end) and adjacent
     * (start == running end) spans — the Java heap+map consumption order
     * (CAND-49 §3.1, D-B). */
    int w = 0;
    for (int i = 1; i < r->count; i++) {
        if (r->v[i].start <= r->v[w].end) {
            if (r->v[i].end > r->v[w].end) r->v[w].end = r->v[i].end;
        } else {
            w++;
            r->v[w] = r->v[i];
        }
    }
    r->count = w + 1;
}

void mmapqi_ranges_prune_up_to(CommittedRanges *r, int64_t offset)
{
    int i = 0;
    while (i < r->count && r->v[i].end <= offset) {
        i++;
    }
    if (i > 0) {
        int remaining = r->count - i;
        memmove(r->v, r->v + i,
                (size_t)remaining * sizeof(CommittedRange));
        r->count = remaining;
    }
}
