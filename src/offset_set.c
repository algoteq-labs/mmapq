#include "internal/offset_set.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>

/* ── OffsetSet (CAND-49 §3.3) ────────────────────────────────────────────── */

int mmapqi_offset_set_init(OffsetSet *s)
{
    if (s == NULL) return MMAPQ_EINVAL;
    s->v     = NULL;
    s->count = 0;
    s->cap   = 0;
    return 0;
}

/* Grow to at least need slots, doubling. calloc + memcpy + free (not realloc)
 * so the -Wl,--wrap=calloc fault seam reaches it (spec §3.3, §7.10). */
static int offset_set_grow(OffsetSet *s, int need)
{
    int new_cap = s->cap == 0 ? OFFSET_SET_INITIAL_CAPACITY : s->cap * 2;
    if (new_cap < need) new_cap = need;

    int64_t *nv = calloc((size_t)new_cap, sizeof(int64_t));
    if (nv == NULL) return MMAPQ_ENOMEM;
    if (s->count > 0)
        memcpy(nv, s->v, (size_t)s->count * sizeof(int64_t));
    free(s->v);
    s->v   = nv;
    s->cap = new_cap;
    return 0;
}

int mmapqi_offset_set_reserve(OffsetSet *s, int extra)
{
    if (s == NULL) return MMAPQ_EINVAL;
    if (extra < 0) return MMAPQ_EINVAL;

    /* Room for `extra` more adds plus the standing spare slot. */
    int64_t need = (int64_t)s->count + extra + 1;
    if (s->cap >= need) return 0;   /* the normal case: one compare */
    return offset_set_grow(s, (int)need);
}

/* Binary search: index of off, or -1- insertion_point (the Java
 * Arrays.binarySearch contract) when absent. */
static int offset_set_find(const OffsetSet *s, int64_t off)
{
    int lo = 0, hi = s->count - 1;
    while (lo <= hi) {
        int mid = (lo + hi) >> 1;
        if (s->v[mid] < off) lo = mid + 1;
        else if (s->v[mid] > off) hi = mid - 1;
        else return mid;
    }
    return -1 - lo;
}

int mmapqi_offset_set_add(OffsetSet *s, int64_t off)
{
    if (s == NULL) return MMAPQ_EINVAL;

    int idx = offset_set_find(s, off);
    if (idx >= 0) return 0;   /* idempotent: already present */
    int ins = -1 - idx;

    if (s->count == s->cap) {
        /* No spare: grow first (may fail — ENOMEM leaves the set
         * unchanged). */
        int rc = offset_set_grow(s, s->count + 2);
        if (rc != 0) return rc;
    }

    /* A slot exists: the insert cannot fail. O(1) tail append for the
     * common ascending flow; memmove otherwise. */
    if (ins < s->count)
        memmove(s->v + ins + 1, s->v + ins,
                (size_t)(s->count - ins) * sizeof(int64_t));
    s->v[ins] = off;
    s->count++;

    /* Restore the standing spare best-effort when the insert consumed it.
     * A failure here is NOT an error — the element is in; the spare is
     * restored by the next reserve or the next add's grow-first path. This
     * is what makes the guaranteed-spare adds (complete's publish-failure
     * add, join completion) never allocate: the insert never does. */
    if (s->count == s->cap)
        (void)offset_set_grow(s, s->count + 1);
    return 0;
}

int mmapqi_offset_set_contains(const OffsetSet *s, int64_t off)
{
    if (s == NULL) return 0;
    return offset_set_find(s, off) >= 0;
}

void mmapqi_offset_set_remove(OffsetSet *s, int64_t off)
{
    if (s == NULL) return;

    int idx = offset_set_find(s, off);
    if (idx < 0) return;   /* no-op if absent */

    if (idx < s->count - 1)
        memmove(s->v + idx, s->v + idx + 1,
                (size_t)(s->count - idx - 1) * sizeof(int64_t));
    s->count--;
}

void mmapqi_offset_set_destroy(OffsetSet *s)
{
    if (s == NULL) return;
    free(s->v);
    s->v     = NULL;
    s->count = 0;
    s->cap   = 0;
}
