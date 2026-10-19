#ifndef COMMITTED_RANGES_H
#define COMMITTED_RANGES_H

#include <stdint.h>

/**
 * CommittedRanges — dynamic array of committed byte ranges, shared by the
 * single-message and batch uncommitted walkers (CAND-49, spec §3.1).
 *
 * Range semantics: start inclusive, end exclusive (unchanged from the
 * pre-extraction single-message walker).
 *
 * Discipline:
 *   - The scan appends RAW spans (mmapqi_ranges_append) — no merging during
 *     the scan; header order is commit order, so spans arrive out of order
 *     under resequenced commits (D-B).
 *   - mmapqi_ranges_sort_merge sorts by start and fuses overlapping/adjacent
 *     spans in one pass — the Java heap+map consumption order, restored.
 *     After it, the array is sorted ascending by start and disjoint.
 *   - mmapqi_ranges_prune_up_to is valid only on a sorted, merged array.
 */

/* ── Committed range entry ────────────────────────────────────────────────── */

typedef struct {
    int64_t start;  /* first byte of committed region (inclusive) */
    int64_t end;    /* first byte AFTER committed region (exclusive) */
} CommittedRange;

typedef struct {
    CommittedRange *v;
    int             count;
    int             cap;
} CommittedRanges;

/**
 * mmapqi_ranges_clear — reset to empty (keeps the allocation).
 */
void mmapqi_ranges_clear(CommittedRanges *r);

/**
 * mmapqi_ranges_append — append a raw span; NO merging (scan-time order).
 * start must be < end. Growth is realloc doubling (CAND-39 pattern, moved
 * verbatim — see committed_ranges.c).
 */
void mmapqi_ranges_append(CommittedRanges *r, int64_t start, int64_t end);

/**
 * mmapqi_ranges_sort_merge — qsort by start, then one merge pass fusing
 * overlapping or adjacent spans. Postcondition: sorted ascending, disjoint,
 * non-adjacent.
 */
void mmapqi_ranges_sort_merge(CommittedRanges *r);

/**
 * mmapqi_ranges_prune_up_to — drop ranges whose end <= offset.
 * Valid only on a sorted, merged array.
 */
void mmapqi_ranges_prune_up_to(CommittedRanges *r, int64_t offset);

#endif /* COMMITTED_RANGES_H */
