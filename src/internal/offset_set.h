#ifndef OFFSET_SET_H
#define OFFSET_SET_H

#include <stdint.h>

/**
 * OffsetSet — sorted set of outstanding (staged, not yet committed) payload
 * or batch offsets; one per appender instance (CAND-49, spec §3.3).
 *
 * The set is the commit-validation gate: commit(offset) accepts exactly the
 * offsets the iterator refilled or append staged (plus the flag-tracked
 * tail, which never enters the set — spec §3.3).
 *
 * STANDING SPARE-SLOT INVARIANT: count < cap after every successful add.
 * The set is allocated at appender create (initial capacity 16 slots) and
 * any add whose insert would consume the last slot grows FIRST (ENOMEM
 * leaves the set unchanged), so the deferred-path adds that must not
 * allocate (commit-failure staging, join completion) never do. The
 * pre-write ensure in append_uncommitted / complete_uncommitted is one
 * compare when the spare exists (the normal case), a grow attempt
 * otherwise (ENOMEM ⇒ nothing written — owner ruling 5).
 *
 * Ordering discipline: real flows add in ascending order (lane-end appends,
 * ascending refill) → O(1) tail append when off > last, sorted-insert
 * fallback otherwise. contains: binary search. remove: O(1) at head/tail,
 * binary search + memmove otherwise.
 *
 * Memory bound (documented): 8 bytes per outstanding offset plus one spare,
 * ×2 at growth peaks; proportional to staged-not-yet-committed messages.
 *
 * IDEMPOTENT ADD (owner ruling): adding an offset already present is a
 * no-op. Without it, re-scans refill duplicates and resurrect double commit.
 */

typedef struct {
    int64_t *v;
    int      count;
    int      cap;
} OffsetSet;

/* Slots allocated at appender create: 15 usable + the standing spare. */
#define OFFSET_SET_INITIAL_CAPACITY 16

/**
 * mmapqi_offset_set_init — zero-init; no allocation.
 */
int  mmapqi_offset_set_init(OffsetSet *s);

/**
 * mmapqi_offset_set_reserve — ensure the set can absorb `extra` more adds
 * without allocating (cap >= count + extra + 1, keeping the standing spare).
 * Returns 0 or MMAPQ_ENOMEM (set unchanged).
 */
int  mmapqi_offset_set_reserve(OffsetSet *s, int extra);

/**
 * mmapqi_offset_set_add — insert off; no-op if already present.
 * Grows (doubling) when the insert would consume the last slot.
 * Returns 0, or MMAPQ_ENOMEM with the set unchanged.
 */
int  mmapqi_offset_set_add(OffsetSet *s, int64_t off);

/**
 * mmapqi_offset_set_contains — 1 if present, 0 otherwise (binary search).
 */
int  mmapqi_offset_set_contains(const OffsetSet *s, int64_t off);

/**
 * mmapqi_offset_set_remove — remove off; no-op if absent.
 */
void mmapqi_offset_set_remove(OffsetSet *s, int64_t off);

/**
 * mmapqi_offset_set_destroy — free the array.
 */
void mmapqi_offset_set_destroy(OffsetSet *s);

#endif /* OFFSET_SET_H */
