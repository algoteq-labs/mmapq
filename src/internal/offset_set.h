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
 * STANDING SPARE-SLOT DISCIPLINE (reviewer-corrected at CAND-49 commit 3):
 * a slot always exists when count < cap. An add with a spare INSERTS FIRST
 * (it cannot fail or allocate), then restores the spare with a best-effort
 * grow whose failure is ignored (the spare is restored by the next reserve
 * or the next add). Only an add with no spare (count == cap) grows first —
 * that ENOMEM leaves the set unchanged. So the deferred-path adds that must
 * not allocate (complete's publish-failure add, join completion) never do:
 * the insert never allocates. Callers that need a guaranteed spare call
 * reserve() before their add (the pre-write ensure in append_uncommitted /
 * complete_uncommitted — one compare when the spare exists, a grow attempt
 * otherwise; ENOMEM ⇒ nothing written — owner ruling 5).
 * The set is allocated at appender create (initial capacity 16 slots).
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
 * With a spare slot the insert cannot fail or allocate (the spare is then
 * restored best-effort); with no spare the grow may fail.
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
