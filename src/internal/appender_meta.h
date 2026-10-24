#ifndef APPENDER_META_H
#define APPENDER_META_H

#include <stdint.h>
#include <stdatomic.h>
#include "mmap_assert.h"
#include "fixed_size_region.h"
#include "header_codec.h"

/**
 * AppenderMeta — per-appender persistent metadata stored in a fixed-size
 * memory-mapped file.
 *
 * File: "<directory>/<queue_name>/appender_meta_<appenderId>"
 * Size: APPENDER_META_ENCODED_LENGTH (64 bytes, one cache line)
 *
 * Layout (byte offsets):
 *   0  : start_offset   (int64, volatile — read/written with atomic ops)
 *   8  : appender_id    (int16)
 *   16 : archive_offset (int64, volatile — read/written with atomic ops)
 *   24 : commit_intent  (int64, volatile — read/written with atomic ops; CAND-54)
 *   32 : padding to 64 bytes
 *
 * start_offset:
 *   The per-appender deletion boundary: payload files wholly below it may
 *   be reclaimed. Initialised to INITIAL_PAYLOAD_POSITION (64) at creation.
 *   Updated by the archiver (forward: raised to the end of the
 *   maximum-position archived entry by CAS from the pre-call snapshot
 *   (CAND-37), lowered by the live-header pass; undo: atomic-min to the
 *   lowest restored position) and lowered by a commit's re-check when a
 *   gap-fill commit races an archive (CAND-30). A joining appender with no
 *   live entry walks forward from the join anchor max(start_offset,
 *   archive_offset) — see appender_join_anchor below (CAND-54 L(ii));
 *   never a write position.
 *   Read with acquire, written by CAS/atomic-min — safe across processes.
 *
 * archive_offset (CAND-49 §3.9, D-G):
 *   The archive line: how far the archive has gone — the uncommitted
 *   iterator's snapshot floor. Initialised to INITIAL_PAYLOAD_POSITION at
 *   creation. Monotonic: only a forward archive moves it, raising it to
 *   the same candidate value computed for start_offset, stored as
 *   max(current, candidate). NOTHING else writes it — not the commit
 *   re-check min, not rollback_start_offsets, not undo. Do not merge with
 *   start_offset; see doc/design/archive-boundaries.md.
 *   CAND-54 L(iii): the line is stored (seq_cst) BEFORE the start_offset
 *   raise, so start_offset <= the line at every state (the model's
 *   SoLeLine). The order is load-bearing against the commit min: the min
 *   can lower start_offset back below the raise, so a reader that sees
 *   the raised boundary must be guaranteed the raised line — litmus
 *   c54_case1_min (Never) vs c54_case1_line_after_raise (Sometimes),
 *   doc/model/litmus/.
 *
 * commit_intent (CAND-54, spec §2.1):
 *   The commit intent: a slow-path commit stores its payload offset here
 *   (seq_cst) BEFORE its overtaken pre-check, and clears it (release) LAST
 *   — after the header CAS, the fence, the re-check and the boundary min.
 *   The listing's deletion bound takes min(start_offset, live_floor,
 *   intent): a file a stalled commit is about to publish into is never
 *   listed. Empty = 0 (valid payload offsets are always >= 64). Bytes
 *   24-63 are zero on every existing meta file (creation zero-fills via
 *   ftruncate; the init pwrites touch only bytes 0, 8 and 16), so no
 *   format change. A joining appender clears the slot (owner ruling 2):
 *   a writer killed after the intent store would otherwise pin the lane's
 *   reclaim; a dead lane never rejoined keeps that pin (stated liveness
 *   cost — files below the intent are unaffected).
 */

/*
 * The appender meta file size must be 64 bytes for binary compatibility with Java.
 */
#define APPENDER_META_ENCODED_LENGTH ((int32_t)64)
#define APPENDER_META_START_OFFSET_OFFSET  ((int32_t)0)
#define APPENDER_META_APPENDER_ID_OFFSET   ((int32_t)8)
#define APPENDER_META_ARCHIVE_OFFSET_OFFSET ((int32_t)16)
#define APPENDER_META_INTENT_OFFSET        ((int32_t)24)

/**
 * AppenderMeta — in-memory view of a single appender's persistent metadata.
 *
 * Backed by a memory-mapped file via a FixedSizeRegion.
 */
typedef struct {
    RegionAccessor *region;  /* FixedSizeRegion backing the meta file */
    uint8_t        *base;    /* base pointer into mapped memory       */
    uint8_t         appender_id;
} AppenderMeta;

/* ── Field accessors ─────────────────────────────────────────────────────── */

/**
 * appender_meta_start_offset — read start_offset with acquire semantics.
 *
 * This is the earliest valid payload position for this appender. Readers
 * and pollers below this offset are in archived territory.
 */
static inline int64_t appender_meta_start_offset(const AppenderMeta *m)
{
    const _Atomic int64_t *p =
        (const _Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    return atomic_load_explicit(p, memory_order_acquire);
}

/**
 * appender_meta_start_offset_cas — raise start_offset from expected to
 * desired in one strong CAS (seq_cst on success, acquire on failure).
 * The archiver's publish (CAND-37): expected is the pre-call snapshot.
 * Failure means a racing commit's re-check lowered the boundary below the
 * snapshot between snapshot and publish — the current value is then
 * already below desired, so there is nothing to do (archive.lock EX
 * admits no other archiver; commits only ever lower). Never retry:
 * raising over that lowering would re-expose a live payload to
 * reclamation.
 * CAND-54 §2.5: the success order is seq_cst — the raise participates in
 * the commit-intent protocol's single total order S (the CAND-54 litmus
 * suite, doc/model/litmus/). The line store precedes it (L(iii)).
 * Returns 1 if the CAS published, 0 if the boundary was already lower.
 */
static inline int appender_meta_start_offset_cas(AppenderMeta *m,
                                                 int64_t expected,
                                                 int64_t desired)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    return atomic_compare_exchange_strong_explicit(p, &expected, desired,
                                                   memory_order_seq_cst,
                                                   memory_order_acquire);
}

/**
 * appender_meta_start_offset_min — atomically lower start_offset to offset
 * when offset is smaller (CAS loop, acq_rel). Never raises it. Used by the
 * archiver's live-header pass and undo, and by the commit re-check
 * (CAND-30): a store that could raise the value would re-expose a live
 * payload to reclamation.
 */
static inline void appender_meta_start_offset_min(AppenderMeta *m, int64_t offset)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    int64_t cur = atomic_load_explicit(p, memory_order_acquire);
    while (offset < cur &&
           !atomic_compare_exchange_weak_explicit(p, &cur, offset,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
    }
}

/**
 * appender_meta_archive_offset — read the archive line with acquire
 * semantics (CAND-49 §3.9). The uncommitted iterator reads it AFTER the
 * queue start index (acquire) — that pairing is the D-G publication rule;
 * reading the line before si is the defect. See doc/design/archive-boundaries.md.
 */
static inline int64_t appender_meta_archive_offset(const AppenderMeta *m)
{
    const _Atomic int64_t *p =
        (const _Atomic int64_t *)(m->base + APPENDER_META_ARCHIVE_OFFSET_OFFSET);
    return atomic_load_explicit(p, memory_order_acquire);
}

/**
 * appender_meta_archive_offset_store — raise the archive line to
 * max(current, candidate) (CAND-49 §3.9): a load, compare and atomic
 * store. Single writer — archive operations are mutually exclusive per
 * queue under archive.lock EX (I6), and nothing else ever writes the
 * field — so no CAS is needed (contrast start_offset's CAND-37 CAS: the
 * commit min is a second writer there). The max is load-bearing: after an
 * undo, a later forward archive can compute a candidate below the current
 * line; a plain store would lower it and reopen D-G. Called by both
 * archivers' forward step BEFORE the start_offset raise CAS (CAND-54
 * L(iii)) and before the queue-meta si release store, which publishes it.
 * CAND-54 §2.5: the store is seq_cst — it is the line store in the
 * commit-intent protocol's total order S; relaxed was correct under I6
 * alone but cannot order against the lock-free commit min.
 */
static inline void appender_meta_archive_offset_store(AppenderMeta *m, int64_t candidate)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_ARCHIVE_OFFSET_OFFSET);
    int64_t cur = atomic_load_explicit(p, memory_order_relaxed);
    if (candidate > cur)
        atomic_store_explicit(p, candidate, memory_order_seq_cst);
}

/**
 * appender_meta_start_offset_sc / appender_meta_archive_offset_sc —
 * seq_cst loads for the CAND-54 protocol points that must participate in
 * the single total order S (spec §2.5): the committer's overtaken
 * pre-check (loads both, floors at max — L(i)) and the lister's
 * start_offset load. Everything else keeps the acquire load above.
 */
static inline int64_t appender_meta_start_offset_sc(const AppenderMeta *m)
{
    const _Atomic int64_t *p =
        (const _Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    return atomic_load_explicit(p, memory_order_seq_cst);
}

static inline int64_t appender_meta_archive_offset_sc(const AppenderMeta *m)
{
    const _Atomic int64_t *p =
        (const _Atomic int64_t *)(m->base + APPENDER_META_ARCHIVE_OFFSET_OFFSET);
    return atomic_load_explicit(p, memory_order_seq_cst);
}

/**
 * appender_meta_intent_store — a slow-path commit's intent: the payload
 * offset it is about to publish, stored (seq_cst) BEFORE the overtaken
 * pre-check (CAND-54 §2.2). seq_cst puts it in S before the pre-check
 * loads, which is what forbids the case-1 outcome (litmus c54_case1;
 * the relaxed-store mutant c54_case1_relaxed_store admits it).
 */
static inline void appender_meta_intent_store(AppenderMeta *m, int64_t payload_offset)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_INTENT_OFFSET);
    atomic_store_explicit(p, payload_offset, memory_order_seq_cst);
}

/**
 * appender_meta_intent_load — the lister's intent load (seq_cst), after
 * its start_offset load and before its live-header scan (CAND-54 §2.3 —
 * the order is load-bearing, see the §9.3 "headers scanned before
 * intents" mutant).
 */
static inline int64_t appender_meta_intent_load(const AppenderMeta *m)
{
    const _Atomic int64_t *p =
        (const _Atomic int64_t *)(m->base + APPENDER_META_INTENT_OFFSET);
    return atomic_load_explicit(p, memory_order_seq_cst);
}

/**
 * appender_meta_intent_clear — release store of 0. Two callers: the
 * commit itself (LAST — after the CAS, fence, re-check and min; and on
 * the refusal path, where a lingering intent would pin the lane's
 * reclaim), and the join (ruling 2: clear a dead incarnation's stale
 * intent). Release suffices: the CAND-30 seq_cst fence plus the lister's
 * seq_cst intent load already forbid the case-3 outcome in RC11 (spec
 * §9.2's double-synchronisation finding); the release states the intent
 * and is kept regardless.
 */
static inline void appender_meta_intent_clear(AppenderMeta *m)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_INTENT_OFFSET);
    atomic_store_explicit(p, 0, memory_order_release);
}

/**
 * appender_join_anchor — the offset a joining appender with no live entry
 * walks forward from: max(start_offset, archive_offset).
 *
 * CAND-54 L(ii) needs the line as the floor: a commit min can lower
 * start_offset into reclaimed territory (hole 1, spec §7.1), and the line
 * never moves back — reclaimed files are wholly below it, so anchoring at
 * or above the line can never re-create one. On every state where the
 * invariant SoLeLine holds (start_offset <= the line — all reachable
 * states; the model pins it) the max IS the line.
 *
 * The start_offset side of the max is not dead code: states with
 * start_offset > the line violate SoLeLine and are exactly the corrupt /
 * 56-bit-limit states the CAND-32 pins plant (start_offset at 2^56 and
 * near it, line untouched). Anchoring below such a start_offset would
 * bypass the append path's unencodable-start refusal and let a writer
 * append into territory the deletion boundary claims is reclaimed.
 * Acquire loads suffice: the join runs before this incarnation writes
 * anything, and both fields only ever move in the safe direction for a
 * late-loading join (the line never falls; a rising start_offset raises
 * the anchor).
 */
static inline int64_t appender_join_anchor(const AppenderMeta *m)
{
    int64_t so   = appender_meta_start_offset(m);
    int64_t line = appender_meta_archive_offset(m);
    return so > line ? so : line;
}

static inline uint8_t appender_meta_appender_id(const AppenderMeta *m)
{
    return m->appender_id;
}

/* ── Factory functions ───────────────────────────────────────────────────── */

/**
 * mmapqi_appender_meta_open_rw — open or create the appender meta file.
 *
 * Creates the file if it does not exist, initialised with the given
 * appender_id and start_offset = INITIAL_PAYLOAD_POSITION.
 * Returns 0 on success. Returns MMAPQ_EINVAL for NULL args,
 * MMAPQ_ENOMEM for allocation failures, MMAPQ_ECORRUPT if the on-disk
 * start_offset is outside [INITIAL_PAYLOAD_POSITION, 2^56] (CAND-32).
 * Sets *out to NULL on failure.
 */
int mmapqi_appender_meta_open_rw(
    const char *directory,
    const char *queue_name,
    uint8_t     appender_id,
    AppenderMeta **out);

/**
 * mmapqi_appender_meta_open_ro — open the appender meta file for reading.
 *
 * Returns 0 on success, MMAPQ_ENOENT if the file does not exist,
 * MMAPQ_EINVAL for NULL args, MMAPQ_ENOMEM for allocation failures,
 * MMAPQ_ECORRUPT if the on-disk start_offset is outside
 * [INITIAL_PAYLOAD_POSITION, 2^56] (CAND-32).
 * Sets *out to NULL on failure.
 */
int mmapqi_appender_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     appender_id,
    AppenderMeta **out);

/**
 * mmapqi_appender_meta_close — unmap and free an AppenderMeta.
 */
void mmapqi_appender_meta_close(AppenderMeta *m);

#endif /* APPENDER_META_H */
