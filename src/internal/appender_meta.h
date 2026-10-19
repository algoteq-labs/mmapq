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
 *   24 : padding to 64 bytes
 *
 * start_offset:
 *   The per-appender deletion boundary: payload files wholly below it may
 *   be reclaimed. Initialised to INITIAL_PAYLOAD_POSITION (64) at creation.
 *   Updated by the archiver (forward: raised to the end of the
 *   maximum-position archived entry by CAS from the pre-call snapshot
 *   (CAND-37), lowered by the live-header pass; undo: atomic-min to the
 *   lowest restored position) and lowered by a commit's re-check when a
 *   gap-fill commit races an archive (CAND-30). It is also the anchor a
 *   joining appender with no live entry walks forward from — never a write
 *   position. Read with acquire, written by CAS/atomic-min — safe
 *   across processes.
 *
 * archive_offset (CAND-49 §3.9, D-G):
 *   The archive line: how far the archive has gone — the uncommitted
 *   iterator's snapshot floor. Initialised to INITIAL_PAYLOAD_POSITION at
 *   creation. Monotonic: only a forward archive moves it, raising it to the
 *   same candidate value computed for start_offset, stored as
 *   max(current, candidate). NOTHING else writes it — not the commit
 *   re-check min, not rollback_start_offsets, not undo. Do not merge with
 *   start_offset; see doc/design/archive-boundaries.md.
 */

/*
 * The appender meta file size must be 64 bytes for binary compatibility with Java.
 */
#define APPENDER_META_ENCODED_LENGTH ((int32_t)64)
#define APPENDER_META_START_OFFSET_OFFSET  ((int32_t)0)
#define APPENDER_META_APPENDER_ID_OFFSET   ((int32_t)8)
#define APPENDER_META_ARCHIVE_OFFSET_OFFSET ((int32_t)16)

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
 * desired in one strong CAS (acq_rel on success, acquire on failure).
 * The archiver's publish (CAND-37): expected is the pre-call snapshot.
 * Failure means a racing commit's re-check lowered the boundary below the
 * snapshot between snapshot and publish — the current value is then
 * already below desired, so there is nothing to do (archive.lock EX
 * admits no other archiver; commits only ever lower). Never retry:
 * raising over that lowering would re-expose a live payload to
 * reclamation.
 * Returns 1 if the CAS published, 0 if the boundary was already lower.
 */
static inline int appender_meta_start_offset_cas(AppenderMeta *m,
                                                 int64_t expected,
                                                 int64_t desired)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    return atomic_compare_exchange_strong_explicit(p, &expected, desired,
                                                   memory_order_acq_rel,
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
 * max(current, candidate) (CAND-49 §3.9): a load, compare and RELAXED
 * atomic store. Single writer — archive operations are mutually exclusive
 * per queue under archive.lock EX (I6), and nothing else ever writes the
 * field — so no CAS is needed (contrast start_offset's CAND-37 CAS: the
 * commit min is a second writer there). The max is load-bearing: after an
 * undo, a later forward archive can compute a candidate below the current
 * line; a plain store would lower it and reopen D-G. Called by both
 * archivers' forward step before the queue-meta si release store, which
 * publishes it.
 */
static inline void appender_meta_archive_offset_store(AppenderMeta *m, int64_t candidate)
{
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + APPENDER_META_ARCHIVE_OFFSET_OFFSET);
    int64_t cur = atomic_load_explicit(p, memory_order_relaxed);
    if (candidate > cur)
        atomic_store_explicit(p, candidate, memory_order_relaxed);
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
