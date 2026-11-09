#include "internal/uncommitted_payload_iterator.h"
#include "internal/header_reader.h"
#include "internal/payload_walk.h"
#include "internal/walk_classify.h"
#include "internal/test_hooks.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── CAND-58 classification helpers ─────────────────────────────────────── */

/* Classify a payload-side walk failure at `position` (a map miss — the walk
 * accessor is read-only, so a miss is never a re-creation — or an inner-init
 * EINVAL against a live start_of_stream that rose past the position
 * mid-walk). known_written_end per site (§4.2). On OVERTAKEN the classifying
 * floor is recorded (walk_ovt_floor, payload domain) for the restart loop.
 * Returns the verdict. */
static MmapqiWalkClass classify_payload_failure(MmapqUncommittedPayloadIterator *it,
                                                int64_t position,
                                                int64_t known_written_end)
{
    int64_t f;
    MmapqiWalkClass c = mmapqi_walk_classify(position, known_written_end,
                                             it->archive_offset_sc,
                                             it->stream_ctx, &f);
    if (c == MMAPQI_WALK_OVERTAKEN) {
        it->walk_ovt_floor  = f;
        it->walk_ovt_domain = MMAPQI_WALK_DOM_PAYLOAD;
    }
    return c;
}

/* ── Header scan: build committed ranges ─────────────────────────────────── */

/* header_index is the frozen queue start index read by init BEFORE the
 * archive line (the D-G publication rule, CAND-49 §3.9). scan_line is the
 * same init's archive-line snapshot — the span walk's clamp floor
 * (CAND-58 §4.3).
 *
 * Return protocol: 0 = success; MMAPQI_WALK_RESTART = a walk-ending failure
 * classified OVERTAKEN (the floor is in it->walk_ovt_floor); a negative
 * MMAPQ_E* — ETIMEDOUT (transient, retryable) verbatim, ECORRUPT for the
 * rest, including the truncated-committed-span cases that returned 0
 * pre-CAND-58. */
static int scan_committed_ranges(MmapqUncommittedPayloadIterator *it,
                                 int64_t header_index,
                                 int64_t scan_line)
{
    mmapqi_ranges_clear(&it->ranges);
    it->current_range = 0;

    /* S3 (D8): never scan headers below the queue's start index — those
     * files may have been reclaimed, and entries below start_of_stream are
     * archived by definition, never uncommitted. One frozen value per scan
     * (read by init before the archive line); a concurrent archive advancing
     * it further is covered by the deletion protocol (doc/design/archiving-design.md),
     * mirroring the appender join's scan_floor. */
    int64_t current_start = PAYLOAD_ITERATOR_NOT_SET;
    int64_t current_end   = PAYLOAD_ITERATOR_NOT_SET;
    int arc;

    /*
     * We need a separate MmapqPayloadIterator for walking payload within each
     * committed span. We re-use it->inner for this purpose during the scan,
     * then reinitialise it for gap traversal after the scan completes.
     */
    int64_t stream_start = it->start_of_stream_offset(it->stream_ctx);
    int64_t span_floor = stream_start > scan_line ? stream_start : scan_line;
    if (mmapqi_payload_iterator_init(it->inner, span_floor, PAYLOAD_ITERATOR_NOT_SET) != 0) {
        /* the live start_of_stream rose past the floor between the two
         * reads — an archive raced the scan */
        if (classify_payload_failure(it, span_floor, span_floor + 1)
                == MMAPQI_WALK_OVERTAKEN)
            return MMAPQI_WALK_RESTART;
        return MMAPQ_ECORRUPT;
    }

    while (1) {
        /* Read header at header_index — checked (F2): a mapping failure
         * fails the scan with its code instead of looking like end of
         * committed headers. */
        uint64_t header;
        C58_HOOK_SCAN_HEADER_PRE_READ(header_index);
        int hrc = mmapqi_header_read_checked(it->header_accessor, &it->header_buf,
                                             header_index, &header);
        if (hrc != 0) return hrc;

        if (header == HEADER_NULL) {
            /* CAND-58: a NULL read below the CURRENT start_index means the
             * scan's snapshot was overtaken (its header file reclaimed
             * mid-scan) — restart from fresh state; the rescan resumes from
             * the new start_index (the archive-hole restart's factory twin,
             * §4.4). At/above start_index it is the end of committed
             * headers. */
            int64_t f;
            MmapqiWalkClass c = mmapqi_walk_classify(header_index, INT64_MIN,
                                                     it->queue_start_index,
                                                     it->queue_start_index_ctx, &f);
            if (c == MMAPQI_WALK_OVERTAKEN) {
                it->walk_ovt_floor  = f;
                it->walk_ovt_domain = MMAPQI_WALK_DOM_HEADER;
                return MMAPQI_WALK_RESTART;
            }
            break;  /* end of committed headers */
        }

        header_index++;

        /* Skip entries from other appenders */
        if (header_appender_id(header) != it->appender_id) continue;

        int64_t header_offset = header_payload_position(header);

        /* Archived entry: archiving advanced start_of_stream or the archive
         * line past this payload while leaving its header readable (only
         * fully-covered header files are deleted). Not corruption — the
         * payload is gone by design, and an archived entry is never
         * uncommitted. Skip without consulting the payload stream.
         * (CAND-58: the skip floors at max(start_of_stream, the line
         * snapshot) — the §4.3 clamp.) */
        if (header_offset < span_floor) continue;

        if (current_start == PAYLOAD_ITERATOR_NOT_SET) {
            /* ── First entry: init range from this offset ──────────────── */
            if (mmapqi_payload_iterator_init(it->inner, header_offset, PAYLOAD_ITERATOR_NOT_SET) != 0) {
                /* the live start_of_stream rose past the span mid-scan */
                if (classify_payload_failure(it, header_offset, header_offset + 1)
                        == MMAPQI_WALK_OVERTAKEN)
                    return MMAPQI_WALK_RESTART;
                return MMAPQ_ECORRUPT;
            }
            C58_HOOK_SCAN_SPAN_PRE_WALK(header_offset, stream_start, it->archive_line);

            /* D8: the inner advance is three-way — a negative error
             * propagates; 0 (end of stream) or an offset mismatch is a
             * truncated committed span — ECORRUPT. CAND-58: a map miss
             * classifies first (the span start is a committed header's
             * position — always known-written): below the line → overtaken
             * restart; at/above → ECORRUPT. */
            arc = mmapqi_payload_iterator_advance(it->inner);
            if (arc == MMAPQI_WALK_MAP_MISS) {
                if (classify_payload_failure(it, header_offset, header_offset + 1)
                        == MMAPQI_WALK_OVERTAKEN)
                    return MMAPQI_WALK_RESTART;
                return MMAPQ_ECORRUPT;
            }
            if (arc < 0) return arc;
            if (arc == 0) return MMAPQ_ECORRUPT;
            if (it->inner->current_message_offset != header_offset) return MMAPQ_ECORRUPT;
            current_start = header_offset;
            current_end   = it->inner->next_probing_offset;

        } else {
            arc = mmapqi_payload_iterator_advance(it->inner);
            if (arc == MMAPQI_WALK_MAP_MISS) {
                if (classify_payload_failure(it, header_offset, header_offset + 1)
                        == MMAPQI_WALK_OVERTAKEN)
                    return MMAPQI_WALK_RESTART;
                return MMAPQ_ECORRUPT;
            }
            if (arc < 0) return arc;
            if (arc == 0 ||
                it->inner->current_message_offset != header_offset) {
                /* ── Gap detected: flush current range, start new one ────── */
                /* D-B (CAND-49 §3.1): raw append — NO merging during the scan.
                 * Header order is commit order, so spans arrive out of order
                 * under resequenced commits; the sort+merge runs once at the
                 * end of init. */
                mmapqi_ranges_append(&it->ranges, current_start, current_end);

                if (mmapqi_payload_iterator_init(it->inner, header_offset, PAYLOAD_ITERATOR_NOT_SET) != 0) {
                    if (classify_payload_failure(it, header_offset, header_offset + 1)
                            == MMAPQI_WALK_OVERTAKEN)
                        return MMAPQI_WALK_RESTART;
                    return MMAPQ_ECORRUPT;
                }
                C58_HOOK_SCAN_SPAN_PRE_WALK(header_offset, stream_start, it->archive_line);
                arc = mmapqi_payload_iterator_advance(it->inner);
                if (arc == MMAPQI_WALK_MAP_MISS) {
                    if (classify_payload_failure(it, header_offset, header_offset + 1)
                            == MMAPQI_WALK_OVERTAKEN)
                        return MMAPQI_WALK_RESTART;
                    return MMAPQ_ECORRUPT;
                }
                if (arc < 0) return arc;
                if (arc == 0) return MMAPQ_ECORRUPT;
                if (it->inner->current_message_offset != header_offset) return MMAPQ_ECORRUPT;

                current_start = header_offset;
                current_end   = it->inner->next_probing_offset;

            } else {
                /* ── Contiguous: extend current range (fast path) ────────── */
                int64_t next = it->inner->next_probing_offset;
                if (next > current_end) current_end = next;
            }
        }
    }

    /* Flush final range */
    if (current_start != PAYLOAD_ITERATOR_NOT_SET) {
        mmapqi_ranges_append(&it->ranges, current_start, current_end);
    }

    return 0;
}

/* ── Init gap traversal after scan ──────────────────────────────────────── */

/* Gap positioning return protocol (CAND-58): 0 = success;
 * MMAPQI_WALK_RESTART = the inner init failed against a live
 * start_of_stream that rose past the position mid-walk, and the position
 * classified below the re-read line (the floor is in it->walk_ovt_floor) —
 * the caller restarts; MMAPQ_ECORRUPT = classified at/above the line (a
 * broken invariant — the pre-CAND-58 behaviour for these sites). */
static int init_first_gap(MmapqUncommittedPayloadIterator *it, int64_t start)
{
    /*
     * Find the first gap at or after `start`.
     * A gap is the space before the first committed range, between ranges,
     * or after the last range up to fixed_end_of_stream.
     *
     * The init arguments here are derived from the validated start or from
     * ranges built only from non-archived entries; an init failure means the
     * LIVE start_of_stream rose past the position (an archive raced the
     * walk) — classify it.
     */

    /* Skip committed ranges that end <= start */
    while (it->current_range < it->ranges.count &&
           it->ranges.v[it->current_range].end <= start) {
        it->current_range++;
    }

    int64_t gap_end;

    if (it->current_range >= it->ranges.count) {
        /* No more committed ranges ahead — gap runs to fixed_end_of_stream */
        it->current_gap_end = PAYLOAD_ITERATOR_NOT_SET;  /* trailing gap */
        if (mmapqi_payload_iterator_init(it->inner, start, it->fixed_end_of_stream) != 0)
            return classify_payload_failure(it, start, it->fixed_end_of_stream)
                       == MMAPQI_WALK_OVERTAKEN ? MMAPQI_WALK_RESTART : MMAPQ_ECORRUPT;
    } else {
        int64_t range_start = it->ranges.v[it->current_range].start;

        if (start >= range_start) {
            /* start is inside a committed range — skip past it */
            int64_t range_end = it->ranges.v[it->current_range].end;
            it->current_range++;

            /* Merge any immediately adjacent committed ranges */
            while (it->current_range < it->ranges.count &&
                   it->ranges.v[it->current_range].start == range_end) {
                range_end = it->ranges.v[it->current_range].end;
                it->current_range++;
            }

            if (it->current_range >= it->ranges.count) {
                gap_end = it->fixed_end_of_stream;
                it->current_gap_end = PAYLOAD_ITERATOR_NOT_SET;
            } else {
                gap_end = it->ranges.v[it->current_range].start;
                it->current_gap_end = gap_end;
            }
            if (mmapqi_payload_iterator_init(it->inner, range_end, gap_end) != 0)
                return classify_payload_failure(it, range_end, it->fixed_end_of_stream)
                           == MMAPQI_WALK_OVERTAKEN ? MMAPQI_WALK_RESTART : MMAPQ_ECORRUPT;
        } else {
            /* start is before the next committed range — gap up to range_start */
            gap_end = range_start;
            it->current_gap_end = gap_end;
            if (mmapqi_payload_iterator_init(it->inner, start, gap_end) != 0)
                return classify_payload_failure(it, start, it->fixed_end_of_stream)
                           == MMAPQI_WALK_OVERTAKEN ? MMAPQI_WALK_RESTART : MMAPQ_ECORRUPT;
        }
    }
    return 0;
}

/* ── Move to next gap ────────────────────────────────────────────────────── */

static int switch_to_next_gap(MmapqUncommittedPayloadIterator *it)
{
    /* Consume the current committed range and any adjacent ones */
    if (it->current_range >= it->ranges.count) return 0;

    int64_t committed_end = it->ranges.v[it->current_range].end;
    it->current_range++;

    /* Merge adjacent committed ranges */
    while (it->current_range < it->ranges.count &&
           it->ranges.v[it->current_range].start <= committed_end) {
        committed_end = it->ranges.v[it->current_range].end;
        it->current_range++;
    }

    if (committed_end >= it->fixed_end_of_stream) {
        return 0;  /* no more gaps */
    }

    int64_t gap_end;
    if (it->current_range >= it->ranges.count) {
        gap_end = it->fixed_end_of_stream;
        it->current_gap_end = PAYLOAD_ITERATOR_NOT_SET;  /* trailing gap */
    } else {
        gap_end = it->ranges.v[it->current_range].start;
        it->current_gap_end = gap_end;
    }

    /* committed_end and gap_end derive from ranges built only from
     * non-archived entries. Pre-CAND-58 an init failure collapsed to
     * exhaustion (0) on the "valid by construction" argument; the CAND-58
     * transient exists though — an archive racing the drain raises the live
     * start_of_stream past a frozen range end — so classify it: below the
     * re-read line → the use-phase restart; at/above → a broken invariant,
     * ECORRUPT. */
    if (mmapqi_payload_iterator_init(it->inner, committed_end, gap_end) != 0)
        return classify_payload_failure(it, committed_end, it->fixed_end_of_stream)
                   == MMAPQI_WALK_OVERTAKEN ? MMAPQI_WALK_RESTART : MMAPQ_ECORRUPT;
    return 1;
}

/* ── mmapqi_uncommitted_iterator_create ─────────────────────────────────────────── */

int mmapqi_uncommitted_iterator_create(
    const char      *queue_name,
    uint8_t          appender_id,
    int64_t        (*start_of_stream_offset)(void *ctx),
    int64_t        (*end_of_stream_offset)(void *ctx),
    void            *stream_ctx,
    int64_t        (*archive_offset)(void *ctx),
    int64_t        (*archive_offset_sc)(void *ctx),
    RegionAccessor  *header_accessor,
    MmapqPayloadIterator *inner,
    int64_t        (*queue_start_index)(void *ctx),
    void            *queue_start_index_ctx,
    int           (*refill)(void *ctx, int64_t offset),
    void            *refill_ctx,
    MmapqUncommittedPayloadIterator **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (start_of_stream_offset == NULL) return MMAPQ_EINVAL;
    if (end_of_stream_offset == NULL) return MMAPQ_EINVAL;
    if (archive_offset == NULL) return MMAPQ_EINVAL;
    if (archive_offset_sc == NULL) return MMAPQ_EINVAL;
    if (header_accessor == NULL) return MMAPQ_EINVAL;
    if (inner == NULL) return MMAPQ_EINVAL;
    if (queue_start_index == NULL) return MMAPQ_EINVAL;
    if (refill == NULL) return MMAPQ_EINVAL;

    MmapqUncommittedPayloadIterator *it = calloc(1, sizeof(MmapqUncommittedPayloadIterator));
    if (it == NULL) return MMAPQ_ENOMEM;

    it->queue_name              = queue_name;
    it->appender_id             = appender_id;
    it->start_of_stream_offset  = start_of_stream_offset;
    it->end_of_stream_offset    = end_of_stream_offset;
    it->stream_ctx              = stream_ctx;
    it->archive_offset          = archive_offset;
    it->archive_offset_sc       = archive_offset_sc;
    it->header_accessor         = header_accessor;
    it->inner                   = inner;
    it->queue_start_index       = queue_start_index;
    it->queue_start_index_ctx   = queue_start_index_ctx;
    it->refill                  = refill;
    it->refill_ctx              = refill_ctx;

    it->current_gap_end         = PAYLOAD_ITERATOR_NOT_SET;
    it->archive_line            = PAYLOAD_ITERATOR_NOT_SET;
    it->init_start_offset       = PAYLOAD_ITERATOR_NOT_SET;
    it->init_end_offset         = PAYLOAD_ITERATOR_NOT_SET;
    it->fixed_end_of_stream     = PAYLOAD_ITERATOR_NOT_SET;
    it->walk_ovt_floor          = PAYLOAD_ITERATOR_NOT_SET;
    it->walk_ovt_domain         = MMAPQI_WALK_DOM_PAYLOAD;
    it->initialised             = 0;

    *out = it;
    return 0;
}

/* ── mmapqi_uncommitted_iterator_init ───────────────────────────────────────────── */

int mmapqi_uncommitted_iterator_init(
    MmapqUncommittedPayloadIterator *it,
    int64_t                     start_offset,
    int64_t                     end_offset)
{
    if (it == NULL) return MMAPQ_EINVAL;

    /* CAND-58 §4.4: the factory's restart loop. A walk-ending failure that
     * classifies OVERTAKEN (the walk's snapshot was overtaken by the archive
     * line / start_index) re-runs the init from fresh state — scan_floor and
     * the line are re-read, the ranges rebuilt, and the §4.3 clamp floors
     * the re-run's span skip and gap walk at the re-read line, so every
     * restart begins at or above the floor that classified the failure.
     * Never surfaced to the caller. The no-progress detector (per floor
     * domain) plus the last-resort cap bound the loop. */
    int64_t last_hdr_floor = INT64_MIN, last_line_floor = INT64_MIN;
    int     restarts = 0;

    for (;;) {

    /* D-G publication rule (CAND-49 §3.9, doc/design/archive-boundaries.md):
     * acquire-load the queue start index FIRST, then the archive line.
     * A stale pairing is then only ever the harmless kind (old si with
     * either line value); the harmful pair — new si, old line — is
     * unreachable. Reading the line before si is the defect. */
    int64_t scan_floor = it->queue_start_index(it->queue_start_index_ctx);
    int64_t line       = it->archive_offset(it->stream_ctx);

    /* Legacy zero line: below INITIAL_PAYLOAD_POSITION the file predates
     * 0.2.0 — refuse (recreate the queue; CAND-32 range-check precedent). */
    if (line < INITIAL_PAYLOAD_POSITION) return MMAPQ_ECORRUPT;

    int64_t stream_start = it->start_of_stream_offset(it->stream_ctx);

    /* Reject out-of-range caller offsets as plain argument validation,
     * before any state is touched (start > end when both are set is EINVAL too). */
    if (start_offset != PAYLOAD_ITERATOR_NOT_SET) {
        if (start_offset < stream_start) return MMAPQ_EINVAL;
    }
    if (end_offset != PAYLOAD_ITERATOR_NOT_SET) {
        if (end_offset < stream_start) return MMAPQ_EINVAL;
    }
    if (start_offset != PAYLOAD_ITERATOR_NOT_SET &&
        end_offset   != PAYLOAD_ITERATOR_NOT_SET &&
        start_offset > end_offset) return MMAPQ_EINVAL;

    it->init_start_offset = start_offset;
    it->init_end_offset   = end_offset;

    int64_t stream_end = it->end_of_stream_offset(it->stream_ctx);
    it->fixed_end_of_stream = (end_offset == PAYLOAD_ITERATOR_NOT_SET)
        ? stream_end
        : (end_offset < stream_end ? end_offset : stream_end);

    /* The gap walk starts at the archive line (never start_offset); a
     * caller-supplied start below the line clamps up to it (§3.9). */
    int64_t effective_start = (start_offset == PAYLOAD_ITERATOR_NOT_SET)
        ? line
        : start_offset;
    if (effective_start < line) effective_start = line;
    it->archive_line = line;

    /* Scan headers to build committed ranges. Three-way (D8): a negative
     * code propagates verbatim (MMAPQ_ETIMEDOUT is transient — retry with a
     * NEW factory call, not rewind, which returns EINVAL after a failed
     * init); ECORRUPT is genuine truncation — a committed header's payload
     * would not walk for a non-timeout reason. CAND-58: MMAPQI_WALK_RESTART
     * is the shared classification's OVERTAKEN verdict — restart from fresh
     * state, never surfaced. */
    int src = scan_committed_ranges(it, scan_floor, line);
    if (src == MMAPQI_WALK_RESTART) {
        int64_t *slot = it->walk_ovt_domain == MMAPQI_WALK_DOM_HEADER
                        ? &last_hdr_floor : &last_line_floor;
        if (mmapqi_walk_restart_permitted(it->walk_ovt_floor, slot, &restarts) != 0) {
            it->initialised = 0;
            return MMAPQ_ECORRUPT;
        }
        continue;
    }
    if (src < 0) {
        it->initialised = 0;
        return src;
    }

    /* D-B: sort + merge the raw spans once (out-of-order commits). */
    mmapqi_ranges_sort_merge(&it->ranges);

    /* Prune committed ranges that end before our start (sorted array). */
    mmapqi_ranges_prune_up_to(&it->ranges, effective_start);
    it->current_range = 0;

    /* Position inner iterator at first gap */
    int fgr = init_first_gap(it, effective_start);
    if (fgr == MMAPQI_WALK_RESTART) {
        if (mmapqi_walk_restart_permitted(it->walk_ovt_floor, &last_line_floor, &restarts) != 0) {
            it->initialised = 0;
            return MMAPQ_ECORRUPT;
        }
        continue;
    }
    if (fgr != 0) {
        it->initialised = 0;
        return MMAPQ_ECORRUPT;
    }

    it->initialised = 1;
    return 0;

    }  /* restart loop */
}

/* ── mmapqi_uncommitted_iterator_advance ────────────────────────────────────────── */

/* CAND-58 §4.4 — the use-phase restart: an overtaken mid-drain failure
 * prunes the committed ranges to the new line and resumes the gap walk at
 * max(current position, new line). Records below the new line are
 * uncommittable by design (L(i)) and are dropped from recovery,
 * consistently. The resume's gap init can itself fail against a further-risen
 * floor — loop through the detector. Returns 1 when resumed, MMAPQ_ECORRUPT
 * when the detector or the resume refuses. */
static int use_phase_restart(MmapqUncommittedPayloadIterator *it,
                             int64_t *last_line, int *restarts)
{
    for (;;) {
        if (mmapqi_walk_restart_permitted(it->walk_ovt_floor, last_line, restarts) != 0)
            return MMAPQ_ECORRUPT;
        mmapqi_ranges_prune_up_to(&it->ranges, it->walk_ovt_floor);
        it->archive_line  = it->walk_ovt_floor;
        it->current_range = 0;
        int64_t resume = it->inner->next_probing_offset > it->walk_ovt_floor
                         ? it->inner->next_probing_offset : it->walk_ovt_floor;
        int fgr = init_first_gap(it, resume);
        if (fgr == MMAPQI_WALK_RESTART) continue;   /* the floor rose again */
        if (fgr != 0) return MMAPQ_ECORRUPT;
        return 1;
    }
}

int mmapqi_uncommitted_iterator_advance(MmapqUncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL,         "mmapqi_uncommitted_iterator_advance: it is NULL");
    if (!it->initialised) return 0;

    C58_HOOK_USE_PHASE_STEP(it->inner->next_probing_offset);

    /* The no-progress detector state is per CALL: after a restart the probing
     * position is >= the recorded floor and only moves forward, so a later
     * failure in a following call can never classify below the same floor. */
    int64_t last_line = INT64_MIN;
    int     restarts  = 0;

    while (1) {
        /* CAND-49 §3.2 pre-check (D-A): a bounded gap whose probing offset
         * reached current_gap_end is exhausted — switch gaps WITHOUT calling
         * the inner advance. The inner iterator never probes at a committed
         * range's start. */
        if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
            it->inner->next_probing_offset == it->current_gap_end) {
            int sgr = switch_to_next_gap(it);
            if (sgr == MMAPQI_WALK_RESTART) {
                if (use_phase_restart(it, &last_line, &restarts) != 1)
                    return MMAPQ_ECORRUPT;
                continue;
            }
            if (sgr < 0) return sgr;   /* ECORRUPT */
            if (sgr == 0) {
                return 0;  /* no more gaps */
            }
            continue;
        }

        /* CAND-51e (finding #4; owner ruling: adopted, generalised): a
         * padding skip — short (<= 4 bytes) or a long-padding marker —
         * whose target equals current_gap_end is gap exhaustion; the walker
         * never reads a length at or past the gap end. Principle: a walker
         * applies the writer's rule at each position, and the gap end is
         * where the committed record begins. The batch twin skips both
         * kinds inline and re-tests the gap end at its loop top
         * (uncommitted_batch_iterator.c); here the skip happens inside the
         * inner advance (payload_walk), so the landing must be detected
         * before it. */
        if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
            it->inner->next_probing_offset < it->current_gap_end) {
            int64_t p = it->inner->next_probing_offset;
            WrapResult wr = region_accessor_wrap(it->inner->payload_accessor,
                                                 p, &it->inner->payload_buf);
            if (wr != WRAP_SUCCESS) {
                if (wr == WRAP_MAP_TIMED_OUT) return MMAPQ_ETIMEDOUT;
                /* CAND-58: a map miss classifies (the use phase is bounded
                 * by fixed_end_of_stream — positions below it were written
                 * at init): below the line → the use-phase restart;
                 * at/above → ECORRUPT. EOS is unreachable here (p is below
                 * the gap end, hence below fixed_end_of_stream). */
                if (wr == WRAP_FILE_NOT_FOUND || wr == WRAP_BEYOND_FILE_SIZE) {
                    MmapqiWalkClass c = classify_payload_failure(it, p,
                                                                 it->fixed_end_of_stream);
                    if (c == MMAPQI_WALK_OVERTAKEN) {
                        if (use_phase_restart(it, &last_line, &restarts) != 1)
                            return MMAPQ_ECORRUPT;
                        continue;
                    }
                    return MMAPQ_ECORRUPT;
                }
                return MMAPQ_ECORRUPT;
            }
            int64_t cap = it->inner->payload_buf.capacity;
            int64_t target = p;                     /* no padding */
            if (cap <= PAYLOAD_LENGTH_SIZE) {
                target = p + cap;                   /* short padding */
            } else {
                int32_t v;
                memcpy(&v, it->inner->payload_buf.data + PAYLOAD_LENGTH_OFFSET,
                       sizeof(int32_t));
                if (v < 0) {
                    int64_t skip = -(int64_t)v + PAYLOAD_LENGTH_SIZE;
                    if (skip != cap)
                        return MMAPQ_ECORRUPT;      /* the exact-extent rule */
                    target = p + skip;              /* long-padding marker */
                }
            }
            if (target == it->current_gap_end) {
                /* the gap's remainder is padding: exhausted */
                it->inner->next_probing_offset = it->current_gap_end;
                continue;
            }
            if (target > it->current_gap_end)
                return MMAPQ_ECORRUPT;  /* padding over a committed start: torn */
        }

        /* D8: three-way — a negative error from the inner advance
         * propagates; 0 means the current gap is exhausted. A negative is
         * truthy in C: `if (advance(...))` here would report an error as a
         * found message (stale buffer), and the `while (advance(...))`
         * test idiom would loop forever on a repeating error. */
        int r = mmapqi_payload_iterator_advance(it->inner);
        /* CAND-58: a map miss (MMAPQI_WALK_MAP_MISS) classifies. The EOS
         * verdict is reachable on the trailing gap when a terminal padding
         * skip lands past the frozen end of stream: the frozen stream is
         * exhausted by definition — report exhaustion (0). */
        if (r == MMAPQI_WALK_MAP_MISS) {
            MmapqiWalkClass c = classify_payload_failure(it,
                it->inner->next_probing_offset, it->fixed_end_of_stream);
            if (c == MMAPQI_WALK_OVERTAKEN) {
                if (use_phase_restart(it, &last_line, &restarts) != 1)
                    return MMAPQ_ECORRUPT;
                continue;
            }
            if (c == MMAPQI_WALK_EOS) return 0;
            return MMAPQ_ECORRUPT;
        }
        if (r < 0) return r;
        if (r == 0) {
            /* CAND-49 §3.2 undershoot check: a bounded gap must end exactly
             * at current_gap_end; stopping short is torn framing —
             * MMAPQ_ECORRUPT (previously a silent end of stream). */
            if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
                it->inner->next_probing_offset != it->current_gap_end)
                return MMAPQ_ECORRUPT;
            /* Current gap exhausted — move to next gap */
            int sgr = switch_to_next_gap(it);
            if (sgr == MMAPQI_WALK_RESTART) {
                if (use_phase_restart(it, &last_line, &restarts) != 1)
                    return MMAPQ_ECORRUPT;
                continue;
            }
            if (sgr < 0) return sgr;   /* ECORRUPT */
            if (sgr == 0) {
                return 0;  /* no more gaps */
            }
            continue;
        }

        /* CAND-49 §3.2 post-yield corruption check (replaces the pre-fix
         * MMAP_ASSERT here, which compiled out in release): the message
         * must sit cleanly before the next committed range — an overlap or
         * an overshoot past the range's start is corruption. Runtime check
         * in both configs (owner ruling). */
        if (it->current_range < it->ranges.count &&
            (it->inner->current_message_offset >= it->ranges.v[it->current_range].start ||
             it->inner->next_probing_offset   >  it->ranges.v[it->current_range].start))
            return MMAPQ_ECORRUPT;

        /* Refill on yield (CAND-49 §3.3): make the yielded offset
         * committable via the owning appender's outstanding set (the
         * callback skips the flag-tracked tail). ENOMEM: park the inner
         * iterator back at this message so the state is unchanged and the
         * next advance re-walks and re-yields it (ruling 5). */
        int64_t msg_off = it->inner->current_message_offset;
        int rrc = it->refill(it->refill_ctx, msg_off);
        if (rrc != 0) {
            it->inner->next_probing_offset    = msg_off;
            it->inner->current_message_offset = PAYLOAD_ITERATOR_NOT_SET;
            it->inner->current_message_length = (int32_t)PAYLOAD_ITERATOR_NOT_SET;
            return rrc;
        }

        return 1;  /* found an uncommitted message */
    }
}

/* ── mmapqi_uncommitted_iterator_has_next ───────────────────────────────────────── */

int mmapqi_uncommitted_iterator_has_next(const MmapqUncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "mmapqi_uncommitted_iterator_has_next: it is NULL");
    if (!it->initialised) return 0;

    if (mmapqi_payload_iterator_has_next(it->inner)) return 1;

    /* Check if any gap remains after current committed ranges */
    if (it->current_range >= it->ranges.count) return 0;

    /* A gap exists if committed_end < fixed_end_of_stream */
    return it->ranges.v[it->current_range].end < it->fixed_end_of_stream;
}

/* ── mmapqi_uncommitted_iterator_current ────────────────────────────────────────── */

Buffer *mmapqi_uncommitted_iterator_current(MmapqUncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "mmapqi_uncommitted_iterator_current: it is NULL");
    return mmapqi_payload_iterator_current(it->inner);
}

/* ── mmapqi_uncommitted_iterator_rewind ─────────────────────────────────────────── */

int mmapqi_uncommitted_iterator_rewind(MmapqUncommittedPayloadIterator *it)
{
    if (it == NULL) return MMAPQ_EINVAL;
    if (!it->initialised) return MMAPQ_EINVAL;

    return mmapqi_uncommitted_iterator_init(it, it->init_start_offset, it->init_end_offset);
}

/* ── mmapqi_uncommitted_iterator_destroy ────────────────────────────────────────── */

void mmapqi_uncommitted_iterator_destroy(MmapqUncommittedPayloadIterator *it)
{
    if (it == NULL) return;
    free(it->ranges.v);
    /* inner and header_accessor are NOT owned */
    free(it);
}
