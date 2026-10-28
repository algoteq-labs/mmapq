#include "internal/uncommitted_batch_iterator.h"
#include "internal/header_reader.h"
#include "internal/walk_classify.h"
#include "internal/test_hooks.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>

/* ── Header scan: build committed batch spans ────────────────────────────── */

/* Classify a payload-side walk failure at `position` (the batch twin of the
 * single-message helper in uncommitted_payload_iterator.c — the scan
 * skeletons mirror per-format by design, §3.6; do not unify them). On
 * OVERTAKEN the classifying floor is recorded for the restart loop. */
static MmapqiWalkClass classify_payload_failure(MmapqUncommittedBatchIterator *it,
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

/* Return protocol (CAND-58): 0 = success; MMAPQI_WALK_RESTART = a
 * walk-ending failure classified OVERTAKEN (the floor is in
 * it->walk_ovt_floor); a negative MMAPQ_E* verbatim. */
/* The scan skeleton mirrors uncommitted_payload_iterator.c per-format by
 * design (§3.6) — do not unify them. The single-message scan's contiguous
 * fast path has no batch analogue: every committed batch is measured by its
 * count + message walk. header_index is the frozen queue start index read by
 * init BEFORE the archive line (the D-G publication rule, §3.9). */

static int scan_committed_batch_ranges(MmapqUncommittedBatchIterator *it,
                                       int64_t header_index,
                                       int64_t scan_line)
{
    mmapqi_ranges_clear(&it->ranges);
    it->current_range = 0;

    int64_t stream_start = it->start_of_stream_offset(it->stream_ctx);
    /* The §4.3 clamp (the load-bearing F2 clamp): the span measurement never
     * probes below max(start_of_stream, the archive-line snapshot). */
    int64_t span_floor = stream_start > scan_line ? stream_start : scan_line;

    while (1) {
        /* Checked read (D8/F2): a mapping failure fails the scan with its
         * code instead of looking like the end of committed headers. */
        uint64_t header;
        C58_HOOK_SCAN_HEADER_PRE_READ(header_index);
        int hrc = mmapqi_header_read_checked(it->header_accessor, &it->header_buf,
                                             header_index, &header);
        if (hrc != 0) return hrc;
        if (header == HEADER_NULL) {
            /* CAND-58: NULL below the CURRENT start_index = the scan's
             * snapshot was overtaken (the header file reclaimed mid-scan) —
             * restart from fresh state; at/above it is the end of committed
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
            break;   /* end of committed headers */
        }
        header_index++;

        if (header_appender_id(header) != it->appender_id) continue;

        int64_t pos = header_payload_position(header);
        /* Archived entry: payload gone by design, never uncommitted — skip
         * without consulting the payload stream. (CAND-58: the skip floors
         * at max(start_of_stream, the line snapshot) — the §4.3 clamp.) */
        if (pos < span_floor) continue;

        /* Measure the committed batch's span: the count is trusted as
         * written (CAND-23 docs); the message walk applies the full framing
         * rules (short/long padding). Three-way: errors propagate verbatim.
         * CAND-58: a map miss (MMAPQI_WALK_MAP_MISS) classifies first — the
         * span start is a committed header's position, always known-written:
         * below the line → overtaken restart; at/above → ECORRUPT. */
        C58_HOOK_SCAN_SPAN_PRE_WALK(pos, stream_start, it->archive_line);
        int32_t count = mmapq_batch_iterator_read_message_count(&it->msgs, pos);
        if (count == MMAPQI_WALK_MAP_MISS) {
            if (classify_payload_failure(it, pos, pos + 1) == MMAPQI_WALK_OVERTAKEN)
                return MMAPQI_WALK_RESTART;
            return MMAPQ_ECORRUPT;
        }
        if (count < 0) return count;
        if (count == 0) return MMAPQ_ECORRUPT;   /* a committed batch is never empty */
        mmapq_batch_iterator_init(&it->msgs, pos, count, 0);
        mmapq_batch_iterator_exhaust(&it->msgs);
        if (it->msgs.last_error == MMAPQI_WALK_MAP_MISS) {
            if (classify_payload_failure(it, pos, pos + 1) == MMAPQI_WALK_OVERTAKEN)
                return MMAPQI_WALK_RESTART;
            return MMAPQ_ECORRUPT;
        }
        if (it->msgs.last_error != 0) return it->msgs.last_error;
        mmapqi_ranges_append(&it->ranges, pos,
                             batch_iterator_next_payload_position(&it->msgs));
    }
    return 0;
}

/* ── Gap positioning (positions only — no wraps here) ────────────────────── */

static void init_first_gap(MmapqUncommittedBatchIterator *it, int64_t start)
{
    /* Skip committed ranges that end <= start */
    while (it->current_range < it->ranges.count &&
           it->ranges.v[it->current_range].end <= start) {
        it->current_range++;
    }

    if (it->current_range >= it->ranges.count) {
        it->gap_pos         = start;
        it->current_gap_end = PAYLOAD_ITERATOR_NOT_SET;  /* trailing gap */
        return;
    }

    int64_t range_start = it->ranges.v[it->current_range].start;
    if (start >= range_start) {
        /* start is inside a committed range — skip past it (and any
         * immediately adjacent ranges) */
        int64_t range_end = it->ranges.v[it->current_range].end;
        it->current_range++;
        while (it->current_range < it->ranges.count &&
               it->ranges.v[it->current_range].start == range_end) {
            range_end = it->ranges.v[it->current_range].end;
            it->current_range++;
        }
        it->gap_pos = range_end;
        it->current_gap_end = (it->current_range >= it->ranges.count)
            ? PAYLOAD_ITERATOR_NOT_SET
            : it->ranges.v[it->current_range].start;
    } else {
        it->gap_pos         = start;
        it->current_gap_end = range_start;
    }
}

static int switch_to_next_gap(MmapqUncommittedBatchIterator *it)
{
    if (it->current_range >= it->ranges.count) return 0;

    int64_t committed_end = it->ranges.v[it->current_range].end;
    it->current_range++;
    while (it->current_range < it->ranges.count &&
           it->ranges.v[it->current_range].start <= committed_end) {
        committed_end = it->ranges.v[it->current_range].end;
        it->current_range++;
    }

    if (committed_end >= it->fixed_end_of_stream) return 0;  /* no more gaps */

    it->gap_pos = committed_end;
    it->current_gap_end = (it->current_range >= it->ranges.count)
        ? PAYLOAD_ITERATOR_NOT_SET
        : it->ranges.v[it->current_range].start;
    return 1;
}

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

int mmapqi_uncommitted_batch_iterator_create(
    const char      *queue_name,
    uint8_t          appender_id,
    int64_t        (*start_of_stream_offset)(void *ctx),
    int64_t        (*end_of_stream_offset)(void *ctx),
    void            *stream_ctx,
    int64_t        (*archive_offset)(void *ctx),
    int64_t        (*archive_offset_sc)(void *ctx),
    RegionAccessor  *header_accessor,
    RegionAccessor  *payload_acc,
    int64_t        (*queue_start_index)(void *ctx),
    void            *queue_start_index_ctx,
    int           (*refill)(void *ctx, int64_t offset),
    void            *refill_ctx,
    MmapqUncommittedBatchIterator **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (start_of_stream_offset == NULL) return MMAPQ_EINVAL;
    if (end_of_stream_offset == NULL) return MMAPQ_EINVAL;
    if (archive_offset == NULL) return MMAPQ_EINVAL;
    if (archive_offset_sc == NULL) return MMAPQ_EINVAL;
    if (header_accessor == NULL) return MMAPQ_EINVAL;
    if (payload_acc == NULL) return MMAPQ_EINVAL;
    if (queue_start_index == NULL) return MMAPQ_EINVAL;
    if (refill == NULL) return MMAPQ_EINVAL;

    MmapqUncommittedBatchIterator *it = calloc(1, sizeof(MmapqUncommittedBatchIterator));
    if (it == NULL) return MMAPQ_ENOMEM;

    it->queue_name            = queue_name;
    it->appender_id           = appender_id;
    it->start_of_stream_offset = start_of_stream_offset;
    it->end_of_stream_offset  = end_of_stream_offset;
    it->stream_ctx            = stream_ctx;
    it->archive_offset        = archive_offset;
    it->archive_offset_sc     = archive_offset_sc;
    it->header_accessor       = header_accessor;
    it->payload_acc           = payload_acc;
    it->queue_start_index     = queue_start_index;
    it->queue_start_index_ctx = queue_start_index_ctx;
    it->refill                = refill;
    it->refill_ctx            = refill_ctx;

    it->current_gap_end   = PAYLOAD_ITERATOR_NOT_SET;
    it->archive_line      = PAYLOAD_ITERATOR_NOT_SET;
    it->fixed_end_of_stream = PAYLOAD_ITERATOR_NOT_SET;
    it->walk_ovt_floor    = PAYLOAD_ITERATOR_NOT_SET;
    it->walk_ovt_domain   = MMAPQI_WALK_DOM_PAYLOAD;
    it->msgs.queue_name   = queue_name;
    it->msgs.payload_acc  = payload_acc;
    /* CAND-58: the walker-facing batch iterator reports map misses as
     * MMAPQI_WALK_MAP_MISS (never ECORRUPT) so the scan/next classify them */
    it->msgs.walk_mode    = 1;
    it->initialised       = 0;

    *out = it;
    return 0;
}

int mmapqi_uncommitted_batch_iterator_init(MmapqUncommittedBatchIterator *it)
{
    if (it == NULL) return MMAPQ_EINVAL;

    /* CAND-58 §4.4: the factory's restart loop (the single-message twin in
     * uncommitted_payload_iterator.c carries the full comment). Every
     * restart begins at or above the floor that classified the failure (the
     * §4.3 clamp in the re-run); the no-progress detector (per floor domain)
     * plus the last-resort cap bound the loop. Never surfaced. */
    int64_t last_hdr_floor = INT64_MIN, last_line_floor = INT64_MIN;
    int     restarts = 0;

    for (;;) {

    /* D-G publication rule (§3.9, doc/design/archive-boundaries.md):
     * acquire-load the queue start index FIRST, then the archive line.
     * Reading the line before si is the defect. */
    int64_t scan_floor = it->queue_start_index(it->queue_start_index_ctx);
    int64_t line       = it->archive_offset(it->stream_ctx);

    /* Zero line: a corrupt appender meta — refuse (pre-0.2.0 queues are
     * refused at open with MMAPQ_ESCHEMA). */
    if (line < INITIAL_PAYLOAD_POSITION) return MMAPQ_ECORRUPT;

    it->fixed_end_of_stream = it->end_of_stream_offset(it->stream_ctx);
    it->archive_line        = line;

    int src = scan_committed_batch_ranges(it, scan_floor, line);
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
    mmapqi_ranges_prune_up_to(&it->ranges, line);
    it->current_range = 0;

    init_first_gap(it, line);
    it->pending_advance = 0;
    it->initialised     = 1;
    return 0;

    }  /* restart loop */
}

/* ── next() ──────────────────────────────────────────────────────────────── */

/* Walk past the previously yielded batch (deferred advance — §3.6 step 4
 * after the yield in step 3). Three-way via the batch iterator's last_error. */
static int advance_past_yielded(MmapqUncommittedBatchIterator *it)
{
    mmapq_batch_iterator_init(&it->msgs, it->yielded_offset, it->yielded_count, 0);
    mmapq_batch_iterator_exhaust(&it->msgs);
    /* CAND-58: a map miss walking the yielded batch classifies: below the
     * re-read line → the use-phase restart (the batch is uncommittable by
     * L(i) and dropped); at/above → ECORRUPT (today's behaviour). */
    if (it->msgs.last_error == MMAPQI_WALK_MAP_MISS) {
        if (classify_payload_failure(it, it->yielded_offset,
                                     it->fixed_end_of_stream)
                == MMAPQI_WALK_OVERTAKEN)
            return MMAPQI_WALK_RESTART;
        return MMAPQ_ECORRUPT;
    }
    if (it->msgs.last_error != 0) return it->msgs.last_error;
    it->gap_pos = batch_iterator_next_payload_position(&it->msgs);

    /* Bounded gap: the batch must end at or before the boundary. */
    if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
        it->gap_pos > it->current_gap_end)
        return MMAPQ_ECORRUPT;   /* overshoot */
    return 0;
}

/* CAND-58 §4.4 — the use-phase restart (the single-message twin in
 * uncommitted_payload_iterator.c carries the full comment): prune the
 * committed ranges to the new line and resume the gap walk at max(current
 * position, new line); records below the new line are uncommittable (L(i))
 * and dropped from recovery. Positions only — no wraps here. Returns 1 when
 * resumed, MMAPQ_ECORRUPT when the detector refuses. */
static int use_phase_restart(MmapqUncommittedBatchIterator *it,
                             int64_t *last_line, int *restarts)
{
    if (mmapqi_walk_restart_permitted(it->walk_ovt_floor, last_line, restarts) != 0)
        return MMAPQ_ECORRUPT;
    mmapqi_ranges_prune_up_to(&it->ranges, it->walk_ovt_floor);
    it->archive_line  = it->walk_ovt_floor;
    it->current_range = 0;
    int64_t resume = it->gap_pos > it->walk_ovt_floor
                     ? it->gap_pos : it->walk_ovt_floor;
    init_first_gap(it, resume);
    return 1;
}

int mmapqi_uncommitted_batch_iterator_next(
    MmapqUncommittedBatchIterator *it,
    int64_t     *batch_offset,
    int32_t     *message_count,
    MmapqBatchPayloadIterator **msgs)
{
    MMAP_ASSERT(it != NULL, "mmapqi_uncommitted_batch_iterator_next: it is NULL");
    if (!it->initialised) return 0;

    C58_HOOK_USE_PHASE_STEP(it->gap_pos);

    /* Per-call no-progress detector state (see the single-message twin). */
    int64_t last_line = INT64_MIN;
    int     restarts  = 0;

    /* Deferred advance past the previously yielded batch. */
    if (it->pending_advance) {
        int arc = advance_past_yielded(it);
        if (arc == MMAPQI_WALK_RESTART) {
            /* the yielded batch fell below the new line — dropped by
             * design (L(i)); resume above it */
            if (use_phase_restart(it, &last_line, &restarts) != 1)
                return MMAPQ_ECORRUPT;
            it->pending_advance = 0;
        } else {
            if (arc != 0) return arc;
            it->pending_advance = 0;
        }
    }

    while (1) {
        /* Bounded-gap pre-check (§3.2 batch analogue): the boundary is a
         * committed range's start — never probe it. */
        if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
            it->gap_pos == it->current_gap_end) {
            if (!switch_to_next_gap(it)) return 0;
            continue;
        }
        /* End of the frozen stream (covers a boundary position whose file
         * does not exist yet). */
        if (it->gap_pos >= it->fixed_end_of_stream) return 0;

        /* Wrap at the position; three-way, never collapsed into
         * exhaustion. CAND-58: a map miss classifies — the use phase is
         * bounded by fixed_end_of_stream (positions below it were written
         * at init): below the re-read line → the use-phase restart;
         * at/above → ECORRUPT. (EOS is unreachable: positions past the
         * frozen end return 0 above without wrapping.) */
        WrapResult wr = region_accessor_wrap(it->payload_acc, it->gap_pos,
                                             &it->msgs.payload_buf);
        if (wr != WRAP_SUCCESS) {
            if (wr == WRAP_MAP_TIMED_OUT) return MMAPQ_ETIMEDOUT;
            if (wr == WRAP_FILE_NOT_FOUND || wr == WRAP_BEYOND_FILE_SIZE) {
                if (classify_payload_failure(it, it->gap_pos,
                                             it->fixed_end_of_stream)
                        == MMAPQI_WALK_OVERTAKEN) {
                    if (use_phase_restart(it, &last_line, &restarts) != 1)
                        return MMAPQ_ECORRUPT;
                    continue;
                }
                return MMAPQ_ECORRUPT;
            }
            return MMAPQ_ECORRUPT;
        }

        int32_t cap = it->msgs.payload_buf.capacity;

        /* CAND-51e unified tail rule: <= 4 bytes left means padding —
         * skip to the next region; a batch count slot never occupies a
         * region tail (the ensure_space_for_batch_header rule). */
        if (cap <= MMAPQ_BATCH_MSG_COUNT_SIZE) {
            it->gap_pos += cap;
            continue;
        }

        int32_t count;
        memcpy(&count, it->msgs.payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET,
               sizeof(int32_t));

        if (count == 0) {
            /* A count is written only by complete/complete_uncommitted/
             * non-empty discard/join completion, so a count-0 record can
             * exist only at the true tail. */
            if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET)
                return MMAPQ_ECORRUPT;   /* undershoot: torn framing in a bounded gap */
            return 0;                    /* trailing gap: end of the walk */
        }

        if (count < 0) {
            /* §3.8 boundary padding marker (D-F): legal region-skip iff its
             * extent is exactly the rest of the region, else ECORRUPT. */
            int64_t skip = -(int64_t)count + MMAPQ_BATCH_MSG_COUNT_SIZE;
            if (skip != cap) return MMAPQ_ECORRUPT;
            it->gap_pos += cap;
            continue;
        }

        /* count > 0: a staged batch. Refill BEFORE publishing the yield —
         * on ENOMEM nothing advanced: the next next() re-probes the same
         * position and re-yields the same batch (ruling 5). */
        int rrc = it->refill(it->refill_ctx, it->gap_pos);
        if (rrc != 0) return rrc;

        *batch_offset  = it->gap_pos;
        *message_count = count;
        mmapq_batch_iterator_init(&it->msgs, it->gap_pos, count, 0);
        *msgs = &it->msgs;

        it->yielded_offset  = it->gap_pos;
        it->yielded_count   = count;
        it->pending_advance = 1;
        return 1;
    }
}

void mmapqi_uncommitted_batch_iterator_destroy(MmapqUncommittedBatchIterator *it)
{
    if (it == NULL) return;
    free(it->ranges.v);
    /* accessors are NOT owned */
    free(it);
}
