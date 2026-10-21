#include "internal/uncommitted_payload_iterator.h"
#include "internal/header_reader.h"
#include "internal/payload_walk.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Header scan: build committed ranges ─────────────────────────────────── */

/* header_index is the frozen queue start index read by init BEFORE the
 * archive line (the D-G publication rule, CAND-49 §3.9). */
static int scan_committed_ranges(MmapqUncommittedPayloadIterator *it,
                                 int64_t header_index)
{
    mmapqi_ranges_clear(&it->ranges);
    it->current_range = 0;

    /* S3 (D8): never scan headers below the queue's start index — those
     * files may have been reclaimed, and entries below start_of_stream are
     * archived by definition, never uncommitted. One frozen value per scan
     * (read by init before the archive line); a concurrent archive advancing
     * it further is covered by the deletion protocol (archiving-design.md),
     * mirroring the appender join's scan_floor. */
    int64_t current_start = PAYLOAD_ITERATOR_NOT_SET;
    int64_t current_end   = PAYLOAD_ITERATOR_NOT_SET;
    int arc;

    /*
     * We need a separate MmapqPayloadIterator for walking payload within each
     * committed span. We re-use it->inner for this purpose during the scan,
     * then reinitialise it for gap traversal after the scan completes.
     *
     * The inner iterator must be initialised from the start of stream.
     */
    int64_t stream_start = it->start_of_stream_offset(it->stream_ctx);
    if (mmapqi_payload_iterator_init(it->inner, stream_start, PAYLOAD_ITERATOR_NOT_SET) != 0)
        return 0;

    while (1) {
        /* Read header at header_index — checked (F2): a mapping failure
         * fails the scan with its code instead of looking like end of
         * committed headers. */
        uint64_t header;
        int hrc = mmapqi_header_read_checked(it->header_accessor, &it->header_buf,
                                             header_index, &header);
        if (hrc != 0) return hrc;

        if (header == HEADER_NULL) break;  /* end of committed headers */

        header_index++;

        /* Skip entries from other appenders */
        if (header_appender_id(header) != it->appender_id) continue;

        int64_t header_offset = header_payload_position(header);

        /* Archived entry: archiving advanced start_of_stream past this
         * payload while leaving its header readable (only fully-covered
         * header files are deleted). Not corruption — the payload is gone
         * by design, and an archived entry is never uncommitted. Skip
         * without consulting the payload stream. */
        if (header_offset < stream_start) continue;

        if (current_start == PAYLOAD_ITERATOR_NOT_SET) {
            /* ── First entry: init range from this offset ──────────────── */
            if (mmapqi_payload_iterator_init(it->inner, header_offset, PAYLOAD_ITERATOR_NOT_SET) != 0)
                return 0;

            /* D8: the inner advance is three-way — a negative error
             * propagates; 0 (end of stream) or an offset mismatch is a
             * truncated committed span — scan failure. */
            arc = mmapqi_payload_iterator_advance(it->inner);
            if (arc < 0) return arc;
            if (arc == 0) return 0;
            if (it->inner->current_message_offset != header_offset) return 0;
            current_start = header_offset;
            current_end   = it->inner->next_probing_offset;

        } else if ((arc = mmapqi_payload_iterator_advance(it->inner)) < 0) {
            return arc;
        } else if (arc == 0 ||
                   it->inner->current_message_offset != header_offset) {
            /* ── Gap detected: flush current range, start new one ────────── */
            /* D-B (CAND-49 §3.1): raw append — NO merging during the scan.
             * Header order is commit order, so spans arrive out of order
             * under resequenced commits; the sort+merge runs once at the
             * end of init. */
            mmapqi_ranges_append(&it->ranges, current_start, current_end);

            if (mmapqi_payload_iterator_init(it->inner, header_offset, PAYLOAD_ITERATOR_NOT_SET) != 0)
                return 0;
            arc = mmapqi_payload_iterator_advance(it->inner);
            if (arc < 0) return arc;
            if (arc == 0) return 0;
            if (it->inner->current_message_offset != header_offset) return 0;

            current_start = header_offset;
            current_end   = it->inner->next_probing_offset;

        } else {
            /* ── Contiguous: extend current range (fast path) ───────────── */
            int64_t next = it->inner->next_probing_offset;
            if (next > current_end) current_end = next;
        }
    }

    /* Flush final range */
    if (current_start != PAYLOAD_ITERATOR_NOT_SET) {
        mmapqi_ranges_append(&it->ranges, current_start, current_end);
    }

    return 1;
}

/* ── Init gap traversal after scan ──────────────────────────────────────── */

static int init_first_gap(MmapqUncommittedPayloadIterator *it, int64_t start)
{
    /*
     * Find the first gap at or after `start`.
     * A gap is the space before the first committed range, between ranges,
     * or after the last range up to fixed_end_of_stream.
     *
     * Returns 1 on success, 0 if an inner init failed. The init arguments
     * here are derived from the validated start or from ranges built only
     * from non-archived entries, so they are >= start_of_stream by
     * construction; a failure indicates the invariant was broken.
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
            return 0;
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
                return 0;
        } else {
            /* start is before the next committed range — gap up to range_start */
            gap_end = range_start;
            it->current_gap_end = gap_end;
            if (mmapqi_payload_iterator_init(it->inner, start, gap_end) != 0)
                return 0;
        }
    }
    return 1;
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
     * non-archived entries, so they are >= start_of_stream by construction.
     * An init failure here collapses to exhaustion (0): the arguments are
     * valid by construction, so a failure indicates a broken invariant, not
     * a transient condition the caller can retry. */
    if (mmapqi_payload_iterator_init(it->inner, committed_end, gap_end) != 0)
        return 0;
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
     * before any state is touched (Java init parity: start > end when both
     * are set is EINVAL too). */
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
     * init); 0 is genuine truncation — a committed header's payload would
     * not walk for a non-timeout reason — and stays MMAPQ_ECORRUPT. */
    int src = scan_committed_ranges(it, scan_floor);
    if (src < 0) {
        it->initialised = 0;
        return src;
    }
    if (src == 0) {
        it->initialised = 0;
        return MMAPQ_ECORRUPT;
    }

    /* D-B: sort + merge the raw spans once (out-of-order commits). */
    mmapqi_ranges_sort_merge(&it->ranges);

    /* Prune committed ranges that end before our start (sorted array). */
    mmapqi_ranges_prune_up_to(&it->ranges, effective_start);
    it->current_range = 0;

    /* Position inner iterator at first gap */
    if (!init_first_gap(it, effective_start)) {
        it->initialised = 0;
        return MMAPQ_ECORRUPT;
    }

    it->initialised = 1;
    return 0;
}

/* ── mmapqi_uncommitted_iterator_advance ────────────────────────────────────────── */

int mmapqi_uncommitted_iterator_advance(MmapqUncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL,         "mmapqi_uncommitted_iterator_advance: it is NULL");
    if (!it->initialised) return 0;

    while (1) {
        /* CAND-49 §3.2 pre-check (D-A): a bounded gap whose probing offset
         * reached current_gap_end is exhausted — switch gaps WITHOUT calling
         * the inner advance. The inner iterator never probes at a committed
         * range's start. */
        if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
            it->inner->next_probing_offset == it->current_gap_end) {
            if (!switch_to_next_gap(it)) {
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
            if (wr != WRAP_SUCCESS)
                return wr == WRAP_MAP_TIMED_OUT ? MMAPQ_ETIMEDOUT : MMAPQ_ECORRUPT;
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
        if (r < 0) return r;
        if (r == 0) {
            /* CAND-49 §3.2 undershoot check: a bounded gap must end exactly
             * at current_gap_end; stopping short is torn framing —
             * MMAPQ_ECORRUPT (previously a silent end of stream). */
            if (it->current_gap_end != PAYLOAD_ITERATOR_NOT_SET &&
                it->inner->next_probing_offset != it->current_gap_end)
                return MMAPQ_ECORRUPT;
            /* Current gap exhausted — move to next gap */
            if (!switch_to_next_gap(it)) {
                return 0;  /* no more gaps */
            }
            continue;
        }

        /* CAND-49 §3.2 post-yield corruption check (replaces the pre-fix
         * MMAP_ASSERT here, which compiled out in release): the message
         * must sit cleanly before the next committed range — an overlap or
         * an overshoot past the range's start is corruption. Runtime check
         * in both configs (stricter than Java's silent skip — owner
         * ruling). */
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
