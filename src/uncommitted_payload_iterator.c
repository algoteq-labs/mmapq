#include "uncommitted_payload_iterator.h"
#include "header_reader.h"
#include "payload_walk.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Range array helpers ─────────────────────────────────────────────────── */

#define RANGE_INITIAL_CAPACITY 64

static void ranges_ensure_capacity(UncommittedPayloadIterator *it)
{
    if (it->range_count < it->range_capacity) return;

    int new_cap = it->range_capacity == 0
        ? RANGE_INITIAL_CAPACITY
        : it->range_capacity * 2;

    it->ranges = realloc(it->ranges, (size_t)new_cap * sizeof(CommittedRange));
    if (it->ranges == NULL) return;
    it->range_capacity = new_cap;
}

static void ranges_insert(UncommittedPayloadIterator *it, int64_t start, int64_t end)
{
    MMAP_ASSERT(start < end, "ranges_insert: start must be < end");

    /* Check if we can extend the last range (contiguous) */
    if (it->range_count > 0) {
        CommittedRange *last = &it->ranges[it->range_count - 1];
        if (start <= last->end) {
            /* Merge — extend end if necessary */
            if (end > last->end) last->end = end;
            return;
        }
    }

    ranges_ensure_capacity(it);
    it->ranges[it->range_count].start = start;
    it->ranges[it->range_count].end   = end;
    it->range_count++;
}

static void ranges_clear(UncommittedPayloadIterator *it)
{
    it->range_count   = 0;
    it->current_range = 0;
}

/* Prune ranges whose end <= start_offset (already behind us) */
static void ranges_prune_up_to(UncommittedPayloadIterator *it, int64_t offset)
{
    int i = 0;
    while (i < it->range_count && it->ranges[i].end <= offset) {
        i++;
    }
    if (i > 0) {
        int remaining = it->range_count - i;
        memmove(it->ranges, it->ranges + i,
                (size_t)remaining * sizeof(CommittedRange));
        it->range_count = remaining;
    }
    it->current_range = 0;
}

/* ── Header scan: build committed ranges ─────────────────────────────────── */

static int scan_committed_ranges(UncommittedPayloadIterator *it)
{
    ranges_clear(it);

    int64_t header_index  = 0;
    int64_t current_start = PAYLOAD_ITERATOR_NOT_SET;
    int64_t current_end   = PAYLOAD_ITERATOR_NOT_SET;

    /*
     * We need a separate PayloadIterator for walking payload within each
     * committed span. We re-use it->inner for this purpose during the scan,
     * then reinitialise it for gap traversal after the scan completes.
     *
     * The inner iterator must be initialised from the start of stream.
     */
    int64_t stream_start = it->start_of_stream_offset(it->stream_ctx);
    payload_iterator_init(it->inner, stream_start, PAYLOAD_ITERATOR_NOT_SET);

    while (1) {
        /* Read header at header_index */
        uint64_t header = header_read(it->header_accessor, &it->header_buf,
                                       header_index, it->queue_name);

        if (header == HEADER_NULL) break;  /* end of committed headers */

        header_index++;

        /* Skip entries from other appenders */
        if (header_appender_id(header) != it->appender_id) continue;

        int64_t header_offset = header_payload_position(header);

        if (current_start == PAYLOAD_ITERATOR_NOT_SET) {
            /* ── First entry: init range from this offset ──────────────── */
            payload_iterator_init(it->inner, header_offset, PAYLOAD_ITERATOR_NOT_SET);

            if (!payload_iterator_advance(it->inner)) return 0;
            if (it->inner->current_message_offset != header_offset) return 0;
            current_start = header_offset;
            current_end   = it->inner->next_probing_offset;

        } else if (!payload_iterator_advance(it->inner) ||
                   it->inner->current_message_offset != header_offset) {
            /* ── Gap detected: flush current range, start new one ────────── */
            ranges_insert(it, current_start, current_end);

            payload_iterator_init(it->inner, header_offset, PAYLOAD_ITERATOR_NOT_SET);
            if (!payload_iterator_advance(it->inner)) return 0;
            if (it->inner->current_message_offset != header_offset) return 0;

            current_start = header_offset;
            current_end   = it->inner->next_probing_offset;

        } else {
            /* ── Contiguous: extend current range ───────────────────────── */
            int64_t next = it->inner->next_probing_offset;
            if (next > current_end) current_end = next;
        }
    }

    /* Flush final range */
    if (current_start != PAYLOAD_ITERATOR_NOT_SET) {
        ranges_insert(it, current_start, current_end);
    }

    return 1;
}

/* ── Init gap traversal after scan ──────────────────────────────────────── */

static void init_first_gap(UncommittedPayloadIterator *it, int64_t start)
{
    /*
     * Find the first gap at or after `start`.
     * A gap is the space before the first committed range, between ranges,
     * or after the last range up to fixed_end_of_stream.
     */

    /* Skip committed ranges that end <= start */
    while (it->current_range < it->range_count &&
           it->ranges[it->current_range].end <= start) {
        it->current_range++;
    }

    int64_t gap_end;

    if (it->current_range >= it->range_count) {
        /* No more committed ranges ahead — gap runs to fixed_end_of_stream */
        payload_iterator_init(it->inner, start, it->fixed_end_of_stream);
    } else {
        int64_t range_start = it->ranges[it->current_range].start;

        if (start >= range_start) {
            /* start is inside a committed range — skip past it */
            int64_t range_end = it->ranges[it->current_range].end;
            it->current_range++;

            /* Merge any immediately adjacent committed ranges */
            while (it->current_range < it->range_count &&
                   it->ranges[it->current_range].start == range_end) {
                range_end = it->ranges[it->current_range].end;
                it->current_range++;
            }

            if (it->current_range >= it->range_count) {
                gap_end = it->fixed_end_of_stream;
            } else {
                gap_end = it->ranges[it->current_range].start;
            }
            payload_iterator_init(it->inner, range_end, gap_end);
        } else {
            /* start is before the next committed range — gap up to range_start */
            gap_end = range_start;
            payload_iterator_init(it->inner, start, gap_end);
        }
    }
}

/* ── Move to next gap ────────────────────────────────────────────────────── */

static int switch_to_next_gap(UncommittedPayloadIterator *it)
{
    /* Consume the current committed range and any adjacent ones */
    if (it->current_range >= it->range_count) return 0;

    int64_t committed_end = it->ranges[it->current_range].end;
    it->current_range++;

    /* Merge adjacent committed ranges */
    while (it->current_range < it->range_count &&
           it->ranges[it->current_range].start <= committed_end) {
        committed_end = it->ranges[it->current_range].end;
        it->current_range++;
    }

    if (committed_end >= it->fixed_end_of_stream) {
        return 0;  /* no more gaps */
    }

    int64_t gap_end;
    if (it->current_range >= it->range_count) {
        gap_end = it->fixed_end_of_stream;
    } else {
        gap_end = it->ranges[it->current_range].start;
    }

    payload_iterator_init(it->inner, committed_end, gap_end);
    return 1;
}

/* ── uncommitted_iterator_create ─────────────────────────────────────────── */

int uncommitted_iterator_create(
    const char      *queue_name,
    uint8_t          appender_id,
    int64_t        (*start_of_stream_offset)(void *ctx),
    int64_t        (*end_of_stream_offset)(void *ctx),
    void            *stream_ctx,
    RegionAccessor  *header_accessor,
    PayloadIterator *inner,
    UncommittedPayloadIterator **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (start_of_stream_offset == NULL) return MMAPQ_EINVAL;
    if (end_of_stream_offset == NULL) return MMAPQ_EINVAL;
    if (header_accessor == NULL) return MMAPQ_EINVAL;
    if (inner == NULL) return MMAPQ_EINVAL;

    UncommittedPayloadIterator *it = calloc(1, sizeof(UncommittedPayloadIterator));
    if (it == NULL) return MMAPQ_ENOMEM;

    it->queue_name              = queue_name;
    it->appender_id             = appender_id;
    it->start_of_stream_offset  = start_of_stream_offset;
    it->end_of_stream_offset    = end_of_stream_offset;
    it->stream_ctx              = stream_ctx;
    it->header_accessor         = header_accessor;
    it->inner                   = inner;

    it->init_start_offset       = PAYLOAD_ITERATOR_NOT_SET;
    it->init_end_offset         = PAYLOAD_ITERATOR_NOT_SET;
    it->fixed_end_of_stream     = PAYLOAD_ITERATOR_NOT_SET;
    it->initialised             = 0;

    *out = it;
    return 0;
}

/* ── uncommitted_iterator_init ───────────────────────────────────────────── */

UncommittedPayloadIterator *uncommitted_iterator_init(
    UncommittedPayloadIterator *it,
    int64_t                     start_offset,
    int64_t                     end_offset)
{
    MMAP_ASSERT(it != NULL, "uncommitted_iterator_init: it is NULL");

    int64_t stream_start = it->start_of_stream_offset(it->stream_ctx);

    if (start_offset != PAYLOAD_ITERATOR_NOT_SET) {
        if (start_offset < stream_start) return 0;
    }
    if (end_offset != PAYLOAD_ITERATOR_NOT_SET) {
        if (end_offset < stream_start) return 0;
    }

    it->init_start_offset = start_offset;
    it->init_end_offset   = end_offset;

    int64_t stream_end = it->end_of_stream_offset(it->stream_ctx);
    it->fixed_end_of_stream = (end_offset == PAYLOAD_ITERATOR_NOT_SET)
        ? stream_end
        : (end_offset < stream_end ? end_offset : stream_end);

    int64_t effective_start = (start_offset == PAYLOAD_ITERATOR_NOT_SET)
        ? stream_start
        : start_offset;

    /* Scan headers to build committed ranges */
    if (!scan_committed_ranges(it)) {
        it->initialised = 0;
        return 0;
    }

    /* Prune committed ranges that end before our start */
    ranges_prune_up_to(it, effective_start);

    /* Position inner iterator at first gap */
    init_first_gap(it, effective_start);

    it->initialised = 1;
    return it;
}

/* ── uncommitted_iterator_advance ────────────────────────────────────────── */

int uncommitted_iterator_advance(UncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL,         "uncommitted_iterator_advance: it is NULL");
    if (!it->initialised) return 0;

    while (1) {
        if (payload_iterator_advance(it->inner)) {
            int64_t msg_end = it->inner->next_probing_offset;

            /*
             * If the message end reaches the next committed range,
             * the message either overlaps it (corruption) or sits
             * cleanly before it.
             */
            if (it->current_range < it->range_count &&
                msg_end > it->ranges[it->current_range].start) {
                MMAP_ASSERT(0,
                    "uncommitted_iterator_advance: message overlaps committed "
                    "range — payload stream is corrupt");
            }

            return 1;  /* found an uncommitted message */
        }

        /* Current gap exhausted — move to next gap */
        if (!switch_to_next_gap(it)) {
            return 0;  /* no more gaps */
        }
    }
}

/* ── uncommitted_iterator_has_next ───────────────────────────────────────── */

int uncommitted_iterator_has_next(const UncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "uncommitted_iterator_has_next: it is NULL");
    if (!it->initialised) return 0;

    if (payload_iterator_has_next(it->inner)) return 1;

    /* Check if any gap remains after current committed ranges */
    if (it->current_range >= it->range_count) return 0;

    /* A gap exists if committed_end < fixed_end_of_stream */
    return it->ranges[it->current_range].end < it->fixed_end_of_stream;
}

/* ── uncommitted_iterator_current ────────────────────────────────────────── */

Buffer *uncommitted_iterator_current(UncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL, "uncommitted_iterator_current: it is NULL");
    return payload_iterator_current(it->inner);
}

/* ── uncommitted_iterator_rewind ─────────────────────────────────────────── */

UncommittedPayloadIterator *uncommitted_iterator_rewind(UncommittedPayloadIterator *it)
{
    MMAP_ASSERT(it != NULL,      "uncommitted_iterator_rewind: it is NULL");
    if (!it->initialised) return NULL;

    return uncommitted_iterator_init(it, it->init_start_offset, it->init_end_offset);
}

/* ── uncommitted_iterator_destroy ────────────────────────────────────────── */

void uncommitted_iterator_destroy(UncommittedPayloadIterator *it)
{
    if (it == NULL) return;
    free(it->ranges);
    /* inner and header_accessor are NOT owned */
    free(it);
}
