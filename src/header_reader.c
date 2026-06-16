#include "header_reader.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdint.h>
#include <stdatomic.h>

/* ── header_read ─────────────────────────────────────────────────────────── */

uint64_t header_read(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name)
{
    int64_t position = header_position(index);
    (void)queue_name;  /* used in MMAP_ASSERT message only */

    WrapResult r = region_accessor_wrap_no_premap(header_accessor, position, buf);

    if (r == WRAP_SUCCESS) {
        /*
         * Read with acquire semantics — pairs with the release CAS in
         * the appender. Ensures we see all payload bytes written before
         * the header was committed.
         */
        const _Atomic uint64_t *p = (const _Atomic uint64_t *)buf->data;
        return atomic_load_explicit(p, memory_order_acquire);
    }

    if (r == WRAP_FILE_NOT_FOUND || r == WRAP_BEYOND_FILE_SIZE) {
        /* Queue not yet that large — treat as unwritten */
        return HEADER_NULL;
    }

    /* MAP_TIMED_OUT or WRAP_UNWRAPPED — fatal */
    MMAP_ASSERT(0,
        "header_read: FATAL — unexpected wrap result. "
        "This indicates a mapping timeout or region corruption.");
    return HEADER_NULL;  /* unreachable */
}

/*
 * header_read_sequential — like header_read but uses wrap() to trigger
 * premap of the next region. Use for sequential pollers advancing forward.
 * Use header_read for random-access readers and search algorithms.
 */
uint64_t header_read_sequential(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name)
{
    int64_t position = header_position(index);
    (void)queue_name;

    WrapResult r = region_accessor_wrap(header_accessor, position, buf);

    if (r == WRAP_SUCCESS) {
        const _Atomic uint64_t *p = (const _Atomic uint64_t *)buf->data;
        return atomic_load_explicit(p, memory_order_acquire);
    }

    if (r == WRAP_FILE_NOT_FOUND || r == WRAP_BEYOND_FILE_SIZE) {
        return HEADER_NULL;
    }

    MMAP_ASSERT(0,
        "header_read_sequential: FATAL — unexpected wrap result. "
        "This indicates a mapping timeout or region corruption.");
    return HEADER_NULL;
}

/* ── header_search_last_index ────────────────────────────────────────────── */

/*
 * Mirrors Java HeaderReader.searchLastIndex exactly, including
 * the archive-hole detection restart logic.
 *
 * Algorithm:
 *   1. Verify from_index exists (handles empty queue and archive holes).
 *   2. Exponential probe forward to find upper bound.
 *   3. Binary search in [low, high] to narrow to last committed index.
 *   4. On NULL in either phase: check if start_index advanced past
 *      the probe point (archive hole) — if so, restart.
 */
int64_t header_search_last_index(
    int64_t            from_index,
    StartIndexSupplier start_index_fn,
    void              *start_index_ctx,
    RegionAccessor    *header_accessor,
    Buffer            *buf,
    const char        *queue_name)
{
    while (1) {
        int64_t current_start = start_index_fn(start_index_ctx);
        int64_t low = from_index > current_start ? from_index : current_start;

        /* ── verify low exists ──────────────────────────────────────────── */
        if (!header_found(header_accessor, buf, low, queue_name)) {
            /* Check if start_index has moved past low (archive hole) */
            if (low >= start_index_fn(start_index_ctx)) {
                return HEADER_NULL_INDEX;  /* queue is empty */
            }
            continue;  /* start_index moved, retry with updated low */
        }

        /* ── exponential probe forward ──────────────────────────────────── */
        int64_t high = low + 1;
        if (high < low + 1) high = low + 1;  /* overflow guard */
        /* Start at max(low+1, low<<1) — mirrors Java */
        if (low > 0 && (low << 1) > high) high = low << 1;

        int hole_detected = 0;

        while (1) {
            if (header_found(header_accessor, buf, high, queue_name)) {
                low = high;
                /* Guard against overflow */
                if (high > (INT64_MAX >> 1)) {
                    high = INT64_MAX;
                } else {
                    high <<= 1;
                }
            } else {
                /* NULL at high — archive hole? */
                if (high < start_index_fn(start_index_ctx)) {
                    hole_detected = 1;
                }
                break;
            }
        }

        if (hole_detected) {
            continue;  /* restart from new start_index */
        }

        /* ── binary search in [low, high] ───────────────────────────────── */
        while (low < high) {
            int64_t mid = (low + high + 1) >> 1;  /* round up to avoid infinite loop */
            if (header_found(header_accessor, buf, mid, queue_name)) {
                low = mid;
            } else {
                /* NULL at mid — archive hole? */
                if (mid < start_index_fn(start_index_ctx)) {
                    hole_detected = 1;
                    break;
                }
                high = mid - 1;
            }
        }

        if (hole_detected) {
            continue;  /* restart */
        }

        return low;  /* highest committed index */
    }
}
