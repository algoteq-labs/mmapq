#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/mmap_assert.h"
#include "internal/test_hooks.h"
#include "mmap_platform.h"

#include <stdint.h>
#include <stdatomic.h>

/* ── Test hook (P15) — never compiled into release builds ──────────────────
 * Substitutes the wrap result while armed. Armed only from the archivers'
 * live-header pass (the archiver-side hook decides when); the point is that
 * an injected WRAP_MAP_TIMED_OUT collapses to HEADER_NULL through the plain
 * read but reports MMAPQ_ETIMEDOUT through the checked read. */
#ifdef MMAPQ_TEST_HOOKS
static int header_wrap_inject;
void mmapq_test_hook_header_wrap_inject(int wrap_result)
{
    header_wrap_inject = wrap_result;
}
#endif

/* ── mmapqi_header_read ─────────────────────────────────────────────────────────── */

uint64_t mmapqi_header_read(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name)
{
    int64_t position = header_position(index);
    (void)queue_name;  /* used in MMAP_ASSERT message only */

    WrapResult r = region_accessor_wrap_no_premap(header_accessor, position, buf);
#ifdef MMAPQ_TEST_HOOKS
    if (header_wrap_inject != 0) r = (WrapResult)header_wrap_inject;
#endif

    if (r == WRAP_SUCCESS) {
        /*
         * Read with acquire semantics — pairs with the release CAS in
         * the appender. Ensures we see all payload bytes written before
         * the header was committed.
         */
        const _Atomic uint64_t *p = (const _Atomic uint64_t *)buf->data;
        return atomic_load_explicit(p, memory_order_acquire);
    }

    if (r == WRAP_FILE_NOT_FOUND || r == WRAP_BEYOND_FILE_SIZE || r == WRAP_MAP_TIMED_OUT) {
        /* Queue not yet accessible — treat as unwritten */
        return HEADER_NULL;
    }

    /* WRAP_UNWRAPPED — fatal */
    MMAP_ASSERT(0,
        "mmapqi_header_read: FATAL — unexpected wrap result. "
        "This indicates region corruption.");
    return HEADER_NULL;  /* unreachable */
}

int mmapqi_header_read_checked(
    RegionAccessor *header_accessor, Buffer *buf, int64_t index, uint64_t *header)
{
    *header = HEADER_NULL;
    WrapResult result = region_accessor_wrap_no_premap(header_accessor, header_position(index), buf);
#ifdef MMAPQ_TEST_HOOKS
    if (header_wrap_inject != 0) result = (WrapResult)header_wrap_inject;
#endif
    if (result == WRAP_FILE_NOT_FOUND || result == WRAP_BEYOND_FILE_SIZE) return 0;
    if (result == WRAP_MAP_TIMED_OUT) return MMAPQ_ETIMEDOUT;
    if (result != WRAP_SUCCESS || buf->capacity < HEADER_SIZE) return MMAPQ_ECORRUPT;
    /* Acquire pairs with the appender's publication CAS, as in the legacy reader. */
    *header = atomic_load_explicit((const _Atomic uint64_t *)buf->data, memory_order_acquire);
    return 0;
}

static int header_found_checked(
    RegionAccessor *accessor, Buffer *buffer, int64_t index, const char *name, int *error)
{
    if (error == NULL) return header_found(accessor, buffer, index, name);
    uint64_t header;
    *error = mmapqi_header_read_checked(accessor, buffer, index, &header);
    return header != HEADER_NULL;
}

/* ── mmapqi_header_search_last_index ────────────────────────────────────────────── */

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
int64_t mmapqi_header_search_last_index(
    int64_t from_index, StartIndexSupplier start_index_fn, void *start_index_ctx,
    RegionAccessor *header_accessor, Buffer *buf, const char *queue_name)
{
    return mmapqi_header_search_last_index_checked(
        from_index, start_index_fn, start_index_ctx, header_accessor, buf, queue_name, NULL);
}

int64_t mmapqi_header_search_last_index_checked(
    int64_t            from_index,
    StartIndexSupplier start_index_fn,
    void              *start_index_ctx,
    RegionAccessor    *header_accessor,
    Buffer            *buf,
    const char        *queue_name, int *error)
{
    if (error != NULL) *error = 0;
    while (1) {
        int64_t current_start = start_index_fn(start_index_ctx);
        int64_t low = from_index > current_start ? from_index : current_start;

        /* ── verify low exists ──────────────────────────────────────────── */
        if (!header_found_checked(header_accessor, buf, low, queue_name, error)) {
            if (error != NULL && *error != 0) return HEADER_NULL_INDEX;
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
            /* Never probe beyond the legal domain: entry points reject
             * indices > MMAPQI_MAX_INDEX, and reading there can alias a
             * valid slot (CAND-28 rolled-mapper truncation) — a phantom
             * "found" would park the caller at a bogus index. Treat the
             * first such probe as NULL and cap the binary search so its
             * mids stay <= MAX. */
            if (high > MMAPQI_MAX_INDEX) {
                high = MMAPQI_MAX_INDEX + 1;
                break;
            }
            if (header_found_checked(header_accessor, buf, high, queue_name, error)) {
                low = high;
                /* Guard against overflow */
                if (high > (INT64_MAX >> 1)) {
                    high = INT64_MAX;
                } else {
                    high <<= 1;
                }
            } else {
                if (error != NULL && *error != 0) return HEADER_NULL_INDEX;
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
            /* mid > MAX is possible when high was capped at MAX+1: treat as
             * NULL without reading (a read there can alias — see above). */
            if (mid <= MMAPQI_MAX_INDEX && header_found_checked(header_accessor, buf, mid, queue_name, error)) {
                low = mid;
            } else {
                if (error != NULL && *error != 0) return HEADER_NULL_INDEX;
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
