#ifndef HEADER_READER_H
#define HEADER_READER_H

#include <stdint.h>
#include <stdatomic.h>
#include "internal/region_accessor.h"
#include "header_codec.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

/**
 * HeaderReader — utilities for reading queue header entries and searching
 * the header space.
 *
 * Mirrors Java HeaderReader exactly, including the archive-hole-resilient
 * exponential + binary search for the last committed index.
 *
 * All functions take a pre-allocated Buffer for the header region. The
 * buffer is written by wrap() and read immediately — no allocation.
 *
 * read():
 *   Returns the raw uint64 header at the given index.
 *   Returns HEADER_NULL if the file does not exist yet, the position is
 *   beyond the current file size, or the mapping timed out (transient;
 *   the checked variants below distinguish it). Aborts on UNWRAPPED.
 *
 * found():
 *   Returns 1 if a non-NULL header exists at the index, 0 otherwise.
 *
 * search_last_index():
 *   Finds the highest committed index using exponential probe + binary
 *   search. Resilient to concurrent archiving (archive-hole detection).
 *   Returns -1 (NULL_INDEX) if the queue is empty.
 */

#define HEADER_NULL_INDEX  ((int64_t)-1)

/**
 * mmapqi_header_read — read the header at the given index.
 *
 * header_accessor: the RegionAccessor for the header file.
 * buf:             caller-provided Buffer, filled by wrap().
 * index:           queue index to read.
 * queue_name:      used in assertion messages only.
 *
 * Returns the uint64 header, or HEADER_NULL if not yet written.
 * Aborts on MAP_TIMED_OUT or WRAP_UNWRAPPED.
 */
uint64_t mmapqi_header_read(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name);

/**
 * mmapqi_header_read_sequential — like mmapqi_header_read but uses wrap() to
 * trigger premap of the next region, and distinguishes a mapping timeout
 * from an unwritten entry (CAND-17).
 *
 * Use this for sequential pollers advancing forward through the queue.
 * Use mmapqi_header_read for random-access readers and search algorithms
 * (mmapqi_header_search_last_index).
 *
 * header_accessor: the RegionAccessor for the header file.
 * buf:             caller-provided Buffer, filled by wrap().
 * index:           queue index to read.
 * err:             required output, written ONLY when wrap() fails:
 *                  0 for a genuinely not-yet-accessible entry (file
 *                  absent or not yet grown), MMAPQ_ETIMEDOUT on mapping
 *                  timeout, MMAPQ_ECORRUPT after the fatal assert
 *                  (release-build fallback). A successful wrap of an
 *                  unwritten slot ALSO returns HEADER_NULL without
 *                  touching err — zero-initialise it before the call and
 *                  read it only when the return value is HEADER_NULL.
 *
 * Returns the uint64 header, or HEADER_NULL on an unwritten entry
 * (err untouched or 0) or a mapping failure (err set).
 *
 * static inline (CAND-17): the call must disappear into both pollers —
 * as a separate TU without LTO, inlining saved ~10 Ir/msg on poll
 * (107 → 97, callgrind); the err store is dead on the inlined success path.
 */
static inline uint64_t mmapqi_header_read_sequential(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    int            *err)
{
    int64_t position = header_position(index);

    WrapResult r = region_accessor_wrap(header_accessor, position, buf);

    if (r == WRAP_SUCCESS) {
        /*
         * Read with acquire semantics — pairs with the release CAS in
         * the appender. Ensures we see all payload bytes written before
         * the header was committed.
         */
        const _Atomic uint64_t *p = (const _Atomic uint64_t *)buf->data;
        return atomic_load_explicit(p, memory_order_acquire);
    }

    if (r == WRAP_MAP_TIMED_OUT) {
        *err = MMAPQ_ETIMEDOUT;
        return HEADER_NULL;
    }

    if (r == WRAP_FILE_NOT_FOUND || r == WRAP_BEYOND_FILE_SIZE) {
        /* Queue not yet accessible — genuinely unwritten */
        *err = 0;
        return HEADER_NULL;
    }

    /* WRAP_UNWRAPPED — fatal */
    MMAP_ASSERT(0,
        "mmapqi_header_read_sequential: FATAL — unexpected wrap result. "
        "This indicates region corruption.");
    *err = MMAPQ_ECORRUPT;  /* release-build fallback (was silent EOS) */
    return HEADER_NULL;
}

/**
 * header_found — returns 1 if a valid (non-NULL) header exists at index.
 */
static inline int header_found(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name)
{
    return mmapqi_header_read(header_accessor, buf, index, queue_name) != HEADER_NULL;
}

/**
 * StartIndexSupplier — callback to get the current queue start index.
 *
 * Used by search_last_index for archive-hole detection. The supplier is
 * called when a NULL header is encountered to check if the start index
 * has advanced past the probed position (indicating an archive hole,
 * not a genuine end-of-stream).
 */
typedef int64_t (*StartIndexSupplier)(void *ctx);

/**
 * mmapqi_header_search_last_index — find the highest committed queue index.
 *
 * Algorithm (mirrors Java HeaderReader.searchLastIndex):
 *   1. Exponential probe forward from from_index to find an upper bound.
 *   2. Binary search between last known occupied and the upper bound.
 *   3. On NULL header: check if start_index advanced past probe point
 *      (archive hole) — if so, restart from new start_index.
 *
 * from_index:          start of the search range (typically queue start_index)
 * start_index_fn:      function to get current start_index, for hole detection
 * start_index_ctx:     context passed to start_index_fn
 * header_accessor:     RO accessor for the header file
 * buf:                 caller-provided Buffer (reused across calls)
 * queue_name:          for diagnostic messages
 *
 * Returns the highest valid index, or HEADER_NULL_INDEX if queue is empty.
 */
int64_t mmapqi_header_search_last_index(
    int64_t          from_index,
    StartIndexSupplier start_index_fn,
    void             *start_index_ctx,
    RegionAccessor   *header_accessor,
    Buffer           *buf,
    const char       *queue_name);

/*
 * mmapqi_header_read_checked — read a header without hiding mapping failures.
 *
 * header_accessor: accessor for the header file
 * buf:             caller-provided Buffer, reused for the read
 * index:           queue index to read
 * header:          required output; receives HEADER_NULL on failure or an
 *                  unwritten entry, otherwise the committed header
 *
 * Returns 0 on a successful read or an absent/not-yet-grown header file,
 * MMAPQ_ETIMEDOUT on mapping timeout, or MMAPQ_ECORRUPT on an unexpected
 * mapping result or insufficient capacity for a header.
 */
int mmapqi_header_read_checked(
    RegionAccessor *header_accessor, Buffer *buf, int64_t index, uint64_t *header);

/*
 * mmapqi_header_search_last_index_checked — search with explicit error reporting.
 *
 * Uses the same exponential/binary search and archive-hole restart logic as
 * mmapqi_header_search_last_index(). With a non-NULL error output, mapping
 * failures stop the search rather than being treated as unwritten entries.
 * Missing or not-yet-grown header files still mean an unwritten entry.
 *
 * from_index:      start of the search range (typically queue start_index)
 * start_index_fn:  function supplying the current start index for hole detection
 * start_index_ctx: context passed to start_index_fn
 * header_accessor: accessor for the header file
 * buf:             caller-provided Buffer, reused throughout the search
 * queue_name:      for diagnostic messages in the legacy path
 * error:           receives 0 on success or a negative MMAPQ_* code on failure;
 *                  NULL selects legacy search behavior, including treating
 *                  mapping timeouts as unwritten entries
 *
 * Returns the highest committed index, or HEADER_NULL_INDEX if no entry is
 * found in the search range or a checked read fails. Inspect error to
 * distinguish an empty range from failure; failures are MMAPQ_ETIMEDOUT or
 * MMAPQ_ECORRUPT as described by mmapqi_header_read_checked().
 */
int64_t mmapqi_header_search_last_index_checked(
    int64_t from_index, StartIndexSupplier start_index_fn, void *start_index_ctx,
    RegionAccessor *header_accessor, Buffer *buf, const char *queue_name, int *error);

#endif /* HEADER_READER_H */
