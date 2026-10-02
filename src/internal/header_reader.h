#ifndef HEADER_READER_H
#define HEADER_READER_H

#include <stdint.h>
#include "internal/region_accessor.h"
#include "header_codec.h"
#include "mmap_assert.h"

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
 *   Returns HEADER_NULL if the file does not exist yet or the position
 *   is beyond the current file size (queue not yet that large).
 *   Aborts (MMAP_ASSERT) on MAP_TIMED_OUT or UNWRAPPED — these are
 *   fatal and indicate a serious system problem.
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
 * mmapqi_header_read_sequential — like mmapqi_header_read but uses wrap() to trigger
 * premap of the next region.
 *
 * Use this for sequential pollers advancing forward through the queue.
 * Use mmapqi_header_read for random-access readers and search algorithms (mmapqi_header_search_last_index).
 */
uint64_t mmapqi_header_read_sequential(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name);

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
