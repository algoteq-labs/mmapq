#ifndef HEADER_READER_H
#define HEADER_READER_H

#include <stdint.h>
#include "region_accessor.h"
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
 * header_read — read the header at the given index.
 *
 * header_accessor: the RegionAccessor for the header file.
 * buf:             caller-provided Buffer, filled by wrap().
 * index:           queue index to read.
 * queue_name:      used in assertion messages only.
 *
 * Returns the uint64 header, or HEADER_NULL if not yet written.
 * Aborts on MAP_TIMED_OUT or WRAP_UNWRAPPED.
 */
uint64_t header_read(
    RegionAccessor *header_accessor,
    Buffer         *buf,
    int64_t         index,
    const char     *queue_name);

/**
 * header_read_sequential — like header_read but uses wrap() to trigger
 * premap of the next region.
 *
 * Use this for sequential pollers advancing forward through the queue.
 * Use header_read for random-access readers and search algorithms (header_search_last_index).
 */
uint64_t header_read_sequential(
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
    return header_read(header_accessor, buf, index, queue_name) != HEADER_NULL;
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
 * header_search_last_index — find the highest committed queue index.
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
int64_t header_search_last_index(
    int64_t          from_index,
    StartIndexSupplier start_index_fn,
    void             *start_index_ctx,
    RegionAccessor   *header_accessor,
    Buffer           *buf,
    const char       *queue_name);

#endif /* HEADER_READER_H */
