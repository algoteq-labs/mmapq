#ifndef INDEXED_BATCH_ARCHIVER_H
#define INDEXED_BATCH_ARCHIVER_H

#include <stdint.h>
#include "runtime.h"

/**
 * IndexedBatchArchiver — manages archiving of entries in an indexed batch queue.
 *
 * Functionally identical to IndexedArchiver but operates on a batch queue
 * where each header entry points to a batch payload rather than a single
 * message. The key difference in the forward archiving path is how the
 * "next write offset" is calculated: for batches, the iterator must exhaust
 * the entire batch (all messages) to find nextPayloadPosition, whereas the
 * normal archiver reads a single length field after the committed position.
 *
 * The archiver does NOT delete files — archivable_files() returns the list
 * and the caller decides what to do with them.
 *
 * Archiving can move in two directions:
 *   Forward (index > current start_index): mark batches as archived.
 *   Backward / undo (index < current start_index): restore access —
 *   only possible if the files covering those batches still exist.
 *
 * Thread-confined: not safe for concurrent access.
 *
 * Mirrors Java IIndexedBatchArchiver / IndexedBatchArchiver.
 */

typedef struct IndexedBatchArchiver IndexedBatchArchiver;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create a batch archiver for the named queue.
 *
 * directory:  path to the directory containing queue files
 * queue_name: logical name of the queue
 * ring_size:  number of simultaneously mapped regions
 * runtime:    background region-mapper runtime for header/payload access
 * Returns: a new archiver; never NULL (asserts on allocation failure)
 */
int indexed_batch_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    Runtime    *runtime,
    IndexedBatchArchiver **out);

/* ── Archiving ───────────────────────────────────────────────────────────── */

/**
 * Return 1 if the queue is archivable, 0 otherwise.
 */
int indexed_batch_archiver_archivable(IndexedBatchArchiver *a);

/**
 * Archive all batches up to @p index (exclusive).
 *
 * Forward archiving (index > current start_index):
 *   - Walks headers backwards from index-1 to current start_index,
 *     recording the last committed batch payload position per appender
 *     (putIfAbsent — captures the highest-index batch per appender).
 *   - For each appender, exhausts the batch iterator at that position
 *     to find nextPayloadPosition (byte offset after the last message
 *     in the last batch).
 *   - Writes that offset to appender_meta_N for each appender.
 *   - Commits the new start_index atomically in queue meta.
 *
 * Backward archiving / undo (index < current start_index):
 *   - Walks headers forward from index to current start_index.
 *   - Determines all header and payload file indices touched by the range.
 *   - Validates those files exist before modifying anything.
 *   - Records the first batch payload position per appender.
 *   - Writes those positions to appender_meta_N.
 *   - Commits the new (lower) start_index.
 *
 * No-op if index == current start_index.
 *
 * a:     the archiver
 * index: first unarchived batch index (all prior indices archived)
 * Returns: 0 on success, MMAPQ_EINVAL on invalid argument or non-archivable.
 */
int indexed_batch_archiver_archive_up_to_index(IndexedBatchArchiver *a, int64_t index);

/* ── Query ───────────────────────────────────────────────────────────────── */

/**
 * Return the current start index (first unarchived batch).
 */
int64_t indexed_batch_archiver_start_index(IndexedBatchArchiver *a);

/**
 * Return the payload start offset for the given appender at start_index.
 *
 * Returns -1 if the appender ID is not used by this queue.
 */
int indexed_batch_archiver_start_offset(
    IndexedBatchArchiver *a, uint8_t appender_id, int64_t *out_offset);

/**
 * Populate @p paths_out with paths to files fully covered by the archive
 * boundary and safe to delete, move, or compress.
 *
 * Includes header_N and payload_N_M files whose entire range falls below
 * the current start_index / start_offset. Only existing files are included.
 *
 * a:         the archiver
 * paths_out: caller-allocated array of char* to populate
 * max_paths: capacity of paths_out
 * Returns: number of paths written (may be less than max_paths)
 */
int indexed_batch_archiver_archivable_files(
    IndexedBatchArchiver *a,
    char                **paths_out,
    int                   max_paths);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * Close the archiver and release all associated resources.
 */
void indexed_batch_archiver_close(IndexedBatchArchiver *a);

#endif /* INDEXED_BATCH_ARCHIVER_H */
