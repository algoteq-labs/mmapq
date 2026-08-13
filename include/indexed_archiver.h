#ifndef MMAPQ_INDEXED_ARCHIVER_H
#define MMAPQ_INDEXED_ARCHIVER_H

#include <stdint.h>
#include "runtime.h"

/**
 * MmapqIndexedArchiver — manages archiving of entries in an indexed queue.
 *
 * The archiver advances the queue's start index, updates per-appender
 * start offsets so new appenders resume from the correct payload position
 * after archiving, and reports which files are now fully covered by the
 * archive boundary and can be safely deleted, moved, or compressed.
 *
 * The archiver does NOT delete files itself — that is the caller's
 * responsibility. archivable_files() returns the list; the caller decides
 * what to do with them.
 *
 * Archiving can move in two directions:
 *   Forward (index > current start_index): mark entries as archived.
 *   Backward / undo (index < current start_index): restore access to
 *   previously archived entries — only possible if the files covering
 *   those entries have not been deleted.
 *
 * Thread-confined: not safe for concurrent access.
 *
 * Mirrors Java IIndexedArchiver / MmapqIndexedArchiver.
 */

typedef struct MmapqIndexedArchiver MmapqIndexedArchiver;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create an archiver for the named queue.
 *
 * The queue must have been created by an appender before archiving is
 * possible; create() succeeds immediately but archiveUpToIndex() and
 * archivable_files() will fail with an error if the queue does not yet
 * exist on disk.
 *
 * directory:  path to the directory containing queue files
 * queue_name: logical name of the queue
 * ring_size:  number of simultaneously mapped regions (must be >= 2)
 * runtime:    background region-mapper runtime for header access
 * Returns: a new archiver; never NULL (asserts on allocation failure)
 */
int mmapq_indexed_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    MmapqRuntime    *runtime,
    MmapqIndexedArchiver **out);

/* ── Archiving ───────────────────────────────────────────────────────────── */

/**
 * Return 1 if the queue is archivable, 0 otherwise.
 *
 * A queue is archivable if and only if its meta file was created with
 * archivable=1. Non-archivable queues always have start_index=0.
 */
int mmapq_indexed_archiver_archivable(MmapqIndexedArchiver *a);

/**
 * Archive all entries up to @p index (exclusive).
 *
 * Forward archiving (index > current start_index):
 *   - Walks headers backwards from index-1 to current start_index,
 *     recording the last committed payload position per appender.
 *   - Validates each payload position is readable.
 *   - Advances past the committed message to find the next write offset.
 *   - Writes that offset to appender_meta_N for each appender.
 *   - Commits the new start_index atomically in queue meta.
 *
 * Backward archiving / undo (index < current start_index):
 *   - Walks headers forward from index to current start_index.
 *   - Validates that all header and payload files covering that range
 *     still exist on disk.
 *   - Records the first payload position per appender in that range.
 *   - Writes those positions to appender_meta_N.
 *   - Commits the new (lower) start_index.
 *
 * No-op if index == current start_index.
 *
 * a:     the archiver
 * index: first unarchived index (all prior indices will be archived)
 * Returns: 0 on success, MMAPQ_EINVAL if archiver is NULL, index < 0,
 *         queue not created, or queue not archivable.
 */
int mmapq_indexed_archiver_archive_up_to_index(MmapqIndexedArchiver *a, int64_t index);

/* ── Query ───────────────────────────────────────────────────────────────── */

/**
 * Return the current start index (first unarchived entry).
 */
int64_t mmapq_indexed_archiver_start_index(MmapqIndexedArchiver *a);

/**
 * Return the payload start offset for the given appender at start_index.
 *
 * This is the byte offset in the appender's payload file from which a new
 * appender would resume writing after archiving. Returns -1 if the appender
 * ID is not used by this queue.
 */
int mmapq_indexed_archiver_start_offset(
    MmapqIndexedArchiver *a, uint8_t appender_id, int64_t *out_offset);

/**
 * Populate @p paths_out with paths to files fully covered by the archive
 * boundary (i.e. files safe to delete, move, or compress).
 *
 * Includes header_N and payload_N_M files whose entire index / byte range
 * falls below the current start_index / start_offset respectively.
 * Only files that actually exist on disk are included.
 *
 * a:         the archiver
 * paths_out: caller-allocated array of char* to populate
 * max_paths: capacity of paths_out
 * Returns: number of paths written to paths_out (may be less than max_paths)
 */
int mmapq_indexed_archiver_archivable_files(
    MmapqIndexedArchiver *a,
    char           **paths_out,
    int              max_paths);

/**
 * Append a human-readable summary of the queue's file layout and index
 * ranges to @p sb.
 *
 * a:  the archiver
 * sb: caller-allocated buffer to append to
 * n:  size of @p sb
 * Returns: number of bytes written (excluding null terminator)
 */
int mmapq_indexed_archiver_print_summary(MmapqIndexedArchiver *a, char *sb, int n);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * Close the archiver and release all associated resources.
 *
 * After this call the pointer must not be used.
 */
void mmapq_indexed_archiver_close(MmapqIndexedArchiver *a);

#endif /* MMAPQ_INDEXED_ARCHIVER_H */
