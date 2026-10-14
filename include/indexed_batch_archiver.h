#ifndef MMAPQ_INDEXED_BATCH_ARCHIVER_H
#define MMAPQ_INDEXED_BATCH_ARCHIVER_H

#include <stdint.h>
#include "runtime.h"

/**
 * MmapqIndexedBatchArchiver — manages archiving of entries in an indexed batch queue.
 *
 * Functionally identical to MmapqIndexedArchiver but operates on a batch queue
 * where each header entry points to a batch payload rather than a single
 * message. The key difference in the forward archiving path is how the
 * "next write offset" is calculated: for batches, the iterator must exhaust
 * the entire batch (all messages) to find nextPayloadPosition, whereas the
 * normal archiver reads a single length field after the committed position.
 *
 * The archiver does NOT delete files — archivable_files() returns the list
 * and the caller decides what to do with them. Callers must NOT delete
 * listed files immediately: mark, wait, then delete only files that a fresh
 * archivable_files() call taken after the wait still lists (see
 * doc/archiving-design.md, "Deleting files"). A mark is a snapshot, not
 * permission.
 *
 * The payload boundary is computed from headers — the end of the archived
 * batch with the maximum payload position, lowered below any live batch
 * that sits under it (see indexed_archiver.h for the full rule).
 *
 * Archiving is serialised per queue on <dir>/<queue>/archive.lock (flock):
 * archive_up_to_index holds it exclusively, archivable_files shared — a
 * listing never observes an archive in progress. The lock file is created
 * on first use, so archiving or listing a queue in a read-only directory
 * fails with MMAPQ_EIO.
 *
 * Archiving can move in two directions:
 *   Forward (index > current start_index): mark batches as archived.
 *   Backward / undo (index < current start_index): restore access —
 *   only possible if the files covering those batches still exist.
 *
 * Thread-confined: not safe for concurrent access.
 *
 * Mirrors Java IIndexedBatchArchiver / MmapqIndexedBatchArchiver.
 */

typedef struct MmapqIndexedBatchArchiver MmapqIndexedBatchArchiver;

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
int mmapq_indexed_batch_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    MmapqRuntime    *runtime,
    MmapqIndexedBatchArchiver **out);

/* ── Archiving ───────────────────────────────────────────────────────────── */

/**
 * Return 1 if the queue is archivable, 0 otherwise.
 */
int mmapq_indexed_batch_archiver_archivable(MmapqIndexedBatchArchiver *a);

/**
 * Archive all batches up to @p index (exclusive).
 *
 * Forward archiving (index > current start_index):
 *   - Reads and validates every header in [start_index, index) — each must
 *     reference a complete batch, else MMAPQ_ECORRUPT before any mutation.
 *   - Per appender, computes the candidate boundary as the position after
 *     the archived batch with the maximum payload position (the batch
 *     iterator exhausts that batch to find it).
 *   - Publishes the boundary (never below the current one), then commits
 *     the new start_index atomically in queue meta.
 *   - Makes one pass over the live headers [index, end of stream),
 *     lowering the boundary below any live batch that sits under it.
 *   - If a live-pass read fails, restores every boundary to its pre-call
 *     value and returns the error; the new start_index stands.
 *
 * Backward archiving / undo (index < current start_index):
 *   - Walks headers forward from index to current start_index.
 *   - Determines all header and payload file indices touched by the range.
 *   - Validates those files exist before modifying anything.
 *   - Lowers each appender's boundary to the lowest batch payload position
 *     in that range — never raises it.
 *   - Commits the new (lower) start_index.
 *
 * Both directions hold the queue's archive.lock exclusively (see the file
 * comment above).
 *
 * No-op if index == current start_index.
 *
 * a:     the archiver
 * index: first unarchived batch index (all prior indices archived);
 *        must be in [0, 2^56 - 1]
 * Returns: 0 on success, MMAPQ_EINVAL on invalid argument or non-archivable.
 */
int mmapq_indexed_batch_archiver_archive_up_to_index(MmapqIndexedBatchArchiver *a, int64_t index);

/* ── Query ───────────────────────────────────────────────────────────────── */

/**
 * Return the current start index (first unarchived batch).
 */
int64_t mmapq_indexed_batch_archiver_start_index(MmapqIndexedBatchArchiver *a);

/**
 * Return the payload boundary for the given appender.
 *
 * start_offset is the per-appender deletion boundary: payload files wholly
 * below it may be reclaimed. It is also the anchor a joining appender with
 * no entry in the live range walks forward from to find the end of its
 * payload stream — it is NOT a write position. Returns 0 if the appender
 * ID is not used by this queue.
 */
int mmapq_indexed_batch_archiver_start_offset(
    MmapqIndexedBatchArchiver *a, uint8_t appender_id, int64_t *out_offset);

/**
 * Populate @p paths_out with paths to files fully covered by the archive
 * boundary at the time of the call and safe to delete, move, or compress.
 *
 * Includes header_N and payload_N_M files whose entire range falls below
 * the current start_index / start_offset. Only existing files are included.
 *
 * DELETION PROTOCOL: a listing is a snapshot, not permission. Mark the
 * listed files, wait, then delete only files that a fresh
 * archivable_files() call taken AFTER the wait still lists. The wait must
 * outlast a writer's stall between publishing a commit and its boundary
 * re-check — normally a few instructions, so seconds to minutes covers
 * preemption; it cannot cover a writer process stopped in that window
 * (SIGSTOP, a debugger). Deletion safety in that window rests on the
 * caller following this protocol; the library does not enforce it.
 * See doc/archiving-design.md.
 *
 * a:         the archiver
 * paths_out: caller-allocated array of char* to populate
 * max_paths: capacity of paths_out
 * Returns: number of paths written (may be less than max_paths)
 */
int mmapq_indexed_batch_archiver_archivable_files(
    MmapqIndexedBatchArchiver *a,
    char                **paths_out,
    int                   max_paths);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * Close the archiver and release all associated resources.
 */
void mmapq_indexed_batch_archiver_close(MmapqIndexedBatchArchiver *a);

#endif /* MMAPQ_INDEXED_BATCH_ARCHIVER_H */
