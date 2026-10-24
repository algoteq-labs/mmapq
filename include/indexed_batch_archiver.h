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
 * Physical reclamation follows mark -> reclaim: archivable_files() returns
 * the marks, then reclaim_files() deletes the marks that a fresh COMPLETE
 * listing still reports — the re-list and the unlink happen in a single
 * shared hold of archive.lock, so an undo cannot slip between them and lose
 * its restored files (CAND-36; see doc/archiving-design.md, "Deleting
 * files"). There is no wait: CAND-54's commit-intent protocol bounds each
 * lane's listing at the least of start_offset, the live floor and the
 * intent, so a file a
 * stalled commit is about to publish into is never listed. A mark is a
 * snapshot, not permission. Callers who delete files themselves instead of
 * calling reclaim_files() are safe provided no undo (archive_up_to_index
 * to an index below start_index) runs between their archivable_files()
 * call and their last unlink() of the files it returned.
 *
 * The payload boundary is computed from headers — the end of the archived
 * batch with the maximum payload position, lowered below any live batch
 * that sits under it (see indexed_archiver.h for the full rule).
 *
 * Archiving is serialised per queue on <dir>/<queue>/archive.lock (flock):
 * archive_up_to_index holds it exclusively, archivable_files and
 * reclaim_files shared — a listing or reclaim never observes an archive in
 * progress, and an archive or undo never runs while a reclaim holds the
 * lock. The lock file is created on first use, so archiving or listing a
 * queue in a read-only directory fails with MMAPQ_EIO.
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
 * below it may be reclaimed. A joining appender with no entry in the live
 * range walks forward from the archive line — exactly the larger of
 * start_offset and the archive line (CAND-54 L(ii); the line never moves
 * back) — never from a write position. Returns 0 if the appender ID is not used by this
 * queue.
 */
int mmapq_indexed_batch_archiver_start_offset(
    MmapqIndexedBatchArchiver *a, uint8_t appender_id, int64_t *out_offset);

/**
 * Populate @p paths_out with paths to files fully covered by the archive
 * boundary at the time of the call: files no live entry can need at the
 * time of the call. Delete them with reclaim_files() (always safe). If
 * you delete, move or compress them yourself, they are safe provided no
 * undo (archive_up_to_index below the current start_index) runs between
 * this call and your last unlink. An undo that ran before this call is
 * harmless, because the listing already reflects it.
 *
 * Includes header_N and payload_N_M files whose entire range falls below
 * the current start_index / start_offset, the latter bounded by
 * the least of start_offset, the live floor and the commit intent per
 * lane (CAND-35 + CAND-54 — a
 * lane whose commit_batch is in flight is bounded by its stored intent;
 * the pre-CAND-54 deletion wait is retired). Only existing files are
 * included.
 * See doc/archiving-design.md.
 *
 * a:         the archiver
 * paths_out: caller-allocated array of char* to populate
 * max_paths: capacity of paths_out
 * Returns: number of paths written (may be less than max_paths), or a
 *          negative MMAPQ_* code: MMAPQ_ENOMEM (allocation failure),
 *          MMAPQ_ETIMEDOUT (a live-header read timed out during the cap
 *          scan; nothing listed).
 */
int mmapq_indexed_batch_archiver_archivable_files(
    MmapqIndexedBatchArchiver *a,
    char                **paths_out,
    int                   max_paths);

/**
 * Delete the intersection of @p marked_paths with a fresh COMPLETE
 * archivable listing, in a single shared hold of archive.lock (CAND-36).
 *
 * Batch twin of mmapq_indexed_archiver_reclaim_files — see
 * indexed_archiver.h for the full contract (mark -> reclaim, always safe
 * — CAND-54 retired the wait; the re-list and the unlink under one
 * archive.lock SH hold so undo cannot interleave; skipped marks are live
 * again or already gone; ENOENT from a concurrent reclaimer is tolerated,
 * not counted). The re-list bounds each lane at the least of
 * start_offset, live_floor and the intent, exactly like
 * archivable_files() (CAND-35 + CAND-54) — inherited through the shared listing code. Marks are matched
 * by exact string comparison against the fresh listing — pass the path
 * strings exactly as archivable_files() returned them (no relative paths,
 * no patterns).
 *
 * a:            the archiver
 * marked_paths: the marks — paths from an earlier archivable_files()
 *               listing (the array and strings are only read)
 * n_marked:     number of entries in marked_paths
 * Returns: the number of files this call unlinked (>= 0), or a negative
 *          MMAPQ_* code: MMAPQ_EINVAL (invalid arguments), MMAPQ_ENOMEM
 *          (allocation failure), MMAPQ_ETIMEDOUT (a live-header read
 *          timed out during the re-list's cap scan), MMAPQ_EIO (lock or
 *          unlink failure).
 *          A missing queue directory yields 0, mirroring archivable_files.
 */
int mmapq_indexed_batch_archiver_reclaim_files(
    MmapqIndexedBatchArchiver *a,
    char                **marked_paths,
    int                   n_marked);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * Close the archiver and release all associated resources.
 */
void mmapq_indexed_batch_archiver_close(MmapqIndexedBatchArchiver *a);

#endif /* MMAPQ_INDEXED_BATCH_ARCHIVER_H */
