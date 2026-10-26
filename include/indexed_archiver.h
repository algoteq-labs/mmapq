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
 * Physical reclamation follows mark -> reclaim: archivable_files() returns
 * the marks, then reclaim_files() deletes the marks that a fresh COMPLETE
 * listing still reports — the re-list and the unlink happen in a single
 * shared hold of archive.lock, so an undo cannot slip between them and lose
 * its restored files (CAND-36; see doc/design/archiving-design.md, "Deleting
 * files"). There is no wait: CAND-54's commit-intent protocol bounds each
 * lane's listing at the least of start_offset, the live floor and the
 * intent, so a file a
 * stalled commit is about to publish into is never listed. A mark is a
 * snapshot, not permission. Callers who delete files themselves instead of
 * calling reclaim_files() are safe provided no undo (archive_up_to_index
 * to an index below start_index) runs between their archivable_files()
 * call and their last unlink() of the files it returned.
 *
 * Commit order is not write order: append_uncommitted + commit lets an
 * application commit payloads in any order (UDP resequencing). The payload
 * boundary is therefore computed from headers — the end of the archived
 * entry with the maximum payload position, lowered below any live payload
 * that sits under it — and an uncommitted payload overtaken by archiving
 * can no longer be committed (commit returns MMAPQ_EINVAL).
 *
 * Archiving is serialised per queue on <dir>/<queue>/archive.lock (flock):
 * archive_up_to_index holds it exclusively, archivable_files and
 * reclaim_files shared — a listing or reclaim never observes an archive in
 * progress, and an archive or undo never runs while a reclaim holds the
 * lock. The lock file is created on first use, so archiving or listing a
 * queue in a read-only directory fails with MMAPQ_EIO.
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
 * archivable=1. A non-archivable queue's start_index is fixed at
 * creation (it may be non-zero, e.g. a PRESERVE-mode transfer
 * destination); archiving never moves it.
 */
int mmapq_indexed_archiver_archivable(MmapqIndexedArchiver *a);

/**
 * Archive all entries up to @p index (exclusive).
 *
 * Forward archiving (index > current start_index):
 *   - Reads and validates every header in [start_index, index) — each must
 *     reference a complete message, else MMAPQ_ECORRUPT before any mutation.
 *   - Per appender, computes the candidate boundary as the end of the
 *     archived entry with the maximum payload position (commit order is
 *     not write order under resequencing).
 *   - Publishes the boundary (never below the current one), then commits
 *     the new start_index atomically in queue meta.
 *   - Makes one pass over the live headers [index, end of stream),
 *     lowering the boundary below any live payload that sits under it.
 *   - If a live-pass read fails, restores every boundary to its pre-call
 *     value and returns the error; the new start_index stands (a retry of
 *     the same index is a no-op; the next forward archive recomputes the
 *     boundary).
 *
 * Backward archiving / undo (index < current start_index):
 *   - Walks headers forward from index to current start_index.
 *   - Validates that all header and payload files covering that range
 *     still exist on disk.
 *   - Lowers each appender's boundary to the lowest payload position in
 *     that range — never raises it.
 *   - Commits the new (lower) start_index.
 *
 * Both directions hold the queue's archive.lock exclusively (see the file
 * comment above).
 *
 * No-op if index == current start_index.
 *
 * a:     the archiver
 * index: first unarchived index (all prior indices will be archived)
 * Returns: 0 on success, MMAPQ_EINVAL if archiver is NULL, index is outside
 *         [0, 2^56 - 1], queue not created, or queue not archivable.
 */
int mmapq_indexed_archiver_archive_up_to_index(MmapqIndexedArchiver *a, int64_t index);

/* ── Query ───────────────────────────────────────────────────────────────── */

/**
 * Return the current start index (first unarchived entry).
 */
int64_t mmapq_indexed_archiver_start_index(MmapqIndexedArchiver *a);

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
int mmapq_indexed_archiver_start_offset(
    MmapqIndexedArchiver *a, uint8_t appender_id, int64_t *out_offset);

/**
 * Populate @p paths_out with paths to files fully covered by the archive
 * boundary at the time of the call: files no live entry can need at the
 * time of the call. Delete them with reclaim_files() (always safe). If
 * you delete, move or compress them yourself, they are safe provided no
 * undo (archive_up_to_index below the current start_index) runs between
 * this call and your last unlink. An undo that ran before this call is
 * harmless, because the listing already reflects it.
 *
 * Includes header_N and payload_N_M files whose entire index / byte range
 * falls below the current start_index / start_offset respectively, the
 * latter bounded per lane by the least of start_offset, the live floor
 * and the commit intent (CAND-35 + CAND-54): the live-header scan never lists below what it can
 * see is live, and a lane whose slow-path commit is in flight is bounded
 * by its stored intent — a file such a commit is about to publish into is
 * never listed, with no timing assumption (the pre-CAND-54 deletion wait
 * is retired). Only files that actually exist on disk are included.
 * See doc/design/archiving-design.md.
 *
 * a:         the archiver
 * paths_out: caller-allocated array of char* to populate
 * max_paths: capacity of paths_out
 * Returns: number of paths written to paths_out (may be less than
 *          max_paths), or a negative MMAPQ_* code: MMAPQ_ENOMEM
 *          (allocation failure), MMAPQ_ETIMEDOUT (a live-header read
 *          timed out during the cap scan; nothing listed).
 */
int mmapq_indexed_archiver_archivable_files(
    MmapqIndexedArchiver *a,
    char           **paths_out,
    int              max_paths);

/**
 * Delete the intersection of @p marked_paths with a fresh COMPLETE
 * archivable listing, in a single shared hold of archive.lock (CAND-36).
 *
 * This is the deletion step of the mark -> reclaim protocol: mark the
 * files from an archivable_files() listing, then pass the marks here
 * (always safe — no wait; CAND-54 retired it). The re-list and the unlink
 * happen under one archive.lock SH hold, so an undo (which takes the lock
 * exclusively) cannot slip between them and have its restored files
 * deleted. The re-list is complete: it grows until nothing was truncated,
 * so a mark whose files sort after other appenders' in the enumeration is
 * never starved. The re-list bounds each lane at the least of
 * start_offset, live_floor and the intent, exactly like
 * archivable_files() (CAND-35 + CAND-54)
 * — inherited through the shared listing code.
 *
 * Marks that a fresh listing no longer reports are skipped — they are live
 * again (an undo ran before the hold) or already gone. A file already
 * unlinked by a concurrent reclaimer (the shared lock admits them) is
 * tolerated, not counted. Marks are matched by exact string comparison
 * against the fresh listing — pass the path strings exactly as
 * archivable_files() returned them (no relative paths, no patterns).
 *
 * a:            the archiver
 * marked_paths: the marks — paths from an earlier archivable_files()
 *               listing (the array and strings are only read)
 * n_marked:     number of entries in marked_paths
 * Returns: the number of files this call unlinked (>= 0), or a negative
 *          MMAPQ_* code: MMAPQ_EINVAL (invalid arguments), MMAPQ_ENOMEM
 *          (allocation failure), MMAPQ_ETIMEDOUT (a live-header read
 *          timed out during the re-list's cap scan), MMAPQ_EIO (lock or
 *          unlink failure — files unlinked so far stay unlinked; a retry
 *          re-lists and skips them).
 *          A missing queue directory yields 0, mirroring archivable_files.
 */
int mmapq_indexed_archiver_reclaim_files(
    MmapqIndexedArchiver *a,
    char           **marked_paths,
    int              n_marked);

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
