#ifndef QUEUE_FILES_H
#define QUEUE_FILES_H

#include <stdint.h>
#include <string.h>

/**
 * queue_files — shared file-system utilities for the queue archiver layer.
 *
 * Provides file existence checks, appender-ID discovery, and a compact
 * bitset-based file-index tracker used by both archivers.
 */

/* ── File existence ───────────────────────────────────────────────────────── */

/**
 * Return 1 if @p path exists on disk, 0 otherwise.
 */
int mmapqi_queue_file_exists(const char *path);

/* ── Appender ID discovery ───────────────────────────────────────────────── */

/**
 * Scan the queue directory for `appender_meta_N` files and populate
 * @p ids with the found appender IDs (0–255).
 *
 * directory: queue root directory
 * queue_name: queue name
 * ids:       caller-allocated array to populate (capacity max_ids)
 * max_ids:   capacity of @p ids
 * Returns: number of IDs written
 */
int mmapqi_queue_files_get_appender_ids(
    const char *directory,
    const char *queue_name,
    uint8_t    *ids,
    int         max_ids);

/* ── Archive lock ─────────────────────────────────────────────────────────── */

/**
 * mmapqi_archive_lock_acquire — take the queue's archive lock
 * (<directory>/<queue_name>/archive.lock, created on first use) via flock.
 * Archiving holds it exclusively; archivable_files holds it shared, so a
 * listing never observes an archive in progress (I6).
 *
 * The lock is advisory and per-inode: recreating the queue directory while
 * clients hold the lock breaks exclusivity — queue recreation requires all
 * clients closed (same rule as every other queue file).
 *
 * directory:   queue root directory
 * queue_name:  queue name
 * exclusive:   1 for LOCK_EX (archive_up_to_index), 0 for LOCK_SH (listing)
 * out_fd:      receives the lock-holding fd; release with close()
 * Returns: 0 on success; MMAPQ_EINVAL if the queue directory does not exist
 *          (mirrors the missing-queue outcome callers had before the lock);
 *          MMAPQ_EIO on any other failure. Blocking; retries on EINTR.
 */
int mmapqi_archive_lock_acquire(
    const char *directory,
    const char *queue_name,
    int         exclusive,
    int        *out_fd);

/* ── File-index bitset ────────────────────────────────────────────────────── */

/**
 * FileIndexSet — compact O(1) bitset tracking which rolled-file indices
 * have been seen.  The set covers any window of FILE_INDEX_SET_MAX
 * consecutive file numbers; insertion order is irrelevant — an index
 * below the current base rebases the window downward when the widened
 * range still fits.  A backward undo spanning more files is refused by
 * the archivers (remedy: undo in steps spanning fewer than
 * FILE_INDEX_SET_MAX files each).
 *
 * Used by the backward-archiving path to collect the set of header and
 * payload files that must exist before the archive boundary can be moved.
 */
#define FILE_INDEX_SET_MAX   4096
#define FILE_INDEX_SET_WORDS (FILE_INDEX_SET_MAX / 32)

typedef struct {
    uint32_t bits[FILE_INDEX_SET_WORDS];
    int64_t  base;      /* lowest index the window covers                  */
    int64_t  max;       /* highest index recorded                          */
    int      has_base;  /* 0 until the first file_index_set_set() call     */
} FileIndexSet;

static inline void file_index_set_clear(FileIndexSet *s)
{
    memset(s->bits, 0, sizeof(s->bits));
    s->base     = 0;
    s->max      = 0;
    s->has_base = 0;
}

/* Shift the bitmap up by k bit positions (0 < k < FILE_INDEX_SET_MAX),
 * making room for k new file numbers below the current base.  The window
 * invariant (max - new_base < FILE_INDEX_SET_MAX) guarantees no set bit
 * shifts off the top. */
static inline void file_index_set_shift_up(FileIndexSet *s, int64_t k)
{
    int words = (int)(k >> 5);
    int rem   = (int)(k & 31);
    for (int w = FILE_INDEX_SET_WORDS - 1; w >= 0; w--)
        s->bits[w] = (w >= words) ? s->bits[w - words] : 0;
    if (rem) {
        for (int w = FILE_INDEX_SET_WORDS - 1; w > 0; w--)
            s->bits[w] = (s->bits[w] << rem) | (s->bits[w - 1] >> (32 - rem));
        s->bits[0] <<= rem;
    }
}

/**
 * Provided for API completeness — test whether absolute index @p idx is set.
 * Currently unused internally; callers may use it for querying archived ranges.
 */
static inline int file_index_set_test(const FileIndexSet *s, int64_t idx)
{
    if (!s->has_base) return 0;
    int64_t rel = idx - s->base;
    if (rel < 0 || rel >= FILE_INDEX_SET_MAX) return 0;
    return (s->bits[rel >> 5] >> (rel & 31)) & 1;
}

/**
 * Store absolute index @p idx.  The first call fixes the window's base;
 * an index below the base rebases the window downward when the widened
 * range still fits.
 * Returns: 1 when stored; 0 when the window [idx, max] or [base, idx]
 * would span more than FILE_INDEX_SET_MAX files — the archivers refuse
 * the undo as posed.
 */
static inline int file_index_set_set(FileIndexSet *s, int64_t idx)
{
    if (!s->has_base) {
        s->base     = idx;
        s->max      = idx;
        s->has_base = 1;
        s->bits[0] |= 1u;
        return 1;
    }
    if (idx < s->base) {
        if (s->max - idx >= FILE_INDEX_SET_MAX) return 0;
        file_index_set_shift_up(s, s->base - idx);
        s->base = idx;
        s->bits[0] |= 1u;
        return 1;
    }
    if (idx > s->max) {
        if (idx - s->base >= FILE_INDEX_SET_MAX) return 0;
        s->max = idx;
    }
    s->bits[(idx - s->base) >> 5] |= (1u << ((idx - s->base) & 31));
    return 1;
}

/**
 * Iterate over set bits as ABSOLUTE indices (base + bit). Usage:
 *   for (int64_t i = mmapqi_file_index_set_first(s); i >= 0; i = mmapqi_file_index_set_next(s, i))
 */
int64_t mmapqi_file_index_set_first(const FileIndexSet *s);
int64_t mmapqi_file_index_set_next(const FileIndexSet *s, int64_t after);

#endif /* QUEUE_FILES_H */
