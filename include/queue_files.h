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
int queue_file_exists(const char *path);

/* ── Appender ID discovery ───────────────────────────────────────────────── */

/**
 * Scan the queue directory for `appender_meta_N` files and populate
 * @p ids with the found appender IDs (0–255).
 *
 * @param directory  queue root directory
 * @param queue_name queue name
 * @param ids        caller-allocated array to populate (capacity max_ids)
 * @param max_ids    capacity of @p ids
 * @return number of IDs written
 */
int queue_files_get_appender_ids(
    const char *directory,
    const char *queue_name,
    uint8_t    *ids,
    int         max_ids);

/* ── File-index bitset ────────────────────────────────────────────────────── */

/**
 * FileIndexSet — compact O(1) bitset tracking which rolled-file indices
 * have been seen.  Supports up to 4096 distinct file indices, covering
 * a queue of up to 4096 * max_file_size bytes — well beyond any practical
 * queue size.
 *
 * Used by the backward-archiving path to collect the set of header and
 * payload files that must exist before the archive boundary can be moved.
 */
#define FILE_INDEX_SET_MAX   4096
#define FILE_INDEX_SET_WORDS (FILE_INDEX_SET_MAX / 32)

typedef struct {
    uint32_t bits[FILE_INDEX_SET_WORDS];
} FileIndexSet;

static inline void file_index_set_clear(FileIndexSet *s)
{
    memset(s->bits, 0, sizeof(s->bits));
}

/**
 * Provided for API completeness — test whether index @p idx is set.
 * Currently unused internally; callers may use it for querying archived ranges.
 */
static inline int file_index_set_test(const FileIndexSet *s, int idx)
{
    if (idx < 0 || idx >= FILE_INDEX_SET_MAX) return 0;
    return (s->bits[idx >> 5] >> (idx & 31)) & 1;
}

static inline void file_index_set_set(FileIndexSet *s, int idx)
{
    if (idx >= 0 && idx < FILE_INDEX_SET_MAX)
        s->bits[idx >> 5] |= (1u << (idx & 31));
}

/**
 * Iterate over set bits. Usage:
 *   for (int i = file_index_set_first(s); i >= 0; i = file_index_set_next(s, i))
 */
int file_index_set_first(const FileIndexSet *s);
int file_index_set_next(const FileIndexSet *s, int after);

#endif /* QUEUE_FILES_H */
