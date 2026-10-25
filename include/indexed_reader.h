#ifndef MMAPQ_INDEXED_READER_H
#define MMAPQ_INDEXED_READER_H

#include <stdint.h>
#include "runtime.h"

/**
 * MmapqIndexedReader — random access reader for the indexed queue.
 *
 * Reads any entry by index without advancing a cursor. Does not consume
 * messages — purely a reading view.
 *
 * READ OPERATION (read_at_index):
 *   1. If queue not yet created → status = MMAPQ_ENTRY_END_OF_STREAM.
 *   2. If index < startIndex → status = MMAPQ_ENTRY_ARCHIVED.
 *   3. Read header at index; if NULL_HEADER → status = MMAPQ_ENTRY_END_OF_STREAM.
 *   4. Decode appenderId + payloadPosition.
 *   5. Wrap payload accessor, read length, expose buffer.
 *   6. status = MMAPQ_ENTRY_AVAILABLE; data pointer and length are valid.
 *
 * The MmapqReadingContext flyweight is reused across calls. Its data pointer
 * into mapped memory is valid until the next read call.
 *
 * LAZY INIT:
 *   Region accessors created on first read or metadata call.
 *   If the queue does not exist, reads return MMAPQ_ENTRY_END_OF_STREAM.
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One MmapqIndexedReader per thread.
 *
 * lastIndex caching:
 *   last_index() performs an exponential+binary search. The result is
 *   cached and used as the starting point for subsequent searches,
 *   making repeated calls efficient as the queue grows monotonically.
 */

/* ── Entry status ────────────────────────────────────────────────────────── */

typedef enum {
    MMAPQ_ENTRY_AVAILABLE,     /* entry exists and is readable                    */
    MMAPQ_ENTRY_END_OF_STREAM, /* entry not yet written (index beyond last)       */
    MMAPQ_ENTRY_ARCHIVED,      /* entry was archived (index below startIndex)     */
    MMAPQ_ENTRY_ERROR,         /* mapping timeout, I/O failure, or invalid argument  */
} MmapqEntryStatus;

/* ── MmapqReadingContext ──────────────────────────────────────────────────────── */

/**
 * MmapqReadingContext — flyweight result of a read() call.
 *
 * Valid only until the next call to read_at_index() on the same reader.
 * The data pointer references mapped memory — do not cache it.
 */
typedef struct {
    int64_t     index;        /* requested index                              */
    uint8_t     appender_id;  /* which appender wrote this entry              */
    int64_t     msg_offset;   /* absolute payload offset of length prefix     */
    const void *data;         /* pointer into mapped payload (flyweight)      */
    int32_t     length;       /* payload byte count                           */
    MmapqEntryStatus status;       /* AVAILABLE / END_OF_STREAM / ARCHIVED         */
} MmapqReadingContext;

/* ── MmapqIndexedReader ───────────────────────────────────────────────────────── */

typedef struct MmapqIndexedReader MmapqIndexedReader;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_reader_create — create a new random-access reader.
 *
 * directory:    base queue directory
 * queue_name:   queue subdirectory name
 * ring_size:    number of regions in the ring
 * timeout_ns:   mapping timeout in nanoseconds
 * runtime:      region mapper runtime
 *
 * Note: configuration parameters (region_size, max_file_size) are
 * automatically discovered from the queue's metadata file.
 *
 * Lazy init — region accessors created on first use.
 */
int mmapq_indexed_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedReader **out);

/* ── Read operations ─────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_reader_read — read entry at the given index.
 *
 * Fills ctx and returns a pointer to it (the same ctx passed in).
 * ctx->status indicates whether an entry is available.
 * ctx->data and ctx->length are valid only when status == MMAPQ_ENTRY_AVAILABLE.
 *
 * ctx must remain valid for the duration of the call. The reader does NOT
 * take ownership — the caller allocates and owns the MmapqReadingContext.
 *
 * On mapping timeout, ctx->status is set to MMAPQ_ENTRY_ERROR.
 *
 * index must be in [0, 2^56 - 1]; outside that range ctx->status is
 * MMAPQ_ENTRY_ERROR with last_error == MMAPQ_EINVAL (indices beyond
 * 2^56 - 1 would alias already-written slots).
 */
MmapqReadingContext *mmapq_indexed_reader_read(
    MmapqIndexedReader  *r,
    int64_t         index,
    MmapqReadingContext *ctx);

/**
 * mmapq_indexed_reader_read_first — read the entry at startIndex.
 * Equivalent to mmapq_indexed_reader_read(r, startIndex, ctx).
 */
MmapqReadingContext *mmapq_indexed_reader_read_first(
    MmapqIndexedReader  *r,
    MmapqReadingContext *ctx);

/**
 * mmapq_indexed_reader_read_last — read the last committed entry.
 *
 * If the queue is empty, ctx->status = MMAPQ_ENTRY_END_OF_STREAM.
 */
MmapqReadingContext *mmapq_indexed_reader_read_last(
    MmapqIndexedReader  *r,
    MmapqReadingContext *ctx);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_reader_last_index — highest committed index, or HEADER_NULL_INDEX (-1)
 * if queue is empty or discovery fails. Inspect last_error to distinguish
 * failure from empty history. Result is cached for efficient repeated calls.
 */
int64_t mmapq_indexed_reader_last_index(MmapqIndexedReader *r);

/**
 * mmapq_indexed_reader_start_index — lowest readable index (fixed at creation for non-archivable queues — may be non-zero, e.g. a PRESERVE-mode transfer destination).
 */
int64_t mmapq_indexed_reader_start_index(MmapqIndexedReader *r);

/**
 * mmapq_indexed_reader_entry_status — check availability of entry at index.
 *
 * index above 2^56 - 1 returns MMAPQ_ENTRY_ERROR with last_error ==
 * MMAPQ_EINVAL (such indices would alias already-written slots);
 * has_entry inherits this as 0.
 */
MmapqEntryStatus mmapq_indexed_reader_entry_status(MmapqIndexedReader *r, int64_t index);

/**
 * mmapq_indexed_reader_has_entry — convenience: returns 1 if MMAPQ_ENTRY_AVAILABLE.
 */
static inline int mmapq_indexed_reader_has_entry(MmapqIndexedReader *r, int64_t index)
{
    return mmapq_indexed_reader_entry_status(r, index) == MMAPQ_ENTRY_AVAILABLE;
}

/* ── Error accessor ────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_reader_last_error — error from the most recent read or last_index.
 *
 * Zero means no error. Reads and last_index clear this value on entry;
 * inspect it immediately after MMAPQ_ENTRY_ERROR or a last_index failure.
 */
int mmapq_indexed_reader_last_error(const MmapqIndexedReader *r);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_reader_close — close and free the reader.
 * Idempotent.
 */
void mmapq_indexed_reader_close(MmapqIndexedReader *r);

#endif /* MMAPQ_INDEXED_READER_H */
