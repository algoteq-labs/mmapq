#ifndef INDEXED_READER_H
#define INDEXED_READER_H

#include <stdint.h>
#include "region_accessor.h"
#include "runtime.h"
#include "mmap_assert.h"
#include "queue_meta.h"

/**
 * IndexedReader — random access reader for the indexed queue.
 *
 * Reads any entry by index without advancing a cursor. Does not consume
 * messages — purely a reading view.
 *
 * READ OPERATION (read_at_index):
 *   1. If queue not yet created → status = ENTRY_END_OF_STREAM.
 *   2. If index < startIndex → status = ENTRY_ARCHIVED.
 *   3. Read header at index; if NULL_HEADER → status = ENTRY_END_OF_STREAM.
 *   4. Decode appenderId + payloadPosition.
 *   5. Wrap payload accessor, read length, expose buffer.
 *   6. status = ENTRY_AVAILABLE; data pointer and length are valid.
 *
 * The ReadingContext flyweight is reused across calls. Its data pointer
 * into mapped memory is valid until the next read call.
 *
 * LAZY INIT:
 *   Region accessors created on first read or metadata call.
 *   If the queue does not exist, reads return ENTRY_END_OF_STREAM.
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One IndexedReader per thread.
 *
 * lastIndex caching:
 *   last_index() performs an exponential+binary search. The result is
 *   cached and used as the starting point for subsequent searches,
 *   making repeated calls efficient as the queue grows monotonically.
 */

/* ── Entry status ────────────────────────────────────────────────────────── */

typedef enum {
    ENTRY_AVAILABLE,     /* entry exists and is readable                    */
    ENTRY_END_OF_STREAM, /* entry not yet written (index beyond last)       */
    ENTRY_ARCHIVED,      /* entry was archived (index below startIndex)     */
} EntryStatus;

/* ── ReadingContext ──────────────────────────────────────────────────────── */

/**
 * ReadingContext — flyweight result of a read() call.
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
    EntryStatus status;       /* AVAILABLE / END_OF_STREAM / ARCHIVED         */
} ReadingContext;

/* ── IndexedReader ───────────────────────────────────────────────────────── */

typedef struct IndexedReader IndexedReader;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * indexed_reader_create — create a new random-access reader.
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
IndexedReader *indexed_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime);

/* ── Read operations ─────────────────────────────────────────────────────── */

/**
 * indexed_reader_read — read entry at the given index.
 *
 * Fills ctx and returns a pointer to it (the same ctx passed in).
 * ctx->status indicates whether an entry is available.
 * ctx->data and ctx->length are valid only when status == ENTRY_AVAILABLE.
 *
 * ctx must remain valid for the duration of the call. The reader does NOT
 * take ownership — the caller allocates and owns the ReadingContext.
 *
 * Aborts on mapping timeout.
 */
ReadingContext *indexed_reader_read(
    IndexedReader  *r,
    int64_t         index,
    ReadingContext *ctx);

/**
 * indexed_reader_read_first — read the entry at startIndex.
 * Equivalent to indexed_reader_read(r, startIndex, ctx).
 */
ReadingContext *indexed_reader_read_first(
    IndexedReader  *r,
    ReadingContext *ctx);

/**
 * indexed_reader_read_last — read the last committed entry.
 *
 * If the queue is empty, ctx->status = ENTRY_END_OF_STREAM.
 */
ReadingContext *indexed_reader_read_last(
    IndexedReader  *r,
    ReadingContext *ctx);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * indexed_reader_last_index — highest committed index, or HEADER_NULL_INDEX (-1)
 * if queue is empty. Result is cached for efficient repeated calls.
 */
int64_t indexed_reader_last_index(IndexedReader *r);

/**
 * indexed_reader_start_index — lowest readable index (0 if not archivable).
 */
int64_t indexed_reader_start_index(IndexedReader *r);

/**
 * indexed_reader_entry_status — check availability of entry at index.
 */
EntryStatus indexed_reader_entry_status(IndexedReader *r, int64_t index);

/**
 * indexed_reader_has_entry — convenience: returns 1 if ENTRY_AVAILABLE.
 */
static inline int indexed_reader_has_entry(IndexedReader *r, int64_t index)
{
    return indexed_reader_entry_status(r, index) == ENTRY_AVAILABLE;
}

/**
 * indexed_reader_queue_meta — returns the reader's QueueMeta after lazy init,
 * or NULL if the queue does not exist.
 */
QueueMeta *indexed_reader_queue_meta(IndexedReader *r);

/**
 * indexed_reader_directory — returns the base directory string.
 */
const char *indexed_reader_directory(IndexedReader *r);

/**
 * indexed_reader_queue_name — returns the queue name string.
 */
const char *indexed_reader_queue_name(IndexedReader *r);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * indexed_reader_close — close and free the reader.
 * Idempotent.
 */
void indexed_reader_close(IndexedReader *r);

#endif /* INDEXED_READER_H */
