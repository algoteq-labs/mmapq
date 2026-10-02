#ifndef MMAPQ_QUEUE_TRANSFER_H
#define MMAPQ_QUEUE_TRANSFER_H

/**
 * QueueTransfer — export, import, and copy a single-message indexed queue
 *                 to/from a portable flat stream format.
 *
 * Failures return negative MMAPQ_* codes, never a partial success count.
 * Transfers are not atomic: a committed destination prefix may remain.
 * Caller-owned streams are never closed; success excludes a later caller
 * flush/close failure. Chunked exports own their files and check close.
 *
 * Exports do not snapshot the source or seek back to rewrite counts. Keep
 * the exported range available and filters deterministic across passes.
 * A mismatch between declared and written entry/message counts returns
 * MMAPQ_ECORRUPT (an inconsistent export, not necessarily source corruption).
 * On any export error, discard the output stream or all files from that
 * chunked export attempt; partial output is not repaired or removed.
 *
 * EXPORTED STREAM FORMAT (flat):
 *   [ExportMeta: 64 bytes]
 *     schema_version, region_size, start_index, max_file_size,
 *     total_entries, archivable, queue_type
 *   [uint16 appender_id][int32 payload_length][uint8 payload...] ... (× total_entries)
 *
 * USAGE — Export:
 *   mmapq_queue_transfer_export(reader, stdout, 0, reader_last, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
 *
 * USAGE — Import:
 *   mmapq_queue_transfer_import(stdin, "/data/queues", "orders", 0, MMAPQ_QUEUE_TRANSFER_REBASE, rt, rt, 0);
 *
 * USAGE — Copy:
 *   mmapq_queue_transfer_copy(reader, "/backup/queues", "orders_bak",
 *                       0, last, MMAPQ_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL, rt, rt, 0);
 *
 * All functions support optional include/exclude filters.  Pass NULL for
 * both to accept all entries.
 *
 * Mirrors Java QueueTransfer, IndexedQueueCopy, IndexedQueueExport,
 * IndexedQueueImport.
 */

#include <stdint.h>
#include <stdio.h>
#include "indexed_reader.h"
#include "runtime.h"

/* Forward declarations for cross-type copy functions */
typedef struct MmapqIndexedBatchReader MmapqIndexedBatchReader;

/* ── Start index mode ─────────────────────────────────────────────────────── */

/** Start index in the export stream is rebased to 0 (default). */
#define MMAPQ_QUEUE_TRANSFER_REBASE    0

/** Start index in the export stream matches the source range start. */
#define MMAPQ_QUEUE_TRANSFER_PRESERVE  1

/* ── Appender ID mapper ──────────────────────────────────────────────────── */

/** Return -1 to auto-assign the next available appender ID. */
#define MMAPQ_APPENDER_ID_AUTO (-1)
#define MMAPQ_APPENDER_ID_ABORT INT32_MIN

/**
 * Map a source appender ID to a destination appender ID.
 *
 * Called once per entry during import/copy. The returned value is used as
 * the appender ID in the destination queue.
 *
 * source_aid: appender ID from the source (export stream or reader)
 * ctx:        caller-supplied context (passed through from the API call)
 * Returns: a valid appender ID (0-255), MMAPQ_APPENDER_ID_AUTO for auto-assign,
 * or MMAPQ_APPENDER_ID_ABORT to stop with MMAPQ_ECALLBACK after cleanup.
 */
typedef int32_t (*MmapqAppenderIdMapper)(uint16_t source_aid, void *ctx);

/**
 * MmapqConflictBehavior — what to do when a mapped appender ID is already in use.
 *
 * MMAPQ_CONFLICT_AUTO:  fall back to auto-assign only on ID conflict; other failures propagate
 * MMAPQ_CONFLICT_ABORT: return MMAPQ_EIDCONFLICT immediately
 */
typedef enum {
    MMAPQ_CONFLICT_AUTO  = 0,
    MMAPQ_CONFLICT_ABORT = 1,
} MmapqConflictBehavior;

/* ── Filter callback ─────────────────────────────────────────────────────── */

/**
 * Filter callback for single-message queue entries.
 *
 * Export may call this repeatedly for each entry across counting and writing
 * passes; return the same decision each time.
 * Return 1 to include the entry, 0 to skip it.
 *
 * index:    queue index of the entry
 * data:     pointer to the message payload (read-only during callback)
 * length:   payload byte count
 * ctx:      caller-supplied context (passed through from the API call)
 * Returns: 1 to include, 0 to skip
 */
typedef int (*MmapqQueueFilterFn)(int64_t index, const void *data,
                             int32_t length, void *ctx);

/**
 * Filter callback for batch queue messages.
 *
 * Batch export may call this repeatedly per message across counting and
 * writing passes; return the same decision each time.
 * Return 1 to include the message, 0 to skip it.
 *
 * batch_index:  index of the batch containing this message
 * msg_index:    zero-based index of this message within its batch
 * msg_count:    total number of messages in the batch
 * data:         pointer to the message payload (read-only during callback)
 * length:       payload byte count
 * ctx:          caller-supplied context
 * Returns: 1 to include, 0 to skip
 */
typedef int (*MmapqBatchQueueFilterFn)(int64_t batch_index, int32_t msg_index,
                                   int32_t msg_count, const void *data,
                                   int32_t length, void *ctx);

/* ── Export ──────────────────────────────────────────────────────────────── */

/**
 * Export entries [start, end] from a reader to an output stream.
 *
 * Writes a 64-byte ExportMeta header followed by each matching entry in
 * [uint16 appender_id][int32 length][payload] framing.
 * If the range clipped to the queue is empty, writes a zero-entry header
 * and returns 0. A missing/uninitialised source still reports its read error.
 *
 * reader:          source queue reader
 * out:             output stream (FILE*, typically a file or stdout)
 * start:           first index to export (inclusive)
 * end:             last index to export (inclusive)
 * start_index_mode: MMAPQ_QUEUE_TRANSFER_REBASE or MMAPQ_QUEUE_TRANSFER_PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * Returns: number of entries written, or a negative MMAPQ_* error code
 */
int64_t mmapq_queue_transfer_export(
    MmapqIndexedReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    MmapqQueueFilterFn include, MmapqQueueFilterFn exclude, void *ctx);

/**
 * Chunked export — split a range of entries into multiple files of
 * approximately @p chunk_size_mb megabytes each.
 *
 * Each chunk file is a complete, self-contained flat stream with its own
 * ExportMeta header and embedded index range in the file name:
 *   <base_path>_<start>-<end>
 *
 * With REBASE (default), each chunk's start_index resets to the cumulative
 * count.  With PRESERVE, each chunk retains the original source index.
 *
 * reader:         source queue reader
 * base_path:      base file path (chunk files get _start-end suffix)
 * start:          first index to export (inclusive)
 * end:            last index to export (inclusive)
 * chunk_size_mb:  approximate maximum size of each chunk in MB
 * start_index_mode: MMAPQ_QUEUE_TRANSFER_REBASE or MMAPQ_QUEUE_TRANSFER_PRESERVE
 * include:        optional include filter (NULL = accept all)
 * exclude:        optional exclude filter (NULL = skip none)
 * ctx:            caller context passed to filter callbacks
 * Returns: total entries exported across all chunks, or a negative MMAPQ_* error code
 */
int64_t mmapq_queue_transfer_export_chunked(
    MmapqIndexedReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    MmapqQueueFilterFn include, MmapqQueueFilterFn exclude, void *ctx);

/* ── Import ──────────────────────────────────────────────────────────────── */

/**
 * Import a flat stream into a newly created (or appended) normal queue.
 *
 * Reads the ExportMeta header, creates the destination queue with matching
 * parameters, and appends each entry.
 *
 * The start index is determined automatically:
 *   REBASE  → 0
 *   PRESERVE → meta.start_index from the stream header
 *
 * If the destination queue already exists:
 *   - allow_append=0 → returns MMAPQ_EEXIST
 *   - allow_append=1 + PRESERVE → returns MMAPQ_EINVAL
 *   - allow_append=1 + REBASE → appends
 * If destination exists but is a batch queue → returns MMAPQ_ESCHEMA.
 *
 * in:               input stream (FILE*, typically a file or stdin)
 * directory:        queue base directory
 * queue_name:       queue subdirectory name
 * start_index_mode: MMAPQ_QUEUE_TRANSFER_REBASE or MMAPQ_QUEUE_TRANSFER_PRESERVE
 * write_rt:         runtime for writers (NULL = sync)
 * read_rt:          runtime for readers (NULL = sync)
 * allow_append:     1 to allow appending to an existing queue, 0 to forbid
 * mapper:           MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:       caller context passed to the mapper callback
 * on_conflict:      MMAPQ_CONFLICT_AUTO (fall back to auto-assign) or
 *                   MMAPQ_CONFLICT_ABORT (return MMAPQ_EIDCONFLICT)
 * Returns: number of entries imported, or a negative MMAPQ_* error code
 */
int64_t mmapq_queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

/**
 * Import a batched (batch queue) stream into a normal queue, flattening
 * each batch's messages into individual entries.
 *
 * Reads the BatchExportMeta header, creates the destination normal queue,
 * and flattens every batch — each message in the batch becomes its own
 * entry in the output queue.
 *
 * Start index determined automatically (same rules as mmapq_queue_transfer_import).
 * Destination-exists enforcement same as mmapq_queue_transfer_import.
 *
 * Each batch in the stream carries a uint16 appender ID (matching the
 * normal queue framing). The mapper controls how these IDs are assigned
 * in the destination queue.
 *
 * in:               input stream (FILE*) containing batch stream
 * directory:        queue base directory
 * queue_name:       queue subdirectory name
 * start_index_mode: MMAPQ_QUEUE_TRANSFER_REBASE or MMAPQ_QUEUE_TRANSFER_PRESERVE
 * write_rt:         runtime for writers (NULL = sync)
 * read_rt:          runtime for readers (NULL = sync)
 * allow_append:     1 to allow appending to an existing queue, 0 to forbid
 * mapper:           MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:       caller context passed to the mapper callback
 * on_conflict:      MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: total number of individual messages imported, or a negative MMAPQ_* error code
 */
int64_t mmapq_queue_transfer_import_batch_stream(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

/* ── Copy ────────────────────────────────────────────────────────────────── */

/**
 * Copy entries [start, end] directly from a source queue reader to a new
 * destination queue.  Supports optional filters.
 *
 * Destination-exists enforcement same as mmapq_queue_transfer_import.
 *
 * Each entry's appender ID (from MmapqReadingContext) is passed through the
 * mapper to determine its destination appender.
 *
 * reader:          source queue reader
 * dest_dir:        destination queue base directory
 * dest_queue:      destination queue name
 * start:           first index to copy (inclusive)
 * end:             last index to copy (inclusive)
 * start_index_mode: MMAPQ_QUEUE_TRANSFER_REBASE or MMAPQ_QUEUE_TRANSFER_PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * allow_append:    1 to allow appending to an existing queue, 0 to forbid
 * mapper:          MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:      caller context passed to the mapper callback
 * on_conflict:     MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of entries copied, or a negative MMAPQ_* error code
 */
int64_t mmapq_queue_transfer_copy(
    MmapqIndexedReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    MmapqQueueFilterFn include, MmapqQueueFilterFn exclude, void *ctx,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

/**
 * Copy batches [start, end] from a batch queue reader and flatten each
 * batch's messages into individual entries in a normal queue.
 *
 * Each message in the source batch becomes its own entry in the destination
 * normal queue.  Supports optional message-level filters.
 *
 * Destination-exists enforcement same as mmapq_queue_transfer_import.
 *
 * Each batch's appender ID (from MmapqBatchReadingContext.appender_id) is
 * passed through the mapper to determine its destination appender.
 * All messages within a batch share the same appender ID.
 *
 * reader:          source batch queue reader
 * dest_dir:        destination normal queue base directory
 * dest_queue:      destination normal queue name
 * start:           first batch index to copy (inclusive)
 * end:             last batch index to copy (inclusive)
 * start_index_mode: MMAPQ_QUEUE_TRANSFER_REBASE or MMAPQ_QUEUE_TRANSFER_PRESERVE
 * include:         optional batch message include filter (NULL = accept all)
 * exclude:         optional batch message exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * allow_append:    1 to allow appending to an existing queue, 0 to forbid
 * mapper:          MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:      caller context passed to the mapper callback
 * on_conflict:     MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of individual messages copied, or a negative MMAPQ_* error code
 */
int64_t mmapq_queue_transfer_copy_batch_to_normal(
    MmapqIndexedBatchReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

#endif /* MMAPQ_QUEUE_TRANSFER_H */
