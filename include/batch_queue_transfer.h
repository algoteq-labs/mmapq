#ifndef MMAPQ_BATCH_QUEUE_TRANSFER_H
#define MMAPQ_BATCH_QUEUE_TRANSFER_H

/**
 * BatchQueueTransfer — export, import, and copy a batch queue
 *                      to/from a portable batched stream format.
 *
 * Failures return negative MMAPQ_* codes, never a partial success count.
 * Transfers are not atomic: a committed destination prefix may remain,
 * but a failed in-progress batch is not published. Caller-owned streams
 * remain open; success excludes a later caller flush/close failure.
 * Chunked exports own their files and check close.
 *
 * The archivable flag travels with the data: export writes the source
 * queue's flag into the stream header; import and copy create the
 * destination with it (CAND-50 §11). An existing destination keeps its
 * on-disk flag — disk wins on every open — and the transfer proceeds.
 *
 * Exports do not snapshot the source or seek back to rewrite counts. Keep
 * the exported range available and filters deterministic across passes.
 * A mismatch between declared and written entry/message counts returns
 * MMAPQ_ECORRUPT (an inconsistent export, not necessarily source corruption).
 * On any export error, discard the output stream or all files from that
 * chunked export attempt; partial output is not repaired or removed.
 *
 * EXPORTED STREAM FORMAT (batched):
 *   [ExportMeta: 64 bytes; queue_type = QUEUE_TYPE_BATCH]
 *     schema_version, region_size, start_index, max_file_size,
 *     total_entries (= batch count), archivable, queue_type
 *   [batch 0:
 *       uint16 appender_id
 *       int32  msg_count
 *       [int32 payload_length][uint8 payload...] ... (× msg_count)]
 *   [batch 1: ...]
 *   ... (× total_batches)
 *
 * USAGE — Export:
 *   mmapq_batch_queue_transfer_export(breader, stdout, 0, last, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
 *
 * USAGE — Import:
 *   mmapq_batch_queue_transfer_import(stdin, "/data/queues", "orders", 0, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, rt, rt, 0);
 *
 * USAGE — Copy:
 *   mmapq_batch_queue_transfer_copy(breader, "/backup/queues", "orders_bak",
 *                             0, last, MMAPQ_BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL, rt, rt, 0);
 *
 * All functions support optional include/exclude filters.  Pass NULL for
 * both to accept all messages.
 *
 * Cross-over imports:
 *   mmapq_queue_transfer_import_batch_stream() — batch stream → normal queue (flatten)
 *   mmapq_batch_queue_transfer_import_flat_stream() — flat stream → batch queue (1-msg batches)
 *
 */

#include <stdint.h>
#include <stdio.h>
#include "indexed_batch_reader.h"
#include "runtime.h"
#include "queue_transfer.h"

/* Forward declaration for cross-type copy function */
typedef struct MmapqIndexedReader MmapqIndexedReader;

/* ── Start index mode ─────────────────────────────────────────────────────── */

/** Start index in the export stream is rebased to 0 (default). */
#define MMAPQ_BATCH_QUEUE_TRANSFER_REBASE    0

/** Start index in the export stream matches the source range start. */
#define MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE  1

/* ── Filter callback ─────────────────────────────────────────────────────── */

/**
 * Filter callback for batch queue messages.
 *
 * Called once per message within a batch during export/import/copy.
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
 * Export batches [start, end] from a batch reader to an output stream.
 *
 * Writes a 64-byte BatchExportMeta header followed by each matching batch
 * in [int32 msg_count][int32 length][payload]... framing.
 *
 * When filters are active, a batch is included only if it contains at
 * least one matching message.  Only matching messages within the batch
 * are written to the stream.
 * If the range clipped to the queue is empty, writes a zero-batch header
 * and returns 0. A missing/uninitialised source still reports its read error.
 *
 * reader:          source batch queue reader
 * out:             output stream (FILE*)
 * start:           first batch index to export (inclusive)
 * end:             last batch index to export (inclusive)
 * start_index_mode: MMAPQ_BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * Returns: number of batches written, or a negative MMAPQ_* error code
 */
int64_t mmapq_batch_queue_transfer_export(
    MmapqIndexedBatchReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx);

/**
 * Chunked export — split a range of batches into multiple files of
 * approximately @p chunk_size_mb megabytes each.
 *
 * Each chunk file is a complete, self-contained batched stream with its
 * own BatchExportMeta header and embedded index range in the file name.
 *
 * reader:         source batch queue reader
 * base_path:      base file path (chunk files get _start-end suffix)
 * start:          first batch index to export (inclusive)
 * end:            last batch index to export (inclusive)
 * chunk_size_mb:  approximate maximum size of each chunk in MB
 * start_index_mode: MMAPQ_BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:        optional include filter (NULL = accept all)
 * exclude:        optional exclude filter (NULL = skip none)
 * ctx:            caller context passed to filter callbacks
 * Returns: total batches exported across all chunks, or a negative MMAPQ_* error code
 */
int64_t mmapq_batch_queue_transfer_export_chunked(
    MmapqIndexedBatchReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx);

/* ── Import ──────────────────────────────────────────────────────────────── */

/**
 * Import a batched stream into a newly created (or appended) batch queue.
 *
 * Start index determined automatically:
 *   REBASE  → 0
 *   PRESERVE → meta.start_index from the stream header
 *
 * If the destination queue already exists:
 *   - allow_append=0 → returns MMAPQ_EEXIST
 *   - allow_append=1 + PRESERVE → returns MMAPQ_EINVAL
 *   - allow_append=1 + REBASE → appends
 * If destination exists but is a normal queue → returns MMAPQ_ESCHEMA.
 *
 * The stream carries the source queue's archivable flag; a created
 * destination takes it (CAND-50 §11). An existing destination keeps its
 * on-disk flag — disk wins on every open — and the import proceeds.
 *
 * Each batch in the stream carries a uint16 appender ID (matching the
 * normal queue framing). The mapper controls how these IDs are assigned
 * in the destination queue.
 *
 * in:               input stream (FILE*) containing batched stream
 * directory:        queue base directory
 * queue_name:       queue subdirectory name
 * start_index_mode: MMAPQ_BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * write_rt:         runtime for writers (NULL = sync)
 * read_rt:          runtime for readers (NULL = sync)
 * allow_append:     1 to allow appending to an existing queue, 0 to forbid
 * mapper:           MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:       caller context passed to the mapper callback
 * on_conflict:      MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of batches imported, or a negative MMAPQ_* error code
 */
int64_t mmapq_batch_queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

/**
 * Import a flat (single-message) stream into a batch queue, placing each
 * message into its own 1-element batch.
 *
 * Start index determined automatically (same rules as mmapq_batch_queue_transfer_import).
 * Destination-exists enforcement same as mmapq_batch_queue_transfer_import.
 *
 * Each entry in the flat stream carries a uint16 appender ID. The mapper
 * controls how these IDs are assigned in the destination batch queue.
 *
 * in:               input stream (FILE*) containing flat stream
 * directory:        queue base directory
 * queue_name:       queue subdirectory name
 * start_index_mode: MMAPQ_BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * write_rt:         runtime for writers (NULL = sync)
 * read_rt:          runtime for readers (NULL = sync)
 * allow_append:     1 to allow appending to an existing queue, 0 to forbid
 * mapper:           MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:       caller context passed to the mapper callback
 * on_conflict:      MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of batches (and messages) imported, or a negative MMAPQ_* error code
 */
int64_t mmapq_batch_queue_transfer_import_flat_stream(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

/* ── Copy ────────────────────────────────────────────────────────────────── */

/**
 * Copy batches [start, end] directly from a source batch reader to a new
 * destination batch queue.  Supports optional filters.
 *
 * Destination-exists enforcement same as mmapq_batch_queue_transfer_import.
 *
 * Each batch's appender ID (from MmapqBatchReadingContext.appender_id) is
 * passed through the mapper to determine its destination appender.
 * All messages within a batch share the same appender ID.
 *
 * reader:          source batch queue reader
 * dest_dir:        destination queue base directory
 * dest_queue:      destination queue name
 * start:           first batch index to copy (inclusive)
 * end:             last batch index to copy (inclusive)
 * start_index_mode: MMAPQ_BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * allow_append:    1 to allow appending to an existing queue, 0 to forbid
 * mapper:          MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:      caller context passed to the mapper callback
 * on_conflict:     MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of batches copied, or a negative MMAPQ_* error code
 */
int64_t mmapq_batch_queue_transfer_copy(
    MmapqIndexedBatchReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

/**
 * Copy entries [start, end] from a normal queue reader and place each
 * entry as a single-message batch in a batch queue.
 *
 * Each entry in the source becomes its own 1-element batch in the
 * destination batch queue.  Supports optional message-level filters.
 *
 * Destination-exists enforcement same as mmapq_batch_queue_transfer_import.
 *
 * Each entry's appender ID (from MmapqReadingContext) is passed through the
 * mapper to determine its destination batch appender.
 *
 * reader:          source normal queue reader
 * dest_dir:        destination batch queue base directory
 * dest_queue:      destination batch queue name
 * start:           first entry index to copy (inclusive)
 * end:             last entry index to copy (inclusive)
 * start_index_mode: MMAPQ_BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * allow_append:    1 to allow appending to an existing queue, 0 to forbid
 * mapper:          MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:      caller context passed to the mapper callback
 * on_conflict:     MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of batches (1 per entry) copied, or a negative MMAPQ_* error code
 */
int64_t mmapq_batch_queue_transfer_copy_normal_to_batch(
    MmapqIndexedReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict);

#endif /* MMAPQ_BATCH_QUEUE_TRANSFER_H */
