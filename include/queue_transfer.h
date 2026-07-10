#ifndef QUEUE_TRANSFER_H
#define QUEUE_TRANSFER_H

/**
 * QueueTransfer — export, import, and copy a single-message indexed queue
 *                 to/from a portable flat stream format.
 *
 * EXPORTED STREAM FORMAT (flat):
 *   [ExportMeta: 64 bytes]
 *     schema_version, region_size, start_index, max_file_size,
 *     total_entries, archivable, fixed_size_8bytes
 *   [uint16 appender_id][int32 payload_length][uint8 payload...] ... (× total_entries)
 *
 * USAGE — Export:
 *   queue_transfer_export(reader, stdout, 0, reader_last, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
 *
 * USAGE — Import:
 *   queue_transfer_import(stdin, "/data/queues", "orders", 0, QUEUE_TRANSFER_REBASE, rt, rt, 0);
 *
 * USAGE — Copy:
 *   queue_transfer_copy(reader, "/backup/queues", "orders_bak",
 *                       0, last, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL, rt, rt, 0);
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
#include "export_meta.h"

/* Forward declarations for cross-type copy functions */
typedef struct IndexedBatchReader IndexedBatchReader;

/* ── Start index mode ─────────────────────────────────────────────────────── */

/** Start index in the export stream is rebased to 0 (default). */
#define QUEUE_TRANSFER_REBASE    0

/** Start index in the export stream matches the source range start. */
#define QUEUE_TRANSFER_PRESERVE  1

/* ── Filter callback ─────────────────────────────────────────────────────── */

/**
 * Filter callback for single-message queue entries.
 *
 * Called once per entry during export/import/copy.
 * Return 1 to include the entry, 0 to skip it.
 *
 * index:    queue index of the entry
 * data:     pointer to the message payload (read-only during callback)
 * length:   payload byte count
 * ctx:      caller-supplied context (passed through from the API call)
 * Returns: 1 to include, 0 to skip
 */
typedef int (*QueueFilterFn)(int64_t index, const void *data,
                             int32_t length, void *ctx);

/**
 * Filter callback for batch queue messages.
 *
 * Called once per message within a batch during batch queue export/import/copy.
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
typedef int (*BatchQueueFilterFn)(int64_t batch_index, int32_t msg_index,
                                   int32_t msg_count, const void *data,
                                   int32_t length, void *ctx);

/* ── Export ──────────────────────────────────────────────────────────────── */

/**
 * Export entries [start, end] from a reader to an output stream.
 *
 * Writes a 64-byte ExportMeta header followed by each matching entry in
 * [uint16 appender_id][int32 length][payload] framing.
 *
 * reader:          source queue reader
 * out:             output stream (FILE*, typically a file or stdout)
 * start:           first index to export (inclusive)
 * end:             last index to export (inclusive)
 * start_index_mode: QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * Returns: number of entries written, or -1 on I/O error
 */
int64_t queue_transfer_export(
    IndexedReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx);

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
 * start_index_mode: QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * include:        optional include filter (NULL = accept all)
 * exclude:        optional exclude filter (NULL = skip none)
 * ctx:            caller context passed to filter callbacks
 * Returns: total entries exported across all chunks, or -1 on error
 */
int64_t queue_transfer_export_chunked(
    IndexedReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx);

/* ── Import ──────────────────────────────────────────────────────────────── */

/**
 * Import a flat stream into a newly created (or appended) normal queue.
 *
 * Reads the ExportMeta header, creates the destination queue with matching
 * parameters, and appends each entry.
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
 * start_index:      desired start index for the new queue
 * start_index_mode: QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * write_rt:         runtime for writers (NULL = sync)
 * read_rt:          runtime for readers (NULL = sync)
 * allow_append:     1 to allow appending to an existing queue, 0 to forbid
 * Returns: number of entries imported, or -1 on error
 */
int64_t queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index, int start_index_mode,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append);

/**
 * Import a batched (batch queue) stream into a normal queue, flattening
 * each batch's messages into individual entries.
 *
 * Reads the BatchExportMeta header, creates the destination normal queue,
 * and flattens every batch — each message in the batch becomes its own
 * entry in the output queue.
 *
 * Destination-exists enforcement same as queue_transfer_import.
 *
 * in:               input stream (FILE*) containing batch stream
 * directory:        queue base directory
 * queue_name:       queue subdirectory name
 * start_index:      desired start index for the new queue
 * start_index_mode: QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * write_rt:         runtime for writers (NULL = sync)
 * read_rt:          runtime for readers (NULL = sync)
 * allow_append:     1 to allow appending to an existing queue, 0 to forbid
 * Returns: total number of individual messages imported, or -1 on error
 */
int64_t queue_transfer_import_batch_stream(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index, int start_index_mode,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append);

/* ── Copy ────────────────────────────────────────────────────────────────── */

/**
 * Copy entries [start, end] directly from a source queue reader to a new
 * destination queue.  Supports optional filters.
 *
 * Destination-exists enforcement same as queue_transfer_import.
 *
 * reader:          source queue reader
 * dest_dir:        destination queue base directory
 * dest_queue:      destination queue name
 * start:           first index to copy (inclusive)
 * end:             last index to copy (inclusive)
 * start_index_mode: QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * allow_append:    1 to allow appending to an existing queue, 0 to forbid
 * Returns: number of entries copied, or -1 on error
 */
int64_t queue_transfer_copy(
    IndexedReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append);

/**
 * Copy batches [start, end] from a batch queue reader and flatten each
 * batch's messages into individual entries in a normal queue.
 *
 * Each message in the source batch becomes its own entry in the destination
 * normal queue.  Supports optional message-level filters.
 *
 * Destination-exists enforcement same as queue_transfer_import.
 *
 * reader:          source batch queue reader
 * dest_dir:        destination normal queue base directory
 * dest_queue:      destination normal queue name
 * start:           first batch index to copy (inclusive)
 * end:             last batch index to copy (inclusive)
 * start_index_mode: QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * include:         optional batch message include filter (NULL = accept all)
 * exclude:         optional batch message exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * allow_append:    1 to allow appending to an existing queue, 0 to forbid
 * Returns: number of individual messages copied, or -1 on error
 */
int64_t queue_transfer_copy_batch_to_normal(
    IndexedBatchReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    BatchQueueFilterFn include, BatchQueueFilterFn exclude, void *ctx,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append);

#endif /* QUEUE_TRANSFER_H */
