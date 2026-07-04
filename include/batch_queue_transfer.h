#ifndef BATCH_QUEUE_TRANSFER_H
#define BATCH_QUEUE_TRANSFER_H

/**
 * BatchQueueTransfer — export, import, and copy a batch queue
 *                      to/from a portable batched stream format.
 *
 * EXPORTED STREAM FORMAT (batched):
 *   [BatchExportMeta: 64 bytes]
 *     schema_version, region_size, start_index, max_file_size,
 *     total_batches, archivable
 *   [batch 0:
 *       int32  msg_count
 *       [int32 payload_length][uint8 payload...] ... (× msg_count)]
 *   [batch 1: ...]
 *   ... (× total_batches)
 *
 * USAGE — Export:
 *   batch_queue_transfer_export(breader, stdout, 0, last, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL);
 *
 * USAGE — Import:
 *   batch_queue_transfer_import(stdin, "/data/queues", "orders", 0, rt, rt);
 *
 * USAGE — Copy:
 *   batch_queue_transfer_copy(breader, "/backup/queues", "orders_bak",
 *                             0, last, BATCH_QUEUE_TRANSFER_REBASE, NULL, NULL, NULL, rt, rt);
 *
 * All functions support optional include/exclude filters.  Pass NULL for
 * both to accept all messages.
 *
 * Cross-over imports:
 *   queue_transfer_import_batch_stream() — batch stream → normal queue (flatten)
 *   batch_queue_transfer_import_flat_stream() — flat stream → batch queue (1-msg batches)
 *
 * Mirrors Java BatchQueueTransfer, IndexedBatchQueueCopy,
 * IndexedBatchQueueExport, IndexedBatchQueueImport.
 */

#include <stdint.h>
#include <stdio.h>
#include "indexed_batch_reader.h"
#include "runtime.h"
#include "batch_export_meta.h"

/* ── Start index mode ─────────────────────────────────────────────────────── */

/** Start index in the export stream is rebased to 0 (default). */
#define BATCH_QUEUE_TRANSFER_REBASE    0

/** Start index in the export stream matches the source range start. */
#define BATCH_QUEUE_TRANSFER_PRESERVE  1

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
typedef int (*BatchQueueFilterFn)(int64_t batch_index, int32_t msg_index,
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
 *
 * reader:          source batch queue reader
 * out:             output stream (FILE*)
 * start:           first batch index to export (inclusive)
 * end:             last batch index to export (inclusive)
 * start_index_mode: BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * Returns: number of batches written, or -1 on I/O error
 */
int64_t batch_queue_transfer_export(
    IndexedBatchReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    BatchQueueFilterFn include, BatchQueueFilterFn exclude, void *ctx);

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
 * start_index_mode: BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:        optional include filter (NULL = accept all)
 * exclude:        optional exclude filter (NULL = skip none)
 * ctx:            caller context passed to filter callbacks
 * Returns: total batches exported across all chunks, or -1 on error
 */
int64_t batch_queue_transfer_export_chunked(
    IndexedBatchReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    BatchQueueFilterFn include, BatchQueueFilterFn exclude, void *ctx);

/* ── Import ──────────────────────────────────────────────────────────────── */

/**
 * Import a batched stream into a newly created (or appended) batch queue.
 *
 * Reads the BatchExportMeta header, creates the destination batch queue
 * with matching parameters, and commits each batch.
 *
 * in:           input stream (FILE*) containing batched stream
 * directory:    queue base directory
 * queue_name:   queue subdirectory name
 * start_index:  desired start index for the new queue (0 = rebase)
 * write_rt:     runtime for writers (NULL = sync)
 * read_rt:      runtime for readers (NULL = sync)
 * Returns: number of batches imported, or -1 on error
 */
int64_t batch_queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index, Runtime *write_rt, Runtime *read_rt);

/**
 * Import a flat (single-message) stream into a batch queue, placing each
 * message into its own 1-element batch.
 *
 * Reads the ExportMeta header, creates the destination batch queue,
 * and for each entry starts a new batch, appends the single message,
 * and commits it.
 *
 * in:           input stream (FILE*) containing flat stream
 * directory:    queue base directory
 * queue_name:   queue subdirectory name
 * start_index:  desired start index for the new queue (0 = rebase)
 * write_rt:     runtime for writers (NULL = sync)
 * read_rt:      runtime for readers (NULL = sync)
 * Returns: number of batches (and messages) imported, or -1 on error
 */
int64_t batch_queue_transfer_import_flat_stream(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index, Runtime *write_rt, Runtime *read_rt);

/* ── Copy ────────────────────────────────────────────────────────────────── */

/**
 * Copy batches [start, end] directly from a source batch reader to a new
 * destination batch queue.  Supports optional filters.
 *
 * When filters are active, a batch is included only if it contains at
 * least one matching message.  Only matching messages are written to the
 * output batch.
 *
 * reader:          source batch queue reader
 * dest_dir:        destination queue base directory
 * dest_queue:      destination queue name
 * start:           first batch index to copy (inclusive)
 * end:             last batch index to copy (inclusive)
 * start_index_mode: BATCH_QUEUE_TRANSFER_REBASE or PRESERVE
 * include:         optional include filter (NULL = accept all)
 * exclude:         optional exclude filter (NULL = skip none)
 * ctx:             caller context passed to filter callbacks
 * write_rt:        runtime for writers (NULL = sync)
 * read_rt:         runtime for readers (NULL = sync)
 * Returns: number of batches copied, or -1 on error
 */
int64_t batch_queue_transfer_copy(
    IndexedBatchReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    BatchQueueFilterFn include, BatchQueueFilterFn exclude, void *ctx,
    Runtime *write_rt, Runtime *read_rt);

#endif /* BATCH_QUEUE_TRANSFER_H */
