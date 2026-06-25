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
 *   queue_transfer_import(stdin, "/data/queues", "orders", 0, rt, rt);
 *
 * USAGE — Copy:
 *   queue_transfer_copy(reader, "/backup/queues", "orders_bak",
 *                       0, last, QUEUE_TRANSFER_REBASE, NULL, NULL, NULL, rt, rt);
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
 * @param index     queue index of the entry
 * @param data      pointer to the message payload (read-only during callback)
 * @param length    payload byte count
 * @param ctx       caller-supplied context (passed through from the API call)
 * @return 1 to include, 0 to skip
 */
typedef int (*QueueFilterFn)(int64_t index, const void *data,
                             int32_t length, void *ctx);

/* ── Export ──────────────────────────────────────────────────────────────── */

/**
 * Export entries [start, end] from a reader to an output stream.
 *
 * Writes a 64-byte ExportMeta header followed by each matching entry in
 * [uint16 appender_id][int32 length][payload] framing.
 *
 * @param reader           source queue reader
 * @param out              output stream (FILE*, typically a file or stdout)
 * @param start            first index to export (inclusive)
 * @param end              last index to export (inclusive)
 * @param start_index_mode QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * @param include          optional include filter (NULL = accept all)
 * @param exclude          optional exclude filter (NULL = skip none)
 * @param ctx              caller context passed to filter callbacks
 * @return number of entries written, or -1 on I/O error
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
 * @param reader          source queue reader
 * @param base_path       base file path (chunk files get _start-end suffix)
 * @param start           first index to export (inclusive)
 * @param end             last index to export (inclusive)
 * @param chunk_size_mb   approximate maximum size of each chunk in MB
 * @param start_index_mode QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * @param include         optional include filter (NULL = accept all)
 * @param exclude         optional exclude filter (NULL = skip none)
 * @param ctx             caller context passed to filter callbacks
 * @return total entries exported across all chunks, or -1 on error
 */
int64_t queue_transfer_export_chunked(
    IndexedReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx);

/* ── Import ──────────────────────────────────────────────────────────────── */

/**
 * Import a flat stream into a newly created (or appended) normal queue.
 *
 * Reads the ExportMeta header, creates (or opens) the destination queue
 * with matching parameters, and appends each entry.
 *
 * @param in            input stream (FILE*, typically a file or stdin)
 * @param directory     queue base directory
 * @param queue_name    queue subdirectory name
 * @param start_index   desired start index for the new queue (0 = rebase)
 * @param write_rt      runtime for writers (NULL = sync)
 * @param read_rt       runtime for readers (NULL = sync)
 * @return number of entries imported, or -1 on error
 */
int64_t queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index, Runtime *write_rt, Runtime *read_rt);

/**
 * Import a batched (batch queue) stream into a normal queue, flattening
 * each batch's messages into individual entries.
 *
 * Reads the BatchExportMeta header, creates the destination normal queue,
 * and flattens every batch — each message in the batch becomes its own
 * entry in the output queue.
 *
 * @param in            input stream (FILE*) containing batch stream
 * @param directory     queue base directory
 * @param queue_name    queue subdirectory name
 * @param start_index   desired start index for the new queue (0 = rebase)
 * @param write_rt      runtime for writers (NULL = sync)
 * @param read_rt       runtime for readers (NULL = sync)
 * @return total number of individual messages imported, or -1 on error
 */
int64_t queue_transfer_import_batch_stream(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index, Runtime *write_rt, Runtime *read_rt);

/* ── Copy ────────────────────────────────────────────────────────────────── */

/**
 * Copy entries [start, end] directly from a source queue reader to a new
 * destination queue.  Supports optional filters.
 *
 * Creates the destination queue with the same parameters as the source.
 *
 * @param reader           source queue reader
 * @param dest_dir         destination queue base directory
 * @param dest_queue       destination queue name
 * @param start            first index to copy (inclusive)
 * @param end              last index to copy (inclusive)
 * @param start_index_mode QUEUE_TRANSFER_REBASE or QUEUE_TRANSFER_PRESERVE
 * @param include          optional include filter (NULL = accept all)
 * @param exclude          optional exclude filter (NULL = skip none)
 * @param ctx              caller context passed to filter callbacks
 * @param write_rt         runtime for writers (NULL = sync)
 * @param read_rt          runtime for readers (NULL = sync)
 * @return number of entries copied, or -1 on error
 */
int64_t queue_transfer_copy(
    IndexedReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx,
    Runtime *write_rt, Runtime *read_rt);

#endif /* QUEUE_TRANSFER_H */
