#ifndef INDEXED_BATCH_QUEUE_H
#define INDEXED_BATCH_QUEUE_H

#include <stdint.h>
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_archiver.h"
#include "runtime.h"

/**
 * IndexedBatchQueue — factory and lifecycle owner for an indexed batch queue.
 *
 * A batch queue stores ordered sequences of batches, each containing one or
 * more messages. It is the top-level object from which appenders, pollers,
 * and readers are created.
 *
 * Separate runtimes may be specified for the write side (appender) and the
 * read side (pollers, readers), allowing independent background mapper threads
 * with different core pinning. Passing the same runtime for both is valid.
 *
 * The queue does not own the appenders/pollers/readers it creates — each must
 * be closed independently before or after closing the queue.
 *
 * Mirrors Java IIndexedBatchQueue / IndexedBatchQueue.
 */

typedef struct IndexedBatchQueue IndexedBatchQueue;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create an indexed batch queue with explicit parameters.
 *
 * If the queue already exists on disk, the on-disk configuration (region_size,
 * max_file_size, start_index) is authoritative and the corresponding arguments
 * are ignored. The queue meta file is always the source of truth.
 *
 * @param directory           path to the directory for queue files
 * @param queue_name          logical name of the queue
 * @param region_size         payload region size in bytes (power of 2)
 * @param ring_size           number of simultaneously mapped regions
 * @param regions_to_map_ahead number of regions to pre-map in the background (0 = sync mode)
 * @param max_file_size       maximum size of a single rolled payload file
 * @param start_index         initial batch index (typically 0)
 * @param timeout_ns          region-mapping timeout in nanoseconds
 * @param write_runtime       background mapper runtime for the appender side
 * @param read_runtime        background mapper runtime for the poller/reader side
 * @return a new queue; never NULL (asserts on allocation failure)
 */
IndexedBatchQueue *indexed_batch_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    Runtime    *write_runtime,
    Runtime    *read_runtime);

/**
 * Create an indexed batch queue with default parameters.
 *
 * Uses INDEXED_QUEUE_DEFAULT_* constants from indexed_queue.h for
 * region_size, ring_size, regions_to_map_ahead, max_file_size, and timeout_ns.
 * start_index is 0.
 *
 * @param directory     path to the directory for queue files
 * @param queue_name    logical name of the queue
 * @param write_runtime background mapper runtime for the appender side
 * @param read_runtime  background mapper runtime for the poller/reader side
 * @return a new queue with default configuration
 */
IndexedBatchQueue *indexed_batch_queue_create_default(
    const char *directory,
    const char *queue_name,
    Runtime    *write_runtime,
    Runtime    *read_runtime);

/* ── Component factories ─────────────────────────────────────────────────── */

/**
 * Create a batch appender on the write side of this queue.
 *
 * Uses the queue's write_runtime for background region mapping.
 * The appender must be closed independently with indexed_batch_appender_close().
 *
 * @param q  the queue (must not be closed)
 * @return a new appender
 */
IndexedBatchAppender *indexed_batch_queue_create_appender(IndexedBatchQueue *q);

/**
 * Create a batch poller on the read side of this queue.
 *
 * Uses the queue's read_runtime for background region mapping.
 * The poller must be closed independently with indexed_batch_poller_close().
 *
 * @param q  the queue (must not be closed)
 * @return a new poller positioned at the queue's start index
 */
IndexedBatchPoller *indexed_batch_queue_create_poller(IndexedBatchQueue *q);

/**
 * Create a batch reader on the read side of this queue.
 *
 * Uses the queue's read_runtime for background region mapping.
 * The reader must be closed independently with indexed_batch_reader_close().
 *
 * @param q  the queue (must not be closed)
 * @return a new reader
 */
IndexedBatchReader *indexed_batch_queue_create_reader(IndexedBatchQueue *q);

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Create a batch archiver for this queue.
 *
 * The queue meta file must have been created with the archivable flag set
 * (via queue_meta_open_rw with archivable=1). The archiver must be closed
 * independently with indexed_batch_archiver_close().
 *
 * @param q  the queue (must not be closed)
 * @return a new batch archiver
 */
IndexedBatchArchiver *indexed_batch_queue_create_archiver(IndexedBatchQueue *q);

/**
 * Close the queue and release its configuration resources.
 *
 * Does not close appenders, pollers, or readers created from this queue —
 * those must be closed independently. After this call the queue pointer
 * must not be used.
 */
void indexed_batch_queue_close(IndexedBatchQueue *q);

#endif /* INDEXED_BATCH_QUEUE_H */
