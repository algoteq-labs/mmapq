#ifndef INDEXED_QUEUE_H
#define INDEXED_QUEUE_H

#include <stdint.h>
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "queue_meta.h"
#include "runtime.h"
#include "mmap_assert.h"

/**
 * IndexedQueue — top-level factory for an indexed message queue.
 *
 * An IndexedQueue owns the persistent configuration (QueueMeta, AppenderIdPool)
 * and creates appenders, pollers, and readers on demand.
 *
 * FILE LAYOUT (all under "<directory>/<queue_name>/"):
 *   meta                    — queue configuration (QueueMeta)
 *   header                  — shared header file (all appenders write here)
 *   payload_<N>             — one payload file per appender N
 *   appender_meta_<N>       — per-appender start offset
 *   appender_id_pool        — bitset for ID allocation
 *
 * DEFAULTS (mirrors Java IndexedQueueBuilder):
 *   region_size:        4 MB  (4096 * 1024)
 *   ring_size:          4
 *   regions_to_map_ahead: 1
 *   max_file_size:      1 GB  (region_size * 256)
 *   timeout_ns:         10 seconds
 *
 * THREAD MODEL:
 *   - IndexedQueue itself is not thread-safe (construction/destruction).
 *   - Each appender/poller/reader is thread-confined to its creating thread.
 *   - Multiple appenders, pollers, and readers may coexist in the same process.
 *   - The queue may also be accessed simultaneously from other processes
 *     (multi-process shared memory queue).
 *
 * LIFECYCLE:
 *   1. indexed_queue_create() — create or open the queue.
 *   2. indexed_queue_create_appender() / _poller() / _reader() — create participants.
 *   3. Use participants (their own thread-confined lifecycles).
 *   4. Close participants before calling indexed_queue_close().
 *   5. indexed_queue_close() — close the queue and free resources.
 *
 * The queue does NOT close appenders/pollers/readers on close() — the caller
 * is responsible for closing all participants first.
 */

/* ── Default configuration ───────────────────────────────────────────────── */

#define INDEXED_QUEUE_DEFAULT_REGION_SIZE         ((int32_t)(4096 * 1024))
#define INDEXED_QUEUE_DEFAULT_RING_SIZE           ((int32_t)4)
#define INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD ((int32_t)1)
#define INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE       ((int64_t)INDEXED_QUEUE_DEFAULT_REGION_SIZE * 256)
#define INDEXED_QUEUE_DEFAULT_TIMEOUT_NS          ((uint64_t)10 * 1000000000ULL)

/* ── IndexedQueue ────────────────────────────────────────────────────────── */

typedef struct IndexedQueue IndexedQueue;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * indexed_queue_create — open an existing indexed queue or prepare for creation.
 *
 * directory:          base directory (e.g. "/data/queues")
 * queue_name:         name of the queue (becomes a subdirectory)
 * region_size:        preferred region size (used ONLY if creating a new queue)
 * ring_size:          regions in the ring (power of two, typically 4)
 * regions_to_map_ahead: premap count (typically 1)
 * max_file_size:      preferred maximum file size (used ONLY if creating new)
 * start_index:        initial start index (used ONLY if creating new)
 * timeout_ns:         mapping timeout in nanoseconds
 * runtime:            region mapper runtime (shared across all participants)
 *
 * Does NOT create the queue files or directory immediately. The queue is
 * created when the first appender is created.
 *
 * Returns a heap-allocated IndexedQueue. Aborts on failure.
 */
IndexedQueue *indexed_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    int         fixed_size_8bytes);

/**
 * indexed_queue_create_default — create with default parameters.
 *
 * Uses INDEXED_QUEUE_DEFAULT_* constants.
 * runtime must be provided — typically runtime_region_mapper().
 */
IndexedQueue *indexed_queue_create_default(
    const char *directory,
    const char *queue_name,
    Runtime    *runtime);

/**
 * indexed_queue_create_default_opt — create with default parameters and options.
 */
IndexedQueue *indexed_queue_create_default_opt(
    const char *directory,
    const char *queue_name,
    Runtime    *runtime,
    int         fixed_size_8bytes);

/* ── Participant factories ────────────────────────────────────────────────── */

/**
 * indexed_queue_create_appender — create a new appender with a fresh ID.
 *
 * Acquires an appender ID from the pool. The returned appender must be
 * closed with indexed_appender_close() before indexed_queue_close().
 */
IndexedAppender *indexed_queue_create_appender(IndexedQueue *q);

/**
 * indexed_queue_create_appender_with_id — create an appender with a specific ID.
 *
 * Fails (MMAP_ASSERT) if the ID is already in use.
 */
IndexedAppender *indexed_queue_create_appender_with_id(
    IndexedQueue *q,
    uint8_t       appender_id);

/**
 * indexed_queue_create_poller — create a new sequential poller.
 *
 * The poller starts at the queue's current start_index (0 for non-archivable).
 * Close with indexed_poller_close() before indexed_queue_close().
 */
IndexedPoller *indexed_queue_create_poller(IndexedQueue *q);

/**
 * indexed_queue_create_reader — create a new random-access reader.
 *
 * Close with indexed_reader_close() before indexed_queue_close().
 */
IndexedReader *indexed_queue_create_reader(IndexedQueue *q);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * indexed_queue_region_size — configured region size in bytes.
 */
int32_t indexed_queue_region_size(const IndexedQueue *q);

/**
 * indexed_queue_max_file_size — configured maximum file size in bytes.
 */
int64_t indexed_queue_max_file_size(const IndexedQueue *q);

/**
 * indexed_queue_start_index — current start index (0 for non-archivable queues).
 */
int64_t indexed_queue_start_index(const IndexedQueue *q);

/**
 * indexed_queue_fixed_size_8bytes — configured fixed_size_8bytes flag.
 */
int indexed_queue_fixed_size_8bytes(const IndexedQueue *q);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * indexed_queue_close — close the queue and release all resources.
 *
 * Closes the QueueMeta and AppenderIdPool.
 * Does NOT close any appenders, pollers, or readers — those must be
 * closed by the caller first.
 *
 * Idempotent.
 */
void indexed_queue_close(IndexedQueue *q);

#endif /* INDEXED_QUEUE_H */
