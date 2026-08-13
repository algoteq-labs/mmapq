#ifndef MMAPQ_INDEXED_QUEUE_H
#define MMAPQ_INDEXED_QUEUE_H

#include <stdint.h>
#include "indexed_appender.h"
#include "indexed_archiver.h"
#include "indexed_poller.h"
#include "indexed_reader.h"
#include "runtime.h"

/**
 * MmapqIndexedQueue — top-level factory for an indexed message queue.
 *
 * An MmapqIndexedQueue owns the persistent configuration (QueueMeta, AppenderIdPool)
 * and creates appenders, pollers, and readers on demand.
 *
 * FILE LAYOUT (all under "<directory>/<queue_name>/"):
 *   meta                    — queue configuration (QueueMeta)
 *   header_N                — shared header file (N = file index 0, 1, 2...)
 *   payload_A_N             — per-appender payload file (A = appender ID,
 *                             N = file index 0, 1, 2...)
 *   appender_meta_A         — per-appender start offset
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
 *   - MmapqIndexedQueue itself is not thread-safe (construction/destruction).
 *   - Each appender/poller/reader is thread-confined to its creating thread.
 *   - Multiple appenders, pollers, and readers may coexist in the same process.
 *   - The queue may also be accessed simultaneously from other processes
 *     (multi-process shared memory queue).
 *
 * LIFECYCLE:
 *   1. mmapq_indexed_queue_create() — create or open the queue.
 *   2. mmapq_indexed_queue_create_appender() / _poller() / _reader() — create participants.
 *   3. Use participants (their own thread-confined lifecycles).
 *   4. Close participants before calling mmapq_indexed_queue_close().
 *   5. mmapq_indexed_queue_close() — close the queue and free resources.
 *
 * The queue does NOT close appenders/pollers/readers on close() — the caller
 * is responsible for closing all participants first.
 */

/* ── Default configuration ───────────────────────────────────────────────── */

#define MMAPQ_INDEXED_QUEUE_DEFAULT_REGION_SIZE         ((int32_t)(4096 * 1024))
#define MMAPQ_INDEXED_QUEUE_DEFAULT_RING_SIZE           ((int32_t)4)
#define MMAPQ_INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD ((int32_t)1)
#define MMAPQ_INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE       ((int64_t)MMAPQ_INDEXED_QUEUE_DEFAULT_REGION_SIZE * 256)
#define MMAPQ_INDEXED_QUEUE_DEFAULT_TIMEOUT_NS          ((uint64_t)10 * 1000000000ULL)

/* ── Geometry constraint helper ──────────────────────────────────────────── */

/*
 * mmapq_min_ring_size — minimum ring_size to avoid thrashing the header walk.
 *
 * The header vertical walk has a working set of one header block (32 KiB),
 * and the premap engine maps regions_to_map_ahead regions past the current
 * one. The ring span (ring_size * region_size) must hold both.
 *
 * Returns a power-of-two ring_size, or MMAPQ_INDEXED_QUEUE_DEFAULT_RING_SIZE for
 * invalid region_size (<= 0).
 */
int mmapq_min_ring_size(int32_t region_size);

/* ── MmapqIndexedQueue ────────────────────────────────────────────────────────── */

typedef struct MmapqIndexedQueue MmapqIndexedQueue;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_queue_create — open an existing indexed queue or prepare for creation.
 *
 * directory:          base directory (e.g. "/data/queues")
 * queue_name:         name of the queue (becomes a subdirectory)
 * region_size:        preferred region size (used ONLY if creating a new queue)
 * ring_size:          regions in the ring (power of two, typically 4)
 * regions_to_map_ahead: premap count (typically 1)
 * max_file_size:      preferred maximum file size (used ONLY if creating new)
 * start_index:        initial start index (used ONLY if creating new)
 * timeout_ns:         mapping timeout in nanoseconds
 * archivable:        1 to create the queue archivable (archiver may advance
 *                    start_index; readers pay an acquire load). Applies at
 *                    creation only — existing queues keep their on-disk value.
 *                    0 (default) is the performant mode.
 * write_runtime:     region mapper runtime for write-side
 * read_runtime:      region mapper runtime for read-side
 *
 * Does NOT create the queue files or directory immediately. The queue is
 * created when the first appender is created.
 *
 * Returns 0 on success and sets *out_queue. Returns a negative MMAPQ_* error
 * code on failure (EINVAL for bad arguments, ECLOSED for allocation failures)
 * and sets *out_queue to NULL.
 */
int mmapq_indexed_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *write_runtime,
    MmapqRuntime    *read_runtime,
    MmapqIndexedQueue **out_queue);

/**
 * mmapq_indexed_queue_create_default — create with default parameters.
 *
 * Uses INDEXED_QUEUE_DEFAULT_* constants.
 * runtimes must be provided — typically mmapq_runtime_region_mapper() or NULL.
 */
int mmapq_indexed_queue_create_default(
    const char *directory,
    const char *queue_name,
    MmapqRuntime    *write_runtime,
    MmapqRuntime    *read_runtime,
    MmapqIndexedQueue **out_queue);

/* ── Participant factories ────────────────────────────────────────────────── */

/**
 * mmapq_indexed_queue_create_appender — create a new appender with a fresh ID.
 *
 * Acquires an appender ID from the pool. The returned appender must be
 * closed with mmapq_indexed_appender_close() before mmapq_indexed_queue_close().
 *
 * q:   the queue (must not be closed)
 * out: on success, set to the new appender
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_indexed_queue_create_appender(MmapqIndexedQueue *q, MmapqIndexedAppender **out);

/**
 * mmapq_indexed_queue_create_appender_with_id — create an appender with a specific ID.
 *
 * Returns MMAPQ_EIDCONFLICT if the ID is already in use.
 *
 * q:           the queue (must not be closed)
 * appender_id: the desired appender ID (0-255)
 * out:         on success, set to the new appender
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_indexed_queue_create_appender_with_id(
    MmapqIndexedQueue *q,
    uint8_t       appender_id,
    MmapqIndexedAppender **out);

/**
 * mmapq_indexed_queue_create_poller — create a new sequential poller.
 *
 * The poller starts at the queue's current start_index (0 for non-archivable).
 * Close with mmapq_indexed_poller_close() before mmapq_indexed_queue_close().
 *
 * q:   the queue (must not be closed)
 * out: on success, set to the new poller
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_indexed_queue_create_poller(MmapqIndexedQueue *q, MmapqIndexedPoller **out);

/**
 * mmapq_indexed_queue_create_reader — create a new random-access reader.
 *
 * Close with mmapq_indexed_reader_close() before mmapq_indexed_queue_close().
 *
 * q:   the queue (must not be closed)
 * out: on success, set to the new reader
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_indexed_queue_create_reader(MmapqIndexedQueue *q, MmapqIndexedReader **out);

/**
 * mmapq_indexed_queue_create_archiver — create an archiver for this queue.
 *
 * The queue must have been created with archivable=1. Use the archiver to
 * advance the start index, update per-appender start offsets, and obtain
 * the list of files that can be safely deleted or moved.
 *
 * Returns MMAPQ_EINVAL if the queue is not archivable.
 *
 * The archiver must be closed independently with mmapq_indexed_archiver_close().
 *
 * q:   the queue (must not be closed, must be archivable)
 * out: on success, set to the new archiver
 * Returns: 0 on success, or an MMAPQ_* error code
 */
int mmapq_indexed_queue_create_archiver(MmapqIndexedQueue *q, MmapqIndexedArchiver **out);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_queue_region_size — configured region size in bytes.
 */
int32_t mmapq_indexed_queue_region_size(const MmapqIndexedQueue *q);

/**
 * mmapq_indexed_queue_max_file_size — configured maximum file size in bytes.
 */
int64_t mmapq_indexed_queue_max_file_size(const MmapqIndexedQueue *q);

/**
 * mmapq_indexed_queue_start_index — current logical start index (set at creation; may be non-zero after PRESERVE import/copy).
 */
int64_t mmapq_indexed_queue_start_index(const MmapqIndexedQueue *q);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_queue_close — close the queue and release all resources.
 *
 * Closes the QueueMeta and AppenderIdPool.
 * Does NOT close any appenders, pollers, or readers — those must be
 * closed by the caller first.
 *
 * Idempotent.
 */
void mmapq_indexed_queue_close(MmapqIndexedQueue *q);

#endif /* MMAPQ_INDEXED_QUEUE_H */
