#ifndef MMAPQ_INDEXED_BATCH_APPENDER_H
#define MMAPQ_INDEXED_BATCH_APPENDER_H

#include <stdint.h>
#include "batch_payload_iterator.h"
#include "runtime.h"

/**
 * MmapqIndexedBatchAppender — producer-side entry point for appending batched messages
 * to an indexed queue.
 *
 * Batches are constructed and committed in a two-phase flow:
 *
 *   1. Start a new batch:   MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
 *   2. Append messages:     mmapq_mutable_batch_append(b, data, length);
 *   3. Complete the batch:  int64_t offset = mmapq_mutable_batch_complete(b);
 *   4. Commit the batch:    int64_t index  = mmapq_indexed_batch_appender_commit_batch(a, offset);
 *
 * The returned batch index can be used for random-access reading via an
 * indexed batch reader.
 *
 * Thread safety: one appender per producer lane. Multiple producers may
 * operate concurrently on separate appender instances; they must not share
 * a single MmapqIndexedBatchAppender.
 *
 * Mirrors Java IIndexedBatchAppender.
 */

typedef struct MmapqIndexedBatchAppender MmapqIndexedBatchAppender;

/**
 * MmapqMutableBatch — an in-progress batch being constructed before commit.
 *
 * Owned by the appender. Valid only between mmapq_indexed_batch_appender_start_batch()
 * and mmapq_indexed_batch_appender_commit_batch(). Do not retain a pointer to it across
 * a commit or close.
 *
 * The internal iterator used by mmapq_mutable_batch_get() and mmapq_mutable_batch_iterator()
 * is shared — these two accessors must not be used concurrently on the same batch.
 *
 * Mirrors Java IMutableBatch.
 */
typedef struct MmapqMutableBatch       MmapqMutableBatch;

struct MmapqMutableBatch {
    MmapqIndexedBatchAppender *appender;
    int64_t               batch_offset;
    int32_t               message_count;
};

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * Create an appender for the named queue in the given directory.
 *
 * If the queue does not yet exist, it is created. If it already exists the
 * appender adopts the on-disk configuration (region_size, max_file_size)
 * and ignores the corresponding arguments — the queue meta file is
 * authoritative.
 *
 * directory:   path to the directory containing queue files
 * queue_name:  logical name of the queue
 * region_size: preferred payload region size (power of 2, ignored if queue exists)
 * ring_size:   number of simultaneously mapped regions
 * max_file_size: maximum rolled file size
 * timeout_ns:  region-mapping timeout in nanoseconds
 * archivable:  1 to create archivable (applies at creation only; 0 default, performant)
 * start_index: logical start index for the queue (0 for fresh, preserved on import/copy)
 * out:         on success, set to the new appender
 * Returns: 0 on success, or an MMAPQ_* error code. Recovery refuses a
 *          corrupted end-of-stream scan with MMAPQ_ECORRUPT and a
 *          transient mapping timeout with MMAPQ_ETIMEDOUT (retry).
 */
int mmapq_indexed_batch_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out);

/**
 * Create a batch appender with a specific appender ID.
 *
 * Acquires the given ID from the queue's appender ID pool; fails with
 * MMAPQ_EIDCONFLICT if the ID is already in use.
 *
 * All parameters same as mmapq_indexed_batch_appender_create, plus:
 * appender_id: the desired appender ID (0-255)
 */
int mmapq_indexed_batch_appender_create_with_id(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    uint8_t     appender_id,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out);

/* ── Batch lifecycle ─────────────────────────────────────────────────────── */

/**
 * Start a new batch.
 *
 * The returned MmapqMutableBatch is in-progress. Messages must be appended,
 * the batch completed with mmapq_mutable_batch_complete(), and then committed
 * with mmapq_indexed_batch_appender_commit_batch().
 *
 * Only one batch may be in progress at a time per appender.
 *
 * Returns: pointer to the appender-owned MmapqMutableBatch (valid until commit or close),
 *         or NULL on mapping timeout
 */
MmapqMutableBatch *mmapq_indexed_batch_appender_start_batch(MmapqIndexedBatchAppender *a);

/**
 * Start a batch, preserving the failure code. Clear the output on failure.
 *
 * appender: owner of the reusable mutable batch
 * out:      required output pointer; batch is borrowed until complete/discard
 * Returns: 0 on success, or an existing negative MMAPQ_* code.
 */
int mmapq_indexed_batch_appender_start_batch_checked(
    MmapqIndexedBatchAppender *appender, MmapqMutableBatch **out);

/**
 * Finalise the batch by writing the message count to the batch header.
 *
 * After calling this, no further appends may be made to the batch.
 *
 * b: the in-progress batch
 * Returns: the payload offset of the completed batch (to be passed to
 *         mmapq_indexed_batch_appender_commit_batch()), or -1 on mapping timeout
 */
int64_t mmapq_mutable_batch_complete(MmapqMutableBatch *b);

/**
 * Commit a completed batch, making it visible to pollers and readers.
 *
 * The CAS on the header entry is atomic — once this returns, any poller
 * waiting on this index will see the batch.
 *
 * a:           the appender
 * batch_offset: the value returned by mmapq_mutable_batch_complete()
 * Returns: the zero-based index of the committed batch,
 *         or -1 on mapping timeout, or MMAPQ_EINVAL if batch_offset is
 *         above this appender's last written start (a batch it never
 *         started — CAND-32)
 */
int64_t mmapq_indexed_batch_appender_commit_batch(MmapqIndexedBatchAppender *a, int64_t batch_offset);

/**
 * Discard an in-progress batch without committing it.
 *
 * Resets the appender's payload write position to @p batch_offset,
 * effectively undoing all appends made since the corresponding
 * mmapq_indexed_batch_appender_start_batch() call. A zero message count is
 * written at the batch offset so recovery scanners skip past it.
 *
 * Safe to call when the batch was started but never completed, or after
 * mmapq_mutable_batch_complete() was called but before commit. Not needed when
 * commit succeeds — the batch is then permanent.
 *
 * a:           the appender
 * batch_offset: the value returned by mmapq_mutable_batch_complete(),
 *                     or the offset from start_batch if never completed
 */
void mmapq_indexed_batch_appender_discard_batch(MmapqIndexedBatchAppender *a, int64_t batch_offset);

/**
 * Convenience: start, append, complete, and commit a single-message batch
 * in one call.
 *
 * Equivalent to:
 *   MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
 *   mmapq_mutable_batch_append(b, data, length);
 *   int64_t off = mmapq_mutable_batch_complete(b);
 *   return mmapq_indexed_batch_appender_commit_batch(a, off);
 *
 * a:     the appender
 * data:  pointer to the message payload
 * length: number of bytes to write (must be <= region_size - 4)
 * Returns: the zero-based index of the committed batch,
 *         or -1 on mapping timeout, or MMAPQ_EINVAL if message exceeds region size,
 *         or MMAPQ_ECORRUPT if the batch offset is outside the encodable
 *         56-bit field
 */
int64_t mmapq_indexed_batch_appender_append_and_commit(MmapqIndexedBatchAppender *a, const void *data, int32_t length);

/* ── Message append ──────────────────────────────────────────────────────── */

/**
 * Append a message to the end of the in-progress batch.
 *
 * Writes [int32 length][payload bytes] into the payload region. If the
 * current region lacks space a padding marker is written and the accessor
 * is remapped to the next region transparently.
 *
 * b:     the in-progress batch
 * data:  pointer to the message payload
 * length: number of bytes to write (not including the length prefix)
 * length must be <= region_size - 4 (the 4-byte length prefix). Messages larger
 * than one region are rejected with MMAPQ_EINVAL.
 *
 * Returns: the zero-based index of the newly appended message within the batch,
 *         or -1 on mapping timeout, or MMAPQ_EINVAL if message exceeds region size
 */
int32_t mmapq_mutable_batch_append(MmapqMutableBatch *b, const void *data, int32_t length);

/**
 * Reserve space for a message to be written in-place later.
 *
 * Writes a length prefix and advances the payload cursor by @p length bytes,
 * but does not copy any payload. The reserved slot is included in the batch
 * message count. Use mmapq_mutable_batch_get() or mmapq_mutable_batch_iterator() to
 * populate the reserved bytes before calling mmapq_mutable_batch_complete().
 *
 * b:       the in-progress batch
 * length:  number of payload bytes to reserve
 * out_data: set to a pointer to the reserved payload region on return;
 *                 valid until the next append, region remap, or close
 * length must be <= region_size - 4. Larger values are rejected with MMAPQ_EINVAL.
 *
 * Returns: the zero-based index of the reserved message slot within the batch,
 *         or -1 on mapping timeout, or MMAPQ_EINVAL if length exceeds region size
 */
int32_t mmapq_mutable_batch_append_reserved(MmapqMutableBatch *b, int32_t length, void **out_data);

/**
 * Append all remaining messages from a MmapqBatchPayloadIterator into this batch.
 *
 * Useful for copying data between batches (replay, aggregation, re-batching).
 * The iterator must not be backed by the same in-progress batch to avoid
 * undefined behaviour.
 *
 * If any individual message exceeds the destination queue's region_size, the
 * function returns MMAPQ_EINVAL immediately. On failure the batch is partially
 * populated and must be discarded, not committed.
 *
 * b:  the in-progress batch
 * it: iterator positioned at the first message to copy
 * Returns: the number of messages appended on success,
 *         or a negative MMAPQ_* error code on failure
 */
int32_t mmapq_mutable_batch_append_remaining_from(MmapqMutableBatch *b, MmapqBatchPayloadIterator *it);

/* ── Random access within in-progress batch ─────────────────────────────── */

/**
 * Return a pointer to the payload of the message at the given index.
 *
 * The returned pointer is into the mapped payload region — it is a flyweight
 * view valid only until the next append, remap, or close. Do not retain it.
 *
 * WARNING: mmapq_mutable_batch_get() and mmapq_mutable_batch_iterator() share the
 * appender's internal iterator state and must not be used concurrently
 * with an active iteration.
 *
 * b:             the in-progress batch
 * message_index: zero-based index of the message (must be < message_count)
 * out_length:    set to the length of the message on return
 * Returns: pointer to the raw payload bytes; NULL if index is out of range
 */
const void *mmapq_mutable_batch_get(MmapqMutableBatch *b, int32_t message_index, int32_t *out_length);

/**
 * Return a MmapqBatchPayloadIterator positioned at the given message index.
 *
 * Shares the same internal state as mmapq_mutable_batch_get() — the two must not
 * be used simultaneously.
 *
 * b:                  the in-progress batch
 * start_message_index: zero-based index to begin from
 * Returns: pointer to the appender's internal iterator; valid until next append or close
 */
MmapqBatchPayloadIterator *mmapq_mutable_batch_iterator(MmapqMutableBatch *b, int32_t start_message_index);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * Return the number of messages currently in the in-progress batch.
 *
 * b: the in-progress batch
 */
int32_t mmapq_mutable_batch_message_count(const MmapqMutableBatch *b);

/**
 * Return the appender's per-lane ID (0–255).
 *
 * Each appender is assigned a unique ID from the queue's appender ID pool.
 * Payload files are per-appender, named by this ID.
 */
uint8_t mmapq_indexed_batch_appender_id(const MmapqIndexedBatchAppender *a);

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

/**
 * Close the appender and release all associated resources.
 *
 * After this call the pointer must not be used.
 */
void mmapq_indexed_batch_appender_close(MmapqIndexedBatchAppender *a);

#endif /* MMAPQ_INDEXED_BATCH_APPENDER_H */
