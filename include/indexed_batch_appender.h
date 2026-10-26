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
 *   1. Start a new batch:   MmapqMutableBatch *b = NULL;
 *                           mmapq_indexed_batch_appender_start_batch_checked(a, &b);
 *   2. Append messages:     mmapq_mutable_batch_append(b, data, length);
 *   3. Publish the batch:   int64_t index  = mmapq_mutable_batch_complete(b);
 *
 * To stage a batch for a later commit (out-of-order commit, crash-safe
 * staging): complete_uncommitted() instead of complete(), then
 * commit_batch(offset) — any subset, any order, same session or after
 * rejoin (the uncommitted-batches iterator refills found offsets).
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

/* Opaque iterator over uncommitted batches — struct definition is private
 * (src/internal/). */
typedef struct MmapqUncommittedBatchIterator MmapqUncommittedBatchIterator;

/**
 * MmapqMutableBatch — an in-progress batch being constructed before commit.
 *
 * Owned by the appender. Valid only between
 * mmapq_indexed_batch_appender_start_batch_checked() and its consumption by
 * complete / complete_uncommitted / discard. Do not
 * retain a pointer to it across a commit or close.
 *
 * The internal iterator used by mmapq_mutable_batch_get_checked() and
 * mmapq_mutable_batch_iterator() is shared: a call to either repositions
 * the iterator returned by mmapq_mutable_batch_iterator(); do not
 * interleave them, even on one thread.
 *
 * Mirrors Java IMutableBatch.
 */
typedef struct MmapqMutableBatch       MmapqMutableBatch;

struct MmapqMutableBatch {
    MmapqIndexedBatchAppender *appender;
    int64_t               batch_offset;
    int32_t               message_count;
    /* CAND-49 §3.5: set by complete / complete_uncommitted / discard at the
     * count store (or the empty-batch reset). Using a consumed handle is
     * undefined (C); Rust/Python enforce this by ownership. */
    int                   consumed;
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
 * Start a new batch, preserving the failure code. Clear the output on
 * failure.
 *
 * The returned MmapqMutableBatch is in-progress. Messages must be appended,
 * then the batch published with mmapq_mutable_batch_complete(), staged with
 * mmapq_mutable_batch_complete_uncommitted(), or discarded with
 * mmapq_mutable_batch_discard().
 *
 * Producer invariant (CAND-51a): every path that calls start_batch_checked
 * must end in exactly one of complete, complete_uncommitted or discard.
 * Rust and Python enforce this by discarding on scope exit; C callers must
 * do it explicitly.
 *
 * Only one batch may be in progress at a time per appender: starting while
 * one is open fails with MMAPQ_EINVAL. Starting after a discard's
 * pre-store failure first recovers the orphaned open batch to staged (see
 * mmapq_indexed_batch_appender_join_completed_open_batch).
 *
 * appender: owner of the reusable mutable batch
 * out:      required output pointer; batch is borrowed until
 *           complete/complete_uncommitted/discard or close
 * Returns: 0 on success; MMAPQ_EINVAL for a NULL argument or a second
 *         start while a batch is open; MMAPQ_ECLOSED on a closed appender;
 *         MMAPQ_ETIMEDOUT on a mapping timeout; MMAPQ_ECORRUPT from the
 *         orphan recovery.
 */
int mmapq_indexed_batch_appender_start_batch_checked(
    MmapqIndexedBatchAppender *appender, MmapqMutableBatch **out);

/**
 * Finalise the batch and PUBLISH it (CAND-49: complete is now the one-call
 * publish — it stores the message count and commits the batch header).
 *
 * After calling this, no further appends may be made to the batch; the
 * handle is consumed. On a publish failure after the count store (mapping
 * timeout), the batch is staged — its offset becomes committable via
 * mmapq_indexed_batch_appender_commit_batch() (and via the uncommitted
 * iterator after a rejoin).
 *
 * b: the in-progress batch
 * Returns: the zero-based index of the committed batch,
 *         or MMAPQ_EINVAL for a NULL/consumed handle or an empty batch
 *         (message_count == 0 — the handle stays open; discard it instead),
 *         or MMAPQ_ECLOSED on a closed appender,
 *         or -1 on mapping timeout (before the count store the handle
 *         stays open; after it, see above),
 *         or MMAPQ_ECORRUPT if the batch offset is outside the encodable
 *         56-bit field
 */
int64_t mmapq_mutable_batch_complete(MmapqMutableBatch *b);

/**
 * Finalise the batch WITHOUT publishing it (stage): stores the message
 * count and returns the batch offset for a later
 * mmapq_indexed_batch_appender_commit_batch(). The handle is consumed.
 *
 * b: the in-progress batch
 * Returns: the payload offset of the staged batch,
 *         or MMAPQ_EINVAL for a NULL/consumed handle or an empty batch
 *         (the handle stays open),
 *         or MMAPQ_ECLOSED on a closed appender,
 *         or MMAPQ_ENOMEM if the outstanding-offset set cannot grow (nothing
 *         is stored; the handle stays open),
 *         or -1 on mapping timeout (the handle stays open)
 */
int64_t mmapq_mutable_batch_complete_uncommitted(MmapqMutableBatch *b);

/**
 * Discard an in-progress batch without committing it (CAND-49: replaces
 * mmapq_indexed_batch_appender_discard_batch; batch lanes are forward-only).
 *
 * message_count > 0: the count is stored — the batch becomes a staged
 * record that is never committed (the write position is already past it;
 * no reset). In-session, commit_batch of its offset returns EINVAL —
 * discard is final. After a restart, the uncommitted iterator reports it
 * (refill makes it committable; recovery code decides).
 * message_count == 0: the write position resets to the batch start (nothing
 * but the untouched count slot lies between); the next batch overwrites
 * the slot.
 *
 * b: the in-progress batch
 * Returns: 0 on success, MMAPQ_EINVAL for a NULL/consumed handle,
 *         MMAPQ_ECLOSED on a closed appender, or -1 on a pre-store mapping
 *         failure (the handle stays open-orphaned; the next start_batch
 *         recovers it to staged — see
 *         mmapq_indexed_batch_appender_join_completed_open_batch)
 */
int mmapq_mutable_batch_discard(MmapqMutableBatch *b);

/**
 * Commit a staged batch, making it visible to pollers and readers.
 *
 * The CAS on the header entry is atomic — once this returns, any poller
 * waiting on this index will see the batch.
 *
 * a:           the appender
 * batch_offset: the value returned by mmapq_mutable_batch_complete_uncommitted(),
 *              or an offset yielded by the uncommitted-batches iterator
 * Returns: the zero-based index of the committed batch,
 *         or -1 on mapping timeout,
 *         or MMAPQ_EINVAL if batch_offset is not outstanding (never staged
 *         by this incarnation, already committed, a discarded batch, above
 *         the last started batch) or has been overtaken by archiving (its
 *         position is below the larger of start_offset and the archive
 *         line — CAND-54 ruling 1: below the archive line is
 *         uncommittable),
 *         or MMAPQ_ECLOSED on a closed appender,
 *         or MMAPQ_ECORRUPT if the batch offset is outside the encodable
 *         56-bit field
 */
int64_t mmapq_indexed_batch_appender_commit_batch(MmapqIndexedBatchAppender *a, int64_t batch_offset);

/**
 * Report whether this appender's join completed a trailing open batch to
 * staged (at the rejoin, or by start_batch's orphan recovery).
 *
 * a:      the appender
 * offset: receives the completed batch's offset when 1 is returned
 * Returns: 1 once per join-completion event (read-and-clear), 0 otherwise.
 *         The batch was completed by the queue, not the app: it may lack
 *         messages the app intended; the last counted message may have
 *         partial content.
 */
int mmapq_indexed_batch_appender_join_completed_open_batch(
    MmapqIndexedBatchAppender *a, int64_t *offset);

/**
 * mmapq_indexed_batch_appender_uncommitted_batches_checked — iterate only
 * uncommitted batches (CAND-49 §3.6; checked factory).
 *
 * Appender-owned iterator, no destroy (freed by
 * mmapq_indexed_batch_appender_close); a factory call re-initialises it
 * (snapshot at init — new appends/commits after the call are invisible).
 * The batch walk takes no start/end parameters: it runs from
 * appender_meta.archive_offset (the archive line,
 * doc/design/archive-boundaries.md) to the frozen end of stream. The
 * appending context must not be active while iterating.
 *
 * a:   the appender
 * out: receives the iterator (NULL on failure)
 * Returns 0 on success; MMAPQ_EINVAL for NULL args; MMAPQ_ETIMEDOUT on a
 * transient scan timeout (retryable — a NEW factory call);
 * MMAPQ_ECORRUPT on genuine scan truncation or gap-boundary corruption, or
 * a zero archive line — a corrupt appender meta (pre-0.2.0 queues are
 * refused at open with MMAPQ_ESCHEMA).
 *
 * The iterator yields batches staged but never committed, from the archive
 * line onward; archiving may chop older uncommitted batches, even after an
 * undo; a zero archive line means a corrupt appender meta (pre-0.2.0
 * queues are refused at open with MMAPQ_ESCHEMA);
 * a Java archiver must not be run against 0.2.0 queues. Discarded batches
 * reappear in recovery after a restart — the walker reports them and
 * refill makes them committable; recovery code decides (owner ruling 8).
 * A found batch is committed with the existing
 * mmapq_indexed_batch_appender_commit_batch() — any subset, any order, same
 * session or after rejoin; not committing = skip; an offset archiving has
 * overtaken returns EINVAL. Each yielded offset becomes committable
 * (refill).
 */
int mmapq_indexed_batch_appender_uncommitted_batches_checked(
    MmapqIndexedBatchAppender *a,
    MmapqUncommittedBatchIterator **out);

/**
 * mmapq_uncommitted_batch_next — advance and read the current batch.
 *
 * it:            iterator from mmapq_indexed_batch_appender_uncommitted_batches_checked
 * batch_offset:  receives the batch offset (pass to commit_batch)
 * message_count: receives the batch's message count
 * msgs:          receives the appender-owned message iterator over the
 *                batch (re-initialised per next; invalidated by the next
 *                next); read via the existing public batch accessors
 *                (mmapq_batch_iterator_advance / _next / ffi accessors)
 *
 * Three-way: 1 batch / 0 exhausted (outputs cleared) / negative MMAPQ_E*
 * (ETIMEDOUT retryable with unchanged state / ECORRUPT / ENOMEM on refill
 * failure, state unchanged — the next call re-yields the same batch;
 * outputs cleared on any non-1 return). A negative is truthy in C: compare
 * explicitly, never `while (mmapq_uncommitted_batch_next(...))`.
 */
int mmapq_uncommitted_batch_next(
    MmapqUncommittedBatchIterator *it,
    int64_t     *batch_offset,
    int32_t     *message_count,
    MmapqBatchPayloadIterator **msgs);

/**
 * Convenience: start, append, complete, and commit a single-message batch
 * in one call.
 *
 * Equivalent to:
 *   MmapqMutableBatch *b = NULL;
 *   mmapq_indexed_batch_appender_start_batch_checked(a, &b);
 *   mmapq_mutable_batch_append(b, data, length);
 *   return mmapq_mutable_batch_complete(b);
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
 * message count. Use mmapq_mutable_batch_get_checked() or mmapq_mutable_batch_iterator() to
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
 * Read the payload of the message at the given index, preserving the
 * failure code.
 *
 * The returned pointer is into the mapped payload region — it is a flyweight
 * view valid only until the next append, remap, or close. Do not retain it.
 *
 * WARNING: the internal iterator is shared with
 * mmapq_mutable_batch_iterator(): a call to either repositions the
 * iterator it returned; do not interleave them, even on one thread.
 *
 * b:             the in-progress batch
 * message_index: zero-based index of the message
 * out_data:      receives the payload pointer (cleared on failure)
 * out_length:    receives the message length (cleared on failure)
 * Returns: 0 on success; MMAPQ_EINVAL for a NULL argument or an index
 *         outside [0, message_count); MMAPQ_ETIMEDOUT on a transient
 *         mapping timeout (retryable); MMAPQ_ECORRUPT on genuine framing
 *         corruption. On any nonzero return both outputs are cleared.
 */
int mmapq_mutable_batch_get_checked(MmapqMutableBatch *b, int32_t message_index,
                                    const void **out_data, int32_t *out_length);

/**
 * Return a MmapqBatchPayloadIterator positioned at the given message index.
 *
 * Shares the appender's internal iterator with
 * mmapq_mutable_batch_get_checked(): a call to either repositions the
 * iterator returned here; do not interleave them, even on one thread.
 *
 * b:                  the in-progress batch
 * start_message_index: zero-based index to begin from; a start outside
 *                     [0, message_count] is refused with MMAPQ_EINVAL
 *                     (the iterator's last_error; the first advance
 *                     returns false); a start equal to message_count is
 *                     an empty iteration (CAND-50 item 3b).
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
