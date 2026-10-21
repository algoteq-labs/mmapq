#ifndef MMAPQ_INDEXED_APPENDER_H
#define MMAPQ_INDEXED_APPENDER_H

#include <stdint.h>
#include "runtime.h"

/* Opaque iterator types — struct definitions are private (src/internal/). */
typedef struct MmapqPayloadIterator MmapqPayloadIterator;
typedef struct MmapqUncommittedPayloadIterator MmapqUncommittedPayloadIterator;

/**
 * MmapqIndexedAppender — writes messages to the indexed queue.
 *
 * Each appender has:
 *   - A unique appenderId (acquired from AppenderIdPool).
 *   - Its own payload file: "payload_<appenderId>".
 *   - Access to the shared header file.
 *   - Its own AppenderMeta file tracking start_offset after archiving.
 *
 * APPEND OPERATION:
 *   1. Wrap payload accessor at current payload position.
 *   2. If capacity < length + 4: write padding and advance to next region.
 *   3. Write [int32 length][payload bytes] at current payload position.
 *   4. Compute header = encode(appenderId, payloadOffset).
 *   5. Wrap header accessor at header_position(currentIndex).
 *   6. CAS NULL_HEADER → header. If CAS fails, advance currentIndex and retry.
 *   7. Advance currentIndex and currentPayloadPosition.
 *   8. Return the index at which the message was committed.
 *
 * INITIALISATION (after restart):
 *   1. Use a temporary RO accessor to exponential+binary search for lastIndex.
 *   2. Scan backwards from lastIndex to find this appender's last committed header.
 *   3. Walk payload forward past any uncommitted tail (PayloadWalk).
 *   4. Eager-wrap RW regions to trigger file creation before first append.
 *
 * UNCOMMITTED APPEND (appendUncommitted / commit):
 *   appendUncommitted() writes payload but does NOT write the header.
 *   commit(payloadOffset) writes the header for a previously uncommitted payload.
 *   This enables out-of-order commit (e.g. UDP resequencing).
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One MmapqIndexedAppender per thread.
 *   Multiple appenders in the same process each have their own instance.
 */

#define MMAPQ_INDEXED_APPENDER_NOT_INITIALISED  ((int64_t)-1)

/* Public sentinel for "no offset" (same value as the internal
 * PAYLOAD_ITERATOR_NOT_SET). */
#define MMAPQ_PAYLOAD_ITERATOR_NOT_SET  ((int64_t)-1)

typedef struct MmapqIndexedAppender MmapqIndexedAppender;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_appender_create — create an appender, acquiring a free appender ID.
 *
 * directory:   base queue directory (e.g. "/data/queues")
 * queue_name:  queue subdirectory name
 * region_size: region size in bytes (must match existing queue)
 * ring_size:   number of regions in the ring
 * max_file_size: maximum rolled file size
 * timeout_ns:  mapping timeout in nanoseconds
 * archivable:  1 to create the queue archivable (applies at creation only;
 *              existing queues keep their on-disk value). 0 is performant mode.
 * runtime:     region mapper runtime
 * start_index: logical start index for the queue (0 for fresh, preserved on import/copy)
 *
 * Acquires an appender ID from the pool, initialises append position
 * (including recovery scan), and eager-wraps RW regions.
 *
 * Aborts if the appender ID pool is exhausted or initialisation fails.
 */
int mmapq_indexed_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    int64_t     start_index,
    MmapqIndexedAppender **out);

/**
 * mmapq_indexed_appender_create_with_id — create an appender with a specific ID.
 *
 * Fails (MMAP_ASSERT) if the requested appender_id is already in use.
 * Used when an appender must resume with its previous ID after restart.
 */
int mmapq_indexed_appender_create_with_id(
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
    MmapqIndexedAppender **out);

/* ── Append operations ───────────────────────────────────────────────────── */

/**
 * mmapq_indexed_appender_append — append a message and commit it atomically.
 *
 * data:   pointer to message payload bytes
 * length: payload byte count (must be <= region_size - 4)
 *
 * length must be <= region_size - 4 (the 4-byte length prefix). Messages larger
 * than one region are rejected with MMAPQ_EINVAL.
 *
 * Returns the queue index at which the message was committed,
 * or -1 on mapping timeout, or MMAPQ_EINVAL if message exceeds region size,
 * or MMAPQ_ECORRUPT if the payload position is outside the encodable
 * 56-bit field (exhausted position space).
 */
int64_t mmapq_indexed_appender_append(
    MmapqIndexedAppender *a,
    const void      *data,
    int32_t          length);

/**
 * mmapq_indexed_appender_append_uncommitted — write payload without committing.
 *
 * Writes the payload to the payload file but does NOT write the header.
 * The message is invisible to pollers and readers until commit() is called.
 *
 * data:   pointer to message payload bytes
 * length: payload byte count (must be <= region_size - 4)
 * length must be <= region_size - 4. Larger values are rejected with
 * MMAPQ_EINVAL.
 *
 * Returns the payload offset (position of the 4-byte length prefix),
 * or -1 on mapping timeout, or MMAPQ_EINVAL if message exceeds region size.
 * The offset must be passed to mmapq_indexed_appender_commit() later.
 *
 * Use case: UDP resequencing — append as packets arrive, commit when
 * the sequence is gapless.
 *
 * Crash caveat: after a process crash, the lane's last uncommitted
 * payload may have partial content; validate before committing.
 */
int64_t mmapq_indexed_appender_append_uncommitted(
    MmapqIndexedAppender *a,
    const void      *data,
    int32_t          length);

/**
 * mmapq_indexed_appender_commit — commit a previously uncommitted payload.
 *
 * payload_offset: the value returned by mmapq_indexed_appender_append_uncommitted().
 *
 * Writes the header, making the message visible to pollers and readers.
 * Returns the queue index assigned to this message,
 * or MMAPQ_EINVAL if a or payload_offset is NULL/invalid or not
 * outstanding: never written uncommitted by this incarnation, already
 * committed, mid-message, above the last written start, or overtaken by
 * archiving (a payload whose position is below the appender's current
 * start_offset can never become live — CAND-30; its file may already be
 * listed for reclamation). After a rejoin, offsets become committable by
 * running the uncommitted iterator (each yielded offset is refilled into
 * the outstanding set — see
 * mmapq_indexed_appender_uncommitted_iterator_checked).
 * or MMAPQ_ECLOSED if the appender is closed,
 * or -1 on mapping timeout.
 *
 * CONTRACT: Each offset may be committed at most once; a second commit of
 * the same offset returns MMAPQ_EINVAL.
 */
int64_t mmapq_indexed_appender_commit(
    MmapqIndexedAppender *a,
    int64_t          payload_offset);

/* ── Iterators ───────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_appender_iterator — create a payload iterator over this appender's stream.
 *
 * Iterates ALL payloads (committed and uncommitted) in the payload file.
 * start_offset: first offset to iterate from, or PAYLOAD_ITERATOR_NOT_SET.
 * end_offset:   last offset to iterate to (inclusive), or PAYLOAD_ITERATOR_NOT_SET.
 *
 * Returns the appender's internal MmapqPayloadIterator, or NULL if
 * start_offset or end_offset is below the appender's start of stream. The
 * iterator is owned by the appender and freed by mmapq_indexed_appender_close().
 * Do not destroy it manually (there is no public payload_iterator_destroy).
 *
 * INVARIANT: The appending context must not be active while iterating.
 *
 * INCOMPLETE SURFACE (CAND-49 §5.4): there are no public advance/current
 * accessors for this iterator; its purpose is an open owner question under
 * CAND-50. New code should not use it.
 */
MmapqPayloadIterator *mmapq_indexed_appender_iterator(
    MmapqIndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset);

/**
 * mmapq_indexed_appender_uncommitted_iterator_checked — iterate only
 * uncommitted payloads (CAND-49: checked, three-way factory).
 *
 * Skips messages that have already been committed (visible in the header).
 * Operates on a fixed snapshot of the stream as of the call time: a factory
 * call re-initialises the appender-owned iterator (new appends/commits
 * after the call are invisible).
 *
 * start_offset: first offset to iterate from, or MMAPQ_PAYLOAD_ITERATOR_NOT_SET.
 *               Iteration starts at appender_meta.archive_offset (the archive
 *               line, doc/design/archive-boundaries.md); a start_offset below
 *               the line clamps up to the line.
 * end_offset:   last offset to iterate to (inclusive), or MMAPQ_PAYLOAD_ITERATOR_NOT_SET.
 * out:          receives the appender-owned iterator (NULL on failure).
 *
 * Returns 0 on success; MMAPQ_EINVAL for NULL args, offsets below
 * start-of-stream, or start_offset > end_offset when both are set;
 * MMAPQ_ETIMEDOUT on a transient scan timeout (retryable — a NEW factory
 * call); MMAPQ_ECORRUPT on genuine scan truncation or gap-boundary
 * corruption, or a zero archive line (a pre-0.2.0 file — recreate the
 * queue).
 *
 * Ownership/lifetime/contract: the iterator is owned by the appender — no
 * destroy; freed by mmapq_indexed_appender_close(). The appending context
 * must not be active while iterating. The iterator yields messages appended
 * but never committed, from the archive line onward; archiving may chop
 * older uncommitted messages, even after an undo; queues created before
 * 0.2.0 must be recreated (zero-line refusal); a Java archiver must not be
 * run against 0.2.0 queues (it does not maintain archive_offset — a stale
 * non-zero line defeats the zero-line refusal and the archive-line
 * guarantee silently reopens). A concurrent archive reclaiming files the
 * walk has not reached surfaces as a negative MMAPQ_E* (never a crash or
 * stale bytes); retry with a fresh factory call. A found message is
 * committed with the existing mmapq_indexed_appender_commit() — any subset,
 * any order, same session or after rejoin; not committing = skip; an offset
 * archiving has overtaken returns EINVAL. Each yielded offset becomes
 * committable (refill). After a process crash, the lane's last
 * uncommitted payload may have partial content; validate before
 * committing.
 */
int mmapq_indexed_appender_uncommitted_iterator_checked(
    MmapqIndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset,
    MmapqUncommittedPayloadIterator **out);

/**
 * mmapq_uncommitted_iterator_next — advance and read the current message.
 *
 * it:     iterator from mmapq_indexed_appender_uncommitted_iterator_checked
 * data:   receives a pointer into mapped memory (borrowed — valid until the
 *         next next(), a factory re-call, or appender close)
 * length: receives the payload byte count
 * offset: receives the record start (length-prefix position) — the value to
 *         pass to mmapq_indexed_appender_commit()
 *
 * Three-way: returns 1 for a message, 0 when exhausted (outputs cleared),
 * or a negative MMAPQ_E* (MMAPQ_ETIMEDOUT retryable with unchanged state /
 * MMAPQ_ECORRUPT / MMAPQ_ENOMEM on refill failure, state unchanged — the
 * next call re-yields the same message; outputs cleared on any non-1
 * return). A negative is truthy in C: compare explicitly, never
 * `while (mmapq_uncommitted_iterator_next(...))`.
 * After a process crash, the lane's last uncommitted payload may have
 * partial content; validate before committing.
 */
int mmapq_uncommitted_iterator_next(
    MmapqUncommittedPayloadIterator *it,
    const void **data,
    int32_t     *length,
    int64_t     *offset);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_appender_id — return the appender's ID.
 */
uint8_t mmapq_indexed_appender_id(const MmapqIndexedAppender *a);

/**
 * mmapq_indexed_appender_current_index — return the next index that will be written.
 */
int64_t mmapq_indexed_appender_current_index(const MmapqIndexedAppender *a);

/**
 * mmapq_indexed_appender_start_offset — return the earliest valid payload position.
 */
int64_t mmapq_indexed_appender_start_offset(const MmapqIndexedAppender *a);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_appender_close — close and release the appender.
 *
 * Releases the appender ID back to the pool. After close(), the appender
 * must not be used. Safe to call more than once (idempotent).
 */
void mmapq_indexed_appender_close(MmapqIndexedAppender *a);

#endif /* MMAPQ_INDEXED_APPENDER_H */
