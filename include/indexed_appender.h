#ifndef INDEXED_APPENDER_H
#define INDEXED_APPENDER_H

#include <stdint.h>
#include "runtime.h"

/* Opaque iterator types — struct definitions are private (src/internal/). */
typedef struct PayloadIterator PayloadIterator;
typedef struct UncommittedPayloadIterator UncommittedPayloadIterator;

/**
 * IndexedAppender — writes messages to the indexed queue.
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
 *   Not thread-safe. One IndexedAppender per thread.
 *   Multiple appenders in the same process each have their own instance.
 */

#define INDEXED_APPENDER_NOT_INITIALISED  ((int64_t)-1)

typedef struct IndexedAppender IndexedAppender;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * indexed_appender_create — create an appender, acquiring a free appender ID.
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
int indexed_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    Runtime    *runtime,
    int64_t     start_index,
    IndexedAppender **out);

/**
 * indexed_appender_create_with_id — create an appender with a specific ID.
 *
 * Fails (MMAP_ASSERT) if the requested appender_id is already in use.
 * Used when an appender must resume with its previous ID after restart.
 */
int indexed_appender_create_with_id(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    Runtime    *runtime,
    uint8_t     appender_id,
    int64_t     start_index,
    IndexedAppender **out);

/* ── Append operations ───────────────────────────────────────────────────── */

/**
 * indexed_appender_append — append a message and commit it atomically.
 *
 * data:   pointer to message payload bytes
 * length: payload byte count (must be <= region_size - 4)
 *
 * length must be <= region_size - 4 (the 4-byte length prefix). Messages larger
 * than one region are rejected with MMAPQ_EINVAL.
 *
 * Returns the queue index at which the message was committed,
 * or -1 on mapping timeout, or MMAPQ_EINVAL if message exceeds region size.
 */
int64_t indexed_appender_append(
    IndexedAppender *a,
    const void      *data,
    int32_t          length);

/**
 * indexed_appender_append_uncommitted — write payload without committing.
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
 * The offset must be passed to indexed_appender_commit() later.
 *
 * Use case: UDP resequencing — append as packets arrive, commit when
 * the sequence is gapless.
 */
int64_t indexed_appender_append_uncommitted(
    IndexedAppender *a,
    const void      *data,
    int32_t          length);

/**
 * indexed_appender_commit — commit a previously uncommitted payload.
 *
 * payload_offset: the value returned by indexed_appender_append_uncommitted().
 *
 * Writes the header, making the message visible to pollers and readers.
 * Returns the queue index assigned to this message,
 * or MMAPQ_EINVAL if a or payload_offset is NULL/invalid,
 * or MMAPQ_ECLOSED if the appender is closed,
 * or -1 on mapping timeout.
 *
 * CONTRACT: Not idempotent. Committing the same offset twice will cause
 * the message to appear twice to consumers. Each offset must be committed
 * exactly once.
 */
int64_t indexed_appender_commit(
    IndexedAppender *a,
    int64_t          payload_offset);

/* ── Iterators ───────────────────────────────────────────────────────────── */

/**
 * indexed_appender_iterator — create a payload iterator over this appender's stream.
 *
 * Iterates ALL payloads (committed and uncommitted) in the payload file.
 * start_offset: first offset to iterate from, or PAYLOAD_ITERATOR_NOT_SET.
 * end_offset:   last offset to iterate to (inclusive), or PAYLOAD_ITERATOR_NOT_SET.
 *
 * The returned PayloadIterator is owned by the caller and must be destroyed
 * with payload_iterator_destroy() when done.
 *
 * INVARIANT: The appending context must not be active while iterating.
 */
PayloadIterator *indexed_appender_iterator(
    IndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset);

/**
 * indexed_appender_uncommitted_iterator — iterate only uncommitted payloads.
 *
 * Skips messages that have already been committed (visible in the header).
 * Operates on a fixed snapshot of the stream as of the call time.
 *
 * Returns the appender's internal UncommittedPayloadIterator, or NULL if
 * the header scan was truncated (mapping timeout or offset mismatch). The
 * returned pointer is valid until the next call to this function or until
 * indexed_appender_close(). Do not destroy it manually.
 */
UncommittedPayloadIterator *indexed_appender_uncommitted_iterator(
    IndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * indexed_appender_id — return the appender's ID.
 */
uint8_t indexed_appender_id(const IndexedAppender *a);

/**
 * indexed_appender_current_index — return the next index that will be written.
 */
int64_t indexed_appender_current_index(const IndexedAppender *a);

/**
 * indexed_appender_start_offset — return the earliest valid payload position.
 */
int64_t indexed_appender_start_offset(const IndexedAppender *a);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * indexed_appender_close — close and release the appender.
 *
 * Releases the appender ID back to the pool. After close(), the appender
 * must not be used. Safe to call more than once (idempotent).
 */
void indexed_appender_close(IndexedAppender *a);

#endif /* INDEXED_APPENDER_H */
