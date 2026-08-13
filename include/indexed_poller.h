#ifndef MMAPQ_INDEXED_POLLER_H
#define MMAPQ_INDEXED_POLLER_H

#include <stdint.h>
#include "poller.h"
#include "runtime.h"

/**
 * MmapqIndexedPoller — sequential consumer of the indexed queue.
 *
 * The poller maintains a cursor (currentIndex) and moves it forward,
 * backward, or holds based on the handler's directive.
 *
 * POLL OPERATION:
 *   1. If queue not yet created → MMAPQ_POLL_END_OF_STREAM.
 *   2. If currentIndex < startIndex → MMAPQ_POLL_ARCHIVED.
 *   3. Read header at currentIndex.
 *   4. If NULL_HEADER → MMAPQ_POLL_END_OF_STREAM.
 *   5. Decode appenderId + payloadPosition from header.
 *   6. Wrap payload accessor at payloadPosition, read length, wrap message.
 *   7. Call message_handler(index, appenderId, data, length, ctx).
 *   8. Apply the returned directive (ADVANCE / STOP / RETREAT).
 *   9. Return the poll result.
 *
 * MOVEMENT:
 *   ADVANCE  → currentIndex++  (return MMAPQ_POLL_ADVANCED)
 *   STOP     → currentIndex unchanged (return MMAPQ_POLL_STOPPED)
 *   RETREAT  → currentIndex-- if > startIndex (return MMAPQ_POLL_RETREATED or MMAPQ_POLL_STOPPED)
 *
 * CURSOR POSITIONING:
 *   move_to_index():      jump to a specific index
 *   move_to_end():        jump past the last committed entry
 *   move_to_last():       jump to the last committed entry
 *   move_to_start():      jump to the earliest unarchived entry
 *
 * LAZY INIT:
 *   Region accessors are created on the first poll() or move() call.
 *   If the queue does not exist yet, poll() returns MMAPQ_POLL_END_OF_STREAM
 *   and the init is retried on the next call.
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One MmapqIndexedPoller per thread.
 */

/* ── Message handler ─────────────────────────────────────────────────────── */

/**
 * MmapqMessageHandler — callback invoked for each message by poll().
 *
 * index:      queue index of this message
 * appender_id: which appender wrote it
 * data:       pointer into mapped memory — valid only during this call
 * length:     payload byte count
 * ctx:        caller-supplied context
 *
 * Returns a directive controlling cursor movement.
 */
typedef MmapqNextMove (*MmapqMessageHandler)(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx);

/* ── MmapqIndexedPoller ────────────────────────────────────────────────────────── */

typedef struct MmapqIndexedPoller MmapqIndexedPoller;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_poller_create — create a new sequential poller.
 *
 * directory:    base queue directory
 * queue_name:   queue subdirectory name
 * ring_size:    number of regions in the ring
 * timeout_ns:   mapping timeout in nanoseconds
 * runtime:      region mapper runtime
 *
 * Note: configuration parameters (region_size, max_file_size) are
 * automatically discovered from the queue's metadata file.
 *
 * The poller begins at startIndex (typically 0 or the queue's current
 * start_index). Lazy init — region accessors are created on first use.
 */
int mmapq_indexed_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedPoller **out);

/* ── Poll ────────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_poller_poll — read and process one message.
 *
 * handler: called if a message exists at currentIndex. The buffer passed
 *          to the handler is a flyweight — do not cache the pointer.
 * ctx:     passed to handler.
 *
 * Returns a MmapqPollResult describing what happened:
 *   MMAPQ_POLL_ADVANCED, MMAPQ_POLL_STOPPED, MMAPQ_POLL_RETREATED on success;
 *   MMAPQ_POLL_END_OF_STREAM if no message yet;
 *   MMAPQ_POLL_ARCHIVED if currentIndex is below startIndex;
 *   MMAPQ_POLL_ERROR on mapping timeout;
 *   MMAPQ_POLL_CORRUPT if committed payload data is invalid.
 */
MmapqPollResult mmapq_indexed_poller_poll(
    MmapqIndexedPoller *p,
    MmapqMessageHandler handler,
    void          *ctx);

/* ── Cursor positioning ──────────────────────────────────────────────────── */

/**
 * mmapq_indexed_poller_move_to_index — move cursor to a specific index.
 *
 * Returns 1 if successful:
 *   - index >= startIndex, AND
 *   - index <= lastIndex + 1 (one past end is valid — will wait for message)
 * Returns 0 if index is archived (< startIndex) or too far ahead.
 *
 * Aborts on mapping timeout (MMAP_ASSERT).
 */
int mmapq_indexed_poller_move_to_index(MmapqIndexedPoller *p, int64_t index);

/**
 * mmapq_indexed_poller_move_to_end — move cursor to one past the last committed entry.
 *
 * After this call, poll() will return MMAPQ_POLL_END_OF_STREAM until a new
 * message is appended.
 *
 * Always returns 1. Aborts on mapping timeout.
 */
int mmapq_indexed_poller_move_to_end(MmapqIndexedPoller *p);

/**
 * mmapq_indexed_poller_move_to_last — move cursor to the last committed entry.
 *
 * Returns 1 if a committed entry exists, 0 if the queue is empty.
 * Aborts on mapping timeout.
 */
int mmapq_indexed_poller_move_to_last(MmapqIndexedPoller *p);

/**
 * mmapq_indexed_poller_move_to_start — move cursor to the earliest valid entry.
 *
 * Always returns 1.
 */
int mmapq_indexed_poller_move_to_start(MmapqIndexedPoller *p);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_poller_current_index — current cursor position.
 */
int64_t mmapq_indexed_poller_current_index(const MmapqIndexedPoller *p);

/**
 * mmapq_indexed_poller_start_index — lowest readable index (0 if not archivable).
 */
int64_t mmapq_indexed_poller_start_index(MmapqIndexedPoller *p);

/**
 * mmapq_indexed_poller_exists — returns 1 if a committed entry exists at index.
 */
int mmapq_indexed_poller_exists(MmapqIndexedPoller *p, int64_t index);

/* ── Error accessor ────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_poller_last_error — MMAPQ_* code from the most recent MMAPQ_POLL_ERROR.
 *
 * Returns 0 if the last poll did not return MMAPQ_POLL_ERROR. The value is cleared
 * to 0 at the start of each poll() call, so it always describes the most
 * recent call. Meaningful only immediately after a poll that returned MMAPQ_POLL_ERROR.
 */
int mmapq_indexed_poller_last_error(const MmapqIndexedPoller *p);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapq_indexed_poller_close — close and free the poller.
 * Idempotent.
 */
void mmapq_indexed_poller_close(MmapqIndexedPoller *p);

#endif /* MMAPQ_INDEXED_POLLER_H */
