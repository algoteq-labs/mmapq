#ifndef INDEXED_POLLER_H
#define INDEXED_POLLER_H

#include <stdint.h>
#include "region_accessor.h"
#include "runtime.h"
#include "mmap_assert.h"

/**
 * IndexedPoller — sequential consumer of the indexed queue.
 *
 * The poller maintains a cursor (currentIndex) and moves it forward,
 * backward, or holds based on the handler's directive.
 *
 * POLL OPERATION:
 *   1. If queue not yet created → POLL_END_OF_STREAM.
 *   2. If currentIndex < startIndex → POLL_ARCHIVED.
 *   3. Read header at currentIndex.
 *   4. If NULL_HEADER → POLL_END_OF_STREAM.
 *   5. Decode appenderId + payloadPosition from header.
 *   6. Wrap payload accessor at payloadPosition, read length, wrap message.
 *   7. Call message_handler(index, appenderId, data, length, ctx).
 *   8. Apply the returned directive (ADVANCE / STOP / RETREAT).
 *   9. Return the poll result.
 *
 * MOVEMENT:
 *   ADVANCE  → currentIndex++  (return POLL_ADVANCED)
 *   STOP     → currentIndex unchanged (return POLL_STOPPED)
 *   RETREAT  → currentIndex-- if > startIndex (return POLL_RETREATED or POLL_STOPPED)
 *
 * CURSOR POSITIONING:
 *   move_to_index():      jump to a specific index
 *   move_to_end():        jump past the last committed entry
 *   move_to_last():       jump to the last committed entry
 *   move_to_start():      jump to the earliest unarchived entry
 *
 * LAZY INIT:
 *   Region accessors are created on the first poll() or move() call.
 *   If the queue does not exist yet, poll() returns POLL_END_OF_STREAM
 *   and the init is retried on the next call.
 *
 * THREAD CONFINEMENT:
 *   Not thread-safe. One IndexedPoller per thread.
 */

/* ── Poll result ─────────────────────────────────────────────────────────── */

typedef enum {
    POLL_END_OF_STREAM,   /* no message at current index; will retry next call */
    POLL_ADVANCED,        /* message processed; cursor moved forward            */
    POLL_STOPPED,         /* message processed; cursor held                     */
    POLL_RETREATED,       /* message processed; cursor moved backward           */
    POLL_ARCHIVED,        /* current index is below startIndex                  */
} PollResult;

/* ── Message handler ─────────────────────────────────────────────────────── */

/**
 * MessageHandlerDirective — returned by the message handler to control cursor.
 */
typedef enum {
    HANDLER_ADVANCE  = 0,  /* move cursor forward to next index */
    HANDLER_STOP     = 1,  /* hold cursor at current index      */
    HANDLER_RETREAT  = 2,  /* move cursor backward              */
} MessageHandlerDirective;

/**
 * MessageHandler — callback invoked for each message by poll().
 *
 * index:      queue index of this message
 * appender_id: which appender wrote it
 * data:       pointer into mapped memory — valid only during this call
 * length:     payload byte count
 * ctx:        caller-supplied context
 *
 * Returns a directive controlling cursor movement.
 */
typedef MessageHandlerDirective (*MessageHandler)(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx);

/* ── IndexedPoller ────────────────────────────────────────────────────────── */

typedef struct IndexedPoller IndexedPoller;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * indexed_poller_create — create a new sequential poller.
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
IndexedPoller *indexed_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime);

/* ── Poll ────────────────────────────────────────────────────────────────── */

/**
 * indexed_poller_poll — read and process one message.
 *
 * handler: called if a message exists at currentIndex. The buffer passed
 *          to the handler is a flyweight — do not cache the pointer.
 * ctx:     passed to handler.
 *
 * Returns a PollResult describing what happened.
 */
PollResult indexed_poller_poll(
    IndexedPoller *p,
    MessageHandler handler,
    void          *ctx);

/* ── Cursor positioning ──────────────────────────────────────────────────── */

/**
 * indexed_poller_move_to_index — move cursor to a specific index.
 *
 * Returns 1 if successful:
 *   - index >= startIndex, AND
 *   - index <= lastIndex + 1 (one past end is valid — will wait for message)
 * Returns 0 if index is archived (< startIndex) or too far ahead.
 *
 * Aborts on mapping timeout (MMAP_ASSERT).
 */
int indexed_poller_move_to_index(IndexedPoller *p, int64_t index);

/**
 * indexed_poller_move_to_end — move cursor to one past the last committed entry.
 *
 * After this call, poll() will return POLL_END_OF_STREAM until a new
 * message is appended.
 *
 * Always returns 1. Aborts on mapping timeout.
 */
int indexed_poller_move_to_end(IndexedPoller *p);

/**
 * indexed_poller_move_to_last — move cursor to the last committed entry.
 *
 * Returns 1 if a committed entry exists, 0 if the queue is empty.
 * Aborts on mapping timeout.
 */
int indexed_poller_move_to_last(IndexedPoller *p);

/**
 * indexed_poller_move_to_start — move cursor to the earliest valid entry.
 *
 * Always returns 1.
 */
int indexed_poller_move_to_start(IndexedPoller *p);

/* ── Metadata ─────────────────────────────────────────────────────────────── */

/**
 * indexed_poller_current_index — current cursor position.
 */
int64_t indexed_poller_current_index(const IndexedPoller *p);

/**
 * indexed_poller_start_index — lowest readable index (0 if not archivable).
 */
int64_t indexed_poller_start_index(IndexedPoller *p);

/**
 * indexed_poller_exists — returns 1 if a committed entry exists at index.
 */
int indexed_poller_exists(IndexedPoller *p, int64_t index);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * indexed_poller_close — close and free the poller.
 * Idempotent.
 */
void indexed_poller_close(IndexedPoller *p);

#endif /* INDEXED_POLLER_H */
