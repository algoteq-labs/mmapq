#ifndef POLLER_H
#define POLLER_H

#include <stdint.h>

/**
 * Shared poller types — used by both IndexedPoller (single-message) and
 * IndexedBatchPoller (batch).  Extracted into a separate header to break the
 * dependency between the two poller layers: neither includes the other, both
 * include this one.
 *
 * NextMove — returned by the handler to control cursor movement.
 * PollResult — returned by poll() to describe what happened.
 */

/**
 * NextMove — returned by the poll handler to control cursor movement.
 *
 * Shared by both IndexedPoller (single-message) and IndexedBatchPoller (batch).
 *
 * ADVANCE  — move cursor forward (to next message or next batch)
 * STOP     — hold cursor at current position
 * RETREAT  — move cursor backward (to previous message or previous batch)
 */
typedef enum {
    NEXT_ADVANCE = 0,
    NEXT_STOP    = 1,
    NEXT_RETREAT = 2,
} NextMove;

/**
 * PollResult — result of a single poll() call on any poller type.
 *
 * Shared by both IndexedPoller and IndexedBatchPoller.
 *
 * END_OF_STREAM — no data at current index; retry on next poll()
 * ADVANCED      — data delivered and cursor moved forward
 * STOPPED       — data delivered and cursor held (handler returned STOP)
 * RETREATED     — data delivered and cursor moved backward
 * ARCHIVED      — current index is below the queue's start index
 * ERROR         — mapping timeout or I/O failure
 * CORRUPT       — committed header with invalid payload (corruption detected)
 */
typedef enum {
    POLL_END_OF_STREAM = 0,
    POLL_ADVANCED      = 1,
    POLL_STOPPED       = 2,
    POLL_RETREATED     = 3,
    POLL_ARCHIVED      = 4,
    POLL_ERROR         = 5,
    POLL_CORRUPT       = 6,
} PollResult;

#endif /* POLLER_H */
