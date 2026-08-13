#ifndef MMAPQ_POLLER_H
#define MMAPQ_POLLER_H

#include <stdint.h>

/**
 * Shared poller types — used by both MmapqIndexedPoller (single-message) and
 * MmapqIndexedBatchPoller (batch).  Extracted into a separate header to break the
 * dependency between the two poller layers: neither includes the other, both
 * include this one.
 *
 * MmapqNextMove — returned by the handler to control cursor movement.
 * MmapqPollResult — returned by poll() to describe what happened.
 */

/**
 * MmapqNextMove — returned by the poll handler to control cursor movement.
 *
 * Shared by both MmapqIndexedPoller (single-message) and MmapqIndexedBatchPoller (batch).
 *
 * ADVANCE  — move cursor forward (to next message or next batch)
 * STOP     — hold cursor at current position
 * RETREAT  — move cursor backward (to previous message or previous batch)
 */
typedef enum {
    MMAPQ_NEXT_ADVANCE = 0,
    MMAPQ_NEXT_STOP    = 1,
    MMAPQ_NEXT_RETREAT = 2,
} MmapqNextMove;

/**
 * MmapqPollResult — result of a single poll() call on any poller type.
 *
 * Shared by both MmapqIndexedPoller and MmapqIndexedBatchPoller.
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
    MMAPQ_POLL_END_OF_STREAM = 0,
    MMAPQ_POLL_ADVANCED      = 1,
    MMAPQ_POLL_STOPPED       = 2,
    MMAPQ_POLL_RETREATED     = 3,
    MMAPQ_POLL_ARCHIVED      = 4,
    MMAPQ_POLL_ERROR         = 5,
    MMAPQ_POLL_CORRUPT       = 6,
} MmapqPollResult;

#endif /* MMAPQ_POLLER_H */
