# 2. The single-message queue

One header commit per message: maximum throughput for large messages,
minimum overhead per message.

## Append, poll, read

Create a writable directory on a persistent filesystem first. `/dev/shm` is
volatile and `/tmp` may be cleared on reboot; reserve those for disposable
tests and benchmarks.

```c
#include "runtime.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"

/* Background mapper thread — handles file mapping asynchronously.
 * It is a process-wide singleton; call once at startup. */
MmapqRuntime *rt = NULL;
int rc = mmapq_runtime_region_mapper(&rt);
assert(rc == 0);

/* Open or create a queue (separate write and read runtimes — same here) */
MmapqIndexedQueue *q = NULL;
rc = mmapq_indexed_queue_create_default("./data", "orders", rt, rt, &q);
assert(rc == 0);

/* Append a message */
MmapqIndexedAppender *a = NULL;
rc = mmapq_indexed_queue_create_appender(q, &a);
assert(rc == 0);
int64_t idx = mmapq_indexed_appender_append(a, "hello", 5);

/* Poll messages */
MmapqIndexedPoller *p = NULL;
rc = mmapq_indexed_queue_create_poller(q, &p);
assert(rc == 0);

static MmapqNextMove my_handler(int64_t index, uint8_t appender_id,
                            const void *data, int32_t length, void *ctx) {
    printf("[%ld] %.*s\n", index, length, (const char *)data);
    return MMAPQ_NEXT_ADVANCE;
}

MmapqPollResult r = mmapq_indexed_poller_poll(p, my_handler, NULL);

/* Close participants before the queue */
mmapq_indexed_appender_close(a);
mmapq_indexed_poller_close(p);
mmapq_indexed_queue_close(q);
mmapq_runtime_destroy(rt);
```

See `examples/demo.c` for a complete producer/consumer.

## Notes

- A commit publishes an entry to readers but does not flush it to stable
  storage; OS writeback decides when file-backed writes reach disk. See
  [chapter 5](05-crash-and-restart.md) for exactly what survives what.
- Consumers own their progress: the library persists no consumer position.
  Save an index and `move_to_index()` on reopen.
- Poll results: `MMAPQ_POLL_ADVANCED`, `MMAPQ_POLL_END_OF_STREAM`,
  `MMAPQ_POLL_ERROR` (the code is then in
  `mmapq_indexed_poller_last_error()` — a mapping timeout there is
  `MMAPQ_ETIMEDOUT`, never end-of-stream), `MMAPQ_POLL_CORRUPT` (on-disk
  damage, `MMAPQ_ECORRUPT`).
- Random access without a cursor: `mmapq_indexed_queue_create_reader()` +
  `mmapq_indexed_reader_read()` / `read_last()`.
- Appender IDs come from the pool automatically and are released when the
  appender closes; a killed process's ID stays taken (see
  [chapter 5](05-crash-and-restart.md)).
- `region_size` is a creation-time power of two that must be a multiple of
  both 32 KB (one transposed header block) and the OS page size — so the
  smallest valid region is 32 KB. The default (4 MB) is right for almost
  everything.
- Errors: every fallible call returns 0/non-negative or a negative
  [`MMAPQ_*` code](../reference/error-codes.md).
