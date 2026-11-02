# 3. The batch queue

A batch groups multiple messages under a single atomic commit: fewer header
CAS operations, higher throughput for small messages. The batch is a
transaction boundary — consumers never observe a partial batch. This is the
queue type the [event-sourcing pattern](event-sourcing.md) uses for
command → event-batch architectures.

```c
#include "runtime.h"
#include "indexed_batch_queue.h"

MmapqRuntime *rt = NULL;
int rc = mmapq_runtime_region_mapper(&rt);
assert(rc == 0);

MmapqIndexedBatchQueue *q = NULL;
rc = mmapq_indexed_batch_queue_create_default("./data", "events", rt, rt, &q);
assert(rc == 0);

/* Batch three messages together (single header commit) */
MmapqIndexedBatchAppender *a = NULL;
rc = mmapq_indexed_batch_queue_create_appender(q, &a);
assert(rc == 0);

MmapqMutableBatch *b = NULL;
rc = mmapq_indexed_batch_appender_start_batch_checked(a, &b);
assert(rc == 0);
mmapq_mutable_batch_append(b, "msg0", 4);
mmapq_mutable_batch_append(b, "msg1", 4);
mmapq_mutable_batch_append(b, "msg2", 4);
int64_t batch_idx = mmapq_mutable_batch_complete(b);

/* Poll one batch at a time — handler receives the full batch context */
MmapqIndexedBatchPoller *p = NULL;
rc = mmapq_indexed_batch_queue_create_poller(q, &p);
assert(rc == 0);

static MmapqNextMove my_handler(int64_t batch_index, MmapqBatchReadingContext *ctx,
                            void *user_ctx) {
    /* Iterate through messages in the batch */
    MmapqBatchPayloadIterator *it = ctx->iterator;
    while (mmapq_batch_iterator_advance(it)) {
        int32_t len = mmapq_batch_iterator_current_length_ffi(it);
        printf("batch[%ld]: %.*s\n", batch_index, (int)len,
               (const char *)mmapq_batch_iterator_current_data_ffi(it));
    }
    return MMAPQ_NEXT_ADVANCE;
}

MmapqPollResult r = mmapq_indexed_batch_poller_poll(p, my_handler, NULL);

mmapq_indexed_batch_appender_close(a);
mmapq_indexed_batch_poller_close(p);
mmapq_indexed_batch_queue_close(q);
```

## Notes

- Batch lanes support the same deferred-commit machinery as single-message
  lanes (next chapter): staged batches are visible to the restart walk.
- `mmapq_mutable_batch_complete()` publishes the batch and returns its
  index; `mmapq_indexed_batch_appender_commit_batch()` is only for batches
  staged with `mmapq_mutable_batch_complete_uncommitted()` (see [chapter
  5](05-crash-and-restart.md), Crash and restart).
- In 0.2.0 the batch queue is a C/Rust/Python feature: the Java
  implementation does not yet read batch queues, and Java-written queues
  are refused at open regardless of type.
- The checked batch accessors return 0 or a negative `MMAPQ_*` code;
  compare explicitly.
