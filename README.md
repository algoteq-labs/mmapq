# mmapq

**Many processes. One history. No coordinator.**

A persistent, indexed, replayable inter-process queue for Linux — no
broker, no daemon, no garbage collector. The queue is just files: many
processes append to one ordered history through memory-mapped files, with
sub-microsecond latency and allocation-free, syscall-free hot paths. The
queue is the sequencer.

## What happens when a writer dies halfway

Nothing special. A writer that is SIGKILLed between one instruction and the
next loses only its uncommitted tail; every committed message is intact and
readers never see a partial one. Recovery is a deterministic scan, not a
repair — and any process can re-read the whole history from any index.

This works because of one rule: **write first, claim last.** The payload is
fully written before the header that publishes it is committed with a
single atomic CAS. The claim is the commit.

## What you get

- **~280 ns median round-trip** between two processes on isolated cores
  (measured 2026-06-24, commit `d3cfb28`; refreshed at the release gate;
  [tables](doc/guide/08-performance-and-tuning.md))
- **Indexed and replayable** — every message is addressable by index;
  readers can seek, tail, or re-read from zero. Replay is the same
  mechanism as recovery and replication
- **Up to 256 concurrent writers**, lock-free, one atomic commit per
  message — no committed message is ever torn or partial
- **Batch queues with atomic multi-message commit** — append N messages,
  commit once; consumers never observe a partial batch. A transaction
  boundary, designed for command → event-batch architectures
- **Crash-safe by construction** — a killed producer loses only its
  uncommitted tail. Its lane stays occupied until an operator releases it
  with `mmapq-ids --unset` (dead processes only —
  [the procedure](doc/guide/05-crash-and-restart.md))
- **No infrastructure** — two processes and a directory (an optional
  in-process mapper thread hides mmap latency)

Built for **event-sourced systems**: sequenced command queues, replicated
event logs, deterministic replay — where the answer to "what happened in
production" is *re-run it*. The
[event-sourcing guide](doc/guide/event-sourcing.md) describes the pattern.

The file format derives from Algoteq's Java implementation (an evolved
derivative of [tools4j/mmap](https://github.com/tools4j/mmap); not
compatible with upstream tools4j). In 0.2.0, C, Rust and Python do not
share queues with Java: Java-written queues are refused at open; Java
support is planned at schema 2 under the same
[protocol contract](doc/reference/wire-format.md). 0.2.0 writes schema 2
and refuses 0.1.0 files at open — migrate by export/import
([release notes](doc/RELEASE-NOTES-0.2.0.md)).

## When to use it — and when not to

**Use mmapq when** you need same-host inter-process messaging where the
history matters: event-sourced services, sequenced order/command flow,
market-data capture, telemetry logs a second process analyses,
replay-based testing and debugging.

**Use something else when** you need network transport, a dynamic topic
fabric, a full application framework, durability through power loss, or
Windows/macOS. The honest list, with every limit stated, is
[chapter 9 of the guide](doc/guide/09-trade-offs-and-limits.md).

## Quick start: single-message

Use a persistent directory (`./data`, not `/tmp` or `/dev/shm`) for
anything you mean to keep; a commit publishes to readers but does not
flush to stable storage.

```c
#include "runtime.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"

/* Background mapper thread — a process-wide singleton; call once */
MmapqRuntime *rt = NULL;
assert(mmapq_runtime_region_mapper(&rt) == 0);

/* Open or create a queue */
MmapqIndexedQueue *q = NULL;
assert(mmapq_indexed_queue_create_default("./data", "orders", rt, rt, &q) == 0);

/* Append a message */
MmapqIndexedAppender *a = NULL;
assert(mmapq_indexed_queue_create_appender(q, &a) == 0);
int64_t idx = mmapq_indexed_appender_append(a, "hello", 5);

/* Poll messages */
MmapqIndexedPoller *p = NULL;
assert(mmapq_indexed_queue_create_poller(q, &p) == 0);

static MmapqNextMove my_handler(int64_t index, uint8_t appender_id,
                            const void *data, int32_t length, void *ctx) {
    printf("[%ld] %.*s\n", index, length, (const char *)data);
    return MMAPQ_NEXT_ADVANCE;
}

MmapqPollResult r = mmapq_indexed_poller_poll(p, my_handler, NULL);

mmapq_indexed_appender_close(a);
mmapq_indexed_poller_close(p);
mmapq_indexed_queue_close(q);
mmapq_runtime_destroy(rt);
```

`examples/demo.c` is a complete producer/consumer. Full walkthrough:
[chapter 2](doc/guide/02-single-message-queue.md).

## Quick start: batch

A batch groups messages under one atomic commit:

```c
#include "runtime.h"
#include "indexed_batch_queue.h"

MmapqRuntime *rt = NULL;
assert(mmapq_runtime_region_mapper(&rt) == 0);

MmapqIndexedBatchQueue *q = NULL;
assert(mmapq_indexed_batch_queue_create_default("./data", "events", rt, rt, &q) == 0);

MmapqIndexedBatchAppender *a = NULL;
assert(mmapq_indexed_batch_queue_create_appender(q, &a) == 0);

MmapqMutableBatch *b = NULL;
assert(mmapq_indexed_batch_appender_start_batch_checked(a, &b) == 0);
mmapq_mutable_batch_append(b, "msg0", 4);
mmapq_mutable_batch_append(b, "msg1", 4);
mmapq_mutable_batch_append(b, "msg2", 4);
int64_t batch_idx = mmapq_indexed_batch_appender_commit_batch(a, mmapq_mutable_batch_complete(b));

mmapq_indexed_batch_appender_close(a);
mmapq_indexed_batch_queue_close(q);
mmapq_runtime_destroy(rt);
```

Full walkthrough: [chapter 3](doc/guide/03-batch-queue.md).

## Bindings: Rust and Python

Rust (`mmapq-rs`): safe wrappers, same queue files.

```rust
use mmapq::{IndexedQueue, NextMove, QueueConfig, Runtime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("./data")?;
    let rt = Runtime::region_mapper()?;
    let q = IndexedQueue::create(
        QueueConfig::new("./data", "my_queue")
            .write_runtime(&rt)
            .read_runtime(&rt),
    )?;
    let mut appender = q.create_appender()?;
    let index = appender.append(b"hello world")?;
    let mut poller = q.create_poller()?;
    poller.move_to_index(index);
    poller.poll(|_index, _appender_id, data| {
        println!("Received: {:?}", std::str::from_utf8(data));
        NextMove::Advance
    })?;
    Ok(())
}
```

Python (`mmapq-py`): the same queues from a scripting environment —
debug, backtest, audit.

```python
from pathlib import Path
from mmapq import IndexedQueue, NextMove

Path("./data").mkdir(exist_ok=True)

def handler(index, appender_id, data):
    print(f"[{index}] {data!r}")
    return NextMove.ADVANCE

with IndexedQueue.create("./data", "my_queue") as queue:
    with queue.create_appender() as appender:
        index = appender.append(b"hello world")
    with queue.create_poller() as poller:
        poller.move_to_index(index)
        poller.poll(handler)
```

Each binding has its own README with idiomatic detail; exact per-language
coverage is the [feature matrix](doc/reference/feature-matrix.md).

## Go deeper

- [Guarantees and their evidence](doc/reference/guarantees.md) — every
  claim, with the test, model or argument behind it
- The [user guide](doc/guide/01-concepts.md): concepts · single-message ·
  batch · re-sequenced commits · crash and restart · archiving and
  reclamation · export/import · performance · trade-offs
- Reference: [wire format](doc/reference/wire-format.md) (normative) ·
  [error codes](doc/reference/error-codes.md) ·
  [feature matrix](doc/reference/feature-matrix.md) ·
  [API overview](doc/reference/api-overview.md) ·
  [glossary](doc/reference/glossary.md) ·
  [persistence contract](doc/reference/persistence-contract.md)
- Design: [archiving design](doc/design/archiving-design.md) ·
  [archive boundaries](doc/design/archive-boundaries.md) ·
  [header layout](doc/design/header_layout_design.md) ·
  [payload alignment](doc/design/payload_alignment_design.md) ·
  [protocol model](doc/model/README.md) (TLA+)
- [Release notes](doc/RELEASE-NOTES-0.2.0.md) — the single source for
  0.1.0 → 0.2.0 changes

## Building and tests

```bash
make                    # static library + examples; Linux, gcc, pthread, no dependencies
./scripts/test.sh       # normal + release-flags loops
```

CMake is also supported. Details, sanitizers and the gate scripts:
[building and testing](doc/contributing/building-and-testing.md).

## License

BSD 3-Clause — see [LICENSE](LICENSE).
