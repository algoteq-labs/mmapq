# mmapq

**A persistent, indexed, replayable inter-process queue — no broker, no
daemon, no garbage collector. The queue is a file.**

mmapq passes messages between processes on the same Linux machine through
memory-mapped files, with sub-microsecond latency and allocation-free,
syscall-free hot paths. Unlike a transport, the queue is durable state:
every message has a stable index, any process can read at any index at any
time, and a consumer that restarts resumes exactly where it stopped. Unlike
a log framework, it costs a `memcpy` and one atomic CAS to publish.

What you get:

- **~280 ns median round-trip** between two processes on isolated cores;
  P99.999 under 16 µs (measured, tables below)
- **Indexed and replayable** — messages are addressable by index forever
  (or until you archive them); readers can seek, tail, or re-read from zero.
  Replay is the same mechanism as recovery and replication
- **Up to 256 concurrent writers**, lock-free, each message committed by a
  single atomic operation — no torn reads, no partial messages, ever
- **Batch queues with atomic multi-message commit** — append N messages,
  commit once; consumers never observe a partial batch. This is a
  transaction boundary, designed for command → event-batch architectures
- **Crash-safe by construction** — a killed producer loses only its
  uncommitted tail; recovery is a deterministic scan, not a repair
- **No infrastructure** — two processes and a directory. No media driver,
  no broker, no background daemon required (an optional in-process mapper
  thread hides mmap latency)
- **Wire-compatible with Algoteq's Java implementation** (an evolved
  derivative of [tools4j/mmap](https://github.com/tools4j/mmap); not
  compatible with upstream tools4j — the formats have diverged). The same
  queue files can be written from C and read from Java, and vice versa —
  which makes migrating a Java system to C or Rust possible one process at
  a time, over a live queue

Rust and Python bindings live in the companion repositories `mmapq-rs` and
`mmapq-py`; see `doc/feature-matrix.md` for exact coverage per language.

This library was extracted from production trading infrastructure, where
its Java ancestor carried FX flows for years. The design target is not
"fast messaging" in the abstract — it is the storage layer for
**event-sourced systems**: sequenced command queues, replicated event logs,
and deterministic replay, where the answer to "what happened in production"
is *re-run it*. `doc/event-sourcing-guide.md` describes the pattern this
library was built to serve.

---

## When to use it — and when not to

Be suspicious of any infrastructure library that doesn't tell you when to
avoid it. Honest edges:

**Use mmapq when** you need same-host inter-process messaging where the
history matters: event-sourced services, sequenced order/command flow,
market-data capture, sensor/telemetry logs that a second process analyses,
replay-based testing and debugging, or a migration bridge between a Java
system and C/Rust/Python processes sharing one queue.

**Use something else when:**

- **You need network transport.** mmapq is same-machine only, by design;
  the wire path belongs to a network messaging system.
- **You want pub/sub with many dynamic topics** and no interest in
  history. mmapq queues are named, persistent streams, not a topic
  fabric.
- **You want a full application framework** rather than a storage layer —
  see the references in `doc/event-sourcing-guide.md`.
- **You need guaranteed durability through power loss.** mmapq writes
  reach the page cache, not the platter; the OS persists them eventually.
  Queues here are transport-plus-recent-history; long-term data belongs in
  the export format (`QueueTransfer`) or a database.
- **You need Windows or macOS.** Linux only (x86-64, ARM64).

What you give up relative to the alternatives is scope. What you gain is a
system small enough to audit end-to-end: ~10k lines of dependency-free C11,
a frozen on-disk format specified in one header, and a test suite that runs
under the exact release flags that ship.

---

## Stability

The feature set and the on-disk format are **frozen**. This is a design
position, not a maturity disclaimer:

- The wire format never changes. Queue files written today remain readable
  by every future version. Format constants live in `include/header_codec.h`
  and `include/queue_meta.h`, which are the specification.
- The public API only grows for one reason: completing parity with the
  Algoteq Java reference implementation for capability that already exists
  on disk. No convenience layers, no configuration sprawl, no ecosystem.
  Higher-level functionality belongs in separate projects built on top.
- Every open validates the format (`schema_version`, queue type) and
  refuses mismatches with a distinct error rather than misreading data.

If your evaluation horizon is years, this is the property to weigh: the
library you audit today is the library you run in five years.

---

## Quick start

```c
#include "runtime.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"

/* Background mapper thread — handles file mapping asynchronously.
 * It is a process-wide singleton; call once at startup. */
Runtime *rt = runtime_region_mapper();

/* Open or create a queue (separate write and read runtimes — same here) */
IndexedQueue *q = indexed_queue_create_default("/tmp", "orders", rt, rt);

/* Append a message */
IndexedAppender *a = indexed_queue_create_appender(q);
int64_t idx = indexed_appender_append(a, "hello", 5);

/* Poll messages */
IndexedPoller *p = indexed_queue_create_poller(q);

static NextMove my_handler(int64_t index, uint8_t appender_id,
                            const void *data, int32_t length, void *ctx) {
    printf("[%ld] %.*s\n", index, length, (const char *)data);
    return NEXT_ADVANCE;
}

PollResult r = indexed_poller_poll(p, my_handler, NULL);

/* Close participants before the queue */
indexed_appender_close(a);
indexed_poller_close(p);
indexed_queue_close(q);
```

Save, compile with `make`, and run. See `examples/demo.c` for a complete
producer/consumer.

---

## Batch queue — quick start

A batch groups multiple messages under a single atomic commit, reducing
header CAS operations and improving throughput for high-frequency messages.

```c
#include "runtime.h"
#include "indexed_batch_queue.h"

Runtime *rt = runtime_region_mapper();
IndexedBatchQueue *q = indexed_batch_queue_create_default("/tmp", "events", rt, rt);

/* Batch three messages together (single header commit) */
IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
MutableBatch *b = indexed_batch_appender_start_batch(a);
mutable_batch_append(b, "msg0", 4);
mutable_batch_append(b, "msg1", 4);
mutable_batch_append(b, "msg2", 4);
int64_t batch_idx = indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

/* Poll one batch at a time — handler receives the full batch context */
IndexedBatchPoller *p = indexed_batch_queue_create_poller(q);

static NextMove my_handler(int64_t batch_index, BatchReadingContext *ctx,
                            void *user_ctx) {
    /* Iterate through messages in the batch */
    while (batch_iterator_advance(&ctx->batch_iterator)) {
        printf("batch[%ld]: %.*s\n",
               batch_index,
               (int)batch_iterator_current_length(&ctx->batch_iterator),
               (const char *)batch_iterator_current_data(&ctx->batch_iterator));
    }
    return NEXT_ADVANCE;
}

PollResult r = indexed_batch_poller_poll(p, my_handler, NULL);

indexed_batch_appender_close(a);
indexed_batch_poller_close(p);
indexed_batch_queue_close(q);
```

---

## Core concepts


See [doc/feature-matrix.md](doc/feature-matrix.md) for the full feature
matrix across the C, Rust, and Python implementations, including
compatibility scope.

```
  ─────────────────────────────────────────────► Header

       poll header (1)   ▲          ▲  commit header (2)
                         │          │
               POLLER    │          │    APPENDER
                         │          │
    resolve payload (2)  ▼          ▼  write payload (1)

  ─────────────────────────────────────────────► Payload
```

The **appender** writes the message payload to its private payload lane
first (step 1). Only after the payload is fully written does it commit
the entry by publishing an 8‑byte header in the shared header lane via
an atomic CAS operation (step 2). This ordering guarantees that the
payload is always visible before the header — a reader that sees a
non‑NULL header is guaranteed to see the complete payload.

The **poller** (sequential consumer) scans the header lane in index
order. When it finds a committed header (step 1), it decodes the
`(appenderId, payloadPosition)` from the 8‑byte value, navigates to
the corresponding payload lane at that position (step 2), and delivers
the message to the callback. The **reader** (random‑access consumer)
follows the same two‑step protocol for any index, without advancing a
cursor.

Multiple appenders coexist with minimal contention because each
appender writes to its own private payload file, and the vertical‑walk
layout in the shared header lane spreads concurrent CAS operations
across different cache lines (512 byte stride).

| Concept | What it is |
| :--- | :--- |
| **Appender** | A writer. Appends messages to a per-appender payload file and commits an 8-byte header atomically via CAS. Up to 256 concurrent appenders per queue. |
| **Poller** | A sequential consumer. Reads headers in index order, follows the payload pointer, and delivers messages to a callback. |
| **Reader** | A random-access consumer. Reads any entry by index without advancing a cursor. |
| **Runtime** | A background thread that maps/unmaps file regions ahead of the hot loop. Absorbs `ftruncate`, page-zeroing, and `MAP_POPULATE` costs so the hot thread never stalls. |
| **Single-message queue** | One header commit per message. Maximum throughput for large messages, minimum overhead per message. |
| **Batch queue** | Groups N messages under one header commit. Higher throughput for small messages (fewer CAS operations). Wire-format compatible with the Java batch queue. |
| **Deferred commit** | Append payload without committing the header. Commit later, possibly in a different order. Useful for UDP resequencing or pipelined writes. |
| **Mixed Runtime** | Use separate runtimes for writing (async) and reading (sync). The writer offloads `ftruncate` to a background thread; the reader runs without any background interference for minimum latency. |

---

## Performance

Measured against a mature, heavily-optimised Java implementation of the
identical wire format, on the same machine — the one comparison where
every variable (format, framing, hardware, cores) is controlled. On
isolated cores (physical cores 5, 7, 9 on an Intel i7-12700H) with
Transparent Huge Pages enabled:

### Single-message queue — roundtrip to echo server in external process, 20M iterations

| Percentile | C (this library) | Java (JMH, same machine) |
|:-----------|------------------:|-------------------------:|
| min (p0)   |        **182 ns** |                   265 ns |
| p50        |        **284 ns** |                   380 ns |
| p90        |        **333 ns** |                   429 ns |
| p99        |        **396 ns** |                   591 ns |
| p99.9      |       **1869 ns** |                 6,792 ns |
| p99.99     |       **7411 ns** |                16,533 ns |
| p99.999    |      **14984 ns** |                15,090 ns |
| throughput | **3.2 M ops/sec** |           2.41 M ops/sec |

### Batch queue — roundtrip to echo server in external process, 20M iterations

| Percentile |  C (this library) |
|:-----------|------------------:|
| min (p0)   |        **196 ns** |
| p50        |        **296 ns** |
| p90        |        **348 ns** |
| p95        |        **368 ns** |
| p99        |        **422 ns** |
| p99.9      |       **2030 ns** |
| p99.99     |       **7313 ns** |
| p99.999    |      **14895 ns** |
| throughput |   **3 M ops/sec** |

### System configuration for best results

1. **Core isolation** — kernel boot parameter `isolcpus=5,7,9` (or equivalent) to reserve cores for the client loop, server loop, and region mapper.
2. **Transparent Huge Pages** — the library calls `madvise(MADV_HUGEPAGE)` on mapped regions to reduce TLB pressure.
3. **VDSO clock** — use `CLOCK_MONOTONIC` (the library default) to bypass the syscall overhead on timing.
4. **Thread affinity** — always spawn background threads *before* pinning the calling thread; see Anti-Pattern D in `doc/maintainer_guide.md`.

---

## Production features

### Queue archiving

Long-running queues accumulate entries over weeks or months. Archiving
logically prunes old entries by advancing the queue's `start_index` without
physically deleting data.

**Forward archiving** (`index > current start_index`):
Scans headers backwards from `index - 1` to the current start, identifies
the last committed payload position per appender, advances past each message
(or exhausts the batch), and writes the new start offsets to per-appender
metadata. Finally, commits the new `start_index` atomically in the queue
meta — all `poll()` / `read()` calls immediately see the boundary.

**Backward archiving / undo** (`index < current start_index`):
Restores access to previously archived entries by scanning headers
forward, validating that all required header and payload files still exist
on disk, and rolling back the per-appender start offsets.

**Archivable file discovery** (`archivable_files()`):
Returns the set of header and payload files fully covered by the current
archive boundary — safe to delete, compress, or migrate to cold storage.

Typical usage in production:
1. Hourly: call `archiveUpToIndex()` to log the archive boundary.
2. Call `archivableFiles()` to list fully covered files.
3. Upload those files to S3 or Glacier.
4. Delete local copies once confirmed uploaded.

### Export / import (QueueTransfer)

Export flattens queue data into a portable stream format for backup,
migration, or chunked replay. Each stream begins with a 64-byte metadata
header (schema version, region size, max file size, start index, entry
count, archivable flag) followed by framed entries.

**Chunked export** splits a large export into multiple files of
approximately `N` MB each. Each chunk is a complete, self-contained stream
with its own header and embedded index range in the file name
(`base_path_start-end`). Chunks can be uploaded independently to object
storage and downloaded on demand during replay.

**Import** reads a stream and reconstructs the queue. Supports all four
cross-over combinations:

| Import function | Stream format | Destination | Behaviour |
|:---|---|---|---|
| `queue_transfer_import` | flat (single-message) | normal queue | Direct append |
| `queue_transfer_import_batch_stream` | batched | normal queue | Flatten batches |
| `batch_queue_transfer_import` | batched | batch queue | Direct batch commit |
| `batch_queue_transfer_import_flat_stream` | flat | batch queue | 1-msg batches |

**Start index mode** controls how the stream's `start_index` metadata is
interpreted:
- **REBASE** (default): start index resets to 0.
- **PRESERVE**: start index matches the source range — useful for exact
  replica restoration and replay systems that track position by index.

**Filters**: Optional include/exclude callbacks allow selective export,
import, or copy by content or position.

Typical usage in production:
1. Every hour, export the queue with chunked export (`chunk_size_mb=64`).
2. Upload each chunk file to S3.
3. On restart: download the relevant chunks, import with PRESERVE mode,
   and replay to the point of interest.

---

## Design

### Header layout — the vertical walk

Each queue entry has an 8-byte header in a shared header file. To
eliminate false sharing between concurrent appenders, headers use a
non-linear striding algorithm that transposes the memory walk from
horizontal to **vertical** across cache lines.

```
Layout (sectorSize=64, header=8 bytes):

  index 0  →  byte offset 0      (sector 0, word 0)
  index 1  →  byte offset 512    (sector 1, word 0)  ← new cache line
  index 2  →  byte offset 1024   (sector 2, word 0)  ← new cache line
  ...
  index 63 →  byte offset 32256  (sector 63, word 0)
  index 64 →  byte offset 8      (sector 0, word 1)  ← back to sector 0
```

Sequential indices land on a new cache line every step.
Only after 64 indices does the mapping return to a previous sector,
now at the next 8-byte word within it.

The header encodes appender ID and payload position in a single 64-bit
word committed via atomic CAS:

```
bits[63:56] = appenderId   (0–255, up to 256 concurrent appenders)
bits[55:0]  = payloadPosition
0           = HEADER_NULL (slot not yet written)
```

### Payload framing

Each message in the payload file is framed as `[int32 length][payload bytes]`.
Batch entries add a `[int32 message_count]` prefix before the first message.
The format is binary-compatible with the Java Algoteq batch queue implementations.

### Visibility and commit

A message is **visible** to consumers the instant the header CAS succeeds.
The atomic release-store on the appender side and the acquire-load on the
reader side establish the happens-before relationship that guarantees
payload visibility — no locks, no kernel calls on the hot path.

### Async region mapping

A background thread (`runtime_region_mapper`) maps and unmaps file regions
in advance. Writers trigger premap at the start of each region (ratio 0.0);
readers trigger premap at the halfway point (ratio 0.5). The writer
extends the file with `ftruncate` and pre-touches pages via atomic CAS to
absorb dirty-bit penalties before the hot loop reaches the new region.

---

## File layout

```
<directory>/<queue_name>/
  meta                  queue config (region_size, max_file_size, schema version,
                        start_index, archivable flag, queue_type)
  header_N              shared header file (N = file index 0, 1, 2...; the
                        highest-indexed file is the current active one)
  payload_A_N           per-appender payload file (A = appender ID 0–255,
                        N = file index 0, 1, 2...; same pattern)
  appender_meta_A       per-appender start_offset (for archiving)
  appender_id_pool      CAS bitset — allocates appender IDs
```

All files are plain OS files. The format is binary-compatible with
Algoteq's Java implementation — an Algoteq Java producer and C consumer
(or vice versa) can share the same queue directory. (Evolved derivative
of tools4j/mmap; NOT compatible with upstream tools4j.)

---

## Building

Requires: Linux, gcc, pthread. No external dependencies.

```bash
# Build the static library and all binaries
make

# Or build specific targets
make bench echo_server          # single-message benchmark tools
make batch_bench batch_echo_server  # batch benchmark tools
```

CMake is also supported:

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

---

## Running the tests

```bash
./scripts/test.sh            # normal build + tests (19 passed, 0 failed)
./scripts/test.sh --asan     # AddressSanitizer
./scripts/test.sh --tsan     # ThreadSanitizer (run with ASLR disabled: setarch $(uname -m) -R ...)
```

---

## Running the benchmarks

```bash
# Full benchmark suite: single-message + batch, all modes
./scripts/bench.sh
./scripts/batch_bench.sh

# Or via make (runs the scripts above):
make bench-suite
make batch-bench-suite
```

Or run individual configurations manually:

```bash
# Single-message, internal server
./latency_bench 20000000 100000 64 5 7 9

# Single-message, external server
./echo_server /tmp request response 7 9 &
./latency_bench 20000000 100000 64 5 7 9 --external

# Batch queue, internal server
./batch_latency_bench 20000000 100000 64 5 7 9

# Batch queue, external server
./batch_echo_server /tmp batch_req batch_res 7 9 &
./batch_latency_bench 20000000 100000 64 5 7 9 --external
```

Arguments: `<iterations> <warmup> <msg_size> <client_cpu> <server_cpu> <mapper_cpu>`

Override defaults via environment variables:
```bash
BENCH_ITER=5000000 BENCH_MAPPER=11 ./scripts/bench.sh
```

---

## API overview

### Start here — core functions

| Function | What it does |
| :--- | :--- |
| `runtime_region_mapper()` | Get the singleton background mapping thread (call once at startup) |
| `indexed_queue_create_default()` | Open or create a single-message queue |
| `indexed_queue_create_appender()` | Create a writer |
| `indexed_queue_create_poller()` | Create a sequential reader |
| `indexed_appender_append()` | Write and commit a message |
| `indexed_poller_poll()` | Process one message via callback |
| `indexed_batch_queue_create_default()` | Open or create a batch queue |
| `indexed_batch_appender_start_batch()` | Begin a new batch |
| `mutable_batch_append()` | Add a message to an in-progress batch |
| `indexed_batch_appender_commit_batch()` | Commit the batch atomically |

`indexed_queue_create` and `indexed_batch_queue_create` accept an `archivable` parameter
(0 = non-archivable, performant; 1 = archivable, atomic start_index reads).
Applies at creation only; existing queues keep their on-disk value.

### Single-message queue

| Function | Description |
|---|---|
| `indexed_queue_create()` | Open or create with explicit parameters |
| `indexed_queue_create_reader()` | Create a random-access reader |
| `indexed_queue_create_archiver()` | Create an archiver |
| `indexed_queue_close()` | Close the queue |
| `indexed_appender_append_uncommitted()` | Write without committing |
| `indexed_appender_commit()` | Commit a previously written payload |
| `indexed_poller_move_to_end()` | Jump past the last entry |
| `indexed_poller_move_to_index()` | Jump to a specific index |
| `indexed_reader_read()` | Read entry at a specific index |
| `indexed_reader_read_last()` | Read the last committed entry |
| `indexed_archiver_archive_up_to_index()` | Archive entries up to an index |
| `indexed_archiver_archivable_files()` | List files safe to delete/archive |
| `indexed_archiver_print_summary()` | Print queue file layout summary |

The poll callback returns `NEXT_ADVANCE`, `NEXT_STOP`, or `NEXT_RETREAT`.

### Batch queue

| Function | Description |
|---|---|
| `indexed_batch_queue_create()` | Open or create a batch queue with explicit parameters |
| `indexed_batch_queue_create_poller()` | Create a batch sequential consumer |
| `indexed_batch_queue_create_reader()` | Create a batch random-access reader |
| `indexed_batch_queue_create_archiver()` | Create a batch archiver |
| `mutable_batch_complete()` | Finalise the batch |
| `mutable_batch_append_reserved()` | Reserve space in a batch and get raw write address |
| `mutable_batch_get()` | Read a message from an in-progress batch by index |
| `mutable_batch_iterator()` | Iterate over messages in an in-progress batch |
| `indexed_batch_poller_poll()` | Process one message via callback |
| `indexed_batch_reader_read()` | Read a batch at a specific index |
| `indexed_batch_reader_read_last()` | Read the last committed batch |
| `indexed_batch_archiver_archive_up_to_index()` | Archive batches up to an index |
| `indexed_batch_archiver_archivable_files()` | List files safe to delete/archive |

The batch poll callback returns `NEXT_ADVANCE`, `NEXT_STOP`, or
`NEXT_RETREAT` — the same `NextMove` enum used by the single-message
poller.

### Export / import (QueueTransfer)

| Function | Description |
|---|---|
| `queue_transfer_export()` | Export queue entries to a flat stream |
| `queue_transfer_export_chunked()` | Export split into ~N MB chunk files |
| `queue_transfer_import()` | Import flat stream into a normal queue |
| `queue_transfer_import_batch_stream()` | Import batch stream into a normal queue (flatten) |
| `queue_transfer_copy()` | Copy entries directly between queues |
| `batch_queue_transfer_export()` | Export batch queue entries to a batched stream |
| `batch_queue_transfer_export_chunked()` | Batch export split into ~N MB chunk files |
| `batch_queue_transfer_import()` | Import batched stream into a batch queue |
| `batch_queue_transfer_import_flat_stream()` | Import flat stream into a batch queue (1-msg batches) |
| `batch_queue_transfer_copy()` | Copy batches directly between queues |

All export/copy functions support optional include/exclude filter callbacks
and `REBASE` / `PRESERVE` start index mode.

### Runtime

| Function | Description |
|---|---|
| `runtime_create()` | Create a named runtime with a custom idle strategy |
| `runtime_close()` | Stop the runtime thread |
| `runtime_set_affinity()` | Pin the runtime thread to a CPU core |

---

## Documentation

*   `doc/feature-matrix.md` — feature coverage across C, Rust, and Python, including compatibility scope
*   `doc/event-sourcing-guide.md` — building sequenced command/event architectures on mmapq: batch commits as transactions, replay, recovery, replication
*   `doc/header_layout_design.md` — full derivation of the vertical walk, worked example, and verification trace
*   `doc/payload_alignment_design.md` — payload framing and alignment rationale
*   `doc/maintainer_guide.md` — anti-pattern ledger, performance tuning baseline, thread affinity rules, metadata protocol
*   `doc/queue-type-spec-for-implementation.md`, `doc/archivable-param-spec.md` — design records of the two post-extraction changes, retained as history

---

## Corruption recovery

On open, the appender recovery scan validates the committed stream. If it
finds an inconsistency (for example, a committed entry pointing at an empty
payload slot), it refuses to open and returns an error instead of risking
silent overwrites.

Recovery **detects** corruption; it does not repair it. The remediation is
always the same: recreate the queue, restoring any needed history from the
export format (`QueueTransfer`). `mmapq-stat --verify` (see tools, if
present in your build) reports the exact location of an inconsistency.

## Platform

Linux only. Requires naturally aligned lock-free atomics over `MAP_SHARED`
mmap — guaranteed on x86-64 and ARM64.

---

## License

MIT — see [LICENSE](LICENSE).

Copyright (c) Anton Anufriev, Algoteq Pty Ltd, and contributors.
