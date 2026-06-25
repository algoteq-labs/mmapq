# mmap-c

Low-latency memory-mapped queue primitives in C.

Implements a shared-memory indexed queue using memory-mapped files,
cache-line-isolated publication headers, per-appender payload streams,
and deferred-commit semantics. Designed for inter-process messaging on
Linux where allocation-free hot paths and sub-microsecond latency matter.

Originally derived from the [tools4j/mmap](https://github.com/tools4j/mmap) library,
this project has been significantly enhanced by Algoteq with advanced
features for high-performance trading and messaging systems. Both the
original Java implementation and this C port are open-sourced under the
MIT license.

---

## Algoteq Enhancements

This C port extends the original mmap queue design with several critical features for low-latency production environments:

*   **Deferred & Out-of-Order Commits**: Append messages to the payload stream uncommitted and commit them to the shared index later, in an order different from physical insertion order. Enables UDP resequencing and pipelined writes.

*   **False-Sharing-Proof Headers**: The header file uses a non-linear striding algorithm — a **vertical walk** — that maps sequential indices to different cache lines. Sequential writes are physically separated by **512 bytes (8 cache lines)**, providing hardware-level isolation between concurrent appenders.

*   **Batch Queue**: A first-class batch layer (`IndexedBatchAppender`, `IndexedBatchPoller`, `IndexedBatchReader`) that groups multiple messages under a single atomic commit. Wire-format compatible with the Algoteq Java batch queue implementation.

*   **Queue Archiving**: `IndexedArchiver` / `IndexedBatchArchiver` advances the queue's logical `start_index`, pruning per-appender payload offsets without deleting physical files. Supports both forward archiving (prune entries at the head) and backward undo (restore previously archived entries). Discover which files are fully covered by the archive boundary and safe to delete, compress, or migrate to cold storage.

*   **Linearised Export / Import**: `QueueTransfer` / `BatchQueueTransfer` serialises queue contents into a portable stream format for backup, migration, or chunked upload to object storage (e.g. S3). Supports optional include/exclude filters via callback, configurable chunk size for splitting large exports, and cross-format import (batch stream → normal queue, normal stream → batch queue).

*   **Mixed Runtime**: Independent `write_runtime` and `read_runtime` parameters allow asynchronous writer mapping (background thread absorbs `ftruncate` and page-zeroing overhead) while keeping the reader fully synchronous on its hot core — achieving the minimum possible steady-state latency without sacrificing tail stability.

*   **Safe Lifecycle Management**: Explicit deregistration of ring regions from the background mapping thread. When a participant is closed, its mapping tasks are safely drained, preventing resource leaks and races.

*   **High-Performance Runtimes**: A flexible `Runtime` abstraction for background tasks (region mapping, metrics aggregation) with customisable idle strategies (busy-spin, yield, backoff).

---

## Performance

On isolated cores (physical cores 5, 7, 9 on an Intel i7-12700H) with
Transparent Huge Pages enabled:

### Single-message queue — roundtrip to echo server in external process , 20M iterations

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

Sequential indices land on a new cache line every step.
Only after 64 indices does the mapping return to a previous sector,
now at the next 8-byte word within it.
```

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

## Production Features

### Queue Archiving

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

### Export / Import (QueueTransfer)

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

## File layout

```
<directory>/<queue_name>/
  meta                  queue config (region_size, max_file_size, schema version, start_index, archivable flag)
  header                shared header file — one 8-byte entry per queue index (single file, non-archivable)
  header_0              rolled header files — N = 0, 1, 2... (archivable queues)
  header_1
  ...
  payload_N             per-appender payload file (N = appender ID, 0–255) (single file, non-archivable)
  payload_N_0           rolled per-appender payload files (archivable queues)
  payload_N_1
  ...
  appender_meta_N       per-appender start_offset (for archiving)
  appender_id_pool      CAS bitset — allocates appender IDs
```

All files are plain OS files. The format is binary-compatible with the
Java implementation — a Java producer and C consumer (or vice versa) can
share the same queue directory.

---

## Building

Requires: Linux, gcc, pthread.

```bash
# Build the static library and all binaries
make

# Or build specific targets
make bench echo_server          # single-message benchmark tools
make batch_bench batch_echo_server  # batch benchmark tools
```

CMake is also supported (used by the test suite):

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
# Single-message queue — internal + external (async) + external (sync)
make bench-suite

# Batch queue — same three configurations
make batch-bench-suite
```

Or run individual configurations manually:

```bash
# Single-message, internal server
./latency_bench 20000000 100000 64 5 7 9

# Single-message, external server
./echo_server /tmp request response 7 9 &
./latency_bench 20000000 100000 64 5 7 9 --external

# Batch queue, external server
./batch_echo_server /tmp request response 7 9 &
./batch_latency_bench 20000000 100000 64 5 7 9 --external
```

Arguments: `<iterations> <warmup> <msg_size> <client_cpu> <server_cpu> <mapper_cpu>`

Override benchmark defaults via environment variables:
```bash
BENCH_ITER=5000000 BENCH_MAPPER=11 ./scripts/bench.sh
```

---

## Quick start

### Single-message queue

```c
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "runtime.h"

/* Background region-mapper thread (singleton) */
Runtime *rt = runtime_region_mapper();

/* Open or create a queue */
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

### Batch queue

```c
#include "indexed_batch_queue.h"

Runtime *rt = runtime_region_mapper();
IndexedBatchQueue *q = indexed_batch_queue_create_default("/tmp", "events", rt, rt);

/* Append a batch of three messages */
IndexedBatchAppender *a = indexed_batch_queue_create_appender(q);
MutableBatch *b = indexed_batch_appender_start_batch(a);
mutable_batch_append(b, "msg0", 4);
mutable_batch_append(b, "msg1", 4);
mutable_batch_append(b, "msg2", 4);
int64_t batch_idx = indexed_batch_appender_commit_batch(a, mutable_batch_complete(b));

/* Poll one message at a time */
IndexedBatchPoller *p = indexed_batch_queue_create_poller(q);

static BatchNextMove my_batch_handler(int64_t batch_index, int32_t msg_index,
                                       int32_t msg_count, const void *data,
                                       int32_t length, void *ctx) {
    printf("batch[%ld] msg[%d/%d]: %.*s\n",
           batch_index, msg_index, msg_count, length, (const char *)data);
    return BATCH_ADVANCE;
}

BatchPollResult r = indexed_batch_poller_poll(p, my_batch_handler, NULL);

indexed_batch_appender_close(a);
indexed_batch_poller_close(p);
indexed_batch_queue_close(q);
```

See `examples/demo.c` for a complete single-message producer/consumer.

---

## API overview

### Single-message queue

| Function | Description |
|---|---|
| `indexed_queue_create_default()` | Open or create a queue with default parameters |
| `indexed_queue_create()` | Open or create a queue with explicit parameters |
| `indexed_queue_create_appender()` | Create a producer |
| `indexed_queue_create_poller()` | Create a sequential consumer |
| `indexed_queue_create_reader()` | Create a random-access reader |
| `indexed_queue_create_archiver()` | Create an archiver |
| `indexed_queue_close()` | Close the queue |
| `indexed_appender_append()` | Write and commit a message |
| `indexed_appender_append_uncommitted()` | Write without committing |
| `indexed_appender_commit()` | Commit a previously written payload |
| `indexed_poller_poll()` | Process one message via callback |
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
| `indexed_batch_queue_create_default()` | Open or create a batch queue |
| `indexed_batch_queue_create_appender()` | Create a batch producer |
| `indexed_batch_queue_create_poller()` | Create a batch sequential consumer |
| `indexed_batch_queue_create_reader()` | Create a batch random-access reader |
| `indexed_batch_queue_create_archiver()` | Create a batch archiver |
| `indexed_batch_appender_start_batch()` | Begin a new batch |
| `mutable_batch_append()` | Add a message to an in-progress batch |
| `mutable_batch_complete()` | Finalise the batch |
| `indexed_batch_appender_commit_batch()` | Commit the batch atomically |
| `indexed_batch_poller_poll()` | Process one message via callback |
| `indexed_batch_reader_read()` | Read a batch at a specific index |
| `indexed_batch_reader_read_last()` | Read the last committed batch |
| `indexed_batch_archiver_archive_up_to_index()` | Archive batches up to an index |
| `indexed_batch_archiver_archivable_files()` | List files safe to delete/archive |

The batch poll callback returns `BATCH_ADVANCE`, `BATCH_ADVANCE_BATCH`,
`BATCH_STOP`, or `BATCH_RETREAT_BATCH`.

### Export / Import (QueueTransfer)

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
| `runtime_region_mapper()` | Get the singleton background mapping thread |
| `runtime_set_affinity()` | Pin the runtime thread to a CPU core |
| `runtime_create()` | Create a named runtime with a custom idle strategy |
| `runtime_close()` | Stop the runtime thread |

---

## Documentation

*   `doc/header_layout_design.md` — full derivation of the vertical walk, worked example, and verification trace
*   `doc/maintainer_guide.md` — anti-pattern ledger, performance tuning baseline, thread affinity rules, metadata protocol

---

## Platform

Linux only. Requires naturally aligned lock-free atomics over `MAP_SHARED`
mmap — guaranteed on x86-64 and ARM64.

---

## License

MIT — see [LICENSE](LICENSE).

Copyright (c) Anton Anufriev, Algoteq Pty Ltd, and contributors.
