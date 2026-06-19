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

*   **Deferred & Out-of-Order Commits**: Ability to append messages to the payload stream "uncommitted" and commit them to the shared index later. Commits can be performed in an order different from the physical insertion order.
*   **False-Sharing-Proof Headers**: The header file layout is optimized for modern CPU cache architectures. It uses a non-linear striding algorithm where sequential indices are mapped to different **Sectors**. Sequential writes are physically separated by **512 bytes (8 cache lines)**, providing extreme hardware-level isolation and stability.
*   **Safe Lifecycle Management**: Explicit and robust deregistration of ring regions from the background mapping thread. When a participant (appender, poller, or reader) is closed, its associated mapping tasks are safely drained from the runtime, preventing resource leaks and races.
*   **High-Performance Runtimes**: Includes a flexible `Runtime` abstraction for executing background tasks (like region mapping) with customizable idle strategies (busy-spin, yield, backoff), mirroring the Java `tools4j` threading model.

---

## Performance

The `mmap-c` library is optimized to outperform traditional IPC mechanisms and even highly tuned JVM implementations. On isolated cores (e.g., physical cores 5, 7, and 9 on an Intel i7-12700H) with Profile Guided Optimization (PGO) and system-level hardening, `mmap-c` achieves the following round-trip latencies:

| Metric | Results (10M samples) | vs. Java (JMH) |
| :--- | :--- | :--- |
| **Median (P50)** | **353 ns** | **~17% faster** (vs 428ns) |
| **P90** | **440 ns** | **~15% faster** (vs 520ns) |
| **P99.9** | **5.2 µs** | **~40% faster** (vs 8.8µs) |
| **Max (P100)** | **184 µs** | **~2.7x more stable** (vs 497µs) |
| **Throughput** | **~2.1 Million ops/sec** | **Superior** |

### Best Practices for Low Latency
To achieve these results, the following system-level configurations are recommended:
1.  **Core Isolation**: Use `isolcpus` or `cset` to reserve cores for the client, server, and region mapper.
2.  **Memory Locking**: Use `mlockall` (enabled in the benchmark) to prevent page-outs and swap jitter.
3.  **Transparent Huge Pages (THP)**: The library uses `MADV_HUGEPAGE` to reduce TLB pressure, which is critical for stable tail latencies.
4.  **VDSO Clock**: Ensure `CLOCK_MONOTONIC` is used (default) to utilize the kernel's fast VDSO path for timing.

---

## Design

Each queue entry has an 8-byte header stored in a shared header file. To eliminate
CPU cache contention (false sharing) between concurrent appenders, headers are
mapped using a non-linear striding algorithm that transposes the memory walk
from horizontal to **vertical** across different cache lines.

Sequential indices are spread across 64 different **Sectors** (each spanning 8 cache lines), providing
hardware-level isolation. After 64 indices, the mapping rotates back to the first
sector and shifts to the next 8-byte word.

```
bits[63:56] = appenderId   (up to 256 concurrent appenders)
bits[55:0]  = payloadPosition
0           = NULL (not yet written)

Layout (sectorSize=64, header=8b):
  index 0:  byte offset 0      (Sector 0, Word 0)
  index 1:  byte offset 512    (Sector 1, Word 0) — The Vertical Stride
  ...
  index 63: byte offset 32256  (Sector 63, Word 0)
  index 64: byte offset 8      (Sector 0, Word 1) — The Horizontal Shift
  index 65: byte offset 520    (Sector 1, Word 1)
```

Note: While the original design intended for a 64-byte (1 cache line) stride, benchmarking
confirmed that a larger **512-byte (8 cache lines)** stride provides significantly better
performance and tail stability on modern CPU architectures. The library maintains this
optimized layout (matching the Algoteq Java implementation).

The header is committed via a single atomic CAS. Readers acquire-load the
header; the happens-before relationship with the appender's release-store
guarantees payload visibility. No locks, no kernel calls on the hot path.

Payload is written to a per-appender file before the header is committed.
An entry is only visible to consumers after its header CAS succeeds.

Region mapping is async: a background thread (`runtime_region_mapper`)
maps and unmaps file regions in advance. Writers request premap immediately
on entering a new region (ratio 0.0); readers request premap at the halfway
point (ratio 0.5). Sync regions are also supported for testing.

### File layout

```
<directory>/<queue_name>/
  meta                  queue config (region_size, max_file_size, schema version)
  header                shared header file — one 8-byte entry per queue index
  payload_N             per-appender payload file (N = appender ID)
  appender_meta_N       per-appender start_offset (for future archiving)
  appender_id_pool      CAS bitset — allocates appender IDs (0-255)
```

All files are plain OS files. The format remains binary-compatible with the
original Java tools4j/mmap implementation — a Java producer and C consumer
(or vice versa) can share the same queue directory.

### Visibility vs durability

**Visibility** is provided by the atomic header commit. Once the CAS
succeeds, any reader that acquire-loads the header will see the payload.

**Persistence** is best-effort mmap/page-cache. After a process crash,
page-cache usually preserves data. After an OS crash or power loss,
there is no guarantee without an explicit msync/fsync durability mode
(not currently implemented).

---

## Building

Requires: Linux, gcc, pthread.

```bash
# Build directly with an application
gcc -std=gnu11 -O2 -D_GNU_SOURCE -I include src/*.c examples/demo.c -o demo -pthread -lm
```

CMake is also supported (used by the test suite):

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

To build and run the latency benchmark:
```bash
make bench
./latency_bench <iterations> <warmup> <msg_size> <client_cpu> <server_cpu> <mapper_cpu>
```

---

## Running the tests

```bash
./scripts/test.sh            # normal build + tests
./scripts/test.sh --asan     # AddressSanitizer (leak + memory error detection)
./scripts/test.sh --tsan     # ThreadSanitizer (data race detection)
```

All three modes should report `15 passed, 0 failed`.

---

## Quick start

```c
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "runtime.h"

// Create the background mapping thread
Runtime *rt = runtime_region_mapper();

// Open or prepare a queue (creator settings)
IndexedQueue *q = indexed_queue_create(
    "/data/queues",              // directory
    "orders",                    // queue name
    4096 * 1024,                 // region size (4 MB)
    4,                           // ring size
    1,                           // regions to map ahead
    (int64_t)4096 * 1024 * 256, // max file size (1 GB)
    0,                           // start index
    5000000000ULL,               // mapping timeout (5 s)
    rt);

// Append a message (establishes queue_meta if new)
IndexedAppender *a = indexed_queue_create_appender(q);
const char *msg = "hello";
int64_t idx = indexed_appender_append(a, msg, 5);

// Poll messages (automatically discovers region_size from metadata)
IndexedPoller *p = indexed_queue_create_poller(q);

MessageHandlerDirective my_handler(int64_t index, uint8_t appender_id,
                                   const void *data, int32_t length,
                                   void *ctx) {
    printf("[%ld] %.*s\n", index, length, (const char *)data);
    return HANDLER_ADVANCE;
}

PollResult r = indexed_poller_poll(p, my_handler, NULL);

// Always close participants before the queue
indexed_appender_close(a);
indexed_poller_close(p);
indexed_queue_close(q);
```

See `examples/demo.c` for a complete producer/consumer running 1,000,000
messages end-to-end.

---

## API overview

### Queue lifecycle

| Function | Description |
|---|---|
| `indexed_queue_create()` | Open or create a queue |
| `indexed_queue_create_default()` | Create with default parameters |
| `indexed_queue_create_appender()` | Create an appender |
| `indexed_queue_create_poller()` | Create a sequential poller |
| `indexed_queue_create_reader()` | Create a random-access reader |
| `indexed_queue_close()` | Close the queue |

### Appender

| Function | Description |
|---|---|
| `indexed_appender_append()` | Write and commit a message |
| `indexed_appender_append_uncommitted()` | Write without committing |
| `indexed_appender_commit()` | Commit a previously written payload |
| `indexed_appender_close()` | Close and release the appender ID |

### Poller (sequential cursor)

| Function | Description |
|---|---|
| `indexed_poller_poll()` | Process one message via callback |
| `indexed_poller_move_to_end()` | Jump past the last entry |
| `indexed_poller_move_to_start()` | Jump to the first entry |
| `indexed_poller_move_to_index()` | Jump to a specific index |
| `indexed_poller_create()` | Create a poller (discovers config from meta) |
| `indexed_poller_close()` | Close the poller |

The callback returns `HANDLER_ADVANCE`, `HANDLER_STOP`, or `HANDLER_RETREAT`.

### Reader (random access)

| Function | Description |
|---|---|
| `indexed_reader_read()` | Read entry at a specific index |
| `indexed_reader_read_first()` | Read the first entry |
| `indexed_reader_read_last()` | Read the last committed entry |
| `indexed_reader_last_index()` | Find the highest committed index |
| `indexed_reader_create()` | Create a reader (discovers config from meta) |
| `indexed_reader_close()` | Close the reader |

### Runtime

| Function | Description |
|---|---|
| `runtime_region_mapper()` | Get the singleton background mapping thread |
| `runtime_create()` | Create a named runtime with a custom idle strategy |
| `runtime_register()` | Register an executable (any thread) |
| `runtime_unregister_and_wait()` | Unregister and block until confirmed (any thread) |
| `runtime_close()` | Stop the runtime thread |
| `runtime_destroy()` | Free all resources |

---

## Platform

Linux only. Requires naturally aligned lock-free atomics over MAP_SHARED
mmap — guaranteed on x86-64 and ARM64. Validated at startup via
`atomic_is_lock_free()`.

---

## License

MIT — see [LICENSE](LICENSE).

Copyright (c) Anton Anufriev, Algoteq Pty Ltd, and contributors.
