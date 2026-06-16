# mmap-c

Low-latency memory-mapped queue primitives in C.

Implements a shared-memory indexed queue using memory-mapped files,
cache-line-isolated publication headers, per-appender payload streams,
and deferred-commit semantics. Designed for inter-process messaging on
Linux where allocation-free hot paths and sub-microsecond latency matter.

---

## Design

Each queue entry has an 8-byte header stored in a shared header file:

```
bits[63:56] = appenderId   (up to 256 concurrent appenders)
bits[55:0]  = payloadPosition
0           = NULL (not yet written)
```

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

All files are plain OS files. The format is binary-compatible with the
Java tools4j/mmap implementation — a Java producer and C consumer
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
gcc -std=gnu11 -O2 -I include src/file_initialiser.c src/file_mapper_single_ro.c \
    src/file_mapper_single_rw.c src/file_mapper_rolled.c src/region_sync.c      \
    src/region_async.c src/region_ring.c src/region_ring_accessor.c             \
    src/fixed_size_region.c src/runtime.c src/queue_meta.c src/appender_meta.c  \
    src/appender_id_pool.c src/queue_region_accessor.c src/header_reader.c      \
    src/payload_walk.c src/payload_iterator.c src/uncommitted_payload_iterator.c \
    src/indexed_appender.c src/indexed_poller.c src/indexed_reader.c            \
    src/indexed_queue.c examples/demo.c -o demo -pthread -lm
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

// Open or create a queue
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

// Append a message
IndexedAppender *a = indexed_queue_create_appender(q);
const char *msg = "hello";
int64_t idx = indexed_appender_append(a, msg, 5);

// Poll messages
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
| `indexed_poller_close()` | Close the poller |

The callback returns `HANDLER_ADVANCE`, `HANDLER_STOP`, or `HANDLER_RETREAT`.

### Reader (random access)

| Function | Description |
|---|---|
| `indexed_reader_read()` | Read entry at a specific index |
| `indexed_reader_read_first()` | Read the first entry |
| `indexed_reader_read_last()` | Read the last committed entry |
| `indexed_reader_last_index()` | Find the highest committed index |
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
