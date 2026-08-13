# Shape decision — queue-scoped factory parity

**Date:** 2026-08-12
**Status:** Decided, implemented (Python phase 9e).

## Context

C and Rust both offer queue-scoped *and* standalone factories for all
participant types. Python offered only standalone for readers and
archivers, meaning a user who passed `read_runtime=` to the queue and
then built a reader via `MmapqIndexedReader.create(dir, name)` got a reader
that silently did not use that runtime — the same silent-divergence
class as the last three phases.

## Decision (option a)

- Add `create_reader()` and `create_archiver()` to both `MmapqIndexedQueue`
  and `MmapqIndexedBatchQueue`.
- Keep the standalone factories, documented as the "queue may not exist
  yet / independent lifetime" path.
- Queue-scoped forms inherit the queue's `read_runtime`, `ring_size`, and
  `timeout_ns`; the standalone form does not.
- Non-breaking — pure additive.

## macOS support (12b)

**Decision:** removed. 

The only platform-specific code was thread naming (3 sites:
`mmap_platform.h` x2, `runtime.c`). No mmap, atomics, or file-layout
differences existed — but cross-process lock-free atomics over `MAP_SHARED`
on Apple Silicon were never tested. Re-adding macOS support requires macOS
CI exercising the multiprocess test.

## MmapqRuntime gap closure

The `runtime=`-only path in Python (the originally documented path) was
already closed in Phase 8: separate `write_runtime` and `read_runtime`
keyword arguments work on all factory methods. The 11d `TypeError` on
explicit runtimes was also closed in Phase 8. This item closes the last
remaining gap: queued-scoped factories that propagate the queue's runtime
to the created participant.
