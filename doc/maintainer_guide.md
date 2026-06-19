# mmap-c Maintainer's Guide

This document preserves the critical architectural insights and hard-won optimizations developed during the C11 port of the Algoteq mmap-queue.

---

## 1. The "Vertical Walk" (Header Layout)

The header layout is the "secret sauce" for low-latency concurrency. It is designed to physically separate sequential writes to eliminate CPU cache-line bouncing (false sharing).

### Logical vs. Physical Transposition
A conventional memory walk is **horizontal**: fill all words in Cache Line 0, then move to Cache Line 1. This causes immediate false sharing.

This library implements a **Vertical Walk**:
*   **Sector Index** (`index % 64`): This is the **Fast Term**. It changes on every index increment and is multiplied by the sector size in the formula to create a **512-byte (8 cache line) stride**.
*   **Word Index** (`index / 64`): This is the **Slow Term**. It shifts the 8-byte position within the line only after all 64 sectors in the block have been touched.

**The Invariant:** Sequential indices (0, 1, 2...) are physically separated by 512 bytes. They land on the same cache line only after 64 other indices have been written, at which point they are shifted by 8 bytes.

*Verification Trace (`sectorSize=64`):*
*   `index 0` -> `byte offset 0` (Line 0, Word 0)
*   `index 1` -> `byte offset 512` (Line 8, Word 0)
*   `index 64` -> `byte offset 8` (Line 0, Word 1)

---

## 2. The Alignment Contract

The payload framing uses a dense `[int32 length][payload bytes...]` format.

### Rationale
We deliberately avoided 8-byte message alignment (which would require a `[length][reserved]` 8-byte header and trailing padding).
1.  **Binary Compatibility**: The current dense format is 100% compatible with the Java `tools4j/mmap` and `algoteq-ipc` implementations.
2.  **Memory Density**: No wasted padding bytes between small messages.

### The "C Rule" for Payloads
Because of the 4-byte length prefix, a message body starting at an 8-byte aligned offset (like the initial offset 64) will land at offset 68, which is **not 8-byte aligned**.
*   **Never** cast the payload pointer directly to a struct with `int64_t` or `double` fields. This will trigger UBSan "misaligned address" errors.
*   **Always** use `memcpy` to copy data into a local struct, or use an offset-based flyweight/codec.
*   Modern compilers (GCC/Clang) optimize these `memcpy` calls into the same unaligned-load instructions that Java's `Unsafe` uses. There is **zero performance penalty** for this safety.

---

## 3. Performance Hardening Stack

To reproduce the **~350ns median round-trip** latencies, the following system-level stack must be active:

*   **Core Isolation**: Threads must be pinned to isolated physical cores (e.g., the 5, 7, 9 pattern used in benchmarks).
*   **Memory Locking**: `mlockall(MCL_CURRENT | MCL_FUTURE)` must be called to prevent page-outs.
*   **Huge Pages**: Transparent Huge Pages (THP) must be enabled. The library calls `madvise(addr, length, MADV_HUGEPAGE)` to reduce TLB pressure.
*   **VDSO Timing**: Use `CLOCK_MONOTONIC`. The `RAW` variant bypasses the kernel's fast VDSO path and adds ~50-100ns of overhead per call.
*   **Busy-Spinning**: The client and server poll loops must be tight. Avoid `spin_hint()` (PAUSE/YIELD) in the absolute hottest measurement loops.

---

## 4. Metadata Authority

The `queue_meta` file is the indicator of queue existence and the sole source of the configuration contract.

*   **Appender**: The only component permitted to create/initialize the `meta` file. If the file exists, the appender **must** adopt the on-disk `region_size` and ignore its constructor arguments.
*   **Poller/Reader**: Pure consumers. They must open the `meta` file Read-Only and discover the `region_size` lazily.
*   **Zero Rubbish**: Redundant configuration parameters (`region_size`, `max_file_size`) have been removed from consumer APIs to prevent accidental misconfiguration.

---

## 5. Build & Verification

*   **ThreadSanitizer (TSAN)**: Known conflict with `mmap` on some kernels (`unexpected memory mapping`). Run with `setarch $(uname -m) -R ./scripts/test.sh --tsan` to verify logic races.
*   **Benchmarking**: Use `make bench` and the `latency_bench` tool for high-fidelity performance regression testing.
