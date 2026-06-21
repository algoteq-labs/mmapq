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

## 3. Tuning the High-Performance Baseline

To reproduce the **~280ns median round-trip** latencies, the following optimizations and system-level settings must be maintained:

### Static Inlining Protocol
The `region_accessor_wrap` fast-path (checking if a position is within the currently mapped range) and `header_read` logic are implemented as `static inline` in their respective headers. This eliminates the stack-frame and branching overhead of multiple function calls per message, saving ~40ns per round trip.

### Atomic Background Pre-Touch
The `rw_pretouch` function uses an atomic CAS (`0 -> 0`) on every new page boundary. Unlike a simple read, a CAS forces a hardware **Read-For-Ownership (RFO)** and marks the page as **Dirty** in the kernel. This ensures the background `Runtime` thread takes the "Dirty Bit" penalty so the hot IPC loop never stalls at 4KB boundaries.

### Hardware Prefetch Stride
The `IndexedPoller` uses `__builtin_prefetch` to warm the next logical header. Because of the Vertical Walk, the stride must be **512 bytes**, not 64. Prefetching 512 bytes ahead ensures the L1/L2 cache is ready before the index increments.

### Passive Kernel Strategy (Tail Stability)
High-frequency IPC is sensitive to kernel synchronization noise (TLB Shootdowns).
*   **No aggressive pinning**: Avoid `mlockall(MCL_FUTURE)` as it forces the kernel to track all mappings as pinned, making cross-process `mmap` noisy.
*   **Eager `munmap`**: Roll-over files are unmapped immediately to keep the process page table lean.
*   **Measurement Warmup**: Always `memset` large results/timestamp arrays before measurement to avoid page-fault jitter during the clock run.

---

## 4. Metadata Authority

The `queue_meta` file is the sole source of the configuration contract.

*   **Parity**: All control files (`meta`, `appender_meta`, `id_pool`) are standardized to **64 bytes** to match Java's dense footprint.
*   **FixedSizeRegion**: Supports 64-byte files by rounding up the `mmap` length to the system page size while respecting the actual disk file size.
*   **FixedSize8Bytes**: This flag is supported in the metadata for binary parity but ignored in C logic (standard variable-length framing is the priority).
*   **Lazy Growth**: Files grow region-by-region only when a writer enters a new region, eliminating eager 1GB truncation noise.
