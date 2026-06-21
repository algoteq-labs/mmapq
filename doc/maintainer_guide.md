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

To reproduce the **~280ns median round-trip** latencies, the following optimizations must be maintained:

### Static Inlining Protocol
The `region_accessor_wrap` fast-path (address range check) and `header_read` logic are implemented as `static inline` in their respective headers. This eliminates the stack-frame and branching overhead of 3+ function calls per message, saving ~40ns per round trip.

### Atomic Background Pre-Touch
The `rw_pretouch` function uses an atomic CAS (`0 -> 0`) on every new page boundary. This forces a hardware **Read-For-Ownership (RFO)** and marks the page as **Dirty** in the background. This ensures the background `Runtime` thread takes the "Dirty Bit" penalty so the hot IPC loop never stalls at 4KB boundaries.

### Hardware Prefetch Stride
The `IndexedPoller` uses `__builtin_prefetch` to warm the next logical header. Because of the Vertical Walk, the stride must be **512 bytes**, not 64. Prefetching 512 bytes ahead ensures the L1/L2 cache is ready before the index increments.

### Measurement Memory Warmup
Always `memset` any large results/timestamp arrays (e.g. 160MB for 20M iterations) before measurement. This ensures the benchmark process has fully paged-in its own memory, preventing recording jitter during the clock run.

### System Configuration Requirements
To achieve deterministic sub-300ns performance on Linux:
*   **Core Isolation**: Use the `5, 7, 9` pattern. Pin the client loop to core 5, the server loop to core 7, and the background `Runtime` mapper to core 9.
*   **Huge Pages**: Transparent Huge Pages (THP) should be enabled. The library calls `huge_pages_hint` (MADV_HUGEPAGE) on data segments to reduce TLB pressure.
*   **VDSO Timing**: Use `CLOCK_MONOTONIC`. Bypassing the VDSO path can add ~50-100ns of overhead per clock read.

---

## 4. The Anti-Pattern Ledger (What *Not* to Optimize)

During development, several aggressive optimizations were attempted and subsequently rolled back because they increased complexity without improving tail stability in multi-process environments.

### Anti-Pattern A: `mlockall(MCL_FUTURE)`
*   **Theory**: Use `mlockall` to guarantee zero page faults.
*   **Reality**: In multi-process IPC on non-RT kernels, this forces the kernel to track every page as "pinned," making every `mmap` call and growth event significantly heavier due to kernel-level synchronization.
*   **Java Clue**: Java achieves 17µs stability **without** `mlockall` by being "quiet" and relying on user-space pre-touch.

### Anti-Pattern B: Synchronous Eager Unmapping
*   **Theory**: Call `munmap` and `close` the instant a rolled file is finished to keep the address space lean.
*   **Reality**: Triggers synchronous kernel Inter-Processor Interrupts (IPIs) to all cores. This "Stop-the-World" event (TLB Shootdown) creates 2ms+ spikes.
*   **Java Clue**: Java's GC handles unmapping lazily. During a benchmark, it never happens, so Java never triggers this kernel noise.

### Anti-Pattern C: Aggressive `MAP_POPULATE`
*   **Theory**: Force the kernel to synchronously warm up all memory during mapping.
*   **Reality**: Takes heavy kernel locks and generates high noise.
*   **Java Clue**: Java maps lazily and warms up pages via a simple user-space atomic CAS loop, which is much "quieter" for the kernel.

---

## 5. Metadata Authority

The `meta` file is the sole source of the configuration contract.

*   **Appender (Authoritative)**: The only component permitted to create or initialize the `meta` file. If the file exists, the appender **must** adopt the on-disk parameters and ignore its own constructor arguments.
*   **Poller/Reader (Follower)**: Pure consumers. They must open the `meta` file Read-Only and discover configuration lazily from disk.
*   **Parity**: All control files (`meta`, `appender_meta`, `id_pool`) are standardized to **64 bytes** (matching Java).
*   **FixedSize8Bytes**: This flag is supported in the metadata for binary parity but ignored in C logic (standard variable-length framing is the priority).
*   **Lazy Growth**: Files grow region-by-region (e.g. 4MB) only when a writer enters a new region. This matches Java's behavior and eliminates the massive filesystem journaling noise caused by eager 1GB truncation.
*   **Permissions**: Standardized to **`0664`** (files) and **`0775`** (directories) for seamless cross-process and cross-language access.

---

## 6. Build & Verification

### ThreadSanitizer (TSAN) Workaround
TSAN has a known conflict with `mmap` on some kernels (`unexpected memory mapping`). To verify logic races without mapping failures, run with ASLR disabled:
```bash
setarch $(uname -m) -R ./scripts/test.sh --tsan
```

### Regression Benchmarking
The sub-300ns baseline was verified using:
```bash
./latency_bench 20000000 100000 64 5 7 9
```
Always use at least 20M iterations to ensure all regions are mapped and the system has reached steady-state thermal/power levels.
