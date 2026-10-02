# mmapq Maintainer's Guide

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
1.  **Binary Compatibility**: Single-message queues are compatible with Algoteq's evolved Java implementation, not upstream `tools4j/mmap`. Java does not yet read mmapq batch queues.
2.  **Memory Density**: No wasted padding bytes between small messages.

### The "C Rule" for Payloads
Because of the 4-byte length prefix, a message body starting at an 8-byte aligned offset (like the initial offset 64) will land at offset 68, which is **not 8-byte aligned**.
*   **Never** cast the payload pointer directly to a struct with `int64_t` or `double` fields. This will trigger UBSan "misaligned address" errors.
*   **Always** use `memcpy` to copy data into a local struct, or use an offset-based flyweight/codec.
*   Modern compilers (GCC/Clang) optimize these `memcpy` calls into the same unaligned-load instructions that Java's `Unsafe` uses. There is **zero performance penalty** for this safety.

**Correct Implementation Pattern:**
```c
static MmapqNextMove my_handler(int64_t index, uint8_t id, const void *data, int32_t length, void *ctx) {
    MyStruct m; // Stack-aligned
    memcpy(&m, data, sizeof(MyStruct)); // Optimized to unaligned-load by compiler
    printf("Seq: %ld\n", m.sequence);
    return MMAPQ_NEXT_ADVANCE;
}
```

---

## 3. Tuning the High-Performance Baseline

To reproduce the **~280ns median round-trip** latencies, the following optimizations must be maintained:

### Static Inlining Protocol
The `region_accessor_wrap` fast-path (address range check) and `header_read` logic are implemented as `static inline` in their respective headers. This eliminates the stack-frame and branching overhead of 3+ function calls per message, saving ~40ns per round trip.

### Atomic Background Pre-Touch
The `rw_pretouch` function uses an atomic CAS (`0 -> 0`) on every new page boundary. This forces a hardware **Read-For-Ownership (RFO)** and marks the page as **Dirty** in the background. This ensures the background `MmapqRuntime` thread takes the "Dirty Bit" penalty so the hot IPC loop never stalls at 4KB boundaries.

### Hardware Prefetch Stride
The `MmapqIndexedPoller` uses `__builtin_prefetch` to warm the next logical header. Because of the Vertical Walk, the stride must be **512 bytes**, not 64. Prefetching 512 bytes ahead ensures the L1/L2 cache is ready before the index increments.

### Measurement Memory Warmup
Always `memset` any large results/timestamp arrays (e.g. 160MB for 20M iterations) before measurement. This ensures the benchmark process has fully paged-in its own memory, preventing recording jitter during the clock run.

### System Configuration Requirements
To achieve deterministic sub-300ns performance on Linux:
*   **Core Isolation**: Use the `5, 7, 9` pattern. Pin the client loop to core 5, the server loop to core 7, and the background `MmapqRuntime` mapper to core 9.
*   **External Process Rule**: When running client and server in separate processes, **never** pin multiple mappers to the same isolated core. If cores are scarce, it is better to let the mappers float on the OS housekeeping cores than to oversubscribe an isolated core, which triggers disruptive Inter-Processor Interrupt (IPI) noise.
*   **Async core requirement**: the async `MmapqRuntime` mapper needs a core it can occupy. On a 1-vCPU or oversubscribed host the caller must `sched_yield()` in any wait-for-mapper loop, or the waiter busy-spins the mapper thread out of its scheduler timeslice and the mapping appears to hang (see 13a in `region_async.c` and the region-ring deregistration path). This is why `async` is not a free default — see `doc/shape-decision.md` for the sync-vs-async mapping guidance.
*   **Huge Pages**: Transparent Huge Pages (THP) should be enabled. The library calls `huge_pages_hint` (MADV_HUGEPAGE) on data segments to reduce TLB pressure.
*   **VDSO Timing**: Use `CLOCK_MONOTONIC`. Bypassing the VDSO path can add ~50-100ns of overhead per clock read.

---

## 4. The Anti-Pattern Ledger (What *Not* to Optimize)

During development, several aggressive optimizations were attempted and subsequently rolled back because they increased complexity without improving tail stability in multi-process environments.

### Anti-Pattern A: `mlockall(MCL_FUTURE)`
*   **Theory**: Use `mlockall` to guarantee zero page faults.
*   **Reality**: In multi-process IPC on non-RT kernels, this forces the kernel to track every page as "pinned," making every `mmap` call and growth event significantly heavier due to kernel-level synchronization.
*   **Java Clue**: Java achieves 17µs stability **without** `mlockall` by being "quiet" and relying on user-space pre-touch.
*   **Current code status**: `mlockall` is absent from the codebase. ✓

### Anti-Pattern B: Synchronous Eager Unmapping
*   **Theory**: Call `munmap` and `close` the instant a rolled file is finished to keep the address space lean.
*   **Reality**: Triggers synchronous kernel Inter-Processor Interrupts (IPIs) to all cores. This "Stop-the-World" event (TLB Shootdown) creates 2ms+ spikes.
*   **Java Clue**: Java's GC handles unmapping lazily. During a benchmark, it never happens, so Java never triggers this kernel noise.
*   **Current code status**: The `munmap` call inside `map_req_process` (`region_async.c`) is **not** an instance of this anti-pattern. It is a deliberate fused "unmap old region, map new region" single processing step for a slot transitioning directly from one valid mapping to the next. The anti-pattern is standalone eager unmapping of a region with nothing immediately following it. The dedicated `UNMAP_REQUESTED` state handles that case (triggered only at `close()` time), keeping standalone unmaps off the hot path. Do not conflate the two.

### Anti-Pattern C: Aggressive `MAP_POPULATE` — Removing It
*   **Theory**: Remove `MAP_POPULATE` from `mmap` calls to let the kernel map lazily and avoid heavy locks during mapping.
*   **Reality**: Removing `MAP_POPULATE` was tried and **made latency worse**. Without it, the kernel only allocates virtual address space during `mmap` — when the hot reader thread enters a new region for the first time it takes ~1,000 page faults synchronously on the critical path (one per 4KB page in a 4MB region).
*   **Java Clue**: Java warms pages via a user-space atomic CAS pre-touch loop. The C port achieves the equivalent via `rw_pretouch` (writer side) and `MAP_POPULATE` (mapper thread, reader side). Both keep the hot path page-fault-free.
*   **Current code status**: `MAP_POPULATE` is **present and intentional** at all three `mmap` call sites (`file_mapper_single_rw.c`, `file_mapper_single_ro.c`, `fixed_size_region.c`). The background `MmapqRuntime` mapper thread absorbs all page-fault cost asynchronously, keeping the hot poll/append loop clean. Do not remove it.

### Anti-Pattern D: Early Parent Pinning (Affinity Inheritance)
*   **Theory**: Pin the main thread to its isolated core as early as possible in `main()` to ensure the process starts correctly.
*   **Reality**: On Linux, `pthread_create` inherits the parent's CPU affinity mask. If `main()` pins itself to Core 7 *before* spawning the `MmapqRuntime` mapper thread, the mapper is born hard-locked to `{7}`. If the mapper is then left "floating" (`-1`), it remains stuck on Core 7, directly preempting the hot IPC loop on every scheduler quantum (1–4ms on a non-RT kernel). The symptom is distinctive: p50–p99.99 identical to single-process results, only p99.999 exploding to 2–3ms.
*   **The Fix**: Always spawn child threads (MmapqRuntime, Echo Server, etc.) **before** pinning the main thread to a single isolated core. This ensures children inherit the full system affinity mask and can either float on housekeeping cores or be independently pinned without conflict. This ordering is already correct in `latency_bench.c` and has been applied to `echo_server.c`.

---

## 5. Metadata & Protocol Authority

The `meta` file is the sole source of the configuration contract.

### Binary Layout (64-byte Meta File)
| Offset | Type | Field | Description |
| :--- | :--- | :--- | :--- |
| 0 | int32 | schema_version | Currently version 1 |
| 4 | int32 | region_size | Power of two, typically 4MB or 64MB |
| 8 | int64 | start_index | Current logical start (atomic access) |
| 16 | int64 | max_file_size | Maximum size of a single rolled file |
| 24 | uint8 | archivable | Flag for archiving support |
| 25 | uint8 | queue_type | 0: single-message; 1: Java fixed-8 (refused by C); 2: batch |

### Engagement Rules
*   **Appender (Authoritative)**: The only component permitted to create or initialize the `meta` file. If the file exists, the appender **must** adopt the on-disk parameters and ignore its own constructor arguments.
*   **Poller/Reader (Follower)**: Pure consumers. They must open the `meta` file Read-Only and discover configuration lazily from disk.
*   **Parity**: All control files (`meta`, `appender_meta`, `id_pool`) are standardized to **64 bytes** (matching Java).
*   **Lazy Growth**: Files grow region-by-region (e.g. 4MB) only when a writer enters a new region. This matches Java's behavior and eliminates the massive filesystem journaling noise caused by eager 1GB truncation.
*   **Permissions**: Standardized to **`0664`** (files) and **`0775`** (directories) for seamless cross-process and cross-language access.

### Confirmed Shutdown Handshake
To ensure clean exits in separate processes or languages, the following handshake must be implemented in all peers:
1.  **Client**: Sends a message with `sequence = 0` (SHUTDOWN_SEQ).
2.  **Server**: Receives `0`, sets internal shutdown flag, and **echoes the message back** to the response queue.
3.  **Client**: Spin-waits for the echoed `0` response (with a 1-2s safety timeout).
4.  **Result**: Confirms the server has processed the command and started its own cleanup. This prevents unmapping files while a peer is still touching them.

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

## 7. Cross-Repo Gates and Toolchain Requirements

The gate scripts live in `mmapq/scripts/` and `mmapq-py/scripts/`. Whether a
gate is **local** (runs from a bare `mmapq` clone) or **cross-repo** (needs the
sibling `mmapq-rs` / `mmapq-py` trees) is derived from its declared inputs, not
hand-maintained:

```bash
bash mmapq/scripts/check_gates_fail_closed.sh --list-registry
```

**Deliberate decision:** `check_readme_examples.sh` compiles the Rust README
against the real crate, so `check_sync.sh` — the everyday pre-commit command for
a zero-dependency C library — requires **gcc, python3, and a Rust toolchain
(`cargo`)**. This was a deliberate choice over the alternative (skipping the
Rust arm, which would let the README's Rust examples rot unverified). If a
host lacks `cargo`, the gate reports a hard `FAIL` naming the cause; the
`MMAPQ_SKIP_RUST_GATE=1` and `MMAPQ_SKIP_PYTHON_GATE=1` opt-outs are counted
(`GATE_SKIP`) and must be zero in CI. The correct escape valve if this ever
bites is to split `check_sync.sh` into a fast local subset and a full
cross-repo suite — not to weaken or silently skip an arm.

The opt-out cannot silently reduce coverage: the meta-gate detects it
structurally. When the Rust arm is skipped, the `mmapq-rs/Cargo.toml` input
test stops testing anything and the meta-gate immediately reports
`FAIL: exited 0 when input 'mmapq-rs/Cargo.toml' was absent`. That is the
expected, correct behaviour — not a regression. The `GATE_SKIP == 0` CI
assertion is belt-and-braces on top of this, not the primary defence.

Copies of `mmapq/scripts/` under `mmapq-rs/c/scripts/` are submodule mirror
artefacts and must not be run from there; the cross-repo path assumptions do
not resolve. Run gates from the workspace root.
## 8. Borrowed Mapping Lifetime and Capacity

Each reader and poller owns its payload accessors. A yielded pointer remains
valid until its accessor advances, repositions, or closes; callers must not
retain it across those operations. Background premap requests exclude the
current ring slot, and a mapped slot is not replaced without a new request.
The Rust wrapper enforces this boundary through exclusive reader and lending
cursor borrows, not runtime pins or reference counts.

`RegionAccessor.fast_path_end` is the exclusive absolute fast-path boundary,
which may be a premap trigger rather than the end of the mapping.
`mapped_capacity` is the available mapped length from `active_base`. Never use
the premap trigger as `Buffer.capacity`: valid payloads can cross that trigger
without crossing a mapping boundary. The immutable 32-bit `region_size` and
32-bit `mapped_capacity` replace the size callback slot, retaining a 64-bit
`fast_path_end` and the 64-byte accessor layout on supported 64-bit targets.
No mapping state transitions or atomic orderings change for this correction.
