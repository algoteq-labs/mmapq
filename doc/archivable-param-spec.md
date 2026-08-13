# Implementation Spec: expose `archivable` as a creation parameter

> *Implementation specification for the archivable parameter change, July 2026.
> Retained as a design record — the code and tests are authoritative
> where they have since evolved.*

**Audience:** implementing model/engineer with access to `mmapq`,
`mmapq-rs`, and `mmapq-py`, WITHOUT access to prior design discussion.
Everything needed is in this document; all design decisions are made. Where
this spec and your judgment disagree, the spec wins — flag the disagreement
in your summary instead of deviating. The repositories' `AGENTS.md` files
apply in full, including the completion-report format.

---

## 1. Background and goal

The on-disk meta format (byte 24) and `queue_meta_open_rw` already support
`archivable`; the archiver modules are implemented and tested. But no public
factory exposes the parameter — both appender factories hardcode
`/* archivable = */ 0`, so an archivable queue can only be produced by
pre-creating the meta file directly (as the archiver tests do).

**Goal:** `archivable` becomes a creation-time parameter on the queue
factories in all three languages, default 0/false. This completes parity
with the Java `tools4j/mmap` surface for a capability that already exists on
disk. It is a parity completion, not a scope expansion — the commit message
must say so explicitly:
`"expose existing archivable meta capability through factories; completes tools4j parity"`.

**Why default 0:** non-archivable queues read `start_index` as a plain
cached value; archivable queues pay an acquire `atomic_load` on read paths
and archive-hole re-checks in the header search. 0 is the performant mode
and the overwhelmingly common one.

**Semantics (state these in every factory's doc comment):**
`archivable` applies at **file creation only**. Opening an existing queue
adopts the on-disk value and silently ignores the caller's parameter — the
same reconciliation rule as `region_size` and `max_file_size`. Requesting
`archivable=1` on an existing `archivable=0` queue yields an archivable=0
queue, by design.

---

## 2. Non-goals — do NOT do any of these

- Do NOT change the meta layout, byte offsets, or the meaning of byte 24.
- Do NOT change reconciliation semantics (disk wins on reopen) or add any
  "upgrade to archivable" / migration path.
- Do NOT expose archiver operations through the Rust/Python wrappers beyond
  what already exists. This spec adds a creation flag, nothing else.
- Do NOT alter hot-path code. The acquire-load on `start_index` for
  archivable queues already exists; do not "optimize" it and do not add
  archivable checks to append/poll paths that don't already have them.
- Do NOT reformat surrounding code or convert documentation styles. House
  style only (aligned `name: description`, no `@param`/`@return`).
- Do NOT use `MMAP_ASSERT` for anything this spec adds. Part B explicitly
  converts existing asserts; adding new ones is a spec violation.

---

## 3. Note on tree state

This spec is written against the tree AFTER the queue_type change:
`queue_meta_open_rw(directory, queue_name, start_index, region_size,
max_file_size, archivable, queue_type)` — the `archivable` parameter is the
existing 6th argument. If your tree's signature differs, stop and report
before proceeding.

---

# PART A — plumb the parameter

## A1. C — `mmapq`

The flow mirrors how `region_size` travels today: factory → queue struct →
participant creation → `queue_meta_open_rw`.

1. **`include/indexed_queue.h` / `src/indexed_queue.c`:**
   `mmapq_indexed_queue_create(...)` gains `int archivable` as the parameter
   immediately after `start_index` (keep runtimes last). The `MmapqIndexedQueue`
   struct (src/indexed_queue.c, ~line 18) gains `int archivable;` stored
   from the parameter. `mmapq_indexed_queue_create_default` passes 0.
   Doc comment on the parameter (house style):

   ```
    * archivable:   1 to create the queue archivable (archiver may advance
    *               start_index; readers pay an acquire load). Applies at
    *               creation only — existing queues keep their on-disk value.
    *               0 (default) is the performant mode.
   ```

2. **`include/indexed_appender.h` / `src/indexed_appender.c`:**
   `mmapq_indexed_appender_create` and `mmapq_indexed_appender_create_with_id` gain
   `int archivable` (same position convention). The internal create path
   replaces the hardcoded `/* archivable = */ 0` at the
   `queue_meta_open_rw` call (~line 343) with the parameter.

3. **`src/indexed_queue.c` participant factories:**
   `mmapq_indexed_queue_create_appender` / `_with_id` forward `q->archivable`.
   Pollers/readers open meta RO and need no change.

4. **Batch side, same pattern:** `include/indexed_batch_queue.h`,
   `src/indexed_batch_queue.c` (struct field + factory param + forward),
   `include/indexed_batch_appender.h`, `src/indexed_batch_appender.c`
   (param, replace hardcoded 0 at its `queue_meta_open_rw` call, ~line 158).

5. **Call-site sweep:** `grep -rn "mmapq_indexed_appender_create\|mmapq_indexed_batch_appender_create\|mmapq_indexed_queue_create\b\|mmapq_indexed_batch_queue_create" src/ examples/ bench/ test/ tools/ 2>/dev/null`
   Every call site gains an explicit `0` (or `1` where a test intends
   archivable). Known non-test callers to check: `src/queue_transfer.c`,
   `src/batch_queue_transfer.c`, `src/queue_latency_probe.c`,
   `src/batch_queue_latency_probe.c`, `examples/*`, `bench/*` if present.
   Archiver tests that currently pre-create meta by hand
   (`test/test_indexed_archiver.c`, `test/test_indexed_batch_archiver.c`)
   should be SIMPLIFIED to use the new parameter where that shortens them —
   this is the one sanctioned test modification, and only where behavior is
   identical; report each such simplification explicitly.

## A2. Rust — `mmapq-rs`

Config-level flag, exactly how `region_size` flows. **Participant wrapper
files (`appender.rs`, `poller.rs`, `reader.rs`, batch equivalents) do NOT
change** — appenders are created from the C queue's stored state.

1. Sync the C changes into `c/` and verify with the bidirectional diff
   (AGENTS.md mirror discipline).
2. `src/ffi.rs`: update `mmapq_indexed_queue_create` /
   `mmapq_indexed_batch_queue_create` (if the batch FFI takes the queue-level
   params) and the appender-create declarations to match the new C
   signatures exactly. A signature mismatch here is silent stack corruption,
   not a compile error — copy from the headers, do not type from memory.
3. `src/queue.rs`: `QueueConfig` gains `archivable: bool` (default `false`)
   with builder method:

   ```rust
   /// Create the queue archivable (archiver may advance start_index).
   /// Applies at creation only; existing queues keep their on-disk value.
   /// Non-archivable (default) is the performant mode.
   pub fn archivable(mut self, archivable: bool) -> Self {
       self.archivable = archivable;
       self
   }
   ```

   `MmapqIndexedQueue::create` passes `config.archivable as i32` in the FFI call.
4. Batch config equivalently, if/where the batch path takes queue params.

## A3. Python — `mmapq-py`

1. `setup.py` cdef: update the changed C signatures exactly (same
   silent-corruption warning as Rust).
2. `single_queue.py`: `MmapqIndexedQueue.create(..., archivable: bool = False)`,
   passed as `1 if archivable else 0`. Docstring carries the same
   creation-only + performance sentence. Same for `batch_queue.py`.
3. Appender/poller wrappers unchanged.

---

# PART B — archiver assert audit (required in the same task)

Rationale: `archivable=1` was previously reachable only from tests; this
spec makes the archiver publicly reachable, so its validation must survive
`-DNDEBUG` per AGENTS.md. There are ~64 `MMAP_ASSERT` hits across
`src/indexed_archiver.c`, `src/indexed_batch_archiver.c`, and
`src/header_reader.c`. Classify EVERY hit into exactly one of:

- **(V) Validation** — checks caller input, on-disk state, or cross-process
  state. CONVERT to a real runtime check. Known examples that are (V):
  - `mmapq_indexed_archiver_create`: NULL directory/queue_name, ring_size > 0 →
    return NULL (creation functions return pointers).
  - `archive_up_to_index`: NULL archiver → return; `index >= 0` and
    "queue is not archivable" and "queue not created yet" → these are in a
    `void` function; change its signature to return `int`
    (`0` ok, `MMAPQ_EINVAL` for bad index/NULL, `MMAPQ_ECLOSED`-family for
    not-archivable/not-initialised — use `MMAPQ_EINVAL` for
    not-archivable: the caller passed an operation invalid for this queue).
    Update `include/indexed_archiver.h` doc accordingly. Grep for callers
    of `archive_up_to_index` and handle the new return (tests assert 0).
  - Out-of-memory after calloc/malloc → return NULL / error code.
- **(I) Invariant** — internal impossible-state checks (e.g. wrap results
  that cannot fail absent corruption already guarded elsewhere). KEEP as
  `MMAP_ASSERT`, unchanged.

Your report must include the full classification table: file:line →
V-converted / I-kept, one line each. Do not skip lines; do not batch-convert
without classifying. If a hit is genuinely ambiguous, classify it (I),
keep it, and flag it in the report for human review — do not guess a
conversion.

The same audit and treatment applies to the batch archiver mirror-functions
of every (V) case found in the single archiver.

---

# PART C — tests

All new/changed tests follow neighboring style (Unity, temp dirs cleaned in
teardown, `RUN_TEST` registered — an unregistered test does not exist).

1. **`test_archivable_roundtrip`** (in `test/test_indexed_archiver.c`):
   create a queue via the PUBLIC factory with `archivable=1`; `pread` byte
   24 of `<dir>/<q>/meta` and assert `== 1`; create an archiver and assert
   `mmapq_indexed_archiver_archivable(a) == 1`; close all; reopen the queue with
   `archivable=0` and assert the queue is STILL archivable (disk wins —
   pins the reconciliation semantics).
2. **`test_default_not_archivable`**: create via factory with `archivable=0`;
   byte 24 == 0; `mmapq_indexed_archiver_archivable == 0`.
3. **`test_archive_up_to_index_rejects_non_archivable`**: on an
   archivable=0 queue, `archive_up_to_index` returns `MMAPQ_EINVAL` (this
   pins a Part-B conversion — it MUST be run under release flags, see 5).
4. Rust: extend an existing integration test or add
   `tests/archivable.rs`: `QueueConfig::new(...).archivable(true)` →
   create → byte-24 check via `std::fs` read → assert 1. Python: one test:
   `MmapqIndexedQueue.create(d, "q", archivable=True)` → read byte 24 → 1;
   plus default-False → 0.
5. **Release-flags loop:** add `test_indexed_archiver` to the release test
   list in `scripts/test.sh`. This is the first archiver coverage under
   `-DNDEBUG` and is the enforcement for Part B — a (V) assert left
   unconverted will pass at `-O0` and fail (or silently not-check) here.
   Test 3 is the canary: before converting, run it under release flags and
   observe that the old assert does NOT fire (the fail-first evidence for
   Part B); after converting, it returns `MMAPQ_EINVAL` in both configs.

---

# D — documentation

1. `mmapq/AGENTS.md`, "Scope of the C port" section: REPLACE the line
   "Archiving support exists but `archivable` is always 0 in C-created
   queues." with:
   "`archivable` is a creation-time factory parameter (default 0). Existing
   queues keep their on-disk value on reopen. Archivable reads pay an
   acquire load on `start_index` — see doc/maintainer_guide.md before
   touching those paths."
2. `README.md`: one sentence in the API/usage section documenting the
   parameter, its default, and creation-only semantics.
3. If the operator tools (`tools/mmapq_stat.c`) are present in the tree:
   the archive-hole caveat moves from the tools README into `mmapq-stat`'s
   usage text ("results on actively-archived queues may be stale/racy").
   If the tools are not in the tree, skip and note it.

---

# E — acceptance criteria (all must hold)

1. `bash scripts/test.sh` exits 0 — normal AND release loops, with
   `test_indexed_archiver` now in the release loop and the three new C
   tests visible by name.
2. Mirror diff (both directions, `-x '*.o'`) exits 0.
3. `cargo test` fully green including the new archivable test;
   `cargo test -- --list` output included in the report.
4. Python runner shows the two new tests by name; total = previous + 2.
5. `grep -rn "archivable = \*/ 0\|archivable = 0" src/indexed_appender.c src/indexed_batch_appender.c` shows no hardcoded value at the
   `queue_meta_open_rw` calls (the parameter flows).
6. The Part-B classification table is complete: every `MMAP_ASSERT` in the
   three audited files appears exactly once, marked V or I.
7. Manual: `create(archivable=1)` from Python, then
   `xxd -s 24 -l 1 <dir>/q/meta` shows `01`; recreate with default and it
   shows `00`.

# F — report format

Per repo: files changed (must match manifest — Part A + audited files +
tests + docs, nothing else); tests added by name with runner lines quoted;
per-suite counts before/after; mirror diff exit code; the full Part-B
classification table; every deviation flagged explicitly. Under-claim.
