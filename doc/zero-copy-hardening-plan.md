# Zero-copy safety, transfer correctness, and persistence contracts

Status: implementation present; release acceptance remains incomplete. See
[implementation evidence and outstanding gates](hardening-implementation.md).
The user approved implementation, including checked batch start and a distinct
mapper-abort signal. Package versions remain unchanged until release.

This plan addresses the three follow-up priorities from the ecosystem review.
It is based on the current source, including the completed polish pass. It
originally proposed changes rather than authorizing them. Implementation was
subsequently authorized; publication is still not authorized. The Rust changes belong
in a separate release because they intentionally reject previously accepted
source code. Suggested Rust version: 0.2.0; choose C/Python versions from
their actual compatibility changes rather than mechanically matching Rust.

## 1. Non-negotiable constraints

- Preserve zero-copy Rust reads: returned payload slices point directly into
  the existing mapped region. No implicit `Vec`, `String`, `Box`, payload
  clone, or materialization of a batch.
- No new per-message allocation/deallocation, reference-count operations,
  locks, syscalls, dynamic dispatch, mapping pins, or retained-region lists.
- Bound memory by the configured mapping ring, not batch size or how many
  messages a consumer has visited.
- Preserve the existing append/publication protocol, memory orderings, disk
  layout, file naming, and Java compatibility boundaries.
- No new runtime dependency, general-purpose iterator framework, background
  service, durability subsystem, or production fault-injection framework.
- Existing creation-time allocations, mapping operations at region
  boundaries, and producer-to-queue copies are not eliminated by this plan.
  The promise is no additional payload copy or runtime ownership machinery
  introduced to make Rust reads safe.
- `&mut`, lifetimes, and `PhantomData` enforce ownership at compile time.
  `Result` and `Option` are stack values, not heap allocations. Their actual
  generated code must still be inspected and benchmarked.

The intended tradeoff is explicit: callers cannot retain a borrowed payload
while asking the same cursor to advance. Processing in place remains zero
copy. Applications needing retained data must choose their own storage;
the library will not silently copy it for them.

## 2. Workstream A — Rust safety without copying

### A1. Establish the ownership invariant before editing signatures

Trace these C paths and document which operation can invalidate which view:

- `indexed_reader.c` and `indexed_batch_reader.c`: reusable contexts,
  payload accessors, lazy initialization, positioning, and metadata queries.
- `indexed_poller.c` and `indexed_batch_poller.c`: callback lifetimes and
  state accessed after callbacks return.
- `region_ring_accessor.c`, `region_ring.c`, and `region_async.c`: current
  slot lifetime, forward/backward premap, asynchronous replacement, and close.
- Transfer helpers: any C call that reads through a supplied source reader.
- Batch writer reserve/append/commit and bulk-copy operations.

The required C invariant is that the currently yielded payload stays mapped
until the owning accessor is advanced, repositioned, or closed. Establish
that already-scheduled background premap cannot recycle that current slot
while Rust holds a slice. Test synchronous and asynchronous mapping. If this
invariant fails, borrowing signatures alone are insufficient: stop and fix
the mapping ownership protocol before claiming Rust soundness. Do not mask
that defect by retaining regions or allocating copies.

Each reader/poller must own its own cursor/accessor state. Cloning a runtime
must not implicitly make different readers share a reusable payload buffer.
The guarantee assumes cooperating writers obey publication and immutability
rules; Rust cannot protect mapped files from arbitrary external truncation
or modification.

### A2. Make reader operations exclusive

Change both reader families as follows:

```rust
pub fn read(&mut self, index: i64) -> Result<Option<Message<'_>>, MmapError>;
pub fn read_first(&mut self) -> Result<Option<Message<'_>>, MmapError>;
pub fn read_last(&mut self) -> Result<Option<Message<'_>>, MmapError>;
```

The batch variants return `BatchMessage<'_>` instead of `Message<'_>`.
Require `&mut` on reader operations that enter C and can initialize or mutate
accessors, including `entry_status`, `last_index`, and `start_index` in the
initial conservative design. Pure accessors on an already-returned message,
such as its index and appender ID, retain `&self`.

The existing reader types are documented as thread-confined and contain raw
pointers; neither implements `Send` or `Sync`. Verify this premise for both
reader families and their views with compile-fail checks before changing
signatures, and keep those checks afterward. Do not add unsafe thread-trait
implementations or runtime locks to accommodate migration. Sharing a queue
and creating an independent reader per thread is distinct from sharing one
reader. Existing safe cross-thread sharing of these readers is not a
supported contract to preserve; downstream unsafe sharing needs its own
audit. Within one thread, callers currently using multiple shared references
to a reader must instead arrange sequential exclusive borrows. If callers
already use `RefCell`, their guard must remain alive for the entire use of a
borrowed message; the library must not introduce its own dynamic borrow checks.

Loosen a reader method back to `&self` only after a per-method C call-graph
audit proves it cannot invalidate a live context or payload, reset/advance
an iterator, or create incompatible aliases to Rust-owned fields. Include
lazy initialization, cached metadata, error-state updates, and async mapping
side effects in that audit. Internal C mutation alone does not rule out
`&self`, but its isolation from every outstanding Rust reference must be
demonstrated, not inferred from a metadata-looking name. If header and payload
accessors are independent, document and test that independence. Require tests
holding a live view across the candidate method in sync/async modes and at
ring boundaries, alongside the lifetime audit; passing tests alone are not
a soundness proof. Until those obligations are met, retain `&mut`. Relaxing
a receiver later is separate from making the type thread-safe.

Change all source-reader parameters in `QueueTransfer` and
`BatchQueueTransfer` to `&mut IndexedReader` or `&mut IndexedBatchReader`.
An export or copy must not bypass the exclusive borrow required by a read.
Audit all `as_ptr()` users: returning a raw pointer is not itself a
dereference, but safe wrappers must not use it to evade exclusivity.

Remove reader-context `UnsafeCell` where exclusive methods make it
unnecessary. Keep contexts as ordinary fields and pass mutable pointers to
C only during the corresponding exclusive borrow. Update queue factories
that initialize these fields. Avoid a broader struct/layout refactor.

`Message` remains a small borrowed view. `BatchMessage` must carry an
exclusive lifetime tied to the reader, for example a mutable context
reference plus an exclusive owner marker. Neither view receives a new
`Clone`/`Copy` implementation. Preserve participant thread confinement.

Supported usage:

```rust
let mut reader = queue.create_reader()?;
if let Some(message) = reader.read(42)? {
    process(message.data());
}
let next = reader.read(43)?;
```

The following must fail to compile because the first view is used again:

```rust,compile_fail
let message = reader.read(42)?.unwrap();
let bytes = message.data();
reader.read(43)?;
process(bytes);
```

The safety change requires no runtime borrow flag, lock, reference count,
allocation, or payload copy. A reader local simply becomes `let mut reader`.

### A3. Replace the standard batch Iterator with an inherent lending method

Keep `BatchMessageIter` as the existing concrete cursor type. Remove its
`std::iter::Iterator` implementation. Do not introduce a GAT trait or add
a lending-iterator dependency for one concrete type.

Proposed signatures:

```rust
impl BatchMessage<'_> {
    pub fn iter(&mut self) -> BatchMessageIter<'_>;
}

impl BatchMessageIter<'_> {
    pub fn next<'step>(
        &'step mut self,
    ) -> Result<Option<(&'step [u8], u8)>, MmapError>;
}
```

The crucial lifetime is `'step`: the payload borrows the current call's
exclusive cursor borrow, not the entire reader or batch lifetime. No advance,
reset, bulk consume, or drop of that cursor is possible while the slice is
still used. `BatchMessage::iter(&mut self)` similarly prevents two live
cursors over one C iterator. Sequential re-iteration remains supported after
the first cursor's borrow ends.

Usage changes from `for` to a simple loop:

```rust
let mut reader = queue.create_reader()?;
if let Some(mut batch) = reader.read(42)? {
    let mut messages = batch.iter();
    while let Some((payload, appender_id)) = messages.next()? {
        process(payload, appender_id);
    }
}
```

`for`, `.collect()`, and standard iterator adapters no longer apply to this
cursor. This is deliberate: ordinary `Iterator` permits keeping previous
items while requesting the next one, which is incompatible with reusable
mapped regions. Do not retain the unsound implementation as a deprecated
compatibility adapter.

Borrowed slices are never accumulated inside the library. Cursor state is
only existing pointers, scalar metadata, and zero-sized lifetime markers.

### A4. Give one cursor step a precise, inexpensive C error boundary

The existing boolean `mmapq_batch_iterator_advance` conflates exhaustion and
failure. The polish pass stores `last_error` privately, but Rust must not
mirror that private struct layout to inspect it.

Propose one additive C accessor operation:

```c
int mmapq_batch_iterator_next(
    MmapqBatchPayloadIterator *iterator,
    const void **data,
    int32_t *length);
```

Contract:

- Return 1 for a valid message, 0 for clean exhaustion, or an existing
  negative `MMAPQ_*` code for failure.
- On 1, fill a non-null payload pointer and a positive validated length.
- On 0/error, set outputs to null/zero; never expose a stale buffer.
- Delegate framing and mapping to the existing advance implementation; do
  not duplicate parsing. If advancement stops, inspect the private error
  before deciding that the batch ended cleanly.
- Preserve the original boolean C function and its existing callers.
- Keep errors sticky until explicit iterator reinitialization. Retrying a
  failed batch means reopening/re-reading it, not pretending the cursor
  safely advanced past the failed frame.

This combines advance, data access, and length access into one FFI call,
replacing the current three Rust-to-C calls per message. Out-parameters are
stack storage. It adds no payload copy or heap allocation and does not put a
Rust-owned region list around the mapper.

This is a proposed public C surface addition, explicitly listed for review
because the repository freezes its public API. Implement it only as part of
the accepted safety-release scope. Update the public header, Rust FFI,
binding-symbol/coverage records, feature matrix, and both C mirrors. Python
need not switch its read path merely because this function exists.

Rust converts the returned pointer to a slice only on success. The unsafe
block must explain positive length, pointer validity, the mapping lifetime,
and why another advance cannot overlap the slice's borrow.

### A5. Preserve the no-allocation contract on iteration errors too

The current `map_rc` can allocate through `Corrupt(String)` and
`Internal(String)`. Do not claim all-error-path allocation freedom while
using it unchanged.

As part of the breaking Rust release, change these diagnostic payloads to
`Cow<'static, str>` and map known C errors to borrowed static descriptions.
Represent unknown numeric C codes in a scalar variant such as
`UnexpectedCode(i32)`, formatting only if the caller requests display.
Existing cold-path diagnostics may still own a string; the cursor error
mapping must not construct one. No formatting or logging belongs in `next`.

Update enum pattern matches, constructors, display tests, and the migration
notes. `Cow` comes from the standard library; a borrowed variant does not
allocate. Validate success, exhaustion, corruption, and timeout under a
counting allocator. Error construction for the new step must produce zero
allocations, not just the successful case.

### A6. Keep polling and direct batch copying zero copy

Retain the generic, stack-backed callback mechanism; do not box handlers.
Make the callback lifetime requirement explicit:

```rust
pub fn poll<H>(&mut self, handler: H) -> Result<PollResult, MmapError>
where
    H: for<'batch> FnMut(i64, BatchMessageIter<'batch>) -> NextMove;
```

Keep the existing callback return type and panic containment. Example:

```rust
let mut iteration_error = None;
poller.poll(|batch_index, mut messages| {
    loop {
        match messages.next() {
            Ok(Some((payload, appender_id))) => {
                process(batch_index, appender_id, payload);
            }
            Ok(None) => return NextMove::Advance,
            Err(error) => {
                iteration_error = Some(error);
                return NextMove::Stop;
            }
        }
    }
})?;
if let Some(error) = iteration_error {
    return Err(error);
}
```

This sketch assumes the enclosing function returns `MmapError`. An error
may also be returned by `poll` itself. Preserve the existing distinction
between its corruption result and other error results; document both.
The C poller already inspects the iterator error after the callback and
returns before moving the queue cursor, even if the handler swallows the
error and asks to advance (`mmapq_indexed_batch_poller_poll` in
`src/indexed_batch_poller.c`). This is an invariant to preserve, not missing
C functionality to implement. Add Rust wrapper tests exercising that case
with the proposed lending cursor and verify that the same batch remains
current after the failure.
Already-executed application side effects are not automatically undone.

`append_remaining_from_iter(&mut self, &mut BatchMessageIter)` remains a
direct C-to-C copy path. It cannot run while a slice borrowed from that
cursor is live. Validate that source and destination accessors are distinct
and no safe handle combination permits overlapping mutation of source data.
Specify whether the current message has already been consumed: the present
C helper advances before copying, so copying remaining messages starts
after the last item returned by `next`. Preserve and test that behavior.

Make iterator construction from raw contexts private to the safe wrapper,
or explicitly unsafe if it must remain public. Raw constructors must not
derive an owner lifetime merely from the address of a stack context.
Document raw-pointer escape requirements and require an exclusive cursor
borrow for raw advancement. Do not add `Clone`, `Send`, or `Sync` to views
or cursors to work around compiler errors.

### A7. Rust validation and performance gates

Before changing signatures, create fixtures demonstrating that the current
API accepts invalid borrowing. Compile these without running invalid memory
access. After correction they must fail for the expected borrowing reason.
Use existing doctests or a small rustc fixture harness; no runtime dependency.
Compile-fail tests alone can pass for an unrelated syntax error, so retain
matching positive fixtures and verify relevant diagnostics.

Required rejected programs:

1. Read/read_first/read_last again while a prior message slice is used later.
2. Export/copy or run a remapping metadata operation with that slice live.
3. Advance a batch cursor while its previous payload is used later.
4. Reset/re-iterate a batch while its first cursor is still used.
5. Bulk-copy remaining messages while a cursor payload is live.
6. Drop or move the reader/poller owner while a derived view is still used.
7. Escape a poll callback with its cursor or payload.

Positive tests process in place, release each borrow, then advance. Cover
single-message readers, batch readers, pollers, repeated iteration, reserved
writer buffers, and source-to-destination bulk copying. Existing explicit
`to_vec` remains an application opt-in, never the implementation of safety.

Runtime tests must span more than one complete mapping-ring reuse cycle,
in both mapping modes and with different appender lanes. Include boundary
padding, large batches, invalid lengths, mapping failures after several
valid messages, and caller-requested early stop. Check exact message bytes,
indexes, IDs, terminal errors, and whether polling advanced.

For performance verification:

- Use the current post-polish tree as the baseline, not historical README
  numbers. Keep toolchain, optimization/LTO settings, affinity, payloads,
  storage, and mapping configuration identical.
- Count allocations on the calling thread after initialization/warm-up.
  Also inspect C call paths for added allocation; a Rust allocator counter
  does not observe C `malloc` or the mapper thread.
- Inspect optimized code for added payload copies, allocator calls,
  reference-count operations, locks, and new indirect calls. Compare the
  old three-call batch step with the proposed one-call step.
- Measure single reads, batch traversal, bulk copy, and end-to-end echo.
  Use single-region and ring-spanning batches, small and large payloads,
  sync/async mapping, and multiple batch sizes. Keep printing and result
  allocations outside timed loops.
- Run paired baseline/candidate trials on isolated cores using the existing
  benchmark tools and maintainer protocol (including the documented warm-up
  and 20-million-iteration runs where applicable). Report throughput and
  latency distributions, including tails, across multiple trials.
- Acceptance requires zero added steady-state allocations/copies and no
  repeatable latency/throughput regression outside the measured baseline
  variability. Investigate any systematic regression; do not declare a
  blanket acceptable slowdown or trade safety for an unmeasured shortcut.

Do not claim C FFI/mmap paths are Miri-verified. Compile checks prove borrow
restrictions; integration tests, sanitizers, mapping audits, and benchmarks
cover different obligations.

## 3. Workstream B — transfer failure correctness

### B1. Define observable success precisely

For every export/import/copy variant, inventory every fallible operation:
source discovery/read/iteration, destination create, ID mapping/acquisition,
batch start, append/reserve, complete, commit, fwrite/fread, and file close.

A returned count means successfully published entries/batches, or fully
written stream entries according to the export API's ownership of the FILE.
Increment a destination count only after successful append/commit. A failure
must return its actual cause rather than a success count for a partial run.

This does not make transfers atomic. On failure:

- Already committed destination messages may remain.
- The currently uncommitted batch must never be published as successful.
- Output files can contain partial data; an error invalidates the output.
- Automatic retry is not promised to be idempotent, especially when
  `allow_append` is enabled. Do not silently restart or duplicate data.
- No automatic repair, destination deletion, rollback of committed history,
  or new resumable-transfer subsystem is introduced.

### B2. Correct the C paths at their existing call sites

Start with `mmapq_queue_transfer_copy_batch_to_normal` and
`mmapq_batch_queue_transfer_copy`, where append/commit results are currently
ignored. Then audit the single-message, batch, cross-type, filtered,
chunked, preserve/rebase, and import variants systematically.

For each function:

1. Check allocation/factory and append results before using returned state.
2. Capture the original error before cleanup can overwrite errno/state.
3. Stop after failure; never complete/commit a batch whose construction failed.
4. Release only owned resources, exactly once, via the existing cleanup
   pattern or a local cleanup label. No generic transfer framework.
5. If batch rollback cannot complete because mapping failed, retain the
   failure and close the appender; do not claim rollback succeeded.
6. Propagate existing `MMAPQ_*` values where they correctly describe the
   cause. Distinguish timeouts, corruption, allocation, ID conflicts, and
   I/O; do not label all failed writes as a timeout.

Resolve missing error information at its source. For example, batch start
currently returns a pointer/NULL; a caller cannot infer the exact cause of
NULL. Inventory which cases are actually distinguishable before proposing
a minimal checked factory/error accessor. Any required additional public
surface is a separate explicit item in the implementation manifest, not a
guessed code or an unreviewed feature expansion.

For C functions taking a caller-owned `FILE *`, document that a successful
buffered write does not imply a successful later caller-owned flush/close.
Never close a caller-owned stream. For helpers that open their own files,
check both write and close errors. Preserve the completed polish pass's
Rust/Python close-error checks and `/dev/full` regressions.

Preserve primary-error precedence in both wrappers: always close an owned
stream, return an original export failure if present, and surface a close
failure only when export itself succeeded. Test successful export/close,
export failure alone, close failure alone, and simultaneous failures.
The review follow-up corrects this ordering in the current polish pass;
the separate hardening release must retain it.

### B3. Inject failures only in test binaries

Prefer existing seams and real errors: `/dev/full` for buffered output,
invalid/corrupt source fixtures, conflicts, and controlled mapping failures.
For failures that cannot be made deterministic, use narrow test-only
substitution at transfer call sites or link-time wrappers where reliable.

Inject the Nth operation with a specific existing error code. Exercise the
first operation, a later operation after some success, and cleanup. Assert
that the injection was consumed so a link-time wrapper bypassed by LTO
cannot produce a false passing test.

There must be no production fault counter, environment check, callback,
global switch, branch, or function pointer in append/poll/iteration paths.
If compile-time test macros are necessary, compile them out completely in
production and verify symbol/code absence in release artifacts. Run the
instrumented tests with debug and release optimization/NDEBUG flags and
report that they are instrumented; separately run uninstrumented release
tests against actual failure fixtures.

Minimum matrix:

| Failure | Required assertions |
| --- | --- |
| Source fails before/after valid entries | Exact error; no success count; no later entries read |
| Destination factory/cache allocation fails | Correct cause; prior handles cleaned once |
| Append fails on message N | No count increment for N; no later append; earlier commits preserved |
| Batch append fails after valid members | Incomplete batch never committed; cleanup verified |
| Complete or commit fails | Failure preserved; no success count; no blind retry |
| fwrite/flush/close fails | Owned output reports failure; caller-owned stream ownership preserved |
| Callback fails or panics | Does not unwind through C or silently select a successful fallback |
| Failure with preserve/allow_append/conflicting ID | Existing destination data preserved; mapping rules honored |

Use deterministic call/resource counters and reopened destination reads;
RSS measurements alone do not prove resources were released. Run ASan/UBSan
and leak checking on a host that supports them, recording any unavailable
checks. Concurrency changes, if required, also need TSan under repo policy.

### B4. Wrapper error and callback behavior

Rust and Python must propagate the C failure without turning it into a
partial success count or silently auto-assigning an ID after a failed mapper.
Rust must catch mapper panics before crossing `extern C`, unwind the C
operation through a defined failure result, release resources, then resume
the panic. Python must retain and re-raise the original exception.

The mapper's current numeric return contract includes an auto-ID sentinel;
do not reuse that sentinel for failure. Determine whether an existing
failure value can propagate through every cache/transfer path. If not,
specify one distinct callback-abort signal and its binding mappings as an
explicit compatibility change. No callback error may silently commit data
under a different ID. Existing Python fallback behavior is documented, so
changing it needs release notes and a version decision.

Use stack-scoped state for Rust where possible and the existing Python
handle mechanism; avoid new per-message callback allocation. This work is
confined to transfer callbacks, not a replacement of the poll fast path.

## 4. Workstream C — precise persistence guarantees

### C1. Publish one contract, then align examples and API docs

Trace the actual append/CAS, mapping, close, recovery, and export code before
writing guarantees. The contract must distinguish:

| Event/property | Contract to document and verify |
| --- | --- |
| Commit/publication | Concurrent cooperating readers observe a completed entry/batch; no implied storage flush |
| Consumer restart | History can be reopened; the application owns its checkpoint and seeks explicitly |
| Producer process failure | Recovery behavior for committed and uncommitted tails, with OS still running; test before promising |
| Clean system restart | File-backed data depends on filesystem and completed OS writeback; do not infer from a process-reopen test |
| Kernel crash or power loss | No guaranteed durable commit; writeback ordering can differ from reader-visible publication ordering |
| `/dev/shm` | Volatile tmpfs; useful for disposable benchmarks, not retained history |
| `/tmp` | Location/lifetime depends on host configuration; not a persistence example default |
| Export/close success | Stream completion/error handling, not an fsync-level power-loss guarantee |

Document that deleting archived files differs from externally truncating
live mapped files. State what happens to consumer checkpoints when entries
are archived. Qualify Java compatibility by queue type.

### C2. Make the contract executable without adding durability overhead

- Run write/commit/close and reopen/read in separate processes on an
  explicitly chosen file-backed test directory. Verify its backing
  filesystem rather than assuming `/tmp` is disk-backed.
- Test a saved application checkpoint and exact resume index; do not add an
  automatic checkpoint writer to the library.
- Use deterministic parent/child coordination to terminate a writer before
  publication and after acknowledged publication. Verify the existing
  recovery contract and incomplete-batch visibility. No sleep-based race
  fixtures and no production test hooks.
- Explain that SIGKILL/process-restart tests are not power-loss tests.
  Controlled reboot/power-cut testing is separate infrastructure and cannot
  justify stronger guarantees than the algorithm provides.
- Keep README examples runnable and clearly separate retained-history
  examples, rollback examples, and disposable benchmark directories.

### C3. Do not add synchronous durability to this workstream

No `msync`, `fsync`, `fdatasync`, per-commit flush flag, WAL, extra commit
record, or background durability worker is proposed. Adding any of these
would require its own API, ordering, failure, and latency design.

The value delivered here is an accurate persistent-file/replay contract
without changing the fast path. Applications requiring power-loss-durable
acknowledgments need a separately designed storage boundary; documentation
must not imply that exporting alone supplies one.

## 5. Implementation sequence and review units

1. Record the current post-polish test, allocation, and benchmark baseline;
   complete the C mapping-lifetime audit and transfer call/error inventory.
2. Add reader aliasing compile fixtures; implement exclusive reader and
   transfer borrows, remove unnecessary reader `UnsafeCell`, migrate callers.
3. Add checked C batch stepping, allocation-free code-backed Rust errors,
   and lending cursor signatures; update callback and bulk-copy paths.
4. Run lifetime, region-reuse, allocation, and latency gates. Review the
   breaking migration examples before accepting the Rust API.
5. Add failure fixtures before fixing each transfer path. Correct C cleanup
   and result handling, then wrappers and callback-abort semantics.
6. Consolidate persistence documentation and executable examples against the
   verified behavior; finish crash/reopen/checkpoint tests.
7. Run complete debug/release, wrapper, sanitizer, package, mirror, and
   benchmark verification; prepare release notes and version changes.

Each unit has a bounded file manifest and independent evidence. Tests must
fail before their behavioral fix; compile-fail fixtures must be demonstrated
to compile on the old unsound API. Do not bundle style-only rewrites.

## 6. Expected implementation file manifest

The following is the original prospective manifest. The implementation report
records the final manifest, necessary deviations, and executed verification.

Rust:

- `src/reader.rs`, `src/batch/batch_reader.rs`, `src/batch/batch_iter.rs`.
- `src/batch/batch_poller.rs`, `src/batch/batch_appender.rs`.
- `src/queue.rs`, `src/batch/batch_queue.rs`, `src/transfer/mod.rs`.
- `src/ffi.rs`, `src/error.rs`, relevant crate/module docs and `README.md`.
- Existing reader/transfer tests and examples requiring mutable bindings or
  lending loops; new compile fixtures, mapping-reuse and allocation tests.
- Existing benchmark examples only where needed to measure the changed API.

C upstream, mirrored verbatim into both wrappers:

- `include/batch_payload_iterator.h`, `src/batch_payload_iterator.c` for the
  checked step; private iterator header only if proven necessary.
- `src/queue_transfer.c`, `src/batch_queue_transfer.c`,
  `src/internal/appender_cache.h`, and the specific appender/error boundary
  needed to preserve causes. Avoid speculative edits elsewhere.
- Relevant transfer/public headers if error contracts change; error code
  definitions/string mappings only if a genuinely new failure code is needed.
- Relevant registered Unity tests, a narrowly scoped fault fixture, and
  `scripts/test.sh` registration/release coverage as needed.
- `doc/feature-matrix.md`, persistence documentation, maintainer contracts,
  and runnable C examples where claims or usage change.

Python:

- `setup.py` declarations only for C functions/types actually used.
- `src/mmapq/transfer.py` and error mapping/export definitions as required.
- Transfer failure tests, process-restart examples/tests, and `README.md`.
- No rewrite of Python reads into a new zero-copy API; Rust is the zero-copy
  API workstream in this plan.

### Explicitly deferred: Python borrowed-view lifetime safety

**Subsequent resolution:** the user approved enforced memoryview borrowing.
`mmapq-py` now retains the reader through a read-only buffer exporter and rejects
reads/index queries/close with `BufferError` while exports or aliases survive.
The last buffer release ends the borrow; saved exporters cannot reopen an old
snapshot. Tests cover aliases, buffer consumers, garbage collection, shutdown,
and overlapping operations. See `../mmapq-py/docs/reader-borrowing.md`.
The following records the original deferral and design constraints.

`IndexedReader.read_view` currently exposes a raw `memoryview` into a C
mapping. Its documented lifetime ends at the next read or close, but Python
does not enforce that restriction. Closed-handle guards protect calls on
the reader, not access through an already-returned view. Retaining the reader
alone also cannot prevent its mapping ring from recycling a region. This
remains an open memory-safety issue; the Rust borrowing changes do not fix it.

Before a Python release claims safe zero-copy reads, make an explicit,
separately approved compatibility decision: deprecate/remove this raw-view
surface from the safe API, or design an owner-aware buffer/lifecycle mechanism
that prevents invalidation while any exported alias survives. The latter
needs its own performance and bounded-memory analysis; it is not assumed
compatible with this plan's no-pinning/no-runtime-ownership constraints.
Do not silently copy payloads or add view tracking to satisfy the claim.
Python's ordinary `read` already returns owned bytes and remains distinct.

An explicitly unsafe/manual-lifetime API can document caller obligations
but is not a safety fix. Neither a callback nor a context manager alone
prevents a caller from retaining a view or a derived slice. Any future
enforced design must cover aliases, subsequent reads, close/finalization,
region eviction, and concurrent use. For the current scope, preserve the
borrowed-view warning, carry this limitation in release notes, and track
the Python API decision separately rather than marking it resolved when
workstream A completes. Do not test a known dangling view by dereferencing
unmapped memory in the ordinary test process.

Release/package manifests change only after compatibility decisions are
settled. Do not create additional public convenience APIs to make the
migration look smaller.

## 7. Definition of done

- Rust cannot express the identified reader/cursor invalidations through
  safe API usage; positive zero-copy processing still compiles and works.
- No library-imposed payload copies, added steady-state allocation, dynamic
  borrow checks, locks, or per-message reference counts.
- Checked batch iteration distinguishes clean exhaustion from corruption
  and mapping failure without allocating its error representation.
- Every inventoried transfer failure is checked and tested; no failed
  append/commit/write is represented as successful completion.
- Persistence language matches tested scope and makes no power-loss or
  automatic-consumer-resume promise.
- C debug and release suites, Rust unit/integration/doctests/compile
  fixtures, Python tests, package builds and isolated artifact tests pass.
- Applicable sanitizer results are recorded honestly; unavailable tests
  remain explicitly unverified. Mirror comparisons have exit code zero.
- Paired performance results and generated-code/allocation checks support
  the low-latency claim on the tested hardware. No speculative percentage
  regression is accepted in exchange for safety.
- Release notes show concrete migrations (`mut` readers, `&mut` transfer
  inputs, lending loops, error payload changes, and callback failure policy).

The implementation is not done merely because the compiler accepts the new
types. Mapping lifetime, error behavior, and measured hot-path cost are
separate acceptance requirements.
