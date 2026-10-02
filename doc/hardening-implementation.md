# Hardening implementation and evidence

## Status and compatibility

The implementation is present in all three working trees. Release qualification
is **not complete**: leak checking and the full isolated-core performance matrix
remain open. Do not treat passing correctness tests as a latency guarantee.

Package versions remain 0.1.0 at the user's request. Nothing has been committed,
published, or version-bumped. Queue headers, metadata layouts, payload framing,
file names, and export layouts are unchanged.

New public C surface:

- `mmapq_batch_iterator_next`: 1 means a checked payload, 0 clean exhaustion,
  negative means failure. Both outputs are cleared on exhaustion/error.
- `mmapq_indexed_batch_appender_start_batch_checked`: returns the available
  underlying error instead of collapsing it to NULL. The old entry point stays.
- `MMAPQ_APPENDER_ID_ABORT` (INT32_MIN), and `MMAPQ_ECALLBACK` (-12):
  transfer callbacks can stop without silently requesting an automatic ID.

These are the checked step from the plan and the two additional API decisions
explicitly approved by the user. The checked header-read/search helpers added
during the audit are private, `mmapqi_`-prefixed implementation interfaces.

Rust source compatibility changes are described in
`../mmapq-rs/doc/hardening-migration.md`. Python mapper exceptions now abort and
re-raise after cleanup. A previously committed transfer prefix may remain;
transfers are not transactions and retries are not automatically idempotent.

## A. Zero-copy ownership and mapping audit

- Ordinary and batch readers keep plain reusable contexts, not reader-context
  `UnsafeCell`. Reads, remapping metadata operations, and transfer source
  parameters borrow readers exclusively.
- A batch view exclusively lends its cursor. The cursor's inherent
  `next(&mut self)` returns a payload borrowing that step, not the whole batch.
  There is no standard `Iterator` implementation or compatibility adapter.
- Poll callbacks have higher-ranked cursor lifetimes. C's existing rule that
  iterator errors prevent poll advancement remains in force even if a handler
  swallows an error and requests Advance.
- Reader, view, and cursor thread confinement is preserved. The compiler tests
  reject Send/Sync requirements; no unsafe thread-trait implementation was added.
- `Corrupt`/`Internal` diagnostics use `Cow<'static, str>`; unknown error codes
  remain integers. Error construction does not allocate on tested paths.
- The error mapper is cold/non-inlined, while cursor stepping and batch start
  expose inlineable fast paths. This was motivated by measured code-generation
  overhead, not by changing correctness checks or accepting a slowdown.

### Mapping invariant

The audit traced the reader and poller payload accessors, ring accessor,
synchronous/asynchronous region implementations, runtime request processing,
and writer reserve/bulk-copy paths.

Each participant owns its accessor/cursor state; sharing a runtime does not
share a reusable reader context. Forward/backward premap requests cover fewer
slots than the ring contains and exclude the current slot. A successful wrap
waits for the requested mapping; a mapped async slot is not spontaneously
replaced without another request. Borrowing forbids the owner operations that
could request replacement, reset the context, advance the cursor, or close it.
Runtime ownership already retained by participants protects their mapper.

This assumes cooperating publishers and immutable published payloads. External
truncation or arbitrary modification is outside Rust's ownership guarantee.
Rust lifetimes do not make arbitrary externally modified mmap storage safe.

### Mapping-capacity correction and padding

The ring accessor previously used its premap trigger boundary as reported
buffer capacity. A valid message crossing that trigger could falsely appear
corrupt. Fast-path eligibility and actual mapped capacity are now separate
values: a 64-bit absolute `fast_path_end` and a 32-bit `mapped_capacity`.
A stored 32-bit `region_size` replaces the size callback, preserving the
64-byte accessor layout on the tested target.
No mapping state transitions, atomic orderings, locks, pins, or retained-region
lists were added.

**Sequential padding behavior is unchanged.** A payload walker/batch cursor
skips short padding when remaining capacity is at most the length-prefix size,
and handles long padding through its negative length. The new positive test
covers remaining capacities 1, 2, 3, 4, and 100 bytes.

An indexed committed header is different: the writer records its payload
offset *after* writing/skipping padding. A direct reader/poller must therefore
reject a header pointing into insufficient space, or a positive message length
larger than the actual mapped remainder. It must not invent another message
by silently skipping a corrupt committed offset.

## B. Transfer audit

| Surface | Checked operations and ownership |
|---|---|
| Normal/batch stream export, with and without filters | Metadata discovery, checked reader results, batch terminal errors, framing writes; caller retains FILE ownership |
| Normal/batch chunked export | The above plus chunk limit and owned-file open/write/close; failure invalidates output |
| Normal/batch imports and both cross-type imports | Metadata/framing reads, positive bounded lengths/counts, allocations, factories, mapper results, append/start/complete/commit; one cleanup owner |
| Four source/destination copy combinations | Source discovery/read/iteration; cache/factory/ID errors; publish only successful destination entries/batches |
| Appender cache | Automatic fallback only for ID conflict, not arbitrary allocation/I/O failures; exact negative error retained |
| Rust/Python owned exports | Always close; original export error takes precedence over a simultaneous close error |
| Mapper callbacks | Distinct abort result; Rust catches before FFI unwinding and resumes after cleanup; Python re-raises the original exception |

Header discovery required an additional private correction: legacy header
search treats mapping timeouts as absent headers. Checked reader discovery now
retains the timeout, and transfer callers inspect it before using the discovered
range. The legacy internal interface remains for existing callers.

Normal chunk export also serialized a byte of C struct padding adjacent to its
8-bit appender ID. It now uses the existing framed-entry writer to explicitly
widen the ID to the 16-bit export field. This corrects output without changing
the format.

Failure-injection substitution exists only in test translation units. Production
append/poll functions have no injected counters, environment checks, switches,
or callback machinery. The final transfer matrices exercise 36 copy and 28
import combinations: first/later failures, source timeout, factory, append,
batch start, complete, and commit as applicable. They verify exact errors,
consumed injections, no later operation, retained prefixes, and reuse after
reopening. They do not claim exhaustive injection of every OS failure or a
proof of leak freedom.

## C. Persistence

See `persistence-contract.md`. Publication remains the existing atomic commit,
not msync/fsync/fdatasync. Consumer checkpoints are application-owned.

The six normal/batch process-restart scenarios cover clean reopen/checkpoint
resume, SIGKILL before publication, and SIGKILL after acknowledged publication.
Producer/parent coordination uses pipes, not race-sensitive sleeps. The test
directory's filesystem was verified as ext4 using findmnt. These tests do not
simulate kernel failure, reboot, reordered storage writeback, or power loss.

## Executed regression evidence

Baseline commits: C `4814542`, Rust `9a02750`, Python `a34dbb8`.
Baseline tests: 19 C binaries in each configuration; Rust's existing suite;
116 Python tests before the six new tests.

| Regression | Before | After |
|---|---|---|
| `borrowing_contracts` | Legacy acceptance mode compiled invalid borrowing fixtures; normal gate failed because reader_0 compiled | 25 compiler cases: 23 rejected with expected diagnostics, 2 accepted |
| `mapper_panic_is_resumed_after_c_cleanup` | Child process aborted at the C boundary | Original panic resumed after cleanup; destination reusable |
| `test_accessor_fast_path_preserves_full_capacity` | Capacity assertion failed | Exact physical remainder and 64-byte accessor size pass |
| `test_copy_failure_matrix` | Old normal transfer returned 3 instead of injected ENOMEM (-9) | Exact error and destination-prefix checks pass |
| Three `TestTransferHardening` tests | 16 failures against the old installed Python package | All copy/import combinations pass |
| `test_read_and_poll_reject_payload_beyond_mapping` | Expected ENTRY_ERROR (3), received AVAILABLE (0) | Oversized lengths rejected; poll handler not invoked |
| `test_chunk_export_does_not_serialize_context_padding` | Expected ID 7, received 32519 | Explicit 16-bit encoding preserves ID 7 |
| New checked-step/header-search interfaces | Pre-implementation link failed: symbols absent | Registered tests exercise success, exhaustion, corruption, and timeout |

The absent-symbol checks are API-introduction evidence, **not** a claim that
link failure by itself demonstrates the old runtime bug. The checked-header
test separately records the legacy search returning a prefix and the checked
search returning the timeout. The extra chunk-limit/size arithmetic corrections
were not each demonstrated failing in a dedicated before/after fixture; that
is a deviation from the repository's strict regression-first rule, not hidden
as full coverage.

### C results per binary

`bash scripts/test.sh` passes all 20 debug and all 20 exact-release-flags
binaries. Final source suite log: `/tmp/hardening-c-final5.log`.
The final expanded transfer matrix was additionally rerun in debug, release,
ASan/UBSan, and TSan: `/tmp/hardening-fault-final6.log`.

| Binary | Tests per configuration |
|---|---:|
| test_file_initialiser | 5 |
| test_file_mapper_single_ro | 7 |
| test_file_mapper_single_rw | 6 |
| test_file_mapper_rolled | 5 |
| test_region_sync | 10 |
| test_region_async | 8 |
| test_region_ring | 1 |
| test_region_ring_accessor | 10 |
| test_fixed_size_region | 1 |
| test_runtime | 13 |
| test_header_codec | 23 |
| test_appender_id_pool | 8 |
| test_queue_meta | 9 |
| test_payload_walk | 5 |
| test_indexed_queue | 40 |
| test_queue_transfer | 59 |
| test_transfer_failures | 3 |
| test_indexed_archiver | 11 |
| test_indexed_batch_queue | 27 |
| test_indexed_batch_archiver | 9 |

Additional named C tests: `test_checked_batch_step`,
`test_checked_batch_step_skips_short_and_long_padding`,
`test_checked_header_search_preserves_mapping_failure`,
`test_import_failure_matrix`. Existing tests were retained.

### Rust results per binary

Executed `cargo test --offline`, `cargo test --offline --release`,
`cargo build --offline --release --examples`, and
`cargo package --offline --allow-dirty`. The packaged crate was also tested
from its extracted directory using `cargo test --offline`.

| Binary | Tests in debug/release/package |
|---|---:|
| library unit tests | 33 |
| archivable | 2 |
| borrowing | 1 |
| cross_process | 2 |
| export_errors | 3 |
| integration | 6 |
| mapper_panic | 1 |
| poll_overhead | 1 |
| queue_type | 4 |
| runtime_uaf | 2 |
| transfer | 28 |
| zero_copy | 15 |
| doctests | 1 |

`zero_copy` includes 12 existing error-mapping unit tests plus
`error_construction_does_not_allocate`,
`region_reuse_and_bulk_copy_preserve_borrowed_payloads`, and
`corruption_is_allocation_free_and_cannot_advance_poller`.
The thread-local Rust allocator counter does not count C allocations or other
threads. C hot-path source inspection remains a separate check.

Logs: `/tmp/hardening-rust-final5.log`,
`/tmp/hardening-rust-release5.log`,
`/tmp/hardening-rust-artifact5.log`. Earlier builds exposed pre-existing
release-only unused-parameter warnings in C file-mapper code; these were not
silently fixed as unrelated work.

### Python results

Executed editable rebuild and
`python -m unittest discover -s tests -v`: **122 tests, OK**.
Built sdist and wheel with `python -m build --no-isolation`.
Installed the wheel into a separate environment and reran the 122 tests from
`/tmp`, outside the source tree: **OK**. The imported module path was the
artifact environment's site-packages, not an editable checkout.

New runner lines (module qualification omitted):
```text
test_copy_rejects_oversized_destination_message ... ok
test_mapper_exception_aborts_copy_and_import ... ok
test_truncated_import_is_not_partial_success ... ok
test_clean_reopen_and_application_checkpoint ... ok
test_process_killed_after_publication ... ok
test_process_killed_before_publication ... ok
```

Logs: `/tmp/hardening-pytest5.log`,
`/tmp/hardening-pypackage5.log`, `/tmp/hardening-py-artifact5.log`.
The user's existing project venv was not replaced. Candidate builds used
`/tmp/mmapq-review-venv`; artifact tests used
`/tmp/mmapq-hardening-artifact-env`.

## Sanitizers, gates, and limits

- Full 20-binary ASan/UBSan execution passed with leak detection disabled;
  final affected header/reader/batch/transfer binaries were rerun afterward.
  Compile flags: `-O1 -g -fsanitize=address,undefined -fno-sanitize-recover=all`;
  run with `ASAN_OPTIONS=detect_leaks=0`.
- The repository's normal `--asan` run enables LeakSanitizer. All 20
  instrumented binaries were blocked by its ptrace incompatibility.
  This is **not a passing leak check**.
- TSan initially reported an unexpected memory mapping. With the documented
  `setarch $(uname -m) -R bash scripts/test.sh --tsan` workaround,
  all 20 instrumented binaries passed, as did their normal release reruns.
  The final affected binaries and expanded fault matrix were also rerun with
  ASLR disabled and passed.
- Logs: `/tmp/hardening-asan.log`, `/tmp/hardening-asan-ubsan.log`,
  `/tmp/hardening-tsan-full.log`, `/tmp/hardening-sanitizer-final5.log`.
- Cross-repo gates and mirror comparisons pass; exact results appear below.
- No Miri claim is made for FFI/mmap code.
- Python raw `read_view` still has an unenforced lifetime. Ordinary `read`
  returns owned bytes. The separate Python borrowed-view decision is deferred,
  exactly as specified in the plan.

## Performance evidence and release gates

Initial paired echo trials identified batch overhead from Rust fast paths
remaining out of line after introducing error-bearing results. Moving the error
mapper out of line and making the small fast paths inlineable removed those
Rust symbols from optimized benchmark binaries. The checked C step makes one
FFI call from Rust, with no payload materialization, allocator, lock, or
reference-count operation added. It calls the existing C advancement logic.

Final benchmark conditions: GCC 11.4.0, rustc 1.98.1, x86-64, existing release
profiles, 64-byte payloads, 100,000 warm-up iterations, 20,000,000 measured
round trips per trial, client/server/mapper pinned to 5/7/9, ext4 temporary
directories. Baseline executables were saved before implementation.

The host has **no configured isolated cores**. Affinity is not isolation.
Tail variability and system contention prevent an isolated-host acceptance
claim. The old async batch benchmark timed out in warm-up in all three
120-second attempts; it is not a valid timing baseline for that configuration.
Candidate completion is correctness evidence, not proof of a speedup over
a non-completing baseline.

Still required before claiming the plan's complete performance acceptance:
isolated paired trials, the separate reader/traversal/bulk-copy microbenchmark
matrix across payload/batch sizes, and supported-host leak checking. No fixed
percentage slowdown is declared acceptable, and no durability flush, payload
copy, runtime borrowing, or ownership-tracking machinery was added.

### Final echo measurements

All 21 completed runs exited 0. Nanoseconds per round trip:

| Mode | Trial | Build | Mean | p50 | p99 | p99.9 | p99.99 | p99.999 |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| sync single | 1 | baseline | 369.4 | 299 | 376 | 2654 | 17584 | 3583077 |
| sync single | 1 | candidate | 371.3 | 299 | 391 | 1599 | 4942 | 3718424 |
| sync single | 2 | baseline | 374.8 | 302 | 395 | 2609 | 18915 | 3662435 |
| sync single | 2 | candidate | 362.6 | 290 | 378 | 2642 | 17818 | 3593555 |
| sync single | 3 | baseline | 373.1 | 300 | 393 | 2657 | 19053 | 3657421 |
| sync single | 3 | candidate | 363.9 | 291 | 376 | 2636 | 18307 | 3644422 |
| sync batch | 1 | baseline | 401.3 | 319 | 457 | 2681 | 17512 | 3698022 |
| sync batch | 1 | candidate | 399.5 | 320 | 440 | 2689 | 17508 | 3614983 |
| sync batch | 2 | baseline | 405.5 | 325 | 453 | 1660 | 7880 | 3763570 |
| sync batch | 2 | candidate | 412.1 | 332 | 453 | 2706 | 19802 | 3655157 |
| sync batch | 3 | baseline | 397.1 | 317 | 444 | 2681 | 17474 | 3671320 |
| sync batch | 3 | candidate | 402.1 | 323 | 442 | 2691 | 17030 | 3625973 |
| async single | 1 | baseline | 311.4 | 310 | 382 | 1699 | 7200 | 24310 |
| async single | 1 | candidate | 304.5 | 299 | 409 | 1581 | 6023 | 25069 |
| async single | 2 | baseline | 309.0 | 307 | 376 | 1644 | 4598 | 24761 |
| async single | 2 | candidate | 298.8 | 296 | 376 | 1528 | 4099 | 20883 |
| async single | 3 | baseline | 311.8 | 314 | 381 | 813 | 2740 | 16649 |
| async single | 3 | candidate | 297.6 | 294 | 376 | 1471 | 3017 | 20687 |
| async batch | 1 | candidate | 342.8 | 336 | 449 | 1886 | 4331 | 22188 |
| async batch | 2 | candidate | 338.5 | 330 | 450 | 2491 | 6980 | 21407 |
| async batch | 3 | candidate | 355.0 | 350 | 469 | 2615 | 5400 | 21439 |

Raw log: `/tmp/hardening-benchmark-final6.log`. Each invocation was:
`<binary> 20000000 100000 64 5 7 9 --oneline [--sync] --dir <fresh-ext4-directory>`.
Trials were sequential, with baseline then candidate where the baseline could
complete. No library builds or test suites ran concurrently with this final
benchmark sequence.

Single-message results do not show a consistent slowdown in these runs.
Sync batch medians were 319/325/317 ns baseline and 320/332/323 ns candidate:
a small paired increase remains, alongside overlapping run-to-run ranges and
mixed mean/tail results. It is not waved away as an acceptable percentage.
An isolated-host repeat is required before certifying no regression.
Async batch candidate runs completed; baseline runs did not, so no relative
performance conclusion is justified there.

### Final mechanical gates

`env -u PYTHONPATH PATH=/tmp/mmapq-review-venv/bin:$PATH CARGO_NET_OFFLINE=true bash mmapq/scripts/check_sync.sh`
completed with exit 0, all 11 fail-closed gate self-tests passing, and
`mirrors in sync`. Log: `/tmp/hardening-gates-final6.log`.
The README checker reported PASS=13, FAIL=0, SKIP=18, GATE_SKIP=0; its 18
non-compilable snippet skips are not missing-language opt-outs.
`bash mmapq/scripts/check_mirrors.sh` independently returned 0 for all eight
source/header/test/script mirrors. `git diff --check` returned 0 in every repo.
The production archive's symbol listing contains no `fault_` or `inject`
symbols.

## Manifest and explicit deviations

### mmapq

- `doc/ASSERT_AUDIT.md`
- `doc/feature-matrix.md`
- `doc/hardening-implementation.md`
- `doc/maintainer_guide.md`
- `doc/persistence-contract.md`
- `doc/zero-copy-hardening-plan.md`
- `include/batch_payload_iterator.h`
- `include/batch_queue_transfer.h`
- `include/indexed_batch_appender.h`
- `include/indexed_batch_reader.h`
- `include/indexed_reader.h`
- `include/mmap_platform.h`
- `include/queue_transfer.h`
- `README.md`
- `scripts/test.sh`
- `src/batch_payload_iterator.c`
- `src/batch_queue_transfer.c`
- `src/fixed_size_region.c`
- `src/header_reader.c`
- `src/indexed_batch_appender.c`
- `src/indexed_batch_reader.c`
- `src/indexed_poller.c`
- `src/indexed_reader.c`
- `src/internal/appender_cache.h`
- `src/internal/header_reader.h`
- `src/internal/region_accessor.h`
- `src/mmapq_strerror.c`
- `src/queue_transfer.c`
- `src/region_async.c`
- `src/region_ring_accessor.c`
- `src/region_sync.c`
- `test/test_header_codec.c`
- `test/test_indexed_batch_queue.c`
- `test/test_indexed_queue.c`
- `test/test_region_ring_accessor.c`
- `test/test_transfer_failures.c`
- `test/transfer_fault_batch.c`
- `test/transfer_fault_normal.c`
- `test/transfer_faults.h`

### mmapq-rs

- `doc/hardening-migration.md`
- `examples/batch_echo_server.rs`
- `examples/batch_latency_bench.rs`
- `README.md`
- `src/batch/batch_appender.rs`
- `src/batch/batch_iter.rs`
- `src/batch/batch_poller.rs`
- `src/batch/batch_queue.rs`
- `src/batch/batch_reader.rs`
- `src/error.rs`
- `src/ffi.rs`
- `src/queue.rs`
- `src/reader.rs`
- `src/runtime.rs`
- `src/transfer/mod.rs`
- `tests/borrowing.rs`
- `tests/cross_process.rs`
- `tests/export_errors.rs`
- `tests/integration.rs`
- `tests/mapper_panic.rs`
- `tests/queue_type.rs`
- `tests/transfer.rs`
- `tests/zero_copy.rs`

### mmapq-py

- `README.md`
- `src/mmapq/_errors.py`
- `src/mmapq/transfer.py`
- `tests/test_persistence_contract.py`
- `tests/test_queue.py`
- `tests/test_transfer_hardening.py`

Both wrappers' `c/` trees mirror every changed C source/header/test/script
listed above. Authoritative C documentation lives in the C repository rather
than being copied into submodule docs.

Necessary expansions of the prospective manifest:
region-accessor implementations/cache fields for the proven capacity defect;
normal poller validation for bounded borrowed slices; private header reader
helpers for transfer discovery errors; `src/runtime.rs` only to adapt cold
diagnostic construction to Cow; assertion-inventory updates for shifted lines.
No standalone refactoring, new production dependency, disk migration, version
bump, commit, or publication is included.

## Follow-up: absolute fast-path boundary

The approved layout follow-up replaces `active_length` with the exclusive
absolute `fast_path_end`. Both wrap helpers compare position against
`active_base` and `fast_path_end` before calculating an offset. Capacity is
still calculated independently from `mapped_capacity`; whole-region mapping,
premap scheduling, and padding behavior are unchanged.

The former size-callback slot now stores immutable `region_size` and cached
`mapped_capacity`, both 32-bit. The accessor remains 64 bytes, with
`active_address`, `active_base`, and the absolute endpoint at offsets 40,
48, and 56 respectively. All four factories initialize the stored size.

### Follow-up file manifest

Authoritative C changes (also mirrored verbatim under both wrappers' `c/`):
- `src/internal/region_accessor.h`
- `src/internal/region.h`
- `src/region_sync.c`
- `src/region_async.c`
- `src/region_ring_accessor.c`
- `src/fixed_size_region.c`
- `test/test_indexed_batch_queue.c`
- `test/test_region_ring_accessor.c`

Documentation: `doc/maintainer_guide.md`, `doc/ASSERT_AUDIT.md`, and this report.
No wrapper-language implementation or package version changed in this follow-up.

### Executed verification

Added `test_accessor_absolute_fast_path_bounds`: checks field layout, stored
size, nonzero base, before-base rejection, last eligible byte, exclusive-end
rejection, and capacity beyond the fast-path endpoint for both wrap helpers.
Existing capacity and padding tests remain enabled. This is a representation
change, not a new behavior fix; the new structural test was run after the
implementation, not claimed as a pre-fix failing regression.

- C: `bash scripts/test.sh`, exit 0, all 20 binaries in debug and exact release
  configurations. Per-binary counts in each configuration:
  file_initialiser 5; file_mapper_single_ro 7; file_mapper_single_rw 6;
  file_mapper_rolled 5; region_sync 10; region_async 8; region_ring 1;
  region_ring_accessor 11; fixed_size_region 1; runtime 13; header_codec 23;
  appender_id_pool 8; queue_meta 9; payload_walk 5; indexed_queue 40;
  queue_transfer 59; transfer_failures 3; indexed_archiver 11;
  indexed_batch_queue 27; indexed_batch_archiver 9.
  Names carry the `test_` prefix.
- Rust: `cargo test --offline` and `cargo test --offline --release`, both exit 0.
  Counts in each configuration: unit 33; archivable 2; borrowing 1;
  cross_process 2; export_errors 3; integration 6; mapper_panic 1;
  poll_overhead 1; queue_type 4; runtime_uaf 2; transfer 28; zero_copy 15;
  doctests 1.
- Python: rebuilt editable extension using
  `/tmp/mmapq-review-venv/bin/python -m pip install --no-build-isolation --no-deps -e .`,
  then `python -m unittest discover -s tests -v` with that interpreter and
  PYTHONPATH unset: 122 passed, exit 0.
- `bash mmapq/scripts/check_mirrors.sh`: all eight mirrors match, exit 0.
- `bash mmapq/scripts/audit_asserts.sh --verify`: 70 assertions, exit 0.
- `env -u PYTHONPATH PATH=/tmp/mmapq-review-venv/bin:$PATH CARGO_NET_OFFLINE=true bash mmapq/scripts/check_sync.sh`:
  exit 0, including all 11 fail-closed gate checks.
- `git diff --check` in the C repository and both C mirrors: exit 0.

### Hot-path check and limitations

GCC `-O3 -DNDEBUG` assembly for a wrapper around the inline helper shows
both bounds comparisons before offset subtraction. The size getter is one
32-bit field load followed by return. No allocation, lock, or syscall was
introduced into the accessor hot path.

A temporary probe at `/tmp/mmapq-accessor-probe.c` compared the saved
length-based header with the new header using identical `-O3 -DNDEBUG`
builds. Three paired runs, each 20 million cache hits through a noinline
wrapper, pinned with `taskset -c 5`, measured baseline/candidate ns per wrap:
0.764/0.760, 0.766/0.741, 0.740/0.743. All checksums were 61450491520.
The core was not isolated. These are synthetic cache-hit measurements,
not evidence of an end-to-end latency improvement. Earlier queue benchmark
tables in this document predate this layout follow-up.

No new sanitizer or end-to-end latency benchmark run was performed for this
follow-up. No concurrency, atomic-ordering, mapping-lifecycle, or disk-format
change was made. No scope expansion beyond the approved layout and naming.

## Follow-up: export count consistency

All four normal/batch stream/chunked exporters now reject a mismatch between
serialized header counts and actual writes with `MMAPQ_ECORRUPT`. Batch
exporters additionally verify each batch's message count. Stream exports no
longer stop silently once the originally counted total is reached: growth
as well as shrinkage is detected. Existing read/write errors take precedence;
chunk cleanup does not replace a detected count mismatch with a close error.

This error describes an inconsistent export, not necessarily corrupted
source storage. Exports do not snapshot the source, buffer its contents, or
seek back to patch headers. Filters must be deterministic and the exported
range must remain available. On failure, discard the output stream or every
chunk file from that attempt; partial output is neither repaired nor deleted.
Strict import validation and its committed-prefix behavior are unchanged.
Count equality alone does not prove that two passes selected identical data.

### File manifest

- `src/queue_transfer.c`
- `src/batch_queue_transfer.c`
- `include/queue_transfer.h`
- `include/batch_queue_transfer.h`
- `test/test_transfer_failures.c`
- This report.

The five C source/header/test files are mirrored verbatim in both wrappers.
Header contracts now describe repeated filter invocation, count mismatch
errors, and disposal of partial exports. No new error codes or APIs.

### Regression evidence

Five new regression tests were observed failing before the production fix
(expected -4, received success counts 2, 0, 1, 3, and 3 respectively):
- `test_export_rejects_missing_entries`
- `test_export_rejects_shrinking_filters`
- `test_export_rejects_growing_filters`
- `test_batch_export_rejects_message_count_shrink`
- `test_batch_export_rejects_message_count_growth`

Added positive control `test_export_accepts_stable_filters`. Normal/batch
and stream/chunked variants run inside the tests; message-count cases apply
to batches only. Missing entries use deterministic reader fault injection
of ARCHIVED status during writing, not a timing-dependent concurrent archive.
The transfer-failure binary increases from 3 to 9 tests.

### Executed validation

- Focused debug compilation of `test_transfer_failures` using the existing
  fault translation units: before fix, 8 tests / 5 failures; after fix,
  8 tests / 0 failures, followed by adding the stable-filter control.
- `bash scripts/test.sh`: exit 0 in debug and exact release configurations.
  Counts per binary in each configuration (all names prefixed `test_`):
  file_initialiser 5; file_mapper_single_ro 7; file_mapper_single_rw 6;
  file_mapper_rolled 5; region_sync 10; region_async 8; region_ring 1;
  region_ring_accessor 11; fixed_size_region 1; runtime 13; header_codec 23;
  appender_id_pool 8; queue_meta 9; payload_walk 5; indexed_queue 40;
  queue_transfer 59; transfer_failures 9; indexed_archiver 11;
  indexed_batch_queue 27; indexed_batch_archiver 9.
- Rust `cargo test --offline`: exit 0. Unit 33; archivable 2; borrowing 1;
  cross_process 2; export_errors 3; integration 6; mapper_panic 1;
  poll_overhead 1; queue_type 4; runtime_uaf 2; transfer 28; zero_copy 15;
  doctests 1. Rust release tests were not rerun for this follow-up.
- Python: rebuilt with `/tmp/mmapq-review-venv/bin/python -m pip install --no-build-isolation --no-deps -e .`,
  then the same interpreter's `-m unittest discover -s tests -v`, with
  PYTHONPATH unset: 122 tests passed, exit 0.
- `bash mmapq/scripts/check_mirrors.sh`: all eight mirrors match, exit 0.
- No sanitizer, performance, or real concurrent-archive test was run for this
  follow-up. Scope includes per-batch counts because the same mismatch can
  invalidate framing even when the outer batch count remains unchanged.

## Follow-up: empty export ranges and round-trip coverage

Normal and batch stream exports now write a valid zero-entry metadata header
when clipping a valid requested range to the queue leaves no entries. Batch
exports also do so for an initialized empty queue. Missing/uninitialized
source metadata still returns the existing reader error; no source metadata
is invented. Chunked export behavior is unchanged: an empty selection creates
no files. No seeking, extra buffering, disk-format, or import changes.

Files changed: `src/queue_transfer.c`, `src/batch_queue_transfer.c`,
`include/queue_transfer.h`, `include/batch_queue_transfer.h`,
`test/test_transfer_failures.c`, and this report. All five C files are synced
verbatim into both wrapper mirrors.

New tests:
- `test_normal_empty_range_roundtrip`
- `test_batch_empty_range_roundtrip`
- `test_initialized_empty_queue_roundtrip`
- `test_export_mismatch_in_second_chunk`

The first three failed before the fix because import returned -4 after an
export reported 0. They now pass, verifying exactly 64 output bytes, zero
entries, source metadata, queue type, and successful import. Populated-source
cases cover ranges below the first index and above the last in REBASE and
PRESERVE modes. Empty-queue cases initialize metadata by creating an appender
without committing messages.

The existing `test_export_accepts_stable_filters` now imports normal/batch
stream/chunked output and reads back every payload, including both messages
of each batch. The later-chunk test uses 600 KiB payloads and a 1 MiB chunk
limit to force separate files, injects ARCHIVED on the second chunk's read,
and verifies an error, an importable first chunk, a header-only second chunk,
and no third chunk. It does not imply partial chunk sets should be retained
after a failed export; the documented discard-all policy remains.

### Executed validation

- Focused debug `test_transfer_failures`: 13 tests, 3 failures before the
  production fix; 13 passed afterward.
- `bash scripts/test.sh`: exit 0, all binaries in debug and exact release
  configurations. Per-binary counts in each (names prefixed `test_`):
  file_initialiser 5; file_mapper_single_ro 7; file_mapper_single_rw 6;
  file_mapper_rolled 5; region_sync 10; region_async 8; region_ring 1;
  region_ring_accessor 11; fixed_size_region 1; runtime 13; header_codec 23;
  appender_id_pool 8; queue_meta 9; payload_walk 5; indexed_queue 40;
  queue_transfer 59; transfer_failures 13; indexed_archiver 11;
  indexed_batch_queue 27; indexed_batch_archiver 9.
- Rust `cargo test --offline`: exit 0. Unit 33; archivable 2; borrowing 1;
  cross_process 2; export_errors 3; integration 6; mapper_panic 1;
  poll_overhead 1; queue_type 4; runtime_uaf 2; transfer 28; zero_copy 15;
  doctests 1. No Rust release rerun.
- Python rebuilt using `/tmp/mmapq-review-venv/bin/python -m pip install --no-build-isolation --no-deps -e .`.
  With PYTHONPATH unset, that interpreter's
  `-m unittest discover -s tests -v` passed all 122 tests when run separately.
  An initial concurrent run had one import failure: the synchronization
  gate's README self-test temporarily hides `__init__.py`. No test was
  changed or disabled; the complete isolated rerun passed.
- Focused C fault-test binary built with `-O1 -g -fsanitize=address,undefined
  -fno-omit-frame-pointer`: 13 passed, exit 0, no sanitizer diagnostics.
  `ASAN_OPTIONS=detect_leaks=0`; leak detection was not exercised.
- `check_sync.sh` with the review venv on PATH and CARGO_NET_OFFLINE=true:
  exit 0, including all 11 fail-closed gate checks.
- `check_mirrors.sh`: all eight mirrors match, exit 0.
- `git diff --check` in C and both C mirrors: exit 0.

No performance claim or real concurrent-archive test. No deviations beyond
the requested empty-range fix and the two identified test-coverage gaps.
