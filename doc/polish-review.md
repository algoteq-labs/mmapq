# Ecosystem polish review — September 2026

## Scope and release position

Review of mmapq, mmapq-rs, and mmapq-py, following approval of the review
plan. Public package names are `mmapq`, version `0.1.0`. Downloaded PyPI
and crates.io source archives match all 14 Python and 18 Rust source files
at the starting repository HEADs. Registry API access to crates.io returned
HTTP 403; the official static crate archive was available.

No package version, on-disk layout, public C symbol, or published artifact
is changed. No commit, branch, tag, or publication is created. The C copies
in both wrapper submodules receive the same upstream changes as working-tree
edits; their recorded submodule commits are not advanced.

This is a tested polish pass, not a proof of correctness. In particular,
the Rust borrowing changes below were explicitly deferred by the user to a
separate release. The current Rust API must not be described as preventing
all misuse in safe code.

## Confirmed findings addressed

| Finding | Correction and regression coverage |
| --- | --- |
| Python quick start never committed its batch and used a single-message reader/exporter on the batch queue | Explicit commit, matching batch APIs, deterministic read assertions, context-managed cleanup, separate-process README execution in `tests/test_readme.py` |
| Persistence examples used volatile storage; C/Rust introductory text implied automatic consumer resumption | File-backed `./data` examples, directory creation, explicit application-owned checkpoints, visibility versus power-loss durability explained |
| `mmapqi_*` looked like a naming mistake | Git commit `33d6f6e` confirms intentional internal namespacing; convention documented rather than renaming implementation symbols |
| Python batch appender had both a finalizer and a destructor freeing the same C allocation | Removed duplicate destructor; `test_batch_appender_finalizes_once` intercepts destruction and verifies exactly one call |
| Python committed/rolled-back writers and overlapping batches could reuse the same C mutable batch | Writer state validation and one active batch per appender; three writer lifecycle regressions |
| Closed Python objects raised incidental cffi errors; explicit closed runtimes could silently select synchronous mapping | Validate handles before FFI calls; queue, appender, consumer, and runtime regressions |
| Invalid poll callback results were swallowed at the cffi boundary; callback reentry/close could invalidate the active C call | Validate directives inside the trampoline; reject reentry and close during a handler; subprocess regressions for both queue types |
| C batch polling omitted `appender_id` from its callback context | Populate the lane ID and reserved bytes; `test_batch_poller_reports_appender_id` checks lane 173 |
| Batch readers accepted invalid counts; count reads used stale buffers after mapping failure | Validate mapping/capacity/count and preserve distinct timeout/corruption codes; two C regressions |
| Batch iteration exposed unchecked payload lengths and continued after mapping failure | Validate framing before exposing buffers; stop and retain the error internally; reader/poller/transfer error propagation; two C iterator regressions plus Python read/poll/export/copy corruption coverage |
| Rust/Python exports ignored buffered write failures at `fclose` | Check output close errors; both queue types tested against `/dev/full` |
| Maintainer documentation described obsolete Java compatibility, callback names, and metadata fields | Correct those statements to the current implementation |

Iterator checks are validation at the boundary where stored data becomes a
caller-visible pointer/length. They add no allocation, lock, syscall, or
formatting. Memory orderings and disk framing are unchanged. C streaming
handlers may have processed earlier messages before a later corrupt frame
is detected; a failed batch is not an application transaction rollback.
Transfers can leave partial destination files/queues on failure, so callers
must discard or inspect unsuccessful outputs.

## Deferred Rust safety release

1. **High: reader aliasing.** `IndexedReader::read`, `read_first`, and
   `read_last` in `src/reader.rs`, and their batch equivalents, accept
   `&self` while mutating reusable contexts and mapped regions. A live message
   does not prevent another read. Transfer functions also take shared reader
   references. Change every operation that can invalidate a view to require
   exclusive borrowing, and add compile-fail coverage for reads and transfers.
   Changing only `read` is insufficient.
2. **High: batch iterator lifetimes.** `BatchMessageIter` implements
   `Iterator<Item = (&'a [u8], u8)>`, allowing retained slices while the next
   call can remap their backing region. `BatchMessage::iter(&self)` also
   permits multiple iterators sharing the same mutable C iterator. Choose a
   lending interface, scoped callback, or owned-item iterator; validate with
   batches spanning more regions than the mapping ring. This requires an
   explicit source-compatibility and performance decision.
3. **Error representation.** The current Rust batch `Iterator` cannot return
   an error distinctly from exhaustion. C now records iteration errors and
   pollers surface them, but standalone Rust iteration still ends with `None`.
   Include a fallible iteration contract in the same API revision.
4. **Raw construction.** Review visibility and safety requirements of
   `BatchMessageIter::new` and raw iterator access together with that redesign.

Until then, copy each single-message view before another operation on its
reader, copy each batch payload before advancing, and avoid concurrent batch
iterators. Documentation now states these limitations rather than claiming
the compiler already enforces the future design.

## Other follow-up findings

- `Runtime::region_mapper` and `metrics_processor` in Rust clone an already
  owned C singleton reference and lose the original handle. The C registry
  intentionally retains one permanent reference, so ordinary lifetime tests
  do not reveal this refcount leak. Add a testable ownership audit before
  changing the wrapper's singleton acquisition logic.
- Rust's transfer mapper callback does not catch panics before crossing
  `extern "C"`; Python deliberately falls back to auto-assigned IDs when its
  mapper raises. Define the desired cross-language failure policy separately.
  The existing Python mapper-exception test emits one expected unraisable-
  exception warning; it has not been suppressed.
- Several C transfer paths still ignore destination append/commit return
  values. This pass propagates source batch read/iteration errors and output
  close failures, but does not establish comprehensive fault-injection
  coverage for destination mapping, allocation, or disk exhaustion.
- Python `read_view` exposes a borrowed memoryview that must not outlive its
  mapping. Its documented lifetime is a caller obligation, not an enforced
  Python ownership guarantee.
- ARM64 and crash/power-loss behavior were not validated on this host.
  No nanosecond-level performance claim is inferred from unit tests.

## File manifest

Upstream C:

- `README.md`, `doc/maintainer_guide.md`, `doc/feature-matrix.md`, this report.
- `include/batch_payload_iterator.h`, `include/indexed_batch_reader.h`.
- `src/batch_payload_iterator.c`, `src/internal/batch_payload_iterator_internal.h`.
- `src/indexed_batch_appender.c`, `src/indexed_batch_poller.c`, `src/indexed_batch_reader.c`.
- `src/batch_queue_transfer.c`, `src/queue_transfer.c`.
- `test/test_indexed_batch_queue.c`, `scripts/test.sh` (retain per-binary counts).

All upstream files above are mirrored verbatim under both wrappers' `c/`.

Rust wrapper:

- `README.md`, `src/lib.rs`, `src/transfer/mod.rs`, `tests/export_errors.rs`.

Python wrapper:

- `README.md`, `src/mmapq/_cdata.py`, `src/mmapq/archiver.py`.
- `src/mmapq/batch_appender.py`, `src/mmapq/batch_poller.py`, `src/mmapq/batch_queue.py`.
- `src/mmapq/single_appender.py`, `src/mmapq/single_poller.py`, `src/mmapq/single_queue.py`.
- `src/mmapq/reader.py`, `src/mmapq/transfer.py`.
- `tests/test_safety.py`, `tests/test_readme.py`.

## Verification record

Initial baseline: C 19 binaries passed in debug and release; Rust 33 library
tests, archivable 2, cross_process 2, integration 6, poll_overhead 1,
queue_type 4, runtime_uaf 2, transfer 28 passed; all four examples compiled.
Python: 101 tests passed with one mapper-exception warning. Initial eight
source/header/test/script mirror comparisons passed.

Behavioral regressions were observed failing before their fixes. The new
zero-count fixture initially wrote offset zero instead of the actual batch
offset; it was corrected and rerun against the original reader source,
which still failed. Existing tests were not weakened.

## Final verification results

- `bash scripts/test.sh`: exit 0; all 19 binaries pass in debug and
  release, with the per-binary counts below. The batch binary grows from
  20 to 25 tests.
- `cargo test --all-targets`: exit 0; library 33, archivable 2,
  cross_process 2, export_errors 2, integration 6, poll_overhead 1,
  queue_type 4, runtime_uaf 2, transfer 28; four example targets compile.
- `cargo test --doc`: exit 0; 1 doctest.
- Python rebuilt from current C sources with
  `python -m pip install --no-build-isolation --no-deps -e .`, then
  `python -m pytest -v`: exit 0; **114 passed**, up from 101, with
  43 passing subtests and the same pre-existing mapper warning.
  Commands used the isolated `/tmp/mmapq-review-venv`, cleared the host's
  ROS `PYTHONPATH`, and set `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`.
- `CARGO_NET_OFFLINE=true bash scripts/check_sync.sh`: exit 0;
  README checks, symbol/enum/error-code checks, mirror checks, and all
  11 fail-closed gate checks pass. The initial online attempt failed to
  resolve crates.io; the offline retry used already cached dependencies.
- `cargo package --allow-dirty --offline`: exit 0, including package build
  verification. `cargo test --offline --manifest-path
  target/package/mmapq-0.1.0/Cargo.toml --all-targets` also passes.
- `python -m build --no-isolation --outdir /tmp/mmapq-review-dist`:
  exit 0; sdist and Linux CPython 3.10 wheel built. Installing that wheel
  into `/tmp/mmapq-wheel-test` and running the suite against it also gives
  114 passed, 43 subtests passed, one existing warning.
- The changed C batch binary, compiled with
  `-fsanitize=address,undefined`, passes all 25 tests with
  `ASAN_OPTIONS=detect_leaks=0`. LeakSanitizer with leak detection enabled
  fails because the environment uses ptrace; **leak checking is unverified**.
  No atomic/concurrency changes were made; TSan and latency benchmarks
  were not run.
- `bash scripts/check_mirrors.sh`: exit 0, all eight comparisons match.
  `git diff --check` passes in all three repositories.

| C test binary | Debug passed | Release passed |
| --- | ---: | ---: |
| test_file_initialiser | 5 | 5 |
| test_file_mapper_single_ro | 7 | 7 |
| test_file_mapper_single_rw | 6 | 6 |
| test_file_mapper_rolled | 5 | 5 |
| test_region_sync | 10 | 10 |
| test_region_async | 8 | 8 |
| test_region_ring | 1 | 1 |
| test_region_ring_accessor | 9 | 9 |
| test_fixed_size_region | 1 | 1 |
| test_runtime | 13 | 13 |
| test_header_codec | 22 | 22 |
| test_appender_id_pool | 8 | 8 |
| test_queue_meta | 9 | 9 |
| test_payload_walk | 5 | 5 |
| test_indexed_queue | 39 | 39 |
| test_queue_transfer | 59 | 59 |
| test_indexed_archiver | 11 | 11 |
| test_indexed_batch_queue | 25 | 25 |
| test_indexed_batch_archiver | 9 | 9 |

New C regressions, all observed failing before correction and passing after:

- `test_batch_poller_reports_appender_id`
- `test_batch_reader_rejects_zero_message_count`
- `test_batch_message_count_propagates_mapping_failure`
- `test_batch_iterator_rejects_invalid_lengths`
- `test_batch_iterator_stops_on_mapping_failure`

New Rust regressions in `tests/export_errors.rs`, both failed with `Ok(1)`
before correction and now pass:

- `export_reports_buffered_write_failure`
- `batch_export_reports_buffered_write_failure`

Python runner evidence (progress percentages omitted):

```text
tests/test_readme.py::TestReadme::test_quick_start_commit_and_reopen_in_separate_processes PASSED
tests/test_safety.py::TestSafety::test_batch_appender_finalizes_once PASSED
tests/test_safety.py::TestSafety::test_batch_appender_rejects_overlapping_writers PASSED
tests/test_safety.py::TestSafety::test_batch_writer_rejects_append_after_commit PASSED
tests/test_safety.py::TestSafety::test_batch_writer_rejects_use_after_rollback PASSED
tests/test_safety.py::TestSafety::test_closed_appender_raises_queue_closed PASSED
tests/test_safety.py::TestSafety::test_closed_consumers_raise_queue_closed PASSED
tests/test_safety.py::TestSafety::test_closed_queue_factories_raise_queue_closed PASSED
tests/test_safety.py::TestSafety::test_corrupt_batch_message_is_not_delivered_or_exported PASSED
tests/test_safety.py::TestSafety::test_export_reports_buffered_write_failure PASSED
tests/test_safety.py::TestSafety::test_factories_reject_closed_runtime PASSED
tests/test_safety.py::TestSafety::test_invalid_handler_result_raises_without_advancing PASSED
tests/test_safety.py::TestSafety::test_poller_rejects_close_and_reentry_from_handler PASSED
```

No existing test was removed or weakened. The only approved scope deferral
is source-breaking Rust safety corrections; the additional follow-up
findings above remain open and are not represented as fixed. The broad
review found these beyond the initial naming/documentation concerns.
