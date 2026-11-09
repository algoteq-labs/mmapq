# mmapq Feature Matrix — C / Rust / Python

Feature coverage across the three repositories as of the archivable commit
(feature freeze). Legend: ✓ full · ◐ partial (see note) · — not exposed.

**Compatibility scope:** all three implementations' file format derives
from Algoteq's Java implementation — an evolved derivative of
[tools4j/mmap](https://github.com/tools4j/mmap). The formats have
diverged; these libraries are NOT compatible with upstream tools4j.
Sharing queues with Java is not supported in 0.2.0: Java-written queues
are refused at open (schema_version 1); planned at schema 2, under the
same protocol contract (see `wire-format.md`).

The C library is the complete surface and the source of truth. Rust exposes
near-complete parity. **Python deliberately exposes the producer/consumer
subset plus random-access reading and queue transfer** — append and poll for
both queue types, random-access readers for replay and analysis, and
export/import/copy for durable long-term storage. Archiving and low-level
batch construction remain C/Rust territory. Gaps marked — in the Python
column are design decisions, not roadmap items.

## Queue lifecycle

| Feature | C | Rust | Python |
|---|---|---|---|
| Version macro (`MMAPQ_VERSION_MAJOR`/`_MINOR`/`_PATCH`, `mmapq_version()`) | ✓ | — (via C if needed) | — |
| Error-string lookup (`mmapq_strerror`) | ✓ `src/mmapq_strerror.c`; `scripts/check_error_codes.sh` gate | — (Rust `Display` covers this) | — (exception messages cover this) |
| Create/open single-message queue (full config) | ✓ | ✓ `QueueConfig` builder | ◐ dir/name + `archivable` kwarg; other params defaulted |
| Create/open batch queue | ✓ | ✓ | ✓ `batch_queue.py:MmapqIndexedBatchQueue.create()` with `region_size`, `ring_size`, `max_file_size`, `start_index`, `timeout_ns`, `archivable` |
| `archivable` creation parameter (default 0) | ✓ | ✓ `.archivable(bool)` | ✓ `archivable=False` kwarg |
| Async region mapping via MmapqRuntime (background mapper) | ✓ | ✓ `MmapqRuntime` + singletons | ✓ `MmapqRuntime.region_mapper()` in `runtime.py`; `test_runtime_kept_alive_by_queue` in `test_queue.py` |
| Custom runtime config (idle strategies, close timeout) | ✓ | ✓ | — |
| Factory metadata accessors (region size, max file size, start index) | — removed | — removed | — |
| queue_type discriminator validated on every open (normal/batch/fixed-8) | ✓ | ✓ (via C) | ✓ (via C) |
| schema_version validated on every open | ✓ | ✓ (via C) | ✓ (via C) |

Queue factories retain immutable creation preferences and runtime references;
participants own metadata mappings. The normal queue's three metadata getters
were removed to make shared factory calls independent of lazy metadata state.
Use reader/poller start-index accessors for live state. Creation preferences
must not be interpreted as an existing queue's authoritative disk geometry.

## Appending (single-message)

| Feature | C | Rust | Python |
|---|---|---|---|
| Atomic append-and-commit | ✓ | ✓ | ✓ |
| Append uncommitted + explicit commit | ✓ (CAND-54: on archivable queues the overtaken pre-check floors at the archive line — EINVAL below it; tests `test_c54_l1_semantics_*` in `test/test_cand54_pins.c`) | ✓ | ✓ |
| Appender with specific ID (single-message + batch) | ✓ | ✓ | ✓ |
| Appender ID / current index / start offset accessors | ✓ | ✓ | ◐ id only |
| Uncommitted payload iterator (recovery walk, archive line → end) | ✓ `mmapq_indexed_appender_uncommitted_iterator_checked` + `mmapq_uncommitted_iterator_next` in `src/indexed_appender.c`; test `test_uncommitted_iterator_finds_uncommitted` in `test/test_indexed_queue.c` | ✓ `uncommitted_iter` in `../mmapq-rs/src/appender.rs` + `UncommittedIter::next` in `../mmapq-rs/src/uncommitted.rs`; test `uncommitted_iter_finds_uncommitted` in `../mmapq-rs/tests/recovery.rs` | ✓ `uncommitted` in `../mmapq-py/src/mmapq/single_appender.py`; test `test_uncommitted_finds_uncommitted` in `../mmapq-py/tests/test_recovery.py` |
| Zero-length append rejected (`MMAPQ_EINVAL`) at runtime | ✓ | ✓ `Err(InvalidArgument)` | ✓ raises `ValueError` |
| Crash recovery: appender resumes position after restart | ✓ | ✓ (via C) | ✓ (via C) |

## Batch appending

| Feature | C | Rust | Python |
|---|---|---|---|
| Start batch → append N messages → publish (`complete` is the one-call publish: stores the count and commits the header) | ✓ `mmapq_mutable_batch_complete` in `src/indexed_batch_appender.c`; test `test_batch_de_mode2_empty_open_batch` in `test/test_indexed_batch_queue.c` | ✓ `BatchWriter::commit` consumes writer in `../mmapq-rs/src/batch/batch_appender.rs`; test `rejoin_completes_open_batch` in `../mmapq-rs/tests/recovery.rs` | ✓ `BatchWriter.commit` in `../mmapq-py/src/mmapq/batch_appender.py`; test `test_rejoin_completes_open_batch` in `../mmapq-py/tests/test_recovery.py` |
| Stage without publish → later commit by offset (`complete_uncommitted` + `commit_batch`; the staged/recovered commit path) | ✓ `mmapq_mutable_batch_complete_uncommitted` + `mmapq_indexed_batch_appender_commit_batch` in `src/indexed_batch_appender.c`; test `test_batch_uncommitted_commit_after_rejoin` in `test/test_indexed_batch_queue.c` | ✓ `BatchWriter::complete_uncommitted` + `IndexedBatchAppender::commit_batch` in `../mmapq-rs/src/batch/batch_appender.rs`; test `batch_complete_uncommitted_then_commit_batch` in `../mmapq-rs/tests/recovery.rs` | ✓ `BatchWriter.complete_uncommitted` + `IndexedBatchAppender.commit_batch` in `../mmapq-py/src/mmapq/batch_appender.py`; test `test_complete_uncommitted_then_commit_batch` in `../mmapq-py/tests/test_recovery.py` |
| Discard batch (rollback; discard-only on guard exit — never complete, which publishes now) | ✓ `mmapq_mutable_batch_discard` in `src/indexed_batch_appender.c`; test `test_batch_de_mode1_discard_overlay` in `test/test_indexed_batch_queue.c` | ✓ `BatchWriter::drop` in `../mmapq-rs/src/batch/batch_appender.rs`; test `discarded_batch_reappears_in_recovery` in `../mmapq-rs/tests/recovery.rs` | ✓ `BatchWriter.__exit__` in `../mmapq-py/src/mmapq/batch_appender.py`; test `test_discarded_batch_reappears_in_recovery` in `../mmapq-py/tests/test_recovery.py` |
| Uncommitted batch iterator (recovery walk, archive line → frozen end; discarded batches reappear after a restart) | ✓ `mmapq_indexed_batch_appender_uncommitted_batches_checked` + `mmapq_uncommitted_batch_next` in `src/indexed_batch_appender.c`; test `test_batch_uncommitted_crash_recovery_end_to_end` in `test/test_indexed_batch_queue.c` | ✓ `uncommitted_batches` in `../mmapq-rs/src/batch/batch_appender.rs` + `UncommittedBatches::next` in `../mmapq-rs/src/batch/uncommitted.rs`; test `discarded_batch_reappears_in_recovery` in `../mmapq-rs/tests/recovery.rs` | ✓ `uncommitted_batches` in `../mmapq-py/src/mmapq/batch_appender.py`; test `test_discarded_batch_reappears_in_recovery` in `../mmapq-py/tests/test_recovery.py` |
| Join completion of a trailing open batch (reported once, read-and-clear) | ✓ `mmapq_indexed_batch_appender_join_completed_open_batch` in `src/indexed_batch_appender.c`; test `test_batch_de_mode2_rejoin_completes_open_batch` in `test/test_indexed_batch_queue.c` | ✓ `join_completed_open_batch` in `../mmapq-rs/src/batch/batch_appender.rs`; test `rejoin_completes_open_batch` in `../mmapq-rs/tests/recovery.rs` | ✓ `join_completed_open_batch` in `../mmapq-py/src/mmapq/batch_appender.py`; test `test_rejoin_completes_open_batch` in `../mmapq-py/tests/test_recovery.py` |
| Single-call append_and_commit | ✓ | ✓ | ✓ |
| Reserved append (write into reserved space) | ✓ `mmapq_mutable_batch_append_reserved` | ✓ `BatchWriter::append_reserved` | — (not bound) |
| append_remaining_from (copy the rest of a source batch into an open batch) | ✓ `mmapq_mutable_batch_append_remaining_from` | ✓ `BatchWriter::append_remaining_from_iter` with the `BatchMessageIter` a batch poller handler receives | ◐ `BatchWriter.append_remaining_from` exists, but no public call yields its source iterator (reachable only through `unsafe_lib()`); copy a received batch with a per-message `append()` loop |
| In-batch message iterator / message count | ✓ | ✓ | ✓ (handler-side iteration) |

## Consuming — pollers (sequential)

| Feature | C | Rust | Python |
|---|---|---|---|
| Poll with handler; ADVANCE / STOP / RETREAT flow control | ✓ | ✓ (panic-safe trampoline) | ✓ |
| move_to_index / start / end / last | ✓ | ✓ | ✓ |
| exists(index), current index accessors | ✓ | ✓ | ✓ |
| Batch poller (same controls, batch granularity) | ✓ | ✓ | ✓ |

## Consuming — readers (random access)

Rust readers now require exclusive borrows. Batch cursors lend each payload
until the next step; process it in place before advancing, without copying.
The upcoming release intentionally breaks source compatibility. See
`../mmapq-rs/RELEASE-NOTES-0.2.0.md`.
Python `IndexedReader.read_view` now enforces borrowing: read-only views retain
the reader, and reads/index queries/close raise `BufferError` until all exports
and aliases release. Implementation: `../mmapq-py/src/mmapq/reader.py`
(`IndexedReader.read_view`) and `../mmapq-py/src/mmapq/_borrowed_buffer.c`.
Regression: `../mmapq-py/tests/test_reader_borrowing.py`
(`test_close_rejects_view_and_surviving_alias`).

| Feature | C | Rust | Python |
|---|---|---|---|
| Read at arbitrary index; read_first / read_last; appender_id per entry | ✓ | ✓ `../mmapq-rs/src/reader.rs` (`read`); `../mmapq-rs/tests/borrowing.rs` (`borrowing_contracts`) | ✓ |
| Entry status (present / not yet / archived) | ✓ | ✓ | ✓ |
| Read reports archived distinctly from not yet written | ✓ `MMAPQ_ENTRY_ARCHIVED` / `MMAPQ_BATCH_ENTRY_ARCHIVED` in the reading context | ✓ `ReadResult::{Available, EndOfStream, Archived}` in `../mmapq-rs/src/reader.rs`; tests `test_read_archived_below_moved_start_index`, `test_batch_read_archived_below_moved_start_index` | ✓ `ArchivedError` (`.index`; `None` = not yet written) in `../mmapq-py/src/mmapq/reader.py`; tests `test_reader_archived_raises_archived_error`, `test_batch_reader_archived_raises_archived_error` |
| last_index / start_index | ✓ | ✓ | ✓ |
| Batch reader | ✓ | ✓ | ✓ |

## Archiving

| Feature | C | Rust | Python |
|---|---|---|---|
| Archiver create / archivable check | ✓ `mmapq_indexed_archiver_archivable_checked` + `mmapq_indexed_batch_archiver_archivable_checked` in `src/indexed_archiver.c`, `src/indexed_batch_archiver.c`; tests `test_c60_archivable_checked_missing_meta_is_enoent`, `test_c60_archivable_checked_transient_retry_same_handle`, `test_c60_archivable_checked_short_meta_is_enotready`, `test_c60_archivable_checked_sticky_is_stable` + batch twins | ◐ create only (`IndexedArchiver`; no archivable check) | ✓ `IndexedArchiver.archivable()` / `IndexedBatchArchiver.archivable()` in `../mmapq-py/src/mmapq/archiver.py`; tests `test_archiver_on_archivable_queue`, `test_archiver_rejects_non_archivable` (`../mmapq-py/tests/test_queue.py`) |
| archive_up_to_index (advances start_index; returns `MMAPQ_EINVAL` on non-archivable) | ✓ `mmapq_indexed_archiver_archive_up_to_index` + `mmapq_indexed_batch_archiver_archive_up_to_index`; tests `test_archiver_archive_forward_updates_start_index`, `test_archive_up_to_index_rejects_non_archivable` (`test/test_indexed_archiver.c`) | ✓ | ✓ `IndexedArchiver.archive_up_to_index` / `IndexedBatchArchiver.archive_up_to_index` in `../mmapq-py/src/mmapq/archiver.py`; tests `test_queue_scoped_archiver_works`, `test_queue_scoped_batch_archiver_works` (`../mmapq-py/tests/test_queue.py`) |
| archivable_files (which files may be deleted; per-lane bound min(start_offset, lowest live payload, commit intent); empty on non-archivable queues — CAND-54 §6(c)) | ✓ `mmapq_indexed_archiver_archivable_files` + batch twin; tests `test_c35_*`, `test_c54_refusal_*` | ✓ | ✓ `IndexedArchiver.archivable_files` / `IndexedBatchArchiver.archivable_files` in `../mmapq-py/src/mmapq/archiver.py`; tests `test_archiver_on_archivable_queue` (`../mmapq-py/tests/test_queue.py`), `../mmapq-py/tests/test_recovery.py` |
| reclaim_files (mark → reclaim — the wait is retired (CAND-54); re-list + unlink in one `archive.lock` SH hold; re-list inherits the live-payload + intent bound; no-op on non-archivable queues — §6(c)) | ✓ `mmapq_indexed_archiver_reclaim_files` + `mmapq_indexed_batch_archiver_reclaim_files`; tests `test_reclaim_*` (P1–P6), `test_batch_reclaim_*` (B1–B6), `test_c54_refusal_*` | — | — |
| print_summary | ✓ | ◐ single-message only (no batch C function) | — |
| Batch archiver (archive/undo via `archive_up_to_index`; `archivable_files`; no `reclaim_files` binding in either wrapper) | ✓ `mmapq_indexed_batch_archiver_archive_up_to_index` + `mmapq_indexed_batch_archiver_archivable_files` in `src/indexed_batch_archiver.c`; tests `test_batch_archiver_*`, `test_batch_reclaim_*`, `test_c35_batch_*` in `test/test_indexed_batch_archiver.c` | ✓ `IndexedBatchArchiver::archive_up_to_index` + `archivable_files` in `../mmapq-rs/src/batch/batch_archiver.rs`; test `batch_archiver_forward_and_undo` in `../mmapq-rs/tests/recovery.rs` | ✓ `IndexedBatchArchiver.archive_up_to_index` + `archivable_files` in `../mmapq-py/src/mmapq/archiver.py`; tests `test_queue_scoped_batch_archiver_works` in `../mmapq-py/tests/test_queue.py`, `TestBatchC51bPins.test_batch_archiver_forward_and_undo` in `../mmapq-py/tests/test_recovery.py` |

## Transfer / export (durable long-term format)

| Feature | C | Rust | Python |
|---|---|---|---|
| Export queue → stream (full + chunked) | ✓ | ◐ full only (chunked omitted) | ◐ full only (chunked omitted) |
| Import stream → queue (archivable flag preserved on a created destination; an existing destination keeps its on-disk flag) | ✓ pins `test_transfer_import_archivable_{rebase,preserve}`, `test_transfer_import_append_flag_mismatch` + batch twins in `test/test_queue_transfer.c` | ✓ (via C) | ✓ (via C) |
| Queue-to-queue copy (archivable flag preserved, incl. cross-type) | ✓ pins `test_transfer_copy_preserves_archivable`, `test_transfer_copy_batch_to_normal_preserves_archivable` + batch twins in `test/test_queue_transfer.c` | ✓ (via C) | ✓ (via C) |
| Batch variants (incl. flat-stream import) | ✓ | ✓ | ✓ `export_batch_queue` / `import_batch_queue` / `import_batch_stream` / `import_flat_to_batch` / `copy_batch_queue` / `copy_normal_to_batch` / `copy_batch_to_normal` in `../mmapq-py/src/mmapq/transfer.py`; tests `test_batch_import_preserves_appender_ids`, `test_import_batch_stream_preserve_nonzero`, `test_import_flat_to_batch_preserve_nonzero`, `test_batch_copy_preserve_to_new`, `test_copy_normal_to_batch`, `test_copy_batch_to_normal` (`../mmapq-py/tests/test_queue.py`) |
| Appender ID mapper callback (identity / remap / merge) | ✓ | ✓ | ✓ |
| MmapqConflictBehavior (AUTO fallback / ABORT error) | ✓ | ✓ | ✓ |

## Cross-cutting guarantees (all languages, enforced in the C core)

| Guarantee | Status |
|---|---|
| Java queue sharing | Not supported in 0.2.0: Java-written queues are refused at open (schema_version 1), both queue types — pinned by `test_doc01_schema1_refused_{ro,rw}{,_batch}` (`test_queue_meta.c`); planned at schema 2, under the same protocol contract |
| Up to 256 concurrent appenders, lock-free atomic per-message commit | ✓ |
| Distinct error codes: `ETIMEDOUT/EINVAL/ECLOSED/ECORRUPT/ESCHEMA/EEXIST/ENOENT/EIDCONFLICT/ENOMEM/EIO/ENOTREADY/ECALLBACK` mapped 1:1 — Rust `MmapError` variants, Python exception classes | ✓ |
| Factory functions return `int` (0 success, negative `MMAPQ_*` error code) with `**out` parameter instead of raw pointer | ✓ |
| Corruption refused on open (`ECORRUPT`), never repaired; recreate from export | ✓ |
| Validation survives release builds (`-DNDEBUG`); regression tests run under exact release flags | ✓ |
| Multi-process safety: cross-process interop tests (Rust ↔ C echo servers) | ✓ |
| Fixed-8-byte mode (Algoteq Java only): refused with diagnostic (not implemented) | ✓ documented |

## Hardening APIs

| Contract | Implementation | Executed regression |
|---|---|---|
| Checked batch step, success/exhaustion/error | `src/batch_payload_iterator.c` (`mmapq_batch_iterator_next`); `../mmapq-rs/src/batch/batch_iter.rs` (`next`) | `test/test_indexed_batch_queue.c` (`test_checked_batch_step`); `../mmapq-rs/tests/zero_copy.rs` |
| Checked batch start preserves errors | `src/indexed_batch_appender.c` (`mmapq_indexed_batch_appender_start_batch_checked`) | `test/test_transfer_failures.c` (`test_copy_failure_matrix`, `test_import_failure_matrix`) |
| Checked in-batch message get (out-of-range refused, walk timeout/corruption distinguished; not bound in either wrapper — ruled 2026-10-25) | `src/indexed_batch_appender.c` (`mmapq_mutable_batch_get_checked`) | `test/test_indexed_batch_queue.c` (`test_mutable_batch_get_checked_in_range_exact`, `test_mutable_batch_get_checked_out_of_range_refused`, `test_mutable_batch_get_checked_timeout_propagates`) |
| Distinct mapper abort, `MMAPQ_ECALLBACK` | `include/queue_transfer.h`, `src/internal/appender_cache.h` | `../mmapq-rs/tests/mapper_panic.rs`; `../mmapq-py/tests/test_transfer_hardening.py` |

## Operator tooling (C repo)

| Tool | Purpose | Status |
|---|---|---|
| `mmapq-ids` | Appender ID pool inspection; CAS release of orphaned IDs | available (single-message + batch dirs) |
| `mmapq-stat` | Read-only queue inspection; `--verify` framing/header cross-check | available; batch framing verify not yet supported |

---

<!-- GENERATED by api_surface.sh --presence-matrix; do not edit manually -->

## Auto-generated binding coverage

| C header | Rust (ffi.rs) | Python (cdef) |
|---|---|---|
| `batch_payload_iterator.h` | 7/12 | 6/12 |
| `batch_queue_transfer.h` | 7/7 | 6/7 |
| `indexed_appender.h` | 11/17 | 8/17 |
| `indexed_archiver.h` | 6/14 | 7/14 |
| `indexed_batch_appender.h` | 17/22 | 12/22 |
| `indexed_batch_archiver.h` | 4/11 | 6/11 |
| `indexed_batch_poller.h` | 14/20 | 14/20 |
| `indexed_batch_queue.h` | 12/12 | 12/12 |
| `indexed_batch_reader.h` | 10/14 | 10/14 |
| `indexed_poller.h` | 11/18 | 11/18 |
| `indexed_queue.h` | 13/17 | 13/17 |
| `indexed_reader.h` | 9/13 | 9/13 |
| `mmap_platform.h` | 1/15 | 1/15 |
| `mmapq.h` | 0/1 | 0/1 |
| `poller.h` | 0/1 | 0/1 |
| `queue_transfer.h` | 6/6 | 5/6 |
| `runtime.h` | 15/27 | 3/27 |

<!-- /GENERATED -->
