# mmapq Feature Matrix — C / Rust / Python

Feature coverage across the three repositories as of the archivable commit
(feature freeze). Legend: ✓ full · ◐ partial (see note) · — not exposed.

**Compatibility scope:** all three implementations target the wire format
of Algoteq's Java implementation — an evolved derivative of
[tools4j/mmap](https://github.com/tools4j/mmap). The formats have
diverged; these libraries are NOT compatible with upstream tools4j.

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
| Append uncommitted + explicit commit | ✓ | ✓ | ✓ |
| Appender with specific ID (single-message + batch) | ✓ | ✓ | ✓ |
| Appender ID / current index / start offset accessors | ✓ | ✓ | ◐ id only |
| Committed / uncommitted payload iterators | ✓ | ◐ via FFI layer | — |
| Zero-length append rejected (`MMAPQ_EINVAL`) at runtime | ✓ | ✓ `Err(InvalidArgument)` | ✓ raises `ValueError` |
| Crash recovery: appender resumes position after restart | ✓ | ✓ (via C) | ✓ (via C) |

## Batch appending

| Feature | C | Rust | Python |
|---|---|---|---|
| Start batch → append N messages → atomic commit (command/event transaction pattern) | ✓ | ✓ `BatchWriter`, commit consumes writer | ✓ context manager, discard on any exit without commit |
| Discard batch (rollback) | ✓ | ✓ (drop) | ✓ (any exit without commit) |
| Single-call append_and_commit | ✓ | ✓ | ✓ |
| Reserved append (write into reserved space) / append_remaining_from | ✓ | ✓ | ✓ |
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
`../mmapq-rs/doc/hardening-migration.md` and `hardening-implementation.md`.
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
| last_index / start_index | ✓ | ✓ | ✓ |
| Batch reader | ✓ | ✓ | ✓ |

## Archiving

| Feature | C | Rust | Python |
|---|---|---|---|
| Archiver create / archivable check | ✓ | ✓ | — |
| archive_up_to_index (advances start_index; returns `MMAPQ_EINVAL` on non-archivable) | ✓ | ✓ | — |
| archivable_files (which files may be deleted) | ✓ | ✓ | — |
| reclaim_files (mark → wait → reclaim; re-list + unlink in one `archive.lock` SH hold) | ✓ `mmapq_indexed_archiver_reclaim_files` + `mmapq_indexed_batch_archiver_reclaim_files`; tests `test_reclaim_*` (P1–P6) and `test_batch_reclaim_*` (B1–B6) | — | — |
| print_summary | ✓ | ◐ single-message only (no batch C function) | — |
| Batch archiver | ✓ | ◐ create/close | — |

## Transfer / export (durable long-term format)

| Feature | C | Rust | Python |
|---|---|---|---|
| Export queue → stream (full + chunked) | ✓ | ✓ | ◐ full only (chunked omitted) |
| Import stream → queue | ✓ | ✓ | ✓ |
| Queue-to-queue copy | ✓ | ✓ | ✓ |
| Batch variants (incl. flat-stream import) | ✓ | ✓ | — |
| Appender ID mapper callback (identity / remap / merge) | ✓ | ✓ | ✓ |
| MmapqConflictBehavior (AUTO fallback / ABORT error) | ✓ | ✓ | ✓ |

## Cross-cutting guarantees (all languages, enforced in the C core)

| Guarantee | Status |
|---|---|
| Wire compatibility with Algoteq's Java implementation (tools4j-derived; NOT compatible with upstream tools4j — formats have diverged) | ✓ by construction, single-message queues; batch queues are C-ecosystem only |
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
| `batch_payload_iterator.h` | 7/12 | 5/12 |
| `batch_queue_transfer.h` | 7/7 | 6/7 |
| `indexed_appender.h` | 11/16 | 6/16 |
| `indexed_archiver.h` | 6/14 | 7/14 |
| `indexed_batch_appender.h` | 15/16 | 8/16 |
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
