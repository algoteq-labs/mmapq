# mmapq 0.2.0 release notes

0.2.0 is a breaking release. It is written here against v0.1.0 (the
PyPI/crates.io release of 2026-09-11). The Rust and Python bindings ship
short per-repository notes (`RELEASE-NOTES-0.2.0.md` in mmapq-rs and
mmapq-py) with their binding-specific entries and point here for
everything format- and protocol-level (one home per fact; the format
guarantees are the C library's to state).

## Breaking

- **Queue files: schema_version 2.** 0.2.0 writes `schema_version=2` and
  refuses 0.1.0 files (`schema_version=1`) at open with
  `MMAPQ_ESCHEMA`, on both open paths and both queue types. 0.2.0 changed
  what 0.1.0 files mean and accept — the archive-line field, the geometry
  rule, and the unified tail rule. **0.1.0 queues must be recreated or
  migrated: export with a 0.1.0 library and import into a fresh 0.2.0
  queue — the export stream's framing is unchanged; 0.2.0 writes stream
  version 2 and checks the version on import (1 and 2 accepted); a 0.1.0
  importer reads version-2 streams, since it never checked the field.
  The stream carries
  the source queue's archivable flag: the imported 0.2.0 queue keeps
  it.** (The export(0.1.0) → import(0.2.0) round-trip is verified
  byte-exact for both queue types, and verified for an archivable 0.1.0
  queue: the import opens archivable, archives and reclaims.)
- **Removed functions, with replacements:**
  - `mmapq_indexed_queue_start_index` — use the per-participant
    accessors: `mmapq_indexed_poller_start_index`,
    `mmapq_indexed_reader_start_index`, `mmapq_indexed_archiver_start_index`.
  - `mmapq_indexed_queue_region_size`, `mmapq_indexed_queue_max_file_size` —
    no replacement; the creating application supplies these values.
  - `mmapq_indexed_appender_uncommitted_iterator` (NULL-collapsing,
    appender-owned) — replaced by
    `mmapq_indexed_appender_uncommitted_iterator_checked` +
    `mmapq_uncommitted_iterator_next` (checked, three-way).
  - `mmapq_indexed_batch_appender_discard_batch` (offset-based) —
    replaced by `mmapq_mutable_batch_discard` (handle-based).
  - `mmapq_mutable_batch_get` (NULL-collapsing) — replaced by
    `mmapq_mutable_batch_get_checked` (0 or a negative `MMAPQ_*` code;
    outputs cleared on failure). An out-of-range index — including a
    negative one, which 0.1.0 silently clamped to message 0 — returns
    `MMAPQ_EINVAL`.
  - `mmapq_indexed_batch_appender_start_batch` (NULL-collapsing shim) —
    replaced by `mmapq_indexed_batch_appender_start_batch_checked`. (The
    Python `start_batch` keeps its shape and now routes the C failure
    code; the Rust surface already used the checked call.)
  - `mmapq_indexed_archiver_archivable` and
    `mmapq_indexed_batch_archiver_archivable` (boolean-collapsing) —
    replaced by `mmapq_indexed_archiver_archivable_checked` and
    `mmapq_indexed_batch_archiver_archivable_checked` (0 on success with
    the out-parameter set to 1/0; otherwise a negative `MMAPQ_*` code with
    the out-parameter cleared to 0). The checked form returns
    `MMAPQ_ENOENT`, `MMAPQ_ENOTREADY` or `MMAPQ_ETIMEDOUT` while the meta
    is not yet determinable — retryable on the same archiver — and so
    distinguishes that window from a genuine non-archivable answer, which
    the old boolean collapsed into `0`.
  - `mmapq_indexed_appender_iterator` (the all-payloads appender
    iterator, an incomplete factory-only surface since 0.1.0) — removed
    with no replacement: for committed messages use a poller or reader;
    for uncommitted ones, the checked uncommitted iterator.
- **Batch iterator start bounds:** `mmapq_mutable_batch_iterator` and
  `mmapq_batch_iterator_init` refuse a start index outside
  `[0, message_count]` — the iterator's `last_error` is `MMAPQ_EINVAL`
  and the first advance returns false. At 0.1.0 a negative start read
  from message 0 and an over-run start looked like a legal empty
  iteration. A start equal to `message_count` remains a legal empty
  iteration (no message, `last_error` 0).
- **Transfers preserve the archivable flag** (behaviour change): export
  writes the source queue's flag into the stream header (as at 0.1.0),
  and import/copy now create the destination with it — at 0.1.0 every
  transferred destination was created non-archivable. Appending into an
  existing queue keeps the destination's on-disk flag (disk wins on
  every open) and the import proceeds.
- **`mmapq_runtime_close` returns `int`** (was `void`): 0 if the runtime
  thread stopped and was joined; `MMAPQ_ETIMEDOUT` if the close budget
  was exhausted and the thread was detached (leaked) — on that path
  callers must not free registered executables or their ctx;
  `MMAPQ_EINVAL` for NULL only. A sequential repeat close returns the
  first close's result. Closing from the runtime thread itself returns
  `MMAPQ_ETIMEDOUT` (it cannot join itself).
- **Poll behavior:** a header-read mapping timeout is now
  `MMAPQ_POLL_ERROR` with `mmapq_indexed_poller_last_error() ==
  MMAPQ_ETIMEDOUT` (retryable) — never `MMAPQ_POLL_END_OF_STREAM`. At
  0.1.0 a transient timeout looked like end-of-stream. Batch pollers
  likewise.
- **Geometry rule:** creating a queue with a non-conforming geometry
  (region not a power of two, not a multiple of 32 KB, not a page-size
  multiple; `max_file_size` not a multiple of `region_size`) is refused
  with `MMAPQ_EINVAL`; opening an existing queue whose on-disk geometry
  violates the rule is refused with `MMAPQ_ESCHEMA`. A truncated meta
  file is refused with `MMAPQ_ENOTREADY` (incomplete, not corrupt);
  geometrically impossible on-disk values are refused with
  `MMAPQ_ECORRUPT`.
- **Index bound:** indices above 2^56−1 are rejected — `MMAPQ_EINVAL` at
  the creating/binding entry points, cursor moves refuse them, and the
  reader reports `MMAPQ_ENTRY_ERROR` with last_error `MMAPQ_EINVAL`.
  (Previously such indices aliased through the header encoding.)
- **Commit floor:** committing an offset below the archive line fails
  with `MMAPQ_EINVAL` — an uncommitted payload that archiving has
  overtaken can never become live. Append it again instead.
- **`mmapq_runtime_set_affinity`:** fixes a heap-use-after-free in its
  one-shot executable; now returns `MMAPQ_EINVAL` for a NULL runtime or a
  cpu_core outside `[0, CPU_SETSIZE)`, `MMAPQ_ECLOSED` on a closed runtime
  and `MMAPQ_ENOMEM` on allocation failure (previously -1).

## New

- **`reclaim_files()`** (both archivers): re-lists and deletes marked
  files in one `archive.lock` hold — **the recommended deletion path**,
  safe against every in-flight commit state and against undo interleaving.
- **Uncommitted-message iteration** on both queue types (checked,
  caller-owned): recover and commit (or skip) payloads a crashed writer
  left behind; `append_uncommitted` + `commit` support re-sequenced
  commits (UDP reordering, pipelining).
- **Batch lifecycle API:** `start_batch_checked`, `mutable_batch_discard`,
  `mutable_batch_complete_uncommitted`,
  `join_completed_open_batch` (reports a crash-interrupted open batch the
  rejoin completed to staged), checked `mmapq_batch_iterator_next` /
  `mmapq_uncommitted_*_next` iteration steps.
- **The archive line** (`archive_offset`, appender meta byte 16): the
  archival boundary that commit, join and iteration respect.
- **`mmapq-ids`** (`tools/mmapq-ids/`): operator tool to inspect lane
  occupancy and release a crashed writer's appender ID
  (`--unset`, valid only when the holding process is dead).
- **Transfer abort from the appender-ID mapper:** a mapper passed to an
  export, import or copy may return `MMAPQ_APPENDER_ID_ABORT` to stop the
  transfer; the transfer cleans up and returns the new error code
  `MMAPQ_ECALLBACK` (−12). The Rust binding reports it as
  `MmapError::CallbackAborted`; in Python, a mapper that raises stops the
  transfer and the exception propagates to the caller.

## Fixed

Data loss first, then crashes, then robustness. Every entry notes its
0.1.0 basis and the regression test. Defects introduced and fixed within
the 0.2.0 cycle are not listed.

- **Archiving could delete a file a commit still needed (data loss).**
  At 0.1.0 the archivable-file listing read only the stored boundary, and
  a resequenced, in-flight or slow commit racing the listing could publish
  into a file the operator had just deleted. 0.2.0 closes this
  structurally: per-lane commit intents, a listing bound of
  `min(start_offset, live_floor, intent)`, and a commit floor at the
  archive line. There is no deletion wait and no timing assumption. Use
  `reclaim_files()`; hand-deletion is safe provided no undo runs between
  the listing and the last unlink. (Basis: code-read at
  v0.1.0 — the listing in `src/indexed_archiver.c` (v0.1.0: lines
  340-400) read `start_offset` alone; `commit` in `src/indexed_appender.c`
  (v0.1.0: lines 551-561) had no pre-check; the README documented
  immediate hand-deletion. Tests: `test_c54_hole1_single` /
  `test_c54_hole1_batch` and the pin suite in `test/test_cand54_pins.c`.)
- **Archiving vs resequenced/uncommitted payloads (data loss).** The 0.1.0
  archive walk took the highest-index payload position per lane and
  published past it with no live-header re-check, no atomic-min and no
  fence. (Basis: code-read at v0.1.0 — the forward walk in
  `src/indexed_archiver.c` (v0.1.0: lines 172-181, 219); the bare commit
  in `src/indexed_appender.c` (v0.1.0: lines 551-561). Tests:
  `test_commit_racing_archive_lowers_boundary`,
  `test_commit_archive_race_stress` (`test/test_indexed_archiver.c`),
  `test_c51b_batch_commit_batch_recheck_lowers_boundary`
  (`test/test_indexed_batch_archiver.c`).)
- **Undo could interleave between a deletion listing and the unlink**
  (data loss). `reclaim_files` performs re-list + unlink in one lock hold;
  hand-deleters must not undo in between. (Basis: code-read at v0.1.0 —
  no archive lock existed and undo lowered boundaries with no
  coordination, `src/indexed_archiver.c` (v0.1.0: lines 247-312). Test:
  `test_reclaim_unlink_excludes_concurrent_undo`
  (`test/test_indexed_archiver.c`).)
- **Committing an overtaken or archived offset could republish it**
  (double delivery). Commit now validates against the outstanding set and
  the archive line. (Basis: code-read at v0.1.0 — `commit` validated only
  NULL/closed/positive offset, `src/indexed_appender.c` (v0.1.0: lines
  555-557). Tests: `test_commit_overtaken_payload_rejected`
  (`test/test_indexed_archiver.c`), `test_uncommitted_dg_undo_keeps_line`.)
- **Restart at an exact in-file region end crashed (SIGBUS).** The rejoin
  walk now uses the writer's RW accessor, and one tail rule (≤ 4 bytes
  left → skip to the next region) applies to every item on both queue
  types. (Basis: recorded code-read at the v0.1.0 tag; crash reproduced
  on the pre-fix tree. Tests: `test_c51e_rejoin_exact_infile_region_end_sync`
  / `_async` (`test/test_indexed_queue.c`).)
- **Readers and pollers needed write access to the meta file and could
  re-create it (SIGBUS).** Their meta open was read-write with create, so
  a user with read permission only got `MMAPQ_ETIMEDOUT` on every read and
  poll, and a meta removed during the open was re-created empty and the
  reader crashed on the mapping. Readers and pollers (single and batch)
  now open the queue read-only: they need read permission only and never
  create or modify a file. (Basis: code-read at the v0.1.0 tag; both
  failures reproduced on the pre-fix tree. Tests:
  `test_ro_single_reader_poller_readonly_queue` /
  `test_ro_batch_reader_poller_readonly_queue`
  (`test/test_readonly_open.c`), `test_open_ro_meta_vanishes_before_open`
  (`test/test_queue_meta.c`).)
- **Mapped-read fast path could serve a stale mapping or truncate
  capacity** (crash / spurious corruption). A failed wrap now invalidates
  the accessor cache, and mapped capacity is separate from the premap
  trigger. (Basis: reproduced at v0.1.0 — batch poll reported corruption
  deterministically once data crossed the premap trigger; code-read at
  v0.1.0 — `src/region_sync.c` (v0.1.0: lines 117-123),
  `src/internal/region_accessor.h` (v0.1.0: lines 93-98). Tests:
  `test_accessor_absolute_fast_path_bounds`,
  `test_accessor_fast_path_preserves_full_capacity`
  (`test/test_region_ring_accessor.c`),
  `test_concurrent_appenders_small_region_commit{,_sync}`
  (`test/test_indexed_queue.c`).)
- **Unbounded payload walk trusted on-disk lengths** (crash on corrupt
  input). The walk is now bounded against the mapped capacity. (Basis:
  code-read at v0.1.0 — `src/payload_walk.c` (v0.1.0: lines 81-86).
  Tests: `test_payload_walk_rejects_length_beyond_capacity`,
  `test_payload_walk_rejects_int32_min_length`
  (`test/test_payload_walk.c`).)
- **Iterator lifecycle double-free / use-after-free** (crash): the
  appender payload-iterator factory could free the appender-owned
  iterator on out-of-range input, and iterator init could free the
  caller-owned iterator. (Basis: code-read at v0.1.0 —
  `src/indexed_appender.c` (v0.1.0: lines 571-582),
  `src/payload_iterator.c` (v0.1.0: lines 65, 71). Tests:
  `test_appender_iterator_out_of_range_offset_no_double_free`,
  `test_payload_iterator_init_rejects_out_of_range`.) The public
  all-payloads appender-iterator factory is removed in 0.2.0 (see
  Breaking); the internal init fix is kept and both pins now run against
  the internal factory.
- **Batch iterator init dereferenced NULL.** (Basis: code-read at
  v0.1.0 — `src/batch_payload_iterator.c` (v0.1.0: lines 43-56). Test:
  `test_batch_iterator_init_null_returns_null`.)
- **A truncated batch recovery scan was accepted as end-of-stream.**
  (Basis: code-read at v0.1.0 — the rejoin scan in
  `src/indexed_batch_appender.c` (v0.1.0: lines 88-100). Tests:
  `test_batch_recovery_refuses_short_truncated_scan`,
  `test_batch_recovery_refuses_long_truncated_scan`
  (`test/test_indexed_batch_queue.c`).)
- **Runtime/ring close could hang or crash:** the ring close's
  deregistration wait had no deadline, and the runtime close joined a
  possibly wedged thread without a bound. Both are now deadline-bounded,
  and `close` reports the outcome (see Breaking). (Basis: code-read at
  v0.1.0 — `src/region_ring.c` (v0.1.0: lines 213-229),
  `src/runtime.c` (v0.1.0: lines 588-605). Tests:
  `test_region_ring_close_after_runtime_close_returns`,
  `test_async_ring_close_wedged_runtime_returns_after_deadline`,
  `test_runtime_close_with_wedged_executable_returns`,
  `test_runtime_destroy_after_detach_no_uaf`.)
- **A transient mapping timeout was reported as end-of-stream or
  corruption** on poll, the payload walk, the rejoin walk and the
  uncommitted scan — each layer now distinguishes timeout (retryable)
  from corruption. (Basis: code-read at v0.1.0 — `src/header_reader.c`
  (v0.1.0: lines 66-68), `src/indexed_poller.c` (v0.1.0: lines 153-158),
  `src/payload_walk.c` (v0.1.0: lines 32-33, 45-46, 67-68),
  `src/indexed_appender.c` (v0.1.0: lines 287-288). Tests:
  `test_poll_header_timeout_is_error_not_eos`,
  `test_batch_poll_header_timeout_is_error_not_eos`,
  `test_join_walk_to_end_timeout_*` (`test/test_indexed_queue.c`).)
- **Batch transfer and `append_and_commit` could abandon an open batch or
  silently truncate** on mid-operation failure. All paths now complete or
  discard through checked steps. (Basis: code-read at v0.1.0 —
  `src/indexed_batch_appender.c` (v0.1.0: lines 416-419),
  `src/batch_queue_transfer.c` (v0.1.0: lines 633-640). Tests:
  `test_batch_copy_source_corruption_mid_batch_leaves_nothing`,
  `test_append_and_commit_failure_leaves_lane_usable`.)
- **The batch commit lacked the commit protocol** (no overtaken
  pre-check, no fence, no post-CAS re-check/min) — now identical in
  shape to the single-message commit. (Basis: code-read at v0.1.0 —
  `src/indexed_batch_appender.c` (v0.1.0: lines 381-401). Test:
  `test_c51b_batch_commit_batch_recheck_lowers_boundary`.)
- **Export stream headers carried uninitialised reserved bytes**
  (nondeterministic export files). Reserved bytes are zeroed. (Basis:
  code-read at v0.1.0 — `src/internal/export_meta.h` (v0.1.0: lines
  70-79), `src/queue_transfer.c` (v0.1.0: lines 105-107). Tests:
  `test_export_meta_encode_zeroes_reserved`,
  `test_export_stream_header_reserved_bytes_zero`,
  `test_batch_export_stream_header_reserved_bytes_zero`.)
- **Allocation-failure paths leaked or used freed memory** in region/ring
  creation and runtime registration. Ownership is now total and
  documented. (Basis: code-read at v0.1.0 — `src/region_ring.c`
  (v0.1.0: lines 66, 291-305), `src/runtime.c` (v0.1.0: lines 233-239,
  480-481, 757). Tests: `test_register_node_calloc_fail_returns_zero` and
  the alloc-fault suite (`test/test_alloc_faults.c`),
  `test_async_ring_create_closed_runtime_no_leak`.)
- **Rolled-mapper file index overflowed past 2^31 files** (position ≥
  2^31 × `max_file_size`; 2^46 at the minimum 32 KB file size), aliasing
  file 0. Widened to 64-bit. (Basis: code-read at v0.1.0 —
  `src/file_mapper_rolled.c` (v0.1.0: lines 13, 160, 185). Test:
  `test_batch_read_no_alias_at_file_index_2pow32`.)
- **Meta validation gaps:** truncated meta accepted on RW open; no
  geometry validation; on-disk `start_offset` not validated; commit
  accepted non-pending offsets. (Basis: code-read at v0.1.0 —
  `src/queue_meta.c` (v0.1.0: lines 72-88, 120-148),
  `src/appender_meta.c` (v0.1.0: lines 61-89). Tests:
  `test_rw_meta_open_rejects_truncated_meta` (`test/test_queue_meta.c`),
  `test_c32_p1_start_offset_2pow57_rejoin_refused` and siblings.)
- **Length-before-data store order is now pinned** by a compiler fence on
  the append paths, so the crash-tail guarantee ("no payload bytes past
  end-of-stream without their length") does not rest on un-pinned compiler
  behavior. (Basis: latent at v0.1.0 — code-read at
  `src/indexed_appender.c` (v0.1.0: lines 131-135); not a reproduced
  failure — the store order is verified by objdump and exercised by the
  crash-composition suite, `test/test_crash_composition.c`.)
- **Archiver validation hardened:** archive lengths are validated before
  offsets advance, and batch iterator failures propagate before archive
  state changes. (Basis: pre-program hardening of the 0.1.0 archiver —
  code-read at v0.1.0 `src/indexed_archiver.c` (v0.1.0: lines 183-211).
  Tests: `test_archiver_rejects_truncated_length_prefix`,
  `test_archiver_corruption_preserves_all_appender_offsets`.)
- **Batch framing validation and error propagation hardened** across the
  iterator, poller and reader. (Basis: code-read at v0.1.0 — the count
  read in `src/batch_payload_iterator.c` (v0.1.0: lines 15-21) trusted
  the on-disk count. Tests: `test_batch_iterator_rejects_invalid_lengths`,
  `test_batch_reader_rejects_zero_message_count`,
  `test_batch_message_count_propagates_mapping_failure`.)
- **Async poller, reader and appender close no longer leak mappings and
  fds when the mapping timeout is small; closing a singleton runtime no
  longer leaks it.** `async_ring_close` bounded its deregistration wait by
  the queue's mapping timeout — a per-map latency budget that can
  legitimately be 0 — so a close racing an in-flight region map gave up on
  a healthy runtime thread, leaked the mapper state and skipped the
  region/file-mapper teardown (the appender's rings take the same path);
  the wait is now floored at 1 s — generous for a healthy thread to reach
  the deregistration executable, while a wedged runtime's close stays
  bounded at about 1 s per ring. Separately, the singleton registry kept
  its permanent reference to a runtime after close removed the entry,
  stranding the runtime struct and its thread state; the reference is now
  released when the entry is removed.
  (Basis: LeakSanitizer roots found at c2487a5 by enabling the ASan gate;
  test-side iterator leaks found in the same triage are fixed in the
  tests. Tests: `test_poller_close_with_zero_mapping_timeout_takes_normal_path`
  (`test/test_indexed_queue.c`),
  `test_runtime_get_instance_close_destroy_frees_singleton`
  (`test/test_runtime.c`), and the full `test.sh --asan` loop.)

## Performance

- **`mmapq_indexed_appender_append` is back at the 0.1.0 instruction
  count** — 121 Ir per call in the callgrind reference harness (public
  API, synchronous mapping, 4 MB region, 64 B messages, exact release
  flags), where the commit-path frontier wrap added in 0.2.0 had raised it
  to 128. The corrupt flag now rides the header-wrap return value with
  the cold frontier-recovery loop out of line (−4 Ir/call, −4 Ir per
  batch publish), and the max-message-length bound is cached at appender
  create (−3 Ir/call) — the 0.1.0-era validation checks themselves are
  all still executed. No behaviour change: same return values, error
  codes, stderr diagnostics and memory orderings; the frontier-wrap
  tests and their mutation checks still pass.

No further performance delta against 0.1.0 is claimed — no broader 0.1.0
benchmark comparison has been executed. (A v0.1.0 vs 0.2.0 callgrind
comparison on the house recipe beyond the append/poll harnesses is a
release-gate item.)

## Documentation

The documentation is reorganised into a user guide (`doc/guide/`:
concepts, the single-message and batch queues, re-sequenced commits, crash
and restart, archiving and reclamation, export and import, performance,
trade-offs and limits, and event sourcing) and a reference
(`doc/reference/`: API overview, error codes, guarantees, persistence
contract, wire format, feature matrix, glossary). Chapter 6 of the guide
describes how snapshots, archiving and the ARCHIVED outcome combine so that
a consumer's whole state — the snapshot to start from, the changes, and
the signal to rebase — comes from the queue itself.

Paths from 0.1.0 that moved: `doc/feature-matrix.md` →
`doc/reference/feature-matrix.md`, `doc/wire-format.md` →
`doc/reference/wire-format.md`, `doc/event-sourcing-guide.md` →
`doc/guide/event-sourcing.md`, `doc/maintainer_guide.md` →
`doc/contributing/maintainer_guide.md`.

## Bindings

Binding-specific entries live in the per-repository notes:
`RELEASE-NOTES-0.2.0.md` in **mmapq-rs** (Rust) and **mmapq-py**
(Python).

## Known limitations

- ARM64 is not yet in the regular CI. 0.2.0 is tested on Linux x86-64 in
  CI and passed a one-off run on Linux ARM64 of the full suite (both
  sanitizers, the torture test, the Rust and Python suites) and a 76-run
  soak.
- A permanent failure to look up a queue file other than "it does not
  exist" — no permission on a directory, or a path component that is not a
  directory — is reported as `MMAPQ_ETIMEDOUT` (retryable) on every
  attempt, although retrying does not help; so is a base directory that
  does not exist when the first appender is created.
- A payload file deleted under a committed message (outside the
  persistence contract) is reported as `MMAPQ_ETIMEDOUT` by the
  single-message reader and poller and as `MMAPQ_ECORRUPT` by the batch
  reader and poller. Neither recovers on retry: recreate the queue.
- The archive lock file is created with mode 0644, so run all archivers of
  a queue as one user.

Details: [error codes](reference/error-codes.md) and
[trade-offs and limits](guide/09-trade-offs-and-limits.md).

## Java interop

mmapq's file format derives from Algoteq's Java implementation (an
evolved derivative of tools4j/mmap; not compatible with upstream
tools4j). In 0.2.0, C, Rust and Python do not share queues with Java:
0.2.0 refuses Java-written queues at open (schema_version 1), and the
Java implementation does not yet follow the 0.2.0 protocol (for example,
it does not maintain the archive line). This applies to both queue types.
Read Java-written queues with the Java library. Java support is planned
at schema 2, under the same protocol contract (see `reference/wire-format.md`).
