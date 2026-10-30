# Guarantees and their evidence

Every guarantee mmapq's documentation makes, with the evidence a sceptical
reader can re-run. Evidence classes:

| Class | Meaning |
|---|---|
| **M** | A TLA+ model check in `doc/model/` (the named check; current verdicts in `doc/model/cfg/EXPECTED`) |
| **P** | A test pin that fails when the claim is false (named test function) |
| **C** | A code inventory: a grep or reading anyone can repeat |
| **A** | A stated assumption, with its scope |

The model checks the **protocol**, not the C code; the step-to-code mapping
table in `doc/model/README.md` is part of every release review. Batches are
not modelled: batch rows rest on pins plus the "same protocol at batch
granularity" argument in `../design/archiving-design.md`.

## Publication and visibility

| Claim | Class | Evidence |
|---|---|---|
| A reader that sees a committed header sees the complete payload | C + A | Write order is length, then data, then the header CAS with release; readers acquire the header (`src/internal/header_codec.h`, `src/indexed_appender.c`). C11 release/acquire argument; the suite runs under TSan. The model abstracts payload framing — this row is an argument, not a model result |
| A header-read mapping timeout is `POLL_ERROR` + `MMAPQ_ETIMEDOUT`, never end-of-stream | P | `test_poll_header_timeout_is_error_not_eos` (test/test_indexed_queue.c); batch twin `test_batch_poll_header_timeout_is_error_not_eos` |
| Each offset is committed at most once; a double commit is refused | P + M | `test_commit_double_commit_rejected`, `test_commit_last_pending_double_rejected`, `test_uncommitted_refill_idempotent_double_commit_rejected` (test_indexed_queue.c); batch `test_batch_commit_double_rejected`, `test_batch_complete_twice_rejected`. Model: `NoDoubleCommit` (single-message) |
| Consumers never observe a partial batch | C + A | One header CAS commits a whole batch (`src/indexed_batch_appender.c`); the 1320-case crash-composition suite (test/test_crash_composition.c) kills writers across the commit path and verifies readers |

## Crash, restart, recovery

| Claim | Class | Evidence |
|---|---|---|
| Opening a queue refuses corruption (`MMAPQ_ECORRUPT`) or a transient mapping timeout (`MMAPQ_ETIMEDOUT`, retryable); recovery never lands inside or past the committed stream | P | `test_appender_open_rejects_corrupt_length`, `test_appender_open_rejects_corrupt_uncommitted_tail`; join timeouts `test_join_walk_to_end_timeout_*`, `test_join_committed_walk_timeout_propagates`, `test_join_tail_walk_timeout_propagates` (test_indexed_queue.c) |
| After a SIGKILL, committed and acknowledged data survives; uncommitted tails stay invisible; appends continue — both queue types | P | `mmapq-py/tests/test_persistence_contract.py` (`test_clean_reopen_and_application_checkpoint`, `test_process_killed_before_publication`, `test_process_killed_after_publication`); the C crash-composition suite |
| After a crash, only the lane's highest-offset uncommitted record can be torn; earlier uncommitted records are byte-intact | P | `test_cc_sm_torn_tail_earlier_intact` (test/test_crash_composition.c). Write-side order: the previous record's data precedes the next record's length store |
| A rejoin at an exact in-file region end does not SIGBUS | P | `test_c51e_rejoin_exact_infile_region_end_sync` / `_async` (test_indexed_queue.c) |
| A joining appender never writes over existing bytes | M + P | Model `NoOverwrite`; `test_appender_rejoin_over_abandoned_payload` (test_indexed_archiver.c); batch `test_batch_appender_rejoin_over_abandoned_batch` |
| A rejoin after a payload file was legitimately reclaimed succeeds and never re-creates the file | P | Holds by construction since CAND-58: every writer walk/scan maps read-only and size-checked through the walk accessor (never `O_CREAT`), and a map miss classifies against the re-read archive line (overtaken → in-library restart) — `test_c58_p1a/p1b/p2/p3/p4/p4b/p5/p6` (test_c58_walk_pins.c, both runtimes, batch twins, fail-firsted against the pre-fix tree). Pre-CAND-58 evidence: `test_appender_rejoin_after_payload_file_deleted` (test_indexed_archiver.c); batch twin |
| Durability is across process restarts, **not** power loss | A | No commit path calls `msync`/`fsync`/`fdatasync` (grep `src/`); the one sync call is `msync` on appender-ID release (`src/appender_id_pool.c`) |

## Archiving and deletion

| Claim | Class | Evidence |
|---|---|---|
| No live entry's bytes are lost to reclaim | M + P | Model `NoLiveLoss` with the commit-intent protocol (`cand54l_intent_core` and guards in `doc/model/cfg/EXPECTED`); the listing caps each lane at `min(start_offset, live floor, intent)`, so a commit stalled or killed anywhere in its window is never listed. Pins: the `test_c35_listing_cap_*` and `test_reclaim_*` families (test_indexed_archiver.c; batch twins). Every completed exhaustive check behind this row is single-lane (NA=1); no completed two-lane exhaustive check exists — the two-lane evidence is TLC simulation plus the incomplete `cand54l_intent_na2_core` run (the coverage statement in `../model/README.md`) |
| An uncommitted payload overtaken by archiving can never become live; committing it returns `MMAPQ_EINVAL` | P | `test_commit_overtaken_payload_rejected` (test_indexed_archiver.c); overtake pins across both queue types |
| Undo never raises the readability floor | M + P | `test_archiver_undo_never_raises_start_offset`; batch twin |
| Every archived header references a complete message, or archiving fails `MMAPQ_ECORRUPT` and changes nothing | P | `test_archiver_rejects_truncated_length_prefix`, `test_archiver_corruption_preserves_all_appender_offsets`; batch `test_batch_archiver_undo_rejects_partial_scan` |
| A live-pass read failure rolls the boundary back; a timeout is never mistaken for end-of-stream | M + P | Model: invariant `NoLiveLoss` under check `cand37_cas_publish` (timeouts enabled, so live-pass failure rounds are exercised); `test_archiver_live_pass_failure_restores_boundary` |
| A racing commit's lowering of the floor survives the archiver's publish (CAS from the snapshot, never a blind store) | M + P | `cand37_cas_publish`; `test_archive_publish_cas_survives_racing_commit_min`; batch archiver uses the same CAS-from-snapshot |
| A listing never observes an archive in progress; one archive operation per queue at a time | C + P + A | `archive.lock` flock EX/SH (`src/indexed_archiver.c`); `test_reclaim_lock_is_shared`. Assumption: the lock is per inode — recreating the queue directory voids it |
| Reclaim re-lists and unlinks under one shared hold, so an undo cannot interleave | M + P | Model runs with the fused delete; `test_reclaim_unlink_excludes_concurrent_undo`, `test_reclaim_relist_excludes_undo_restored_files`; batch twins |
| Header-file reclaim never deletes a live header (the 32 KB block geometry) | P + C | `test_c38_reclaim_aligned_32k_file_loses_nothing` / `_96k_` (both queue types). The model abstracts the transposed header layout — this is below its abstraction |
| The archive line never decreases; a chopped uncommitted message is never yielded again, including after undo or rollback | M + P | `ArchiveLineMonotonic`, `ArchiveLineNeverDecreases`; `test_uncommitted_dg_undo_keeps_line`, `test_uncommitted_dg_race_window`, `test_uncommitted_archived_after_rejoin_yields_nothing` |
| `archivable_files()` lists files no live entry can need at the time of the call; `reclaim_files()` is always safe; hand deletion is safe provided no undo runs between the listing and the last unlink | M + P | The NoLiveLoss rows above; the deletion protocol in `../design/archiving-design.md`; the X5 wording in `../reference/persistence-contract.md` |

**The commit floor.** A successful commit never returns an index below the
`start_index` it observed. The commit path never re-creates a header file
that reclaim has deleted, and it re-checks `start_index` whenever it maps a
new header region, so a stale writer can only ever be rejected — never
committed below the floor. Operator-visible effect: a rare, empty header
file may appear in an `archivable_files()` listing. Skip it when copying
files aside; `reclaim_files()` deletes it from the live directory. Pins:
`test_c59_p1`–`p8` (`test/test_c59_commit_floor_pins.c`, both runtimes;
P5/P6 cover the stray file). Code: `src/indexed_appender.c` /
`src/indexed_batch_appender.c` (`header_wrap_frontier`),
`src/file_mapper_single_rw.c` (`no_create`), `src/queue_region_accessor.c`
(the explicit frontier create).

**Side effect — a reclaimed file's space is freed only when its last
mapping unmaps.** `unlink` removes a reclaimed file's *name*; the inode and
its bytes persist until the final mapping of it is unmapped. A lane holding
a warm mapping of a just-reclaimed file (the P1/P2 case) therefore keeps
that space allocated until the mapping goes — reclaim frees no space while
such a mapping lives. This is why a stray re-created zero file costs
nothing beyond its zero-filled mapping and is removed on the next reclaim.

## Concurrency and platform

| Claim | Class | Evidence |
|---|---|---|
| Naturally aligned lock-free atomics over `MAP_SHARED` are relied on for x86-64 and ARM64 | A + C | Stated assumption (address-free lock-free atomics); **asserted at build**: `_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2)` in `src/internal/header_codec.h` |
| All on-disk and stream fields are little-endian | C | **Asserted at build**: `_Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)` in `src/internal/header_codec.h`; the export stream's bytes are pinned by the golden-block pins (`test_c57_export_golden_bytes_flat`/`_batch`) |
| Cross-process happens-before through `flock` handover | A | Outside C11; stated in `doc/model/README.md` |
| Up to 256 concurrent writers | C | `MAX_APPENDERS = 256` (`src/internal/header_codec.h`); the ID pool is 8 × 32-bit words in a 64-byte file (`src/internal/appender_id_pool.h`) |
| `archivable` is fixed at creation; reopen keeps the on-disk value; transfers preserve it | C + P | `src/queue_meta.c` (no writer after creation); transfer preservation pins across export/import/copy in both cross-type directions (test_queue_transfer.c) |

## What is not claimed

- No power-loss durability (above).
- No multi-machine or network replication in the library.
- Mixed C/Java access to one queue is not supported in 0.2.0; Java-written
  queues are refused at open (`MMAPQ_ESCHEMA`).
- Churn and soak runs are soak evidence; the race-freedom argument is the
  model, not the soak.
