# Protocol model: archive, commit, join, delete

`MmapqArchive.tla` is a TLA+ model of the protocol described in
`doc/archiving-design.md`: resequenced (out-of-order) commits, forward
archiving with its live-header pass and rollback, undo, appender join, and
the caller-side deletion protocol. TLC checks it exhaustively at small
bounds. It found three data-loss interleavings in the protocol as landed
at `mmapq 20a37d1` (CAND-35, CAND-36, CAND-37), confirmed that both seq_cst
fences are necessary, and discharged the CAND-31 proof obligation.

The model checks the **protocol**, not the C code. The mapping from model
steps to code is in the table below; keeping that mapping true is a review
obligation for every change to the archiver, the commit path, join, or
`archivable_files`.

## Running it

```
./check.sh            # all checks except the ~70 min one   (~25 min, 2 cores)
./check.sh --quick    # counterexample checks only            (~1 min)
./check.sh --all      # everything                            (~1.5 h)
./check.sh fixed      # one named check
```

Needs Java 11+. `check.sh` downloads `tla2tools.jar` 1.7.4 on first use and
verifies its SHA-256. Each check prints `ok` when TLC's verdict matches the
expected verdict in `cfg/EXPECTED`, and the script exits non-zero on any
mismatch. TLC logs (with full counterexample traces) land in `out/`.

**Expected-FAIL checks are deliberate.** `current` and the `cand3x_*`
checks reproduce open defects. When a fix lands, its check's expectation
flips to PASS in the same commit: CAS publish made `current_no_undo` pass
(CAND-37); adding the reclaim call made `current` pass (CAND-36). The
historical `cand36_undo_race` check keeps failing as the record of the
defect. The listing cap left `cand35_crash` failing as the record of the
defect (CAND-35), and `cand35_narrow_wait` records that the wait may not
be narrowed to the pre-CAS window.

## What is modelled

| Actor | Model actions | Code |
|---|---|---|
| Appender join | `Join` (search own entry in live range, walk to end; "no file = end of stream") | join path in `indexed_appender.c` (`appender_start_of_stream`, `walk_payload_to_end`) |
| Append | `Append` = write, then `FastCommit` (header CAS) | `mmapq_indexed_appender_append` → `write_payload`, `commit_header` |
| Uncommitted append | `AppendUncommitted` | `mmapq_indexed_appender_append_uncommitted` |
| Commit | `Commit` picks fast path iff offset = last appended (`FastCommit`), else slow path: `SlowPreCheck` (load start_offset, EINVAL if below), `SlowCAS`, `SlowRecheck` (after seq_cst fence), `SlowMin` (atomic-min) | `mmapq_indexed_appender_commit` |
| Crash | `Crash` at any appender step; pending payloads become dead | process death (kill -9) |
| Forward archive | `ForwardStart` (validate, candidate = end of highest-position archived message, snapshot `old`), `Publish` (CAS from the snapshot per lane — CAND-37), `PublishSi`, `Fence`, `PassStep` (checked header read, atomic-min), `PassFail` (rollback to `old`), `Finish` | `archive_forward_variable` (publish via `appender_meta_start_offset_cas`), `rollback_start_offsets` in `indexed_archiver.c` |
| Undo | `UndoStart` (check files exist, lower to lowest restored position), `UndoSi` | backward branch of `archive_up_to_index_locked` |
| Listing / deletion | `List1` (mark), `List2`+`Delete` fused under `AtomicDelete=TRUE` (re-list, intersect, unlink in one step); unfused (`Delete` outside the lock) for the hand protocol; `Bound(a)` = `min(so[a], LowestLive(a))` under `ListingLivePass` | `archivable_files` (mark); `mmapq_indexed_archiver_reclaim_files` + batch twin (fused); the hand-deleter protocol in `archiving-design.md` "Deleting files" (unfused); the cap: the live-header scan in `archivable_files_locked` (both twins), checked reads, first `HEADER_NULL` ends the scan |
| Hand-deleter undo rule | `UndoStart` guard (`UndoRule`) | the hand-deleter paragraph in `archiving-design.md` "Deleting files" |
| Memory | `Flush` drains the archiver's buffered `start_offset` stores | store buffer, closed by the archiver's seq_cst fence |

**Abstractions.** Each appender owns a payload lane; a message occupies
one position and ends at `p+1`; payload file `f` holds positions
`[f*F, (f+1)*F)`, header file `h` holds indices `[h*HF, (h+1)*HF)`.
Headers are claimed lowest-empty-slot-first by one atomic CAS, so indices
stay contiguous. Memory is sequentially consistent except where a fence
switch is off: then the archiver's `start_offset` stores sit in a store
buffer, or the committer's re-check load is satisfied before its CAS is
visible (the CAS is `acq_rel`, which does not order a later load of
another location). Ghost variables record written, destroyed and
overwritten bytes so safety can be stated directly. The join's header
search is one atomic step. Payload framing, padding, regions and batches
are not modelled.

**Bounds.** 2 appenders, 3 header slots (2 header files), 3 positions per
lane (2 payload files), at most 1 crash. A PASS means no interleaving at
these bounds violates the properties: it covers the small-scope bugs, but
it is not a proof for every size. Random simulation at 6 slots / 6
positions (see Results) adds depth, not exhaustiveness.

## Properties

| Name | Meaning |
|---|---|
| `NoLiveLoss` | Every live entry (index ≥ `start_index`) is readable: its header file exists and its payload bytes were never destroyed by a delete. **The property that matters.** |
| `NoOverwrite` | A join never resumes writing over existing bytes (design I5). |
| `HeadersReferenceMessages` | Every header points at a message that was actually written. |
| `I1` | Outside an archive operation, each lane's `start_offset` ≤ every live payload of that lane, except a commit between its CAS and its re-check (design I1/I3). |
| `PassWithinSnapshot` | The live pass never reads past the tail it saw at its fence (CAND-31). |

## Variant switches

| Switch | Meaning |
|---|---|
| `PublishCAS` | Landed fix (CAND-37): publish raises `start_offset` only by a CAS from the snapshot value, so it never erases a concurrent commit's lowering. |
| `AtomicDelete` | Landed fix (CAND-36): the reclaim call re-lists and unlinks in one `archive.lock` SH hold, so undo cannot interleave. |
| `UndoRule` | Landed documented rule (CAND-36): for hand-deleters, undo never starts between the re-list and the unlink; `reclaim_files` enforces it by holding the lock across both steps. |
| `ListingLivePass` | Landed fix (CAND-35): the listing caps each lane's boundary at the lowest live payload visible in the headers (`archivable_files`; `reclaim_files` inherits it through the re-list). |
| `WaitPreCasOnly` | Historical record (CAND-35 review): the wait narrowed to the pre-CAS window — UNSAFE even with the cap (`cand35_narrow_wait`); landed so the wait is never narrowed. |
| `BoundedPass` | CAND-31 proposal: the live pass stops at the tail seen after the fence. |
| `ArchFence`, `CommitFence` | The two seq_cst fences. |
| `WaitAssumption` | The deleter's second listing happens only when no commit is between its pre-check and its re-check (what "wait" in the deletion protocol must achieve). |
| `NaiveDelete` | Delete straight after one listing. |
| `AllowCrash`, `AllowUndo`, `AllowTimeout`, `ArbitraryCommit` | Enable crashes, undo, live-pass read failures, commits of non-pending offsets. |

## Results (TLC exhaustive, bounds above)

| Check | Expect | Result | Distinct states |
|---|---|---|---|
| `current` | PASS | pass | 44,763,809 |
| `current_no_undo` | PASS | pass | 7,465,618 |
| `current_no_undo_no_timeout` | PASS | pass | 4,979,768 |
| `cand36_undo_race` | FAIL | `NoLiveLoss` | — |
| `cand37_cas_publish` | PASS | pass | 7,465,618 |
| `fixed` | PASS | pass | 44,590,581 |
| `naive_delete` | FAIL | `NoLiveLoss` | — |
| `no_wait` | FAIL | `NoLiveLoss` | — |
| `no_fence_archiver` | FAIL | `I1` | — |
| `no_fence_committer` | FAIL | `I1` | — |
| `cand33_arbitrary_commit` | FAIL | `HeadersReferenceMessages` | — |
| `cand31_chase` | FAIL | `PassWithinSnapshot` | — |
| `cand31_bounded` | PASS | pass | 36,454,766 |
| `cand35_crash` | FAIL | `I1` | — |
| `cand35_crash_listing_cap` | PASS | pass | 195,522,487 |
| `cand35_no_wait_listing_cap` | FAIL | `NoLiveLoss` | — |
| `cand35_narrow_wait` | FAIL | `NoLiveLoss` | — |
| `hand_delete_undo_rule` | PASS | pass | 56,137,369 |

**Random simulation at larger bounds** (6 header slots, 6 positions per
lane, traces up to depth 120, TLC `-simulate`):

| Configuration | Traces | States checked | Result |
|---|---|---|---|
| current protocol (as landed) | 15 | 1,940 | `NoLiveLoss` violated |
| fixed protocol, undo + timeouts | 847,324 | 129,815,951 | no violation (15 min) |
| fixed + crashes + listing cap + bounded pass | 674,536 | 102,775,355 | no violation (15 min) |

Simulation samples behaviours; it is not exhaustive. Its value here is
depth at sizes the exhaustive search cannot reach.

All results are verified with the pinned 1.7.4 via `check.sh`, including
`cand35_crash_listing_cap` (the ~1 h run): PASS, 195,522,487 distinct
states, 1 h 05 min wall — reproducing the original 2026.10.13-nightly
proof's count exactly.

## Counterexamples, in plain steps

Positions are lane-relative; "file 0" is positions 0–1. Every trace
starts from an empty queue with appender 1 joined.

### CAND-36: undo races deletion (FIXED — library reclaim)

1. Appender 1 appends X uncommitted at 0, then appends and commits Y at 1
   (index 0).
2. Archive to index 1: `start_offset` = 2, `start_index` = 1.
3. Deleter: first listing marks payload file 0; after the wait, the second
   listing still lists it.
4. Undo to index 0: the existence check passes (file 0 is still there),
   `start_offset` lowers to 1, `start_index` = 0. Y is live again.
5. Deleter unlinks file 0 (outside the lock). Y's bytes are gone.
   `NoLiveLoss` violated.

Fix (landed): the reclaim call (`mmapq_indexed_archiver_reclaim_files` and
its batch twin) re-lists and unlinks in one `archive.lock` SH hold
(`AtomicDelete`; `current` now passes with it). For hand-deleters, the
documented rule — undo never starts between the re-list and the unlink —
is modelled as `UndoRule` and passes in `hand_delete_undo_rule`.

### CAND-37: rollback restores a stale boundary (FIXED — CAS publish)

1. X uncommitted at 0; Y at 1 committed (index 0); Z at 2 committed
   (index 1).
2. Commit X (slow path): pre-check passes (`start_offset` = 0).
3. Archive to 1: publishes 2, live pass reads index 1 (Z at 2, not below),
   ends before X's header exists.
4. X's CAS lands at index 2; its re-check sees 2 > 0 and is about to lower.
5. Archive to 2 starts: snapshot `old` = 2, candidate 3.
6. X's atomic-min lowers `start_offset` to 0.
7. The archiver's publish stores 3 (plain store), erasing the lowering.
8. The live pass's first header read fails (mapping timeout): rollback
   lowers to `old` = 2. X at 0 is live and below the boundary; file 0 is
   now listable. `I1` violated (and `NoLiveLoss` once a deleter runs).

Needs a live-pass read failure; without `AllowTimeout` the landed protocol
passes. Fix: `PublishCAS` (landed; passes in `cand37_cas_publish` and
`fixed`, and `current_no_undo` now passes with it).

### CAND-35: crash between CAS and re-check (FIXED — listing cap)

1. X uncommitted at 0; Y at 1 committed (index 0).
2. Commit X (slow path): pre-check passes.
3. Archive to 1: publishes 2; the live pass finishes before X's header
   exists.
4. X's CAS lands (index 1). The process crashes before the re-check.
5. `start_offset` = 2 stays above live X at 0 and nothing will lower it:
   the deleter's wait does not help, because the dead process is not in
   any window. File 0 is listable. `I1` violated.

Fix (landed): the listing caps each lane at the lowest live payload it can
see (`ListingLivePass`, proven in `cand35_crash_listing_cap`) —
`archivable_files` in both twins, inherited by `reclaim_files` through the
shared re-list. It does **not** replace the wait:
`cand35_no_wait_listing_cap` fails in the pre-CAS window: a commit passes
its pre-check, an archive raises the boundary, the listing sees no header
yet and the file is deleted, then the CAS makes the entry live. Nor may
the wait be narrowed to the pre-CAS window: `cand35_narrow_wait` fails —
a commit's late re-check lowering re-admits a deleted file to a second
gap-fill commit (CAND-35 review probe, landed as a regression check).

### The fences

Both are the store-buffering litmus test:

- **Archiver fence removed** (`no_fence_archiver`): the archiver's
  `start_offset` store is still buffered when its live pass reads the
  header slot as empty; the committer CASes, re-checks and reads the old
  boundary; the buffered store then lands above the live payload.
- **Committer fence removed** (`no_fence_committer`): the committer's
  re-check load is satisfied before its CAS is visible and reads 0; the
  archiver publishes and passes without seeing the header; nobody lowers.

Do not weaken either fence.

### CAND-31

`cand31_chase` shows the unbounded pass reading past the tail it saw at
the fence while a producer keeps committing. `cand31_bounded` shows that
stopping at that tail keeps every safety property: commits that land after
the tail are covered by their own re-check, and fast-path commits never sit
below the published boundary.

### CAND-33

`cand33_arbitrary_commit`: a commit of an offset that was never appended
publishes a header to garbage. The model's commit takes offsets only from
the appender's pending set; that set is exactly what the library must
track to close CAND-33 for the plain appender.

## Maintenance rule

Any change to forward archiving, rollback, undo, the commit fast or slow
path, join, or `archivable_files` updates this model and its mapping table
in the same task, and re-runs `./check.sh` (and `--all` before release).
