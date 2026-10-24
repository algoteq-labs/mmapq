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
| Appender join | `Join` (search own entry in live range, walk to end; "no file = end of stream"). Under amendment L(ii) a join with no live entry anchors at `aoff`; the **code** anchors at `max(start_offset, archive_offset)` (`appender_join_anchor`, ruling 2026-10-25) — equal under `SoLeLine`, while the `start_offset` side preserves the CAND-32 corrupt-state refusal for states outside the invariant | join path in `indexed_appender.c` (`appender_join_anchor`, `walk_payload_to_end`) |
| Append | `Append` = write, then `FastCommit` (header CAS) | `mmapq_indexed_appender_append` → `write_payload`, `commit_header` |
| Uncommitted append | `AppendUncommitted` | `mmapq_indexed_appender_append_uncommitted` |
| Commit | `Commit` picks fast path iff offset = last appended (`FastCommit`), else slow path: `SlowPreCheck` (load start_offset, EINVAL if below), `SlowCAS`, `SlowRecheck` (after seq_cst fence), `SlowMin` (atomic-min) | `mmapq_indexed_appender_commit` |
| Crash | `Crash` at any appender step; pending payloads become dead | process death (kill -9) |
| Forward archive | `ForwardStart` (validate, candidate = end of highest-position archived message, snapshot `old`), `Publish` (CAS from the snapshot per lane — CAND-37), `PublishSi`, `Fence`, `PassStep` (checked header read, atomic-min), `PassFail` (rollback to `old`), `Finish` | `archive_forward_variable` (publish via `appender_meta_start_offset_cas`), `rollback_start_offsets` in `indexed_archiver.c` |
| Undo | `UndoStart` (check files exist, lower to lowest restored position), `UndoSi` | backward branch of `archive_up_to_index_locked` |
| Listing / deletion | `List1` (mark), `List2`+`Delete` fused under `AtomicDelete=TRUE` (re-list, intersect, unlink in one step); unfused (`Delete` outside the lock) for the hand protocol; `Bound(a)` = `min(so[a], LowestLive(a))` under `ListingLivePass` | `archivable_files` (mark); `mmapq_indexed_archiver_reclaim_files` + batch twin (fused); the hand-deleter protocol in `archiving-design.md` "Deleting files" (unfused); the cap: the live-header scan in `archivable_files_locked` (both twins), checked reads, first `HEADER_NULL` ends the scan |
| Hand-deleter undo rule | `UndoStart` guard (`UndoRule`) | the hand-deleter paragraph in `archiving-design.md` "Deleting files" |
| Memory | `Flush` drains the archiver's buffered `start_offset`/`archive_offset` stores | store buffer, closed by the archiver's seq_cst fence |
| Uncommitted iteration (CAND-49) | `IterateUncommitted` refills the pending set from the lane's surviving uncommitted positions at or above the archive line (`IterEffAoffChoices`: acquire si first, then the line — the D-G publication rule; the harmless early-line direction is a modelled nondeterministic choice) | the `_checked` iterator factories (snapshot at init; refill on yield) |
| The pending flag (CAND-49) | `flag[a]` — the `last_pending_offset` tail; set only by `AppendUncommitted`, cleared on commit success/overtake and by `Crash`/`Join`; the set is uniform, the flag is modelled | `indexed_appender.c` `last_pending_offset` (§3.3/§3.4 double conjunct) |
| The archive line (D-G) | `aoff[a]` — raised only by a forward archive (`Publish`/`Flush`, `Max2`); never lowered. `ArchiveLineMonotonic` pins it | `appender_meta.archive_offset` — see `doc/design/archive-boundaries.md` |
| Commit intent (CAND-54 proposal; spec `audit/CAND-54-spec.md` — NOT yet landed in code) | `CommitIntent`: slow-path commit stores `intent[a]` before the pre-check (`SlowIntent`, c0), clears it last (`SlowClear`, c5; refusal path c5r); crash leaves it stale, `Join` clears it (plain). The re-list is decomposed (`RLoadSo` → `RLoadIntents` → `RScan` → `RCompute`) with `Bound` gaining the inflight term, the SH hold transcribed as the `UndoStart` mid-round guard; `WaitAssumption` is off. Amendment H: `MinSkipArchived` — the commit min is skipped when the committed entry is already archived (`cidx[a] < si`; in code the si read runs under a brief `archive.lock` SH hold taken on the fired re-check path only) | proposal: `indexed_appender.c:787-829` slow path + `indexed_batch_appender.c:736-776` twin; the intent slot at appender-meta byte 24; `archivable_files_locked` load order; `reclaim_files` SH hold |

**Abstractions.** Each appender owns a payload lane; a message occupies
one position and ends at `p+1`; payload file `f` holds positions
`[f*F, (f+1)*F)`, header file `h` holds indices `[h*HF, (h+1)*HF)`.
The on-disk header layout transposes indices within each 32 KB block, so
the header-file row is true of the implementation only because the CAND-38
geometry rule (region a multiple of the 32 KB block and page-aligned, file
a multiple of the block) guarantees block-aligned header files; without
it, reclaim deletes
live committed headers — a defect below this model's abstraction, found by
probing, not by TLC.
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
| `NoDoubleCommit` | No two header slots, live or archived, ever reference the same message — a duplicate is a double delivery to pollers (CAND-49 D-G; the property that pins the archive-line design). Model-only: the C counterpart is the test-only lane validator's header-uniqueness check, never a commit-path runtime check. |
| `ArchiveLineMonotonic` | `archive_offset` never decreases (via the `aoffPrev` ghost): only a forward archive raises it; the commit min, live pass, rollback, and undo never touch it (D-G). |
| `ArchiveLineNeverDecreases` | The total form, a checked PROPERTY: *every* step keeps `aoff'[a] >= aoff[a]` — a lowering added to any action violates it on the spot, where the ghost only watches Flush. Mutation record (PassFail with `aoff' := 0`): this property fails at 0 s, the ghost fails later (2 s), `NoDoubleCommit` dies at 1 s to the reopened D-G. |
| `NoDeleteUnderPending` | CAND-54: no payload file is deleted while an intent into it is pending and its commit may still publish (vacuous before the pre-check passes — a delete there forces the pending pre-check to observe the raise and refuse, so it is benign by construction). |

## Variant switches

| Switch | Meaning |
|---|---|
| `PublishCAS` | Landed fix (CAND-37): publish raises `start_offset` only by a CAS from the snapshot value, so it never erases a concurrent commit's lowering. |
| `AtomicDelete` | Landed fix (CAND-36): the reclaim call re-lists and unlinks in one `archive.lock` SH hold, so undo cannot interleave. |
| `UndoRule` | Landed documented rule (CAND-36): for hand-deleters, undo never starts between the re-list and the unlink; `reclaim_files` enforces it by holding the lock across both steps. |
| `ListingLivePass` | Landed fix (CAND-35): the listing caps each lane's boundary at the lowest live payload visible in the headers (`archivable_files`; `reclaim_files` inherits it through the re-list). |
| `WaitPreCasOnly` | Historical record (CAND-35 review): the wait narrowed to the pre-CAS window — UNSAFE even with the cap (`cand35_narrow_wait`); landed so the wait is never narrowed. |
| `IterateRefill` | CAND-49: enable `IterateUncommitted` (default FALSE; inert by construction when FALSE). |
| `BoundedPass` | CAND-31 proposal: the live pass stops at the tail seen after the fence. |
| `ArchFence`, `CommitFence` | The two seq_cst fences. |
| `WaitAssumption` | The deleter's second listing happens only when no commit is between its pre-check and its re-check (what "wait" in the deletion protocol must achieve). |
| `NaiveDelete` | Delete straight after one listing. |
| `AllowCrash`, `AllowUndo`, `AllowTimeout`, `ArbitraryCommit` | Enable crashes, undo, live-pass read failures, commits of non-pending offsets. |
| `CommitIntent` | CAND-54 proposal: the commit intent protocol (default FALSE, inert: `intent[a]` stays empty, the re-list stays fused, and existing checks keep their state counts). |
| `MinSkipArchived` | CAND-54 amendment H: the commit min is skipped when the committed entry is already archived (TLC-found holes §7.1/§7.2 of the spec). |
| `JoinAtArchiveLine` | CAND-54 amendment candidate A (superseded by H; kept for the record checks). |
| `ClearBeforeCAS`, `ScanBeforeIntents`, `ClearBeforeMin` | CAND-54 mutants; the first two must FAIL, the third must PASS (live_floor carries the min window). |

## Results (TLC exhaustive, bounds above)

Counts below are the CAND-49 encoding (`flag[a]`, `aoff[a]`/`sbAoff[a]`;
2026-10-19): every pre-existing check kept its verdict — FAIL checks fail
on the recorded invariant — and the count deltas are the flag/archive-line
state encoding (owner-accepted). Pre-CAND-49 counts are in git history.
The CAND-54 encoding (`intent`, `cidx`, decomposed re-list, flush split)
is inert when its switches are FALSE: `current_no_undo` re-run on
2026-10-24 reports exactly 11,467,666 distinct states, unchanged.

| Check | Expect | Result | Distinct states |
|---|---|---|---|
| `current` | PASS | pass | 141,812,096 |
| `current_no_undo` | PASS | pass | 11,467,666 |
| `current_no_undo_no_timeout` | PASS | pass | 7,650,853 |
| `cand36_undo_race` | FAIL | `NoLiveLoss` | — |
| `cand37_cas_publish` | PASS | pass | 11,467,666 |
| `fixed` | PASS | pass | 140,849,111 |
| `naive_delete` | FAIL | `NoLiveLoss` | — |
| `no_wait` | FAIL | `NoLiveLoss` | — |
| `no_fence_archiver` | FAIL | `I1` | — |
| `no_fence_committer` | FAIL | `I1` | — |
| `cand33_arbitrary_commit` | FAIL | `HeadersReferenceMessages` | — |
| `cand31_chase` | FAIL | `PassWithinSnapshot` | — |
| `cand31_bounded` | PASS | pass | 117,668,995 |
| `cand35_crash` | FAIL | `I1` | — |
| `cand35_crash_listing_cap` | PASS | pass | 195,522,487 † |
| `cand35_no_wait_listing_cap` | FAIL | `NoLiveLoss` | — |
| `cand35_narrow_wait` | FAIL | `NoLiveLoss` | — |
| `hand_delete_undo_rule` | PASS | pass | 179,748,536 |
| `cand49_iterate_regression` | PASS | pass | 9,798,122 |
| `cand54_no_join_anchor` | FAIL | `NoOverwrite` | — |
| `cand54_no_minskip` | FAIL | `NoDeleteUnderPending` | — |
| `cand54_intent` (H lock-free record) | FAIL | `NoOverwrite` | — |
| `cand54_intent_hand` (H record) | FAIL | `NoOverwrite` | — |
| `cand54_clear_before_min` (H; moot) | FAIL | `NoOverwrite` | — |
| `cand54_clear_before_cas` / `cand54_scan_before_intents` / `cand54_no_live_floor` (H mutants) | FAIL | `NoLiveLoss` | — |
| `cand54l_clear_before_cas` / `cand54l_scan_before_intents` / `cand54l_no_live_floor` (L mutants) | FAIL | `NoLiveLoss` | — |
| `cand54l_intent` (L) | PASS | pass | 401,057,143 |
| `cand54l_intent_hand` (L) | PASS | pass | 439,733,123 |
| `cand54l_clear_before_min` (L) | PASS | pass | 459,672,512 |
| `cand54l_intent_core` / `cand54l_intent_hand_core` / `cand54l_clear_before_min_core` (L, tier-1 guards) | PASS | pass | 13,908,615 / 15,286,465 / 16,179,432 |
| `cand49_iterate_i1` | PASS | pass | 141,812,096 ‡ |
| `cand49_iterate` | PASS | pass | 878,242,588 †† |

† Not re-run at the CAND-49 encoding (speed 2, outside the default tier);
the count is the pre-encoding run. The pre-release `--all` re-run re-records
it.

‡ Equals the control's count exactly: refill is inert without crashes
(nothing clears the pending set/flag), so this check's value is I1 over
the CAND-49 encoding; refill is exercised by the crash checks.

†† `cand49_iterate` passed on the v2.2-draft (deterministic-read) encoding;
its re-run on the final encoding (the nondeterministic early-line read) is
deferred to the pre-release `--all` run (owner, 2026-10-19) —
`cand49_iterate_regression` covers crash + the branch in the default tier.

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

Historical: the deletion wait was retired by CAND-54 (`cand54l_*`); the
CAND-35/36 narratives below record why it existed.

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

### CAND-54: the commit intent and the two holes in the wait's replacement

The CAND-54 proposal (spec `audit/CAND-54-spec.md`, not yet landed in
code) replaces the deletion wait with a per-lane **commit intent** (an
appender-meta slot stored before a slow commit's pre-check, released
after the boundary repair) and a third term in the listing's bound
(`min(start_offset, live_floor, inflight)`).

Two design holes were found by this model before any code existed:

1. **Hole 1 — `cand54_no_join_anchor` (FAIL, NoOverwrite).** The wait
   also covered a *stale* `start_offset` read in the re-list: a racing
   commit's re-check min lowers the boundary below the **archived**
   region (the commit cannot tell its entry was archived); a re-list
   holding the pre-min value deletes the file — legitimately (every
   entry in it is archived) — and a later crash+join anchors at the
   lowered `start_offset`, re-creating a reclaimed file. Undo then
   passes its existence check over NEW bytes (`NoLiveLoss` one step
   later). Same mechanism as `cand35_narrow_wait`.
2. **Hole 2 — `cand54_no_minskip` (FAIL, NoDeleteUnderPending).** Two
   generations: generation 1's min lowers the boundary after the
   re-list's load; generation 2's pre-check passes against the lowered
   value while its intent store postdates the lister's intent load
   (which read generation 1's clear — case-3 visibility points at an
   *archived* entry, invisible to the scan). The case-1 proof's
   "something later can't be a lowering" missed the commit min as a
   lowering source for *later* generations. Option B (the request's
   fallback) has the same hole: its SH hold does not cover the min
   either.

Amendment H (skip the min when the entry is archived, deciding under a
brief SH hold) was modelled and **rejected** (reviewer, 2026-10-24): the
`si` read needs the lock for currency (Option B reasons 1 and 4), and
the lock-free transcription fails hole 1 (`cand54_intent` FAIL
NoOverwrite — the re-check raced the covering archive).

**Amendment L (reviewer, 2026-10-24; lock-free):** (i) the overtaken
pre-check compares against `max(start_offset, archive_offset)`; (ii) a
join with no live entry anchors at `archive_offset`; (iii) the archiver
stores the line (seq_cst) before raising the boundary, so
`start_offset ≤ line` always (`SoLeLine` pins it; the flush drains
`sbAoff` before `sbSo`); (iv) the commit min is unchanged. The line is
monotone, so the case-1 chain's "later value" can never be lower — both
holes close at the root. Semantics change (owner ruling, rule 13 —
`archive-boundaries.md:95-96` kept commit on `start_offset`): an
uncommitted payload below the line becomes uncommittable (EINVAL),
consistent with the iterator's chop.

Checks: `cand54l_*` (L family) and `cand54_*` (H records + mutants);
litmus under `litmus/` (herd7/RC11). A bring-up note: a bookkeeping
error (a variable both assigned and listed UNCHANGED) once disabled the
model's slow-path CAS and produced vacuous PASSes — caught by chasing
the unexpected PASS of `cand54_clear_before_cas`; TLC's "changed while
UNCHANGED" warning is not surfaced by check.sh's verdict grep, so the
run procedure now greps logs for `Warning`.

### CAND-49 (D-G): the iterator and the archive line

`cand49_iterate*` model the uncommitted iterator: `IterateUncommitted`
refills an appender's pending set from the lane's written-but-uncommitted
positions that survive on disk after a crash (the queue itself does not
recover anything; the iterator is a tool for *application* recovery).
The modelling decisions, all from executed counterexamples:

- The pending **flag** is modelled (`flag[a]`): a uniform pending set let
  a refilled, previously-committed offset equal to the last appended take
  the commit fast path and publish a second header below the boundary
  (I1, 15-state trace). The C fast path's double conjunct
  (`offset == last_appended && offset == last_pending_offset`) excludes
  this; the model now transcribes it.
- **D-G:** the iterator could report an already-committed message as
  uncommitted whenever the readability floor (`start_offset`) sat below
  an archived committed position — reachable by a mid-archive race, by
  the commit re-check min, by a rollback, and mid-undo (four
  `NoDoubleCommit` traces). The fix splits the floor from the new
  monotonic **archive line** (`aoff[a]`, raised only by a forward
  archive; `ArchiveLineMonotonic` (ghost) and `ArchiveLineNeverDecreases`
  (total action property) pin it). The full design, semantics
  (including the aborted-archive case), worked example, and rejected
  alternatives: **`doc/design/archive-boundaries.md`**.
- The iterator's read is the publication pair: acquire si first, then the
  line; when si does not yet show the in-flight archive, the model
  nondeterministically allows the line's new value to be visible early
  (harmless; checked, not just argued — branch on/off state counts on the
  regression check differ: 9,798,122 vs 9,797,714).

`cand49_iterate` (crashes on, full bounds) and `cand49_iterate_i1`
(crashes off, I1 on) are speed-2 (`--all`); `cand49_iterate_regression`
(crash on, reduced bounds, ~3 min) is the default-tier guard. Fail-first
record: the regression check FAILs `NoDoubleCommit` in 3 s against the
pre-archive-line model (rollback trace).

## Maintenance rule

Any change to forward archiving, rollback, undo, the commit fast or slow
path, join, `archivable_files`, the uncommitted iterator's snapshot
discipline, or anything that writes `archive_offset` updates this model
and its mapping table in the same task, and re-runs `./check.sh` (and
`--all` before release). `cand49_iterate_regression` in the default tier
is the D-G tripwire.
