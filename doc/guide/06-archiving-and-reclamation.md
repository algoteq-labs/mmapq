# 6. Archiving and reclamation

Archiving lets a long-running queue drop old entries without rewriting
data. A visual walk-through:
[The Trail](https://claude.ai/artifact/9tzCHa3MMWTgrm4j7PHzeS).

Archiving is two separate things:

1. **Logical archiving** moves the queue's `start_index`. Entries below it
   are *archived*: readers and pollers report them as unavailable.
2. **Physical reclamation** deletes the files the archived entries lived
   in. The library path is **mark → `reclaim_files()`**, always safe;
   hand deletion is safe under the rule below.

## The three boundaries

| Boundary | Where | Meaning |
|---|---|---|
| `start_index` | `meta` | The first index that is not archived. Header files wholly below it may be listed. |
| `start_offset` | `appender_meta_<id>` | The lane's readability floor: the lowest payload position a live entry may still need. May move both ways (forward archive raises it by CAS, the live pass or an undo can lower it). |
| archive line (`archive_offset`) | `appender_meta_<id>` | How far archiving has gone on this lane. Raised only by a forward archive; **never lowered**. The uncommitted iterator starts here. |

The two per-appender values answer different questions and are not
interchangeable; [archive-boundaries](../design/archive-boundaries.md) is
the design record.

## Forward archiving: `archive_up_to_index(N)`

Under the exclusive `archive.lock` (one archive operation at a time per
queue):

1. The archiver reads and validates every header in `[start_index, N)`;
   if any does not reference a complete message, archiving fails with
   `MMAPQ_ECORRUPT` and changes nothing.
2. Each lane's candidate boundary is the end of its archived entry with
   the highest **payload position** (commit order is not write order).
3. The boundary is published by a CAS from the snapshot value, so a racing
   commit's lowering survives; then `start_index = N`.
4. One live pass over `[N, end)` lowers each lane's boundary below any
   live payload that sits under it.
5. If a header read in that pass fails (timeout, corruption), every
   boundary rolls back to its pre-call value and the call returns the
   error. `start_index = N` stands — the entries are archived — but no
   payload file becomes reclaimable from this call. Retrying
   `archive_up_to_index(N)` after such a failure is a no-op returning 0;
   a 0 from that retry does not mean the boundary advanced.

**Undo** — `archive_up_to_index(N)` with `N < start_index` — restores
access to previously archived entries after checking that every file they
need still exists, and only ever lowers lanes' boundaries. Undo is
possible only while the files it needs still exist.

## Reclamation: mark → `reclaim_files()`

`archivable_files()` (holding `archive.lock` shared, so it never observes
an archive in progress) lists the files no live entry can need **at the
time of the call**:

- header files wholly below `start_index`;
- payload files of each lane wholly below the lane's deletion bound
  `min(start_offset, live_floor, intent)` — the lane's floor, capped at
  the lowest payload the live-header scan can see, and capped at a
  commit's recorded intent while one is set, so a file a stalled or
  stopped commit is about to publish into is never listed.

The production loop:

1. Call `archivable_files()`; mark the listed files.
2. Copy the marked files aside (object storage, an archive directory) if
   you keep long-term history.
3. Delete the local copies with `reclaim_files()`.

`reclaim_files()` re-lists — completely — and unlinks the overlap in one
shared hold of `archive.lock`, so an undo cannot slip between the re-list
and the unlink. Marks that dropped out of the re-list are skipped: they
are live again (an undo ran before the hold) or already gone. **A mark is
a snapshot, not permission** — that is why the re-list exists.

## Hand deletion (the X5 rule)

If you delete, move or compress the listed files yourself instead of
calling `reclaim_files()`: they are safe provided **no undo** runs between
your `archivable_files()` call and your last unlink of the files it
returned. An undo that ran *before* the call is harmless — the listing
already reflects it.

Never truncate or rewrite a mapped file (the
[persistence contract](../reference/persistence-contract.md)). Unlinking
archived files is not permission to touch live ones.

## Snapshots plus archiving

The application-level pattern (the
[event-sourcing guide](event-sourcing.md)):

1. Periodically fold the events into a snapshot of your derived state and
   record the log index the snapshot covers.
2. Archive past snapshotted history with `archive_up_to_index()`, then
   reclaim.
3. Recovery becomes: load the snapshot, replay events since its index.

A snapshot recorded below the current `start_index` is useless after
reclamation unless you kept the copies (step 2 of the production loop)
— consumers own their checkpoints, and a checkpoint that
precedes the archived start index must be detected by the application,
which applies its own recovery policy (re-snapshot, or rebuild from an
export).

## Interactions with the rest of this guide

- A SIGKILLed writer's lane holds its appender ID until the operator
  releases it; reclaiming IDs follows the `--unset` procedure in
  [chapter 5](05-crash-and-restart.md), dead-process precondition
  included. ID reclamation and file reclamation are independent:
  archiving works regardless of dead lanes.
- Dead payloads left by a crash are referenced by no header, so they never
  hold a boundary back.
- An uncommitted payload below the archive line can never become live;
  committing it returns `MMAPQ_EINVAL` — append it again instead. This is
  what makes reclamation safe under resequenced commits
  ([chapter 4](04-resequenced-commits.md)).

## Evidence

The safety property — no live entry's bytes are lost to reclaim — is model
checked, and the listing's per-lane cap is pinned by test; every claim in
this chapter maps to a row in [guarantees](../reference/guarantees.md).
The protocol's design record, including the invariants and the commit
windows, is [archiving-design](../design/archiving-design.md).
