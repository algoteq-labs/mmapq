# Archiving: start index, start offsets, and what may be deleted

> Introduced by CAND-30 (0.2.0). Design: Anton Anufriev, 2026-10-14.

Archiving lets a long-running queue drop old entries without rewriting
data. It is two separate things:

1. **Logical archiving** moves the queue's `start_index`. Entries below it
   are *archived*: readers and pollers report them as unavailable.
2. **Physical reclamation** is done by the caller, outside the library.
   `archivable_files()` lists header and payload files that hold nothing the
   queue still needs; the caller may delete, move or compress them. The
   library never deletes files.

This document defines when a file may be listed, and how a caller must
delete, because that is the step that can lose data if it is wrong.

## The persistent fields

| File | Field | Meaning |
|---|---|---|
| `meta` | `start_index` (int64, atomic) | First index that is not archived. Header files wholly below it may be listed. |
| `appender_meta_<id>` | `start_offset` (int64, atomic) | Payload files of this appender wholly below it may be listed. It is also the anchor a joining appender walks forward from when it has no entry left in the live range. |
| `archive.lock` | (no content) | `flock` target: one archive operation at a time per queue. |

`start_offset` exists because a joining appender must find the end of its
payload stream without reading headers below `start_index` — those header
files may have been deleted. If the appender has an entry in the live range,
it walks forward from that entry's payload. If it has none, it walks forward
from `start_offset`. It never *writes* at `start_offset`.

## Payload states

| State | Definition | Protected from reclamation? |
|---|---|---|
| **Archived** | Referenced by a header with index < `start_index` | No |
| **Live** | Referenced by a header with index ≥ `start_index` | **Yes** |
| **Uncommitted** | Referenced by no header — pending (current appender incarnation) or dead (a previous incarnation; discarded on restart by contract) | No — and once below `start_offset` it can no longer be committed |

Commit order is not write order. `append_uncommitted` + `commit` lets an
application commit payloads in any order (UDP resequencing: append packets
as they arrive, commit when the sequence is gapless). So a live payload can
sit at a lower position than an archived one, and the boundary has to be
computed with that in mind.

## Invariants

- **I1** Once `archive_up_to_index` returns, `start_offset` ≤ every live
  payload of that appender. The only exception is a commit racing that
  archive, for the few instructions between its CAS and its re-check (see
  "Commit"); the deletion protocol covers that window.
- **I2** An uncommitted payload below `start_offset` can never become live:
  committing it returns `MMAPQ_EINVAL`. Append it again instead.
- **I3** Forward archiving never sets `start_offset` above the lowest live
  payload; undo never raises it.
- **I4** Every archived header must reference a complete message (or
  batch); otherwise archiving fails with `MMAPQ_ECORRUPT` and changes
  nothing.
- **I5** A joining appender never reads headers below `start_index`, and
  never writes at `start_offset` or over existing bytes.
- **I6** One archive operation at a time per queue; `archivable_files`
  waits for it.

## Forward archiving: `archive_up_to_index(N)`

Under the exclusive `archive.lock`:

1. Read the headers `[start_index, N)` and validate each one (I4).
2. For each appender, take the archived entry with the **highest payload
   position** (not the highest index). The candidate is the end of that
   message, or of that batch.
3. Publish `start_offset = max(current, candidate)` for each appender, then
   `start_index = N`.
4. Make one pass over the live headers, from `N` to the end of the stream.
   For every live entry whose payload lies below its appender's
   `start_offset`, lower `start_offset` to it.
5. If a header read in that pass fails (mapping timeout, corruption),
   lower every `start_offset` back to its value before this call and return
   the error. `start_index = N` stands — the entries are archived — but no
   payload file becomes reclaimable from this call. A timeout is never
   mistaken for the end of the stream.
   Retrying `archive_up_to_index(N)` after such a failure is a no-op that
   returns 0, because `start_index` is already `N`. That is safe — the
   boundary simply stays where it was — and reclamation catches up when a
   later archive moves past it. A 0 from that retry does not mean the
   boundary advanced.

With commits in write order, step 4 never lowers anything and the result is
exactly the pre-0.2.0 value.

Example (one appender, 4 KiB files; B was committed before A):

```
payload stream:  [A @64] [pad] [B @4096] [C @8192]
headers:         idx0 -> B    idx1 -> A    idx2 -> C

archive_up_to_index(1):  archived {B}; candidate = end of B
  publish start_offset = end of B
  live pass: idx1 -> A @64 is below it -> start_offset = 64
  payload file 0 (holding A) is not listed

archive_up_to_index(2):  archived {B, A}; candidate = end of B
  live pass: idx2 -> C @8192, not below -> start_offset = end of B
  payload file 0 may be listed
```

Dead payloads (left by a crash) are referenced by no header, so they never
hold the boundary back: reclamation after a crash works as before.

## Commit

`commit(offset)` has two paths:

- **Fast path** — `offset` is the payload this appender appended most
  recently (the normal in-sequence case). Nothing changes: no shared loads.
  It must be keyed on the last *appended* offset. "Greater than the last
  committed offset" is not safe under resequencing.
- **Slow path** — any other offset (filling a gap):
  1. Load `start_offset`. If `offset` is below it, return `MMAPQ_EINVAL`:
     the payload has been overtaken by archiving and may already be listed.
  2. Publish the header (CAS), as today.
  3. Fence, load `start_offset` again. If `offset` is now below it — an
     archive published a higher value between steps 1 and 2 — lower
     `start_offset` to `offset`.

Why this is enough: the archiver *stores* `start_offset` and then *reads*
the live headers; the appender *stores* its header and then *reads*
`start_offset`. With sequentially consistent ordering, at least one of them
sees the other, so one of them lowers the boundary. The archiver needs no
stability loop. `start_offset` changes only when archiving runs, so both
loads hit a shared cache line.

The batch appender has one batch in flight at a time and always commits
the batch it appended last, so it always takes the fast path.

## Undo: `archive_up_to_index(N)` with N < `start_index`

Under the exclusive lock, undo checks that every header and payload file
the restored entries need still exists, then lowers each appender's
`start_offset` to the lowest restored payload position if that is lower.
It never raises it.

## Appender join

1. Search for the appender's own latest entry **only** in the live range
   (indices ≥ `start_index`).
2. If found, walk forward from that payload to the end of the stream.
3. If not found (or the queue is empty), walk forward from `start_offset`
   to the end of the stream.
4. Start writing there.

A position with no file yet, or beyond the end of a file, is the end of
the stream. The walk uses read-only mappings and never creates files.
Walking (rather than writing at `start_offset`) matters: bytes at
`start_offset` may belong to a dead payload, and writing a shorter message
over a longer one leaves stale bytes that a later recovery would misread.

## Deleting files

`archivable_files()` (taking `archive.lock` shared) lists:

- header files wholly below `start_index`;
- payload files of each appender wholly below its `start_offset`.

**Do not delete immediately.** Mark the listed files, wait, then delete
only the files that a fresh `archivable_files()` call — taken *after* the
wait — still lists. The re-check is what closes I1's window: if a racing
commit lowered `start_offset` after the first listing, its file drops out of
the second. A mark is a snapshot, not permission.

The wait only has to outlast a writer's stall between its CAS and its
re-check — normally a few instructions — so seconds to minutes covers
preemption and scheduling. It does not cover a writer process stopped in
exactly that window (SIGSTOP, a debugger). Deletion safety in that window
rests on callers following this protocol; the library does not enforce it.

Never truncate or rewrite a mapped file (see `persistence-contract.md`).
Undo is possible only while the files it needs still exist.

## Concurrency summary

| Actor | Writes | Then reads |
|---|---|---|
| Archiver | `start_offset` (release), `start_index` | live headers (acquire, after a seq_cst fence) |
| Appender, slow-path commit | header (CAS) | `start_offset` (acquire, after a seq_cst fence) |
| `archivable_files` | — | `start_index`, `start_offset`, under the shared lock |

The two seq_cst fences carry the argument; the accesses themselves need
only release/acquire. Lowering is always an atomic-min loop; nothing stores
a higher value except the archiver's publish in step 3, which is followed by
the live pass.

`archive.lock` is locked by inode. Deleting or recreating the queue
directory while archivers or listers are open breaks exclusivity; recreate
a queue only with all clients closed.

## Cost

- **`append()` and in-sequence commits:** unchanged.
- **Gap-fill commits:** two loads of a rarely-written shared field, a fence,
  and in the rare racing case one CAS.
- **Join:** one forward walk when the appender has no live entry.
- **Archiver:** one validation read per archived message, plus one pass
  over all live headers per call. It runs off the hot path.

## Java parity

Mixed C/Java queues are safe only when Java follows the same rules:

1. Exclusive archive lock; listing takes it shared.
2. Forward: candidate = end of the archived message with the highest
   position; publish; one pass over the live headers lowering it.
3. Every archived header must reference a complete message.
4. Undo: lower to the lowest restored position; never raise.
5. Commit: fast path for the last appended offset; otherwise reject below
   `start_offset`, CAS, fence, re-check, lower if needed.
6. Join: never read headers below `start_index`; with no own live entry,
   walk from `start_offset` to the end of the stream; never write at it.
7. Deletion: mark, wait, re-validate, delete.

## History

Before 0.2.0, forward archiving set `start_offset` just past the payload of
the highest-index archived entry, undo used the first-index restored
payload, a joining appender wrote at `start_offset`, and its header scan
could reach below `start_index`. All of that assumed an appender's payload
positions rise with its indices. Resequenced commits break that, and the
0.2.0 review reproduced:

- a live entry's payload file listed as deletable (read fails after
  cleanup), for resequenced commits and for an uncommitted payload
  committed after archiving overtook it;
- 442 live or pending payloads below `start_offset` in a 479-archive race
  run;
- an appender that could not restart (`MMAPQ_ECORRUPT`) after crash,
  archive and one restart;
- an appender that could not restart because its join scan read an
  archived header whose payload file had been legitimately deleted.

Tracked as CAND-30 in the hardening plan.
