# Archive boundaries: `start_offset` and `archive_offset` (D-G)

Two per-appender values in `appender_meta_<id>` answer two different
questions. They are **not** interchangeable, and they must never be merged
back into one field — the defect that split them (D-G) returns the moment
they are.

| | `start_offset` (byte 0) | `archive_offset` (byte 16) |
|---|---|---|
| Question | How far back must data stay readable? | How far has the archive gone? |
| Role | Readability floor: the lowest payload a live header may still need; the deletion boundary | Archive line: the iterator's snapshot floor |
| Forward archive | raised to the candidate by CAS (CAND-37), then possibly lowered by the live pass | raised to the same candidate, **max with the current value** — never lowered |
| Commit re-check min (CAND-30) | may lower it | never touched |
| Rollback (live-pass failure) | rolls it back toward the pre-call value | never touched |
| Undo | may lower it (atomic-min to the lowest restored position) | never touched |
| Read by | commit path, pollers, readers | the uncommitted iterator (single-message and batch) |

`archive_offset` is written only by the archiver's forward step, and
archive operations are mutually exclusive per queue under `archive.lock`
EX (I6). A single writer means the store is a load, compare, and
**relaxed atomic store** of `max(current, candidate)` — no CAS. The max
is load-bearing: after an undo, a later forward archive can compute a
candidate *below* the current line (archiving fewer headers), and a plain
store would lower the line and reopen D-G. The atomicity (not plain)
matters because the iterator reads the field concurrently.

## The defect (D-G)

The uncommitted iterator builds a snapshot: committed ranges from the
headers at index ≥ si (the D8/S3 floor — load-bearing: archived header
files may be reclaimed), gaps from a boundary upward. Before the split,
that boundary was `start_offset`. But `start_offset` is the readability
floor, and the protocol lowers it by design — the commit re-check min, the
live-pass, rollback, and undo. Whenever it sits below a committed position
whose header was archived, the iterator cannot tell that message from an
uncommitted one: its header is below the si floor, its payload above the
boundary. The iterator yields it; committing the yielded offset publishes
a **second header** for the same message — a double delivery to any
poller that read it before archiving (and both indices go live under
undo). TLC found four paths to this state (traces in the CAND-49 model
rule-11 package, `doc/model` `NoDoubleCommit` counterexamples):

1. **Race:** between the si store and the visibility of the boundary
   store, an iterator reading the boundary first pairs the new si with
   the old boundary.
2. **Commit re-check min:** a slow commit whose CAS lands before a racing
   archive chops its header lowers `start_offset` back to its own
   position — below its just-archived header. Steady state; no race.
3. **Rollback:** a live-pass read failure rolls the boundaries back while
   si stands. Steady state.
4. **Undo window:** undo lowers boundaries first and stores si second; an
   iterator in between pairs the old (high) si with the new (low)
   boundary.

Resequenced commits — the primary use of `append_uncommitted` — make the
divergence normal, not exotic.

## The rule

- **Archiver:** store each affected appender's `archive_offset`
  (`max(current, candidate)`, relaxed), then do the release store of
  queue-meta si. The `start_offset` stores are unchanged and stay ordered
  before si as they already were.
- **Iterator init (single-message and batch):** acquire-load queue si
  **first**, then read `archive_offset`; the gap walk starts at the
  archive line.

Why a stale pairing is only ever the harmless kind: if the iterator sees
the new si, release/acquire guarantees it sees the new line, so the pair
is consistent. If it sees the old si, it may get the old or the new line:
old/old is a consistent earlier snapshot; old si with the new line means
it still sees the archived headers *and* starts its gaps later, which only
skips the chopped region. The harmful pair — new si with an old line —
is unreachable. **Reading the line before si is the defect.**

## Worked example

Two uncommitted appends at 0 and 1, then a slow commit of 0 whose header
CAS lands, then an archive to index 1 that chops it, then the commit's
re-check lowers `start_offset` to 0:

- With `start_offset` as the walk floor: si = 1 hides the header; the
  floor 0 admits position 0; the iterator yields a committed message;
  committing it publishes a second header. Double delivery.
- With the archive line: the archive raised `archive_offset` to 1 and the
  re-check never touches it; the iterator starts at 1 and yields only
  position 1 (still genuinely uncommitted).

## Semantics

- Archiving may chop uncommitted messages; once chopped they are never
  yielded again — **including after an undo or a rollback** (the line
  does not move back). Iterate (application recovery) before an archive
  passes your uncommitted messages.
- Committing an offset the application holds is unaffected: `start_offset`
  governs that path, and the commit slow path's checks are unchanged.
- **Aborted archive:** if an archive raises `archive_offset` but never
  publishes si (the archiver crashes between the two stores, or a
  rollback leaves si standing), uncommitted messages below the new line
  are no longer yielded even though nothing was actually archived. This
  is intended — it is the same "archiving may chop uncommitted" contract.
- **Pre-0.2.0 files:** a zero `archive_offset` (below
  `INITIAL_PAYLOAD_POSITION`) means the file predates the field; the
  iterator factory refuses with `MMAPQ_ECORRUPT`. Queues created before
  0.2.0 must be recreated.
- **Java:** Java interop is not supported in 0.2.0. A Java archiver does
  not maintain `archive_offset`. A queue created by C but archived by
  Java keeps a stale *non-zero* line, so the refusal does not trigger and
  D-G reopens without any error — a Java archiver must not be run against
  0.2.0 queues. (Post-0.2.0: CAND-52.)

## How we got here

The race (path 1) was found first, by TLC, during the CAND-49 model work
(2026-10-18), and the publication rule was designed for it. Re-running
with the rule encoded then produced the three steady-state traces
(paths 2–4) the rule does not reach — the publication rule orders
visibility, but the defect is semantic: the iterator's inputs could not
express the answer at all once the floor had moved. The split of
`start_offset`'s two roles followed. The model records the design:
`aoff[a]` raised only by a forward archive, `ArchiveLineMonotonic` as the
action property, `NoDoubleCommit` as the property that pins the whole
design, and `cand49_iterate_regression` as the default-tier guard.

## Rejected alternatives

- **`start_offset` alone:** its meaning is lost the moment a min lowers
  it — this was the defect.
- **CAS(X,X) on queue si:** orders visibility, but the defect is semantic,
  and it bounces a shared cache line on every iterator init.
- **A per-appender si copy:** does not address the lowered floor.
- **Scanning archived headers:** the D8/S3 floor is load-bearing —
  archived header files may already be reclaimed.
