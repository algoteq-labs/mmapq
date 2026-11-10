# 5. Crash and restart

## What survives a SIGKILL

Process termination with the OS still running is distinct from kernel
failure or power loss. After a SIGKILL:

- every committed, acknowledged message survives intact, on both queue
  types;
- uncommitted tails stay invisible to consumers;
- the queue keeps accepting appends;
- a writer can rejoin its lane and recover its uncommitted tail (the
  restart walk, [chapter 4](04-resequenced-commits.md)).

Durability is across **process restarts, not power loss**: no commit path
calls `msync`/`fsync`/`fdatasync`. The one synchronous writeback in the
library is on appender-ID release. Use an explicitly chosen file-backed
directory for retained history; `/dev/shm` is volatile tmpfs. The full
contract is the [persistence contract](../reference/persistence-contract.md).

## The torn-tail rule and the recipe

Writers append sequentially and write each record's length before its data.
It follows that a crash tears at most one record:

> Only the record being written when the crash happens — the lane's
> **highest-offset uncommitted record at that crash** — can be torn. Every
> record written before it is byte-intact as written.

A record that is not committed is never removed. A record you discard or
skip stays in the lane and reappears in every later walk, until archiving
passes it. After the rejoined writer appends again it is no longer the
highest-offset record, so after a second crash the walk can yield a record
torn at the first crash at an earlier offset — by position it looks intact.

The library has no payload integrity check that can tell a torn record from
a valid one. The recipe for the restart walk:

1. Walk the lane's uncommitted iterator.
2. For the highest-offset record: **discard it** — do not commit it.
   Re-request it or re-append it from the source.
3. For every other record: first rule out what an earlier restart
   discarded. Either record the discarded offsets durably before the writer
   appends again, and pass over them in later walks, or validate every
   record (a checksum, or the application's own sequence number, in the
   payload). A lane that can crash more than once needs one of the two.
   Then commit or skip by your application policy; a record you skip
   reappears too.

Batch caveat: the rule holds only when the caller fills `append_reserved`
bytes before reserving the next message. No fence can order caller writes
made after a later reservation — if you reserve two buffers and fill the
second first, the first may be torn at any offset.

## Batch lanes

A batch lane follows the same rule, and the library reports the one batch
a crash can leave incomplete:

- A crash while a batch is open — started but neither staged nor
  committed — leaves it to the rejoin, which completes it to staged.
  `mmapq_indexed_batch_appender_join_completed_open_batch()` reports its
  offset once: the call clears the report, and a later rejoin does not
  repeat it. The queue completed this batch, not the application: it may
  lack messages the application meant to add, and its last message may be
  partially written. Every other staged batch is byte-intact as written
  (subject to the batch caveat above).
- No staged or discarded batch is ever removed. The uncommitted-batches
  walk yields each of them — the batch the rejoin completed, and every
  batch you staged, skipped or discarded — at every restart, until
  archiving passes them. A discarded batch comes back committable.

The recipe for the restart:

1. Call `join_completed_open_batch()` first. If it reports an offset,
   record it durably as completed by the queue before the writer appends
   again.
2. Walk `mmapq_indexed_batch_appender_uncommitted_batches_checked()`.
3. Do not commit a batch the queue completed — at this restart or an
   earlier one — unless your own check of its messages proves it complete
   and intact (for example a message count and checksum, or the
   application's own group id). Rule out the batches you discarded or
   skipped at an earlier restart the same way as for single-message lanes:
   by recorded offsets or by validation.
4. Commit the rest with `mmapq_indexed_batch_appender_commit_batch()` in
   your order, or skip them; a skipped batch reappears.

## The `--unset` restart procedure

A SIGKILLed writer never releases its appender ID: the lane's bit stays set
in `appender_id_pool`, and re-acquisition is refused with
`MMAPQ_EIDCONFLICT`. The supported recovery is the operator tool
**mmapq-ids** ([`tools/mmapq-ids/`](../../tools/mmapq-ids/) — the tool
README is its contract):

```sh
make mmapq-ids
tools/mmapq-ids/build/release/mmapq-ids /data orders            # list lanes
tools/mmapq-ids/build/release/mmapq-ids /data orders --unset 7  # release ID 7
```

The restart procedure:

1. **Establish that the process holding the ID is dead.** `--unset` is
   valid only then: a SIGSTOPped live process plus `--unset` means two
   writers on one lane, and the single-writer guarantee the whole commit
   protocol rests on is void. Reaping the process (or a supervisor's
   equivalent knowledge) is what establishes the precondition — the tool
   verifies nothing about ownership or liveness.
2. Work in a maintenance window: producers stopped, automatic restarts
   disabled. Clearing an active ID can let two writers into the same
   payload lane and corrupt the queue.
3. Run `--unset <id>`. Exit codes: 0 success, 1 already free, 2 error. A
   successful release changes exactly one bit of exactly one file, with
   checked synchronous writeback — not a power-loss guarantee.
4. Restart the writer with `mmapq_indexed_queue_create_appender_with_id()`
   on its lane; the join runs the lane's crash recovery and the restart
   walk surfaces its uncommitted tail (recipe above).

The tool is a separate build that does not link libmmapq and never opens
queue metadata — a damaged meta file does not prevent releasing a known ID.

## Corruption: detected, never repaired

On open, the recovery scan validates the committed stream and refuses with
`MMAPQ_ECORRUPT` (or `MMAPQ_ESCHEMA` for a version or geometry mismatch)
rather than risk a silent misread. A refused start never recovers to a
position inside or past the committed stream.

Recovery detects; it does not repair. The remediation is always the same:
recreate the queue, restoring any needed history from the export format
([chapter 7](07-export-import-migration.md)). Queues are transport plus
recent history; long-term data belongs in exports or a database.
