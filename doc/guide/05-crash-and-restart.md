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

## What a crash can leave: framing, not bodies

The queue guarantees the **framing** of every lane after a crash, and makes
no promise about the **bodies** of uncommitted records:

- **Guaranteed:** every record keeps its full declared length, and every
  batch a complete message count (the rejoin sets an open batch's count
  from the messages it finds). A crash never misframes a lane or wedges
  it: the restart walk steps from record to record by those lengths, and
  the writer rejoins and appends after them.
- **Not guaranteed:** the bytes of an uncommitted record after a crash.
  Writers store a record's length before its data, so a crash in between
  leaves a record whose length is complete and whose data may be partly
  written. Unwritten bytes read as zeros, and zeros are valid payload, so
  the library cannot tell a partly written record from a complete one.
- **A hint, not a promise:** a crash leaves at most one record partly
  written — the one being written, the lane's last at that crash. Do not
  rely on position: a record that is not committed is never removed, so a
  record from an earlier crash reappears in every later walk, below the
  records appended since, and looks no different from the rest. (On a
  batch lane the hint also needs each `append_reserved` buffer filled
  before the next is reserved.)

The recipe for the restart walk:

1. Walk the lane's uncommitted iterator.
2. **Validate each record before committing it**, with a check carried in
   the payload — the application's own sequence number, or a checksum.
   Commit a record only if it passes; re-request or re-append one that
   fails.
3. Commit or skip the records that pass by your application policy. A
   record you skip, or one that failed the check, stays in the lane and
   reappears in every later walk until archiving passes it — the check
   handles it again each time. (Alternatively, record the offsets you
   discarded durably before the writer appends again, and pass over them in
   later walks.)

## Batch lanes

A batch lane has the same contract, and the library reports the batch a
crash left open:

- A crash while a batch is open — started but neither staged nor
  committed — leaves it to the rejoin, which completes it to staged with
  the messages it finds by their lengths.
  `mmapq_indexed_batch_appender_join_completed_open_batch()` reports its
  offset once: the call clears the report, and a later rejoin does not
  repeat it. The queue completed this batch, not the application: it may
  lack messages the application meant to add.
- Bodies are not guaranteed in any batch after a crash, including batches
  the application staged: a crash while a message is being written can
  leave its data partly written, and the store of a batch's message count
  is not ordered after its last message's data, so a batch staged just
  before the crash can carry a partly written last message. Its count and
  every message length are complete.
- No staged or discarded batch is ever removed. The uncommitted-batches
  walk yields each of them — the batch the rejoin completed, and every
  batch you staged, skipped or discarded — at every restart, until
  archiving passes them. A discarded batch comes back committable.

The recipe for the restart:

1. Call `join_completed_open_batch()` first. If it reports an offset,
   record it durably as completed by the queue before the writer appends
   again.
2. Walk `mmapq_indexed_batch_appender_uncommitted_batches_checked()`.
3. Validate every batch's messages before committing the batch (for
   example a message count and checksum, or the application's own group id
   and sequence numbers). Do not commit a batch the queue completed — at
   this restart or an earlier one — unless that check proves it is the
   batch you meant to write.
4. Commit the batches that pass with
   `mmapq_indexed_batch_appender_commit_batch()` in your order, or skip
   them; a skipped batch reappears.

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
