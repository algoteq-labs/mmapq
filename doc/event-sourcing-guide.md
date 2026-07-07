# Building event-sourced systems on mmapq

This guide describes the architecture mmapq was built to serve: a
sequenced command/event system in which the single-message queue carries
commands and the batch queue carries events, and in which replay, recovery,
replication, and audit are all the same mechanism — reading a committed
log.

The pattern predates this library. It was developed for production FX
trading platforms and exists in two open-source Java implementations by the
same lineage: [tools4j/event-sourcing](https://github.com/tools4j/event-sourcing)
and its successor [elara](https://github.com/tools4j/elara), whose
terminology this guide borrows. It is a variation of the *sequencer*
pattern known from the LMAX architecture — with one structural difference
explained below (outputs are replicated, not inputs). mmapq provides the
storage layer for this architecture; it deliberately does not provide the
framework (see the last section).

## The shape of the system

```
 inputs                                                        outputs
 (gateways, sensors,          ┌─────────────────────┐
  upstream systems)           │  Command Processor   │
        │                     │                      │
        │   ┌──────────────┐  │  validate against    │   ┌──────────────┐
        ├──►│ COMMAND queue│─►│  state, decide,      │──►│  EVENT queue │
        ├──►│ (single-msg, │  │  emit events         │   │ (batch,      │
        └──►│  N appenders)│  │                      │   │  1 appender) │
            └──────────────┘  │  apply own events    │   └──────┬───────┘
                              │  to in-memory state  │          │
                              └─────────────────────┘          │
                                                                ▼
                                          ┌──────────────────────────────┐
                                          │ Event Appliers (followers,   │
                                          │ outputs, projections,        │
                                          │ analytics, replicas)         │
                                          └──────────────────────────────┘
```

- **Commands** are inputs: requests to change state (place order, start
  spray run, transfer funds). Anyone may submit one; nothing is true yet.
- The **Command Processor** is the single point of decision. It reads
  commands in queue order, validates them against the current application
  state (read-only), and emits **events** — statements of what *did*
  happen. It is the only writer of the event queue.
- **Events** are the truth. Application state is never modified directly;
  it exists only as the fold of the event stream. The processor applies
  its own events to its in-memory state exactly as any follower would.
- **Event Appliers** consume the event queue: replicas maintaining
  identical state, output publishers, projections, analytics. They never
  decide anything; they apply.

The queue-type mapping is not incidental — each queue type exists for its
role:

**The command queue is a single-message queue** because commands arrive
from many concurrent sources. mmapq's multi-appender design (up to 256
writers, each message committed by one atomic operation) lets every
gateway, sensor loop, or upstream bridge append directly, lock-free, with
the queue's index order serving as the arrival order the processor
consumes. Each command is independent, so per-message atomicity is the
right grain.

**The event queue is a batch queue** because one command may produce many
events, and those events are one transaction. The processor opens a batch,
appends each event as it produces it (applying it to its own state as it
goes), and commits the batch once — when, and only when, the command is
fully processed:

```
one command  ──processed into──►  one committed event batch
```

This one-to-one mapping is the backbone of the whole system. The batch
index correlates to the command; "what did command K do" is a single
indexed read.

## Why the batch commit is the transaction

`mmapq`'s batch commit publishes all messages in the batch with a single
atomic header write. Until that instant, the batch's events exist only in
the appender's uncommitted payload tail — invisible to every reader. This
buys three properties that otherwise require machinery:

**Followers never observe a partial command.** The replicated stream is a
sequence of complete command effects. Every inter-batch position is a
consistent snapshot of the system, which is what makes "catch up from
index N" trivially correct with no further coordination.

**Crash recovery is rollback, for free.** If the processor dies mid-command
— after appending three of five events — those three events were never
committed. On restart, appender recovery discards the uncommitted tail;
the processor rebuilds its in-memory state from *committed* events only,
and the half-processed command simply never happened. Re-submit it or
re-read it from the command queue. There is no undo log, no compensation
logic, no repair step: the failure state is unrepresentable in the
committed log. (For contrast: elara ships a *repair plugin* that appends a
rollback event when the last command's processing didn't finish — a fix-up
its Chronicle-backed store needs and the mmapq batch protocol makes
structurally unnecessary.)

**Applying before committing is safe.** The processor mutates its own
in-memory state while the batch is still uncommitted. This looks like it
violates "events are the truth" — it doesn't, because in-memory state is
disposable by definition. If the process survives, the commit lands and
disk agrees with memory. If it dies, memory is gone anyway and the rebuild
uses only committed events. The commit, not the apply, is the point of
truth.

The Rust and Python bindings encode the same semantics in their APIs:
`BatchWriter::commit(self)` consumes the writer (a batch cannot be touched
after commit), and dropping it — or leaving a Python `with` block via an
exception — discards the batch. An exception mid-command *is* a rollback.

## Determinism: replicate outputs, not inputs

The classic sequencer (LMAX-style active replication) sequences the
*commands* and requires every replica to re-execute them identically —
which makes determinism a property every line of processing code must
preserve forever: no wall clocks, no hash-iteration order, no floating
point divergence, or replicas silently fork.

This architecture replicates the *events* instead. Only the processor
executes; followers apply, which is a mechanically deterministic operation.
Anything non-deterministic — timestamps, generated IDs, random values,
external lookups — is resolved once by the processor and **captured into
the event**, so appliers never recompute it. Determinism by construction,
not by discipline. The costs are honest: followers cannot take over
mid-command (they wait for a batch boundary — which they'd want anyway),
and the event stream is larger than the command stream. For systems where
processing is expensive or hard to keep bit-identical, this is the better
trade.

## Replication, failover, and the appender lock

Followers are just batch pollers on the event queue — the same files over
a shared filesystem view, or shipped via the export format. Because every
committed batch boundary is a consistent state, a follower's position is
always a valid snapshot.

Failover reduces to a question the queue already answers: **who holds the
event queue's appender?** Appender IDs are exclusive (a shared atomic pool
enforces single ownership), so leadership of the system is ownership of
one appender. A promoted follower finishes applying the committed stream,
acquires the appender, and continues processing commands from where the
event stream proves the old leader stopped — the recovery scan positions
the new appender exactly after the last committed batch. Under a
supervisor, use a fixed appender ID for the processor so restart-after-crash
re-acquires deterministically; the `mmapq-ids` tool covers manual
reclamation of an orphaned ID (releasing an ID whose owner is alive will
corrupt the lane — the tool warns accordingly).

## Replay is the product

Because state is a fold over the event log, and the log is an indexed
file, the following are all the same one-line operation — create a reader
or poller and consume from index N:

- **Recovery** — the processor rebuilding state at startup.
- **Replication** — a follower staying current.
- **Debugging** — copy the queue directory from production to a dev
  machine, replay to the batch *before* the anomaly, attach a debugger,
  and step through the exact command that produced it, against the exact
  state it saw. Not similar state: the state.
- **Backtesting** — replay a recorded command stream (or feed a captured
  input log) through a modified processor and diff the event streams.
- **Audit** — "show me everything command K did" is a read of batch K.
  A regulator's question, a counterparty dispute, or a field incident
  resolves to an indexed lookup, at zero runtime logging cost. The hot
  path never formats a string; the event append *is* the record.

This is why the design tolerates no ambiguity in the log: mmapq refuses
to open corrupted or mismatched queues (`MMAPQ_ECORRUPT`, `MMAPQ_ESCHEMA`)
rather than repairing them. A repaired log is a forked timeline; a refused
log is a recreate-from-export. Determinism is only worth what the log's
integrity guarantees.

## Snapshots, archiving, and the long term

The event log grows forever only if you let it. The supported pattern:

1. Periodically export state — either fold the events into a snapshot
   artifact of your own, or use `QueueTransfer` to export the raw stream
   to the durable, simplified export format.
2. Create the event queue `archivable` (a creation-time flag) and use the
   archiver to advance `start_index` past snapshotted history;
   `archivable_files` reports which rolled files may be deleted.
3. Recovery then means: load snapshot, replay events since.

Non-archivable queues (the default) skip an acquire-load on the read path;
choose archivable only for queues you actually intend to trim. Queues are
transport plus recent history — long-lived truth belongs in the export
format or downstream storage.

## The processor loop, concretely

Pseudocode of the heart of the system (C API; Rust and Python are
one-to-one):

```c
IndexedQueue      *cmd_q = indexed_queue_create(dir, "commands", ...);
IndexedBatchQueue *evt_q = indexed_batch_queue_create(dir, "events", ...);

IndexedPoller        *commands = indexed_queue_create_poller(cmd_q);
IndexedBatchAppender *events   = indexed_batch_queue_create_appender(evt_q);

/* 1. Recover: rebuild state from committed events (or snapshot + tail). */
replay_committed_events(&state, evt_q);

/* 2. Position the command poller after the last command whose batch
      committed — the event stream records how far processing got. */
indexed_poller_move_to_index(commands, last_processed_command(&state) + 1);

/* 3. The loop. One command in, one batch out, atomically. */
NextMove on_command(int64_t index, uint8_t src, const void *cmd, int32_t len, void *ctx) {
    MutableBatch *b = indexed_batch_appender_new_batch(events);

    process_command(&state, cmd, len, b);   /* validates, emits N events  */
                                            /* via mutable_batch_append,  */
                                            /* applying each to `state`   */

    mutable_batch_commit(b);                /* command happened, atomically */
    return NEXT_ADVANCE;                    /* crash before this line ==   */
}                                           /* command never happened      */
```

Two details carry the correctness: the recovery order (events first, then
position the command poller from what the events prove), and the absence
of any state mutation outside `process_command`'s event-emission path.
Everything else is the queue's problem, which is the point.

## What mmapq deliberately does not provide

This library is the storage layer of the pattern — the two logs and their
atomicity, recovery, and replay semantics. It is not the framework. The
processor loop, input/output adapters, timers (genuinely tricky under
event sourcing — see elara's timer plugin for the canonical treatment),
snapshotting policy, and leadership orchestration are application
concerns, or concerns for a higher-level library built on top. That
layering is deliberate: the log's format and semantics are frozen so that
everything above them can change freely. If you need the full framework on
the JVM today, use [elara](https://github.com/tools4j/elara) — it is this
pattern, productized, by its co-originators.

## References

- [tools4j/elara](https://github.com/tools4j/elara) — zero-garbage Java
  event-sourcing framework; source of the terminology used here
- [tools4j/event-sourcing](https://github.com/tools4j/event-sourcing) —
  the earlier implementation of the same pattern
- Martin Fowler, [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
  and [The LMAX Architecture](https://martinfowler.com/articles/lmax.html)
