# 4. Re-sequenced commits and the uncommitted iterator

## The pattern

Commit order is index order — but write order need not be. A writer can
append payloads without committing them (`mmapq_indexed_appender_append_uncommitted`)
and commit later (`mmapq_indexed_appender_commit`), in any order. This is
how UDP resequencing and pipelined writes work on mmapq: packets are
appended as they arrive and committed when their sequence gap closes, so
readers see a strictly ordered history even though arrivals were
reordered.

Semantics:

- An uncommitted payload is invisible to consumers until its commit CAS.
- Committing assigns the next index at that moment and points it at the
  payload where it was written in the lane. Commit in sequence order and
  index order is sequence order, whatever order the payloads arrived in.
- An uncommitted payload **overtaken by archiving** can never become live:
  committing it returns `MMAPQ_EINVAL`; append it again instead. This is
  what keeps reclamation safe under resequenced commits.
- The per-lane state that makes this auditable is the archive line and the
  committed-range set, both in `appender_meta` — see
  [archive-boundaries](../design/archive-boundaries.md) for the design.

## The restart walk

After a crash, the rejoining lane owner recovers what it appended but never
committed with the uncommitted iterator:

```c
int rc;
MmapqUncommittedPayloadIterator *it = NULL;
rc = mmapq_indexed_appender_uncommitted_iterator_checked(
    a, MMAPQ_PAYLOAD_ITERATOR_NOT_SET, MMAPQ_PAYLOAD_ITERATOR_NOT_SET,
    &it);
assert(rc == 0);

const void *data; int32_t len; int64_t off;
while ((rc = mmapq_uncommitted_iterator_next(it, &data, &len, &off)) == 1) {
    /* decide: commit it, or skip it — after the torn-tail check below */
    int64_t idx = mmapq_indexed_appender_commit(a, off);
    assert(idx >= 0);
}
/* Three-way: 1 message / 0 exhausted / negative MMAPQ_E*. A negative is
 * truthy in C — compare explicitly, never
 * while (mmapq_uncommitted_iterator_next(...)) */
assert(rc == 0);
```

The iterator snapshots at the factory call; the appending context must not
be active while iterating. Commit found offsets in any subset, any order —
skipping is fine. Archiving may remove messages the walk has not yielded,
even after an undo.

In the bindings: Rust `IndexedAppender::append_uncommitted`, `commit` and
`uncommitted_iter`; Python `append_uncommitted()`, `commit()` and
`uncommitted()` on the appender.

Batch lanes: `mmapq_indexed_batch_appender_uncommitted_batches_checked()`
yields staged-but-uncommitted batches (offset + message iterator);
`mmapq_indexed_batch_appender_commit_batch()` publishes a found offset;
discarded batches reappear in recovery after a restart — recovery code
decides. `mmapq_indexed_batch_appender_join_completed_open_batch()`
reports a crash-interrupted open batch the rejoin completed to staged.

## The committed prefix under archiving

A re-sequencing writer that commits strictly in sequence order — message
*n* only after messages 0 to *n*−1 — knows after a restart that what it
committed is a prefix 0..C of its sequence numbers. This needs each
message to carry its sequence number (and, if other writers share the
queue, something that identifies the writer). Finding C:

- **If any of its messages is still live** (at or above `start_index`), C
  is the highest sequence number among them: scan the live range.
- **If archiving has passed all of them**, C has to come from outside the
  live queue: the writer's own commit log, corrected by the archived copies
  of the queue's files if you keep them.

The correction is needed because a commit and its log line are not one
atomic step. With up to *k* commits in flight, a crash can leave the log
up to *k* entries short of what was committed, and those messages may have
been archived since. Where the log and the archived copies disagree, the
copies are the queue's content and win.

Without archived copies — reclamation deletes the files and keeps nothing —
a commit that never reached the log and whose message is gone cannot be
told apart from one that never happened. Resolve it as **at-least-once**:
re-append from the logged C + 1, and have readers deduplicate by sequence
number. For exactly-once across a restart, either keep the archived copies
or never archive past the writer's last logged commit, so that every
message the log may be missing is still live.

## The torn tail

A crash can leave the record being written torn, and a record that is
not committed is never removed: one torn at an earlier crash reappears in
every later walk, no longer at the highest offset. Validate before you
commit, or remember what you discarded — the torn-tail rule and recipe
are in [chapter 5](05-crash-and-restart.md).
