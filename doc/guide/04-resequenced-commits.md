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
- Committing publishes at the payload's own position; a later-arriving
  packet committed earlier simply fills its place in the history.
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
    /* decide: commit it, or skip it */
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

Batch lanes: `mmapq_indexed_batch_appender_uncommitted_batches_checked()`
yields staged-but-uncommitted batches (offset + message iterator);
`mmapq_indexed_batch_appender_commit_batch()` publishes a found offset;
discarded batches reappear in recovery after a restart — recovery code
decides. `mmapq_indexed_batch_appender_join_completed_open_batch()`
reports a crash-interrupted open batch the rejoin completed to staged.

## The torn tail

One caveat applies to what the walk can yield after a crash: see the
torn-tail rule and recipe in [chapter 5](05-crash-and-restart.md).
