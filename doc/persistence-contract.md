# Publication, replay, and persistence

Queue files preserve readable history across cooperating process restarts.
They do not provide power-loss-durable commit acknowledgments.

- Commit publishes completed payloads through the existing atomic header
  protocol. It does not call `msync`, `fsync`, or `fdatasync`.
- Readers can reopen history. Consumers own their checkpoints and must
  explicitly resume at the desired index; the library persists no consumer
  position. If a checkpoint precedes the archived start index, the consumer
  must detect the unavailable history and apply its own recovery policy.
- Recovery refuses to create an appender when the end-of-stream scan
  encounters corruption (`MMAPQ_ECORRUPT`) or a transient mapping timeout
  (`MMAPQ_ETIMEDOUT` — retryable); a refused start never recovers to a
  position inside or past the committed stream. Corrupt queues are
  recreated, not repaired.
- Process termination with the operating system still running is distinct
  from kernel failure or power loss. The coordinated Python restart tests
  cover acknowledged committed data, invisible uncommitted tails, and
  continued appends after SIGKILL for normal and batch queues.
- Successful export and output close mean that buffered writes completed
  without a reported error, not that storage survived a power failure.
- Clean machine restart depends on filesystem/OS writeback. No stronger
  crash-ordering or durable acknowledgment guarantee is supplied here.
- `/dev/shm` is volatile tmpfs. `/tmp` may be tmpfs or be cleaned at reboot.
  Use an explicitly chosen file-backed directory for retained-history
  examples, with retention and filesystem behavior managed by the operator.
- Unlinking archived files is not permission to truncate or overwrite live
  mapped files. Arbitrary external modification can invalidate the queue's
  safety and publication assumptions.
- An uncommitted payload overtaken by archiving can never become live:
  committing it returns `MMAPQ_EINVAL` (append it again instead). This is
  what keeps reclamation safe under resequenced commits
  (`append_uncommitted` + `commit` in any order).
- `archivable_files()` is a snapshot, not permission to delete. Mark the
  listed files, wait, and delete only files that a fresh listing taken
  after the wait still names. The wait must outlast a writer's stall
  between publishing a commit and its boundary re-check; a writer process
  stopped in that window (SIGSTOP, a debugger) is not covered. The library
  does not enforce this protocol — deletion safety rests on the caller.
  See `archiving-design.md`.
- `archive.lock` is locked by inode. Deleting or recreating the queue
  directory while an archiver or lister has it open lets two processes
  hold locks on different files. Recreate a queue only with all clients
  closed — the same rule as for every other queue file.
- Normal queues retain compatibility with Algoteq's evolved Java format;
  Java does not currently read batch queues. No disk layout changes are
  introduced by the hardening work.

`mmapq-py/tests/test_persistence_contract.py` runs producers and consumers
in separate interpreters, checks the backing filesystem with `findmnt`,
uses pipe acknowledgments rather than sleeps, and resumes from an explicit
application checkpoint. Set `MMAPQ_FILE_TEST_DIR` to a writable file-backed
directory to override its repository-local temporary directory. These are
process-restart tests, not simulated power cuts or reboot tests.

No WAL, durability worker, per-commit flush, or automatic repair is added.
