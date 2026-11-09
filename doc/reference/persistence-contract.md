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
- `archivable_files()` lists files no live entry can need at the time of
  the call. Delete them with `reclaim_files()` (always safe). If you
  delete, move or compress them yourself, they are safe provided no undo
  (`archive_up_to_index` below the current `start_index`) runs between
  this call and your last unlink. An undo that ran before this call is
  harmless, because the listing already reflects it. (0.2.0 retired the
  deletion wait: the commit-intent protocol never lists a file a stalled —
  or stopped — commit is about to publish into.)
  See `../design/archiving-design.md`.
- `archive.lock` is locked by inode. Deleting or recreating the queue
  directory while an archiver or lister has it open lets two processes
  hold locks on different files. Recreate a queue only with all clients
  closed — the same rule as for every other queue file.
- Queue geometry is constrained: `region_size` must be a power of
  two and a multiple of 32 KB (one transposed header block), and a multiple
  of the host's page size; `max_file_size` must be a multiple of `region_size`
  (and therefore of the 32 KB block). Header-file reclaim depends on each
  header file holding a contiguous index range, which only block-aligned
  files guarantee. A queue created before this rule, or on a host with a
  larger page size, is refused on reopen with `MMAPQ_ESCHEMA` (not
  `MMAPQ_ECORRUPT` — the files are not damaged): recreate it with a legal
  geometry. Portability consequence: a queue is portable only to hosts
  whose page size divides its `region_size`.
- Schema 2 (0.2.0) changed what 0.1.0 files mean and accept (the
  archive-line field, the geometry rule, the unified tail rule): 0.2.0
  writes `schema_version=2` and refuses 0.1.0 files at open with
  `MMAPQ_ESCHEMA`. To move 0.1.0 data forward, export with a 0.1.0 library
  and import into a fresh 0.2.0 queue — the export stream format is
  unchanged. mmapq's file format derives from Algoteq's Java
  implementation (an evolved derivative of tools4j/mmap; not compatible
  with upstream tools4j). In 0.2.0, C, Rust and Python do not share queues
  with Java: 0.2.0 refuses Java-written queues at open (schema_version 1),
  and the Java implementation does not yet follow the 0.2.0 protocol (for
  example, it does not maintain the archive line). This applies to both
  queue types. Read Java-written queues with the Java library. Java
  support is planned at schema 2, under the same protocol contract (see
  `../reference/wire-format.md`).

`mmapq-py/tests/test_persistence_contract.py` runs producers and consumers
in separate interpreters, checks the backing filesystem with `findmnt`,
uses pipe acknowledgments rather than sleeps, and resumes from an explicit
application checkpoint. Set `MMAPQ_FILE_TEST_DIR` to a writable file-backed
directory to override its repository-local temporary directory. These are
process-restart tests, not simulated power cuts or reboot tests.

No WAL, durability worker, per-commit flush, or automatic repair is added.
