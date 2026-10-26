# 9. Trade-offs and limits

Stated plainly. Each of these is a design position; the site's Trade-offs
page uses the same text.

- **One machine.** mmapq is same-host inter-process messaging. The wire
  path belongs to a network messaging system.
- **Linux only, x86-64/ARM64.** Requires naturally aligned lock-free
  atomics over `MAP_SHARED` mmap — asserted at build. Verified on x86-64;
  ARM64: run `test/multiprocess/` on an aarch64 box before relying on it.
- **Durability across process restarts, not power loss.** No commit path
  calls `fsync`/`fdatasync`; writes reach the page cache and the OS
  persists them eventually. Long-term data belongs in the
  [export format](07-export-import-migration.md) or a database.
- **Consumers own their checkpoints.** The library persists no consumer
  position; a checkpoint that precedes the archived start index must be
  detected by the application, which applies its own recovery policy.
- **A SIGKILLed writer's lane needs `mmapq-ids --unset`** — valid on a
  dead process only (the [restart procedure](05-crash-and-restart.md)).
- **Up to 256 writers** per queue, over the queue's whole lifetime unless
  IDs are released.
- **Maximum message size is `region_size − 4`.**
- **Archiving and deletion are driven by the application.** The library
  provides the listing, the mark and the safe reclaim; scheduling and
  policy are yours ([chapter 6](06-archiving-and-reclamation.md)).
- **Mixed C/Java queues are not supported in 0.2.0** — Java-written queues
  are refused at open; Java support is planned at schema 2 under the
  [protocol contract](../reference/wire-format.md).
- **0.1.0 queues are refused at open** (schema 2). Migrate via
  export/import ([chapter 7](07-export-import-migration.md)) or recreate.

## When to use it — and when not to

**Use mmapq when** you need same-host inter-process messaging where the
history matters: event-sourced services, sequenced order/command flow,
market-data capture, sensor/telemetry logs that a second process analyses,
replay-based testing and debugging.

**Use something else when:**

- **You need network transport.**
- **You want pub/sub with many dynamic topics** and no interest in history
  — mmapq queues are named, persistent streams, not a topic fabric.
- **You want a full application framework** rather than a storage layer —
  see the references in the [event-sourcing guide](event-sourcing.md).
- **You need guaranteed durability through power loss.**
- **You need Windows or macOS.**

What you give up relative to the alternatives is scope. What you gain is a
system small enough to audit end-to-end: ~10k lines of dependency-free C11,
a frozen on-disk format specified in one document, and a test suite that
runs under the exact release flags that ship.
