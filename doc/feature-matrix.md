# mmap-c Feature Matrix — C / Rust / Python

Feature coverage across the three repositories as of the archivable commit
(feature freeze). Legend: ✓ full · ◐ partial (see note) · — not exposed.

**Compatibility scope:** all three implementations target the wire format
of Algoteq's Java implementation — an evolved derivative of
[tools4j/mmap](https://github.com/tools4j/mmap). The formats have
diverged; these libraries are NOT compatible with upstream tools4j.

The C library is the complete surface and the source of truth. Rust exposes
near-complete parity. **Python deliberately exposes the producer/consumer
subset** — append and poll for both queue types — which covers the dominant
Python use case (writing from or feeding into Python processes); random
access, transfer, archiving, and low-level batch construction remain
C/Rust territory. Gaps marked — in the Python column are design decisions,
not roadmap items.

## Queue lifecycle

| Feature | C | Rust | Python |
|---|---|---|---|
| Create/open single-message queue (full config) | ✓ | ✓ `QueueConfig` builder | ◐ dir/name + `archivable` kwarg; other params defaulted |
| Create/open batch queue | ✓ | ✓ | ◐ same |
| `archivable` creation parameter (default 0) | ✓ | ✓ `.archivable(bool)` | ✓ `archivable=False` kwarg |
| Async region mapping via Runtime (background mapper) | ✓ | ✓ `Runtime` + singletons | — (NULL runtime → sync mapping; documented) |
| Custom runtime config (idle strategies, close timeout) | ✓ | ✓ | — |
| Queue metadata accessors (region size, max file size, start index) | ✓ | ✓ | — |
| queue_type discriminator validated on every open (normal/batch/fixed-8) | ✓ | ✓ (via C) | ✓ (via C) |
| schema_version validated on every open | ✓ | ✓ (via C) | ✓ (via C) |

## Appending (single-message)

| Feature | C | Rust | Python |
|---|---|---|---|
| Atomic append-and-commit | ✓ | ✓ | ✓ |
| Append uncommitted + explicit commit | ✓ | ✓ | — |
| Appender with specific ID (fixed-ID deployments) | ✓ | ✓ | — |
| Appender ID / current index / start offset accessors | ✓ | ✓ | ◐ id only |
| Committed / uncommitted payload iterators | ✓ | ◐ via FFI layer | — |
| Zero-length append rejected (`MMAPQ_EINVAL`) at runtime | ✓ | ✓ `Err(InvalidArgument)` | ✓ raises `ValueError` |
| Crash recovery: appender resumes position after restart | ✓ | ✓ (via C) | ✓ (via C) |

## Batch appending

| Feature | C | Rust | Python |
|---|---|---|---|
| Start batch → append N messages → atomic commit (command/event transaction pattern) | ✓ | ✓ `BatchWriter`, commit consumes writer | ✓ context manager, discard on exception |
| Discard batch (rollback) | ✓ | ✓ (drop) | ✓ (exception path) |
| Single-call append_and_commit | ✓ | ✓ | ✓ |
| Reserved append (write into reserved space) / append_remaining_from | ✓ | ✓ | — |
| In-batch message iterator / message count | ✓ | ✓ | ✓ (handler-side iteration) |

## Consuming — pollers (sequential)

| Feature | C | Rust | Python |
|---|---|---|---|
| Poll with handler; ADVANCE / STOP / RETREAT flow control | ✓ | ✓ (panic-safe trampoline) | ✓ |
| move_to_index / start / end / last | ✓ | ✓ | ✓ |
| exists(index), current index accessors | ✓ | ✓ | ✓ |
| Batch poller (same controls, batch granularity) | ✓ | ✓ | ✓ |

## Consuming — readers (random access)

| Feature | C | Rust | Python |
|---|---|---|---|
| Read at arbitrary index; read_first / read_last | ✓ | ✓ (`&mut`, one live message) | — |
| Entry status (present / not yet / archived) | ✓ | ✓ | — |
| last_index / start_index | ✓ | ✓ | — |
| Batch reader | ✓ | ✓ | — |

## Archiving

| Feature | C | Rust | Python |
|---|---|---|---|
| Archiver create / archivable check | ✓ | ✓ | — |
| archive_up_to_index (advances start_index; returns `MMAPQ_EINVAL` on non-archivable) | ✓ | ✓ | — |
| archivable_files (which files may be deleted) | ✓ | ✓ | — |
| print_summary | ✓ | ◐ single-message only (no batch C function) | — |
| Batch archiver | ✓ | ◐ create/close | — |

## Transfer / export (durable long-term format)

| Feature | C | Rust | Python |
|---|---|---|---|
| Export queue → stream (full + chunked) | ✓ | ✓ | — |
| Import stream → queue | ✓ | ✓ | — |
| Queue-to-queue copy | ✓ | ✓ | — |
| Batch variants (incl. flat-stream import) | ✓ | ✓ | — |

## Cross-cutting guarantees (all languages, enforced in the C core)

| Guarantee | Status |
|---|---|
| Wire compatibility with Algoteq's Java implementation (tools4j-derived; NOT compatible with upstream tools4j — formats have diverged) | ✓ by construction, single-message queues; batch queues are C-ecosystem only |
| Up to 256 concurrent appenders, lock-free atomic per-message commit | ✓ |
| Distinct error codes: `ETIMEDOUT/EINVAL/ECLOSED/ECORRUPT/ESCHEMA` mapped 1:1 — Rust `MmapError` variants, Python exception classes | ✓ |
| Corruption refused on open (`ECORRUPT`), never repaired; recreate from export | ✓ |
| Validation survives release builds (`-DNDEBUG`); regression tests run under exact release flags | ✓ |
| Multi-process safety: cross-process interop tests (Rust ↔ C echo servers) | ✓ |
| Fixed-8-byte mode (Algoteq Java only): refused with diagnostic (not implemented) | ✓ documented |

## Operator tooling (C repo)

| Tool | Purpose | Status |
|---|---|---|
| `mmapq-ids` | Appender ID pool inspection; CAS release of orphaned IDs | available (single-message + batch dirs) |
| `mmapq-stat` | Read-only queue inspection; `--verify` framing/header cross-check | available; batch framing verify not yet supported |

---

### Notes for README use

- The Python paragraph above (producer/consumer subset, gaps are design
  decisions) is the "documented subset" statement — lift it verbatim into
  `mmap-c-py/README.md`.
- Rust's two ◐ archiver cells are the only unplanned asymmetries vs C;
  everything else in the Rust column is at parity. If batch
  `print_summary` is ever wanted, it is written properly in C first
  (never type-punned from the single-message function).
- Feature set is frozen as of this matrix. The sole sanctioned addition
  category is parity completion with the Algoteq Java surface
  (the tools4j-derived reference implementation), by explicit task
  (see `mmap-c/AGENTS.md`).
