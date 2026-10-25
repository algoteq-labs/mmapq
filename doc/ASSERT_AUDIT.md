# ASSERT_AUDIT.md — MMAP_ASSERT Inventory

**71 asserts across 21 files** (17 src/*.c, 4 src/internal/*.h). Generated from source.
(CAND-49: the overlap assert in mmapqi_uncommitted_iterator_advance became a
runtime MMAPQ_ECORRUPT return (spec §3.2); the ranges_insert assert moved to
committed_ranges.c with the extracted helpers.)
(CAND-17: mmapqi_header_read_sequential became `static inline` in
internal/header_reader.h so the call disappears into both pollers; its
WRAP_UNWRAPPED fatal assert moved with it — header_reader.c 2→1,
header_reader.h 0→1, total unchanged.)
Last verified: 2026-10-15 (CAND-32 — the header_encode 56-bit field check
(:113) is now guarded by MMAPQ_ECORRUPT runtime checks at both append
commit paths and the appender_meta open-time start_offset validation, and
is reclassified I; the pending-V table has no pending rows. Previously:
D6/CAND-18 — the gate was extended to `src/internal/*.h`, adding 8 header
sites, and the scope and counting rules below were recorded; D6 v2 had
reclassified :113 as V pending CAND-32 and corrected the queue_meta.h
rationale — the setter is called by both archivers).

## Legend

| Symbol | Meaning |
|---|---|
| **I** | Internal invariant — stays as assert |
| **V** | Validation — should be runtime check |

## Scope and counting rules

- Scanned scope: `src/*.c` and `src/internal/*.h`. The gate fails closed
  if either scope is absent.
- A site is a line whose first non-whitespace token is `MMAP_ASSERT(`.
  Comment mentions — including block-comment continuation lines — are not
  sites, and neither are `mmap_assert.h`'s own `#ifndef`/`#define` lines.
- Wrapper macros: a macro whose body is an MMAP_ASSERT is inventoried at
  its definition site only; invocation sites are not counted (an
  invocation does not contain the `MMAP_ASSERT` token).
  `ASSERT_HEADER_ALIGNED` in header_codec.h is the current instance; its
  invocation in indexed_appender.c is not a separate site.

## Summary

| Disposition | Count |
|---|---|
| I (stays, re-verified) | 71 |
| V (pending conversion) | 0 |
| (converted from V in Phase 16e) | 2 |
| **Total** | **71** |

## Per-file inventory

### appender_id_pool.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 143 pool NULL (release) | I | Internal: pool param from private struct, not caller |

### batch_payload_iterator.c — 1 (0 V)

| Line | Disposition | Verified |
|---|---|---|
| 83 padding mismatch (capacity == padded) | (converted, Phase 16e) | Now a runtime check: sets exhausted=1 and returns 0 instead of asserting. Note: the int-returning function has no `last_error` channel, so a corrupt padding record is indistinguishable from clean end-of-stream to a caller — intentional; adding an error field would be a struct/ABI change out of scope. |

### file_mapper_rolled.c — 10 (all I)

Lines 86, 157-158, 162, 184-186, 194, 200, 230.
Internal: null checks on self/entry, invariant checks on
oldest_idx/ref_count/length coherence. No caller-controlled values.

### file_mapper_single_ro.c — 6 (all I)

Lines 71-72, 110-112, 125. Internal dispatch null checks.

### file_mapper_single_rw.c — 7 (all I)

Lines 46, 136-137, 184-186, 198. Internal dispatch null checks.

### fixed_size_region.c — 3 (all I)

Lines 28-29, 52. Internal dispatch null checks.

### header_reader.c — 1 (all I)

Lines 55. Internal dispatch fatal check (WRAP_UNWRAPPED in mmapqi_header_read).

### header_reader.h — 1 (all I)

| Line | Disposition | Verified |
|---|---|---|
| 115 | I | WRAP_UNWRAPPED fatal in the static-inline mmapqi_header_read_sequential — internal accessor-state invariant (region corruption), unreachable from caller input. Release builds report MMAPQ_ECORRUPT via the err out-param instead of the pre-CAND-17 silent HEADER_NULL. |

### header_codec.h — 5 (all I)

| Line | Disposition | Verified |
|---|---|---|
| 136 | I | ASSERT_HEADER_ALIGNED macro definition — counted per the wrapper-macro rule; its invocation is not a separate site. Platform/mapping alignment invariant. |
| 147 | I | header_encode NULL_HEADER check — a non-positive on-disk start_offset is refused at appender_meta open (CAND-32 C1) before header_encode is reached. |
| 149 | I | header_encode 56-bit field check — guarded by MMAPQ_ECORRUPT runtime checks at both append commit paths (indexed_appender.c commit_header, indexed_batch_appender.c commit_batch) and the appender_meta open-time start_offset validation (CAND-32). |
| 227 | I | header_position: non-negativity of internally computed index. |
| 250 | I | header_index: non-negativity of internally computed position. |

### indexed_appender.c — 1 (V→I, Phase 6a)

| Line | Disposition | Verified |
|---|---|---|
| 180 length + 4 > region_size | I | Phase 6a converted. Guarded by EINVAL checks at public entry points. (CAND-49: line moved, site unchanged; CAND-54: moved again by the archivable-field struct addition, site unchanged; CAND-50: moved again by the torn-tail leading fence, site unchanged.) |

### indexed_poller.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 216 unknown handler directive | I | Internal: MMAP_ASSERT(0) guards unreachable default in switch. (CAND-17: line moved, site unchanged. DOC-01: moved by the X6 comment rewrite above it, site unchanged.) |

### indexed_reader.c — 1 (0 V)

| Line | Disposition | Verified |
|---|---|---|
| 163 length > 0 (on-disk) | (converted, Phase 16e) | Now a runtime check: sets MMAPQ_ENTRY_ERROR with last_error=MMAPQ_ECORRUPT. |

### payload_iterator.c — 4 (all I)

Lines 97, 110, 148-149. Internal dispatch null checks. (A5/CAND-22: init
and rewind gained `int` return channels; their NULL/not-initialised checks
became MMAPQ_EINVAL returns. D8: advance gained the three-way error
propagation — comment lines only above the catalogued sites.)

### queue_meta.h — 1 (all I)

Lines 156. (DOC-01: line moved by the schema-2 contract comment above the
constants and again by the X6/Java comment corrections, site unchanged.)
Archivable contract on the start_index setter; called by both archivers
(`queue_meta_set_start_index` in `indexed_archiver.c` and
`indexed_batch_archiver.c`, two sites each), guarded by the
`queue_meta_archivable` EINVAL check in `archive_up_to_index_locked`.
(The in-code comment claiming the setter was unused was corrected under
CAND-32.)

### queue_region_accessor.h — 2 (all I)

Lines 138-139.
Internal dispatch NULL checks.

### region_async.c — 8 (all I)

Lines 216, 297-298, 328, 373, 386, 401, 411. Internal dispatch null checks.

### region_ring.c — 2 (all I)

Lines 21, 189. Internal dispatch null checks. (CAND-24: shifted — the
stdio include and AsyncRingImpl's timeout_ns field.)

### region_ring_accessor.c — 5 (all I)

Lines 66-67, 113-114, 139. Internal dispatch null checks.

### region_sync.c — 7 (all I)

Lines 44, 81, 176-177, 183-184, 190. Internal dispatch null checks.

### runtime.c — 1 (all I)

Lines 548. Internal threading-contract check. (CAND-26: the
unregister_and_wait helper-registration assert became a retry loop —
allocation failure is transient and the function's contract leaves no
room for giving up.)

### uncommitted_payload_iterator.c — 3 (all I)

Lines 364, 475, 491. Internal dispatch null checks. (CAND-51e: the
gap-end padding-skip check shifted the last two from 434/450. A5/CAND-22:
init and rewind NULL checks became MMAPQ_EINVAL returns. D8: scan floor,
checked header read, and three-way advance propagation shifted the
catalogued sites. CAND-49: the gap-overlap MMAP_ASSERT(0) became a runtime
MMAPQ_ECORRUPT return (spec §3.2 — the exact-gap-end contract); the
ranges_insert start < end assert moved to committed_ranges.c.)

### uncommitted_batch_iterator.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 227 next: it NULL check | I | Internal dispatch null check. (CAND-49 §3.6 unit. CAND-50: line moved by the zero-line wording fix, site unchanged.) |

### committed_ranges.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 32 start < end | I | Internal: raw-span append precondition; callers build spans from walked messages. (Moved from uncommitted_payload_iterator.c by CAND-49 §3.1.) |

## Pending V-class conversions

| File | Line | Assert | Action |
|---|---|---|---|
| `batch_payload_iterator.c` | 83 | capacity == padded | (converted Phase 16e — runtime check) |
| `header_codec.h` | 113 | payload_position exceeds 56-bit field | (resolved CAND-32 — guarded invariant: ECORRUPT runtime checks at both append commit paths + appender_meta open-time start_offset validation) |
| `indexed_reader.c` | 163 | length > 0 | (converted Phase 16e — runtime check) |

## Files with zero asserts (all V-class already converted)

`indexed_batch_queue.c`, `indexed_batch_appender.c`,
`indexed_batch_poller.c`, `indexed_batch_reader.c`,
`indexed_batch_archiver.c`, `indexed_archiver.c`,
`indexed_queue.c`, `queue_meta.c`, `appender_meta.c`,
`payload_walk.c`, `queue_region_accessor.c`, `file_initialiser.c`,
`queue_latency_probe.c`, `batch_queue_latency_probe.c`,
`queue_transfer.c`, `batch_queue_transfer.c`, `queue_files.c`

The `src/internal` headers not listed above have only comment mentions of
MMAP_ASSERT, which the site rule excludes.
