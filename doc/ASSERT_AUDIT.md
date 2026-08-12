# ASSERT_AUDIT.md — MMAP_ASSERT Inventory

**76 asserts across 18 src/*.c files.** Generated from source.
Last verified: 2026-08-11 (Phase 7 — regenerated).

## How this document is maintained

The per-file counts are generated mechanically by
`scripts/audit_asserts.sh`. Before editing this file, run:
`bash scripts/audit_asserts.sh` and verify the totals match.
Only the V/I disposition column is hand-maintained — re-verify
every row before changing it.

## Legend

| Symbol | Meaning |
|---|---|
| **I** | Internal invariant — stays as assert (logic error, unreachable code, hot-path sanity after caller-side guard installed) |
| **V** | Validation — should be runtime check (caller input, on-disk data, allocation, I/O, cross-process state). Pending conversion. |

## Summary

| Disposition | Count |
|---|---|
| I (stays, re-verified) | 73 |
| V (pending conversion) | 3 |
| **Total** | **76** |

## Per-file inventory

### appender_id_pool.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 143 pool NULL (release) | I | Internal: pool param comes from private struct, not caller |

### batch_payload_iterator.c — 1 (1 V)

| Line | Disposition | Verified |
|---|---|---|
| 83 padding mismatch (capacity == padded) | V | On-disk padding data compared against wrapped capacity; corruption causes mismatch. Reachable from poll/read via batch_iterator_advance. Convert to MMAPQ_ECORRUPT. |

### file_mapper_rolled.c — 10 (all I)

Lines 81, 152-153, 157, 179-181, 189, 194, 224.
Internal: null checks on self/entry, invariant checks on
oldest_idx/ref_count/length coherence. No caller-controlled values.

### file_mapper_single_ro.c — 6 (all I)

Lines 70-71, 109-111, 124. Internal dispatch null checks.

### file_mapper_single_rw.c — 7 (all I)

Lines 45, 137-138, 185-187, 199. Line 45 checks internal invariant
between two fields set by factory — not caller-accessible after creation.

### fixed_size_region.c — 4 (all I)

Lines 27-28, 51, 67. Internal dispatch null checks.

### header_reader.c — 2 (all I)

Lines 37, 70. MMAP_ASSERT(0) for unreachable code paths.

### indexed_appender.c — 1 (V→I, Phase 6a)

| Line | Disposition | Verified |
|---|---|---|
| 124 length + 4 > region_size | I | Phase 6a converted. Enforcing entry points: `indexed_appender_append` (line 523), `indexed_appender_append_uncommitted` (line 541), `mutable_batch_append` (indexed_batch_appender.c:427), `mutable_batch_append_reserved` (indexed_batch_appender.c:438). No caller-controlled length reaches this assert without a prior EINVAL check. |

### indexed_poller.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 194 unknown handler directive | I | Internal logic error: handler always returns valid directive |

### indexed_reader.c — 1 (1 V)

| Line | Disposition | Verified |
|---|---|---|
| 158 length > 0 (read inner, on-disk) | V | On-disk payload length read via memcpy from mapped file. Could be zero/negative due to truncation or partial write. Reachable from `indexed_reader_read` and `indexed_reader_read_view`. `WRAP_SUCCESS` check directly above already sets `ENTRY_ERROR` — convert to early return with `ENTRY_ERROR` status. |

### payload_iterator.c — 7 (all I)

Lines 57, 99, 112, 147-148, 159-160: NULL/state checks on `it` — I.
Phase 7a converted line 131 to `if (length <= 0) return 0;` (mapping timeout → end-of-stream).

### payload_walk.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 58 long padding size mismatch (capacity == skip) | I | Internal consistency check: both capacity and skip derive from the same on-disk padding marker through different code. Mismatch indicates logic bug rather than externally-reachable corruption. |

### region_async.c — 9 (all I)

Lines 216, 292-293, 319, 359, 367, 380, 395, 405.
Internal dispatch null checks and unreachable-code guards.

### region_ring.c — 2 (all I)

Lines 19, 164. Internal dispatch null checks (close functions).

### region_ring_accessor.c — 6 (all I)

Lines 66-67, 109-110, 131, 139. Internal dispatch null checks.

### region_sync.c — 8 (all I)

Lines 45, 82, 171-172, 178-179, 185, 193.
Internal dispatch null checks.

### runtime.c — 2 (both I)

Lines 505 (same-thread invariant), 761 (helper-register ok).
Internal invariants after public entry-point validation.

### uncommitted_payload_iterator.c — 7 (all I)

Lines 30, 269, 312, 326, 345, 361, 369.
Internal state checks on ranges iterator. No caller-controlled values
reach these paths. Phase 7a converted 5 scan_committed_ranges asserts
to runtime checks (advance failure → return 0 from scan). Phase 7 close
converted lines 312 and 369 (initialised guards) from asserts to runtime
checks.

## Pending V-class conversions

| File | Line | Assert | Action |
|---|---|---|---|
| `batch_payload_iterator.c` | 83 | capacity == padded | Return MMAPQ_ECORRUPT on mismatch |
| `indexed_reader.c` | 139 | length > 0 | Set ENTRY_ERROR instead of assert |

## Files with zero asserts (all V-class already converted)

`indexed_batch_queue.c`, `indexed_batch_appender.c`,
`indexed_batch_poller.c`, `indexed_batch_reader.c`,
`indexed_batch_archiver.c`, `indexed_archiver.c`,
`indexed_queue.c`, `queue_meta.c`, `appender_meta.c`,
`queue_region_accessor.c`, `file_initialiser.c`,
`queue_latency_probe.c`, `batch_queue_latency_probe.c`,
`queue_transfer.c`, `batch_queue_transfer.c`, `queue_files.c`
