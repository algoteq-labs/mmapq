# ASSERT_AUDIT.md — MMAP_ASSERT Inventory

**76 asserts across 18 src/*.c files.** Generated from source.
Last verified: 2026-08-13 (Phase 14 — regenerated).

## Legend

| Symbol | Meaning |
|---|---|
| **I** | Internal invariant — stays as assert |
| **V** | Validation — should be runtime check |

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
| 143 pool NULL (release) | I | Internal: pool param from private struct, not caller |

### batch_payload_iterator.c — 1 (1 V)

| Line | Disposition | Verified |
|---|---|---|
| 83 padding mismatch (capacity == padded) | V | On-disk padding vs capacity mismatch; corruption. Convert to MMAPQ_ECORRUPT. |

### file_mapper_rolled.c — 10 (all I)

Lines 81, 152-153, 157, 179-181, 189, 194, 224.
Internal: null checks on self/entry, invariant checks on
oldest_idx/ref_count/length coherence. No caller-controlled values.

### file_mapper_single_ro.c — 6 (all I)

Lines 71-72, 110-112, 125. Internal dispatch null checks.

### file_mapper_single_rw.c — 7 (all I)

Lines 46, 138-139, 186-188, 200. Internal dispatch null checks.

### fixed_size_region.c — 4 (all I)

Lines 28-29, 52, 68. Internal dispatch null checks.

### header_reader.c — 2 (all I)

Lines 37, 70. Internal dispatch null checks.

### indexed_appender.c — 1 (V→I, Phase 6a)

| Line | Disposition | Verified |
|---|---|---|
| 124 length + 4 > region_size | I | Phase 6a converted. Guarded by EINVAL checks at public entry points. |

### indexed_poller.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 203 unknown handler directive | I | Internal: MMAP_ASSERT(0) guards unreachable default in switch. |

### indexed_reader.c — 1 (1 V)

| Line | Disposition | Verified |
|---|---|---|
| 163 length > 0 (on-disk) | V | On-disk payload length from mapped file. Could be zero/negative due to truncation. |

### payload_iterator.c — 7 (all I)

Lines 57, 99, 112, 147-148, 159-160. Internal dispatch null checks.

### payload_walk.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 58 long padding mismatch (capacity == skip) | I | Internal consistency check. |

### region_async.c — 9 (all I)

Lines 216, 297-298, 324, 364, 372, 385, 400, 410. Internal dispatch null checks.

### region_ring.c — 2 (all I)

Lines 19, 164. Internal dispatch null checks.

### region_ring_accessor.c — 6 (all I)

Lines 66-67, 109-110, 131, 139. Internal dispatch null checks.

### region_sync.c — 8 (all I)

Lines 45, 82, 171-172, 178-179, 185, 193. Internal dispatch null checks.

### runtime.c — 2 (all I)

Lines 501, 757. Internal dispatch null checks.

### uncommitted_payload_iterator.c — 7 (all I)

Lines 30, 269, 312, 326, 345, 361, 369. Internal dispatch null checks.

## Pending V-class conversions

| File | Line | Assert | Action |
|---|---|---|---|
| `batch_payload_iterator.c` | 83 | capacity == padded | Return MMAPQ_ECORRUPT on mismatch |
| `indexed_reader.c` | 163 | length > 0 | Set ENTRY_ERROR instead of assert |

## Files with zero asserts (all V-class already converted)

`indexed_batch_queue.c`, `indexed_batch_appender.c`,
`indexed_batch_poller.c`, `indexed_batch_reader.c`,
`indexed_batch_archiver.c`, `indexed_archiver.c`,
`indexed_queue.c`, `queue_meta.c`, `appender_meta.c`,
`queue_region_accessor.c`, `file_initialiser.c`,
`queue_latency_probe.c`, `batch_queue_latency_probe.c`,
`queue_transfer.c`, `batch_queue_transfer.c`, `queue_files.c`
