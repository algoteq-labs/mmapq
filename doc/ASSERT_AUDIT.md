# ASSERT_AUDIT.md — MMAP_ASSERT Inventory

**63 asserts across 15 src/*.c files.** Generated from source.
Last verified: 2026-10-11 (CAND-26 — `mmapq_runtime_unregister_and_wait`'s
assert on the helper-register result became a real `if (!ok) return;`
when register learned to return 0 on allocation failure).

## Legend

| Symbol | Meaning |
|---|---|
| **I** | Internal invariant — stays as assert |
| **V** | Validation — should be runtime check |

## Summary

| Disposition | Count |
|---|---|
| I (stays, re-verified) | 63 |
| V (pending conversion) | 0 |
| (converted from V in Phase 16e) | 2 |
| **Total** | **63** |

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

### header_reader.c — 2 (all I)

Lines 55, 88. Internal dispatch null checks.

### indexed_appender.c — 1 (V→I, Phase 6a)

| Line | Disposition | Verified |
|---|---|---|
| 127 length + 4 > region_size | I | Phase 6a converted. Guarded by EINVAL checks at public entry points. |

### indexed_poller.c — 1 (1 I)

| Line | Disposition | Verified |
|---|---|---|
| 207 unknown handler directive | I | Internal: MMAP_ASSERT(0) guards unreachable default in switch. |

### indexed_reader.c — 1 (0 V)

| Line | Disposition | Verified |
|---|---|---|
| 163 length > 0 (on-disk) | (converted, Phase 16e) | Now a runtime check: sets MMAPQ_ENTRY_ERROR with last_error=MMAPQ_ECORRUPT. |

### payload_iterator.c — 4 (all I)

Lines 97, 110, 145-146. Internal dispatch null checks. (A5/CAND-22: init
and rewind gained `int` return channels; their NULL/not-initialised checks
became MMAPQ_EINVAL returns.)

### region_async.c — 8 (all I)

Lines 216, 297-298, 328, 373, 386, 401, 411. Internal dispatch null checks.

### region_ring.c — 2 (all I)

Lines 20, 187. Internal dispatch null checks.

### region_ring_accessor.c — 5 (all I)

Lines 66-67, 113-114, 139. Internal dispatch null checks.

### region_sync.c — 7 (all I)

Lines 44, 81, 176-177, 183-184, 190. Internal dispatch null checks.

### runtime.c — 1 (all I)

Lines 548. Internal threading-contract check. (CAND-26: the
unregister_and_wait helper-registration assert became a retry loop —
allocation failure is transient and the function's contract leaves no
room for giving up.)

### uncommitted_payload_iterator.c — 5 (all I)

Lines 30, 348, 362, 381, 397. Internal dispatch null checks. (A5/CAND-22:
init and rewind NULL checks became MMAPQ_EINVAL returns.)

## Pending V-class conversions

| File | Line | Assert | Action |
|---|---|---|---|
| `batch_payload_iterator.c` | 83 | capacity == padded | (converted Phase 16e — runtime check) |
| `indexed_reader.c` | 163 | length > 0 | (converted Phase 16e — runtime check) |

## Files with zero asserts (all V-class already converted)

`indexed_batch_queue.c`, `indexed_batch_appender.c`,
`indexed_batch_poller.c`, `indexed_batch_reader.c`,
`indexed_batch_archiver.c`, `indexed_archiver.c`,
`indexed_queue.c`, `queue_meta.c`, `appender_meta.c`,
`payload_walk.c`, `queue_region_accessor.c`, `file_initialiser.c`,
`queue_latency_probe.c`, `batch_queue_latency_probe.c`,
`queue_transfer.c`, `batch_queue_transfer.c`, `queue_files.c`
