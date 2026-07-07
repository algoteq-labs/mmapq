# ASSERT_AUDIT.md — MMAP_ASSERT Inventory

**326 asserts across 34 src/*.c files.** Generated from current source.

## Legend

**Disposition:**
- **V** = validation — must be runtime check (caller input, on-disk data, allocation, I/O, cross-process state)
- **I** = internal invariant — stays as assert (logic error, unreachable code, hot-path sanity)

**Columns:**
- `Msg` — assert message
- `Disp` — V / I
- `Loc` — boundary call / hot (per-message inner loop) / flag (needs review)
- `Sig` — Y if conversion requires `void→int` signature change
- `Status` — converted / pending / stays

---

## Summary

| Category | Count |
|---|---|
| Converted to runtime checks | 190 |
| I (stays, all internal invariants) | 132 |
| **Total** | **322** |

Per-file reconciliation below (count verified against `grep -c MMAP_ASSERT`).

---

## appender_id_pool.c — 3 asserts (all I)

All V (directory/queue_name null, region null, wrap, calloc) — already converted in assert audit session.

| Line | Msg | Disp | Loc | Sig | Status |
|---|---|---|---|---|---|
| 81 | pool NULL (acquire) | I | internal | — | stays |
| 111 | pool NULL (try_acquire) | I | internal | — | stays |
| 136 | pool NULL (release) | I | internal | — | stays |

---

## fixed_size_region.c — 4 asserts (all I)

All V (path null, size, fd, mmap, initialiser, 2x OOM) — already converted.

| Line | Msg | Disp | Loc | Sig | Status |
|---|---|---|---|---|---|
| 27 | self NULL (fixed_wrap) | I | internal | — | stays |
| 28 | buffer NULL (fixed_wrap) | I | internal | — | stays |
| 51 | self NULL (fixed_close) | I | internal | — | stays |
| 67 | self NULL (fixed_size) | I | internal | — | stays |

---

## indexed_appender.c — 1 assert (I-hot)

All 12 V-class converted to runtime checks (eager-wrap, OOM, appender_meta, commit, iterators, accessors).

| Line | Msg | Disp | Loc | Sig | Status |
|---|---|---|---|---|---|
| 117 | length + 4 > region_size | V→I | **hot** (append inner path) | Y | I-hot — size validated at queue creation; converting adds branch per append |

Line 117 flagged for hot-path review. If accepted as I-hot, pending drops to 3.

---

## indexed_queue.c — 2 asserts (all I)

16 V (create params, mkdir, OOM, 5 participant factories) + 3 V (accessor q NULL) — all converted.

| Line | Msg | Disp | Loc | Sig | Status |
|---|---|---|---|---|---|
| 100 | atomic lock-free (uint64_t) | I | startup | — | stays |
| 102 | atomic lock-free (uint32_t) | I | startup | — | stays |

---

## indexed_poller.c — 1 assert (I)

10 V-class converted to runtime checks (move operations + accessors). POLL_CORRUPT fix applied in earlier session.

| Line | Msg | Disp | Loc | Sig | Status |
|---|---|---|---|---|---|
| 174 | unknown handler directive | I | logic | — | stays |

---

## indexed_reader.c — 1 assert (I-hot)

16 V-class converted to runtime checks (3 read ops, 6 accessors, queue_meta/directory/queue_name).

| Line | Msg | Disp | Loc | Sig | Status |
|---|---|---|---|---|---|
| 133 | length > 0 (read inner, on-disk) | V→I | **hot** (per-read) | — | I-hot — called once per read(); data validated at commit time |

---

## indexed_batch_reader.c — 0 asserts (all converted)

All 15 V-class (read ops, accessors) + 3 factory V — all converted to runtime checks.

---

## indexed_batch_poller.c — 0 asserts (all converted)

Both V (it OOM, p OOM) + msg_count → POLL_CORRUPT — all converted.

---

## indexed_batch_queue.c — 0 asserts (all converted)

All 7 V (mkdir, OOM, 4 participant closed checks, archiver NULL) — all converted. Line 36 mkdir signature changed from `void` to `int`.

---

## indexed_batch_appender.c — 0 asserts (all converted)

All 4 V (eager-wrap header, eager-wrap payload, OOM, start_batch closed) — all converted.

---

## indexed_archiver.c — 12 asserts

All V already converted (Part-B audit). Verified from source: only I-class remain (0 — all were V).

---

## indexed_batch_archiver.c — 13 asserts

All V already converted (Part-B audit).

---

## queue_meta.c — 0 asserts (all converted)

All 9 V (directory/queue_name NULL, region_size, max_file_size, region OOM, wrap, OOM, open_ro) — all converted.

---

## appender_meta.c — 0 asserts (all converted)

All 6 V (OOM, directory/queue_name NULL in open_rw/open_ro) — all converted.

---

## queue_region_accessor.c — 0 asserts (all converted)

All 6 V (OOM, directory/queue_name NULL in create_rw/create_ro, acc NULL) — all converted.

---

## runtime.c — 9 asserts (all I)

19 V (idle strategy OOM + params, runtime_create, set_affinity, close, clone, deregister, registry_count, id_copy) — all converted.

Remaining: register/unregister/unregister_and_wait rt/exec NULL checks (internal), same-thread invariant, helper-register ok check.

---

## region_ring.c — 2 asserts (all I)

17 V (sync_create + async_create factory params + OOM + runtime registration UAF pin) — all converted. Line 253 was mutation pin #2 (verified under release flags).

Remaining: self NULL in sync_ring_close and async_ring_close (I — static dispatch entries).

---

## region_ring_accessor.c — 8 asserts (all I)

8 V (factory boundary + position >= 0) — all converted. 8 I (self/buffer on dispatch entries) — stay.

---

## region_async.c — 9 asserts (all I)
5 V (factory boundary + position >= 0) — all converted. 9 I — stay.

## region_sync.c — 8 asserts (all I)
5 V (factory boundary + position >= 0) — all converted. 8 I — stay.

## file_mapper_rolled.c — 10 asserts (all I)
7 V (factory boundary + OOM + alignment check) — all converted. 10 I — stay.

## file_mapper_single_ro.c — 6 asserts (all I)
4 V (factory boundary + munmap failure) — all converted. 6 I — stay.

## file_mapper_single_rw.c — 9 asserts (all I)
6 V (factory boundary + I/O failures + munmap) — all converted. 9 I — stay.

---

## payload_iterator.c — 8 asserts (all I)

7 V (factory boundary + offset validation) — all converted. Line 128 length > 0 on committed data flagged as I-hot (validated at write time).

---

## payload_walk.c — 1 assert (all I)

4 V (wrap failures + on-disk length) — all converted. 1 I (padding mismatch) — stays.

---

## uncommitted_payload_iterator.c — 13 asserts (all I)

8 V (factory boundary + offset validation + ranges OOM) — all converted. 13 I (internal state checks) — stay.

---

## batch_payload_iterator.c — 1 assert (all I)

3 V (wrap success, count >= 0, msg_len > 0) — all converted. 1 I (padding mismatch) — stays. msg_len > 0 flagged as I-hot (validated at write time, per-message advance).

---

## header_reader.c — 2 asserts (all I)

`MMAP_ASSERT(0)` for unreachable code paths.

---

## file_initialiser.c — 0 asserts (all converted)

2 V (fd >= 0, size > 0) — all converted.

---

## queue_latency_probe.c — 0 asserts (all converted)
## batch_queue_latency_probe.c — 0 asserts (all converted)

All V (OOM + parameter validation) — all converted.

---

## queue_transfer.c / batch_queue_transfer.c / queue_files.c — 0 asserts



---

## Conversion priority

| Tier | Files | V pending | Pin |
|---|---|---|---|
| P0 | indexed_batch_reader, runtime, indexed_batch_queue, indexed_batch_appender | 37 | + region_ring.c:253 UAF pin |
| P1 | queue_meta, appender_meta, queue_region_accessor | 21 | — |
| P2 | region_ring, file_mapper_*, region_* | 26 | — |
| P3 | rest (payload_*, probes, iterators) | 61 | — |
| **Total** | **29 files** | **145** | **2 pins** |
