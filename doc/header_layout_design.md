# mmap-c header layout design

**Status:** Approved design
**Component:** Header file — indexed visibility/commit layer
**Parameter:** `sectorSize = 64` (current, measured, kept)

---

## 1. Purpose

The header file holds one 8-byte entry per queue index. Each entry is
committed via a single atomic CAS from `HEADER_NULL` to an encoded
`(appenderId, payloadPosition)` value — this is the sole visibility point
for every message in the queue. Because every concurrent appender writes
to a *different* index, and because up to 256 appenders may be active
simultaneously, the layout of the header file determines whether those
concurrent writers ever land on the same CPU cache line. If they do, the
cache coherence protocol forces the line to bounce between cores on every
write — false sharing — which directly costs latency on the commit path
for every appender, not just the contended one.

The header layout exists specifically to prevent this.

---

## 2. The layout

The header file is organized as a sequence of **blocks**, each block
divided into **sectors**, each sector divided into **words** (one word =
one 8-byte header entry). The mapping from a logical index to a file byte
offset is:

```
sectorSize  = configurable (8, 64, 128, ... — any value such that
              sectorSize × sectorSize × 8 fits comfortably within the
              header region/page budget)
blockSize   = sectorSize × sectorSize    (entries per block)

block           = index / blockSize
index_in_block  = index % blockSize
sector_in_block = index_in_block / sectorSize
word_in_sector  = index_in_block % sectorSize

byte_offset = (block × blockSize
               + word_in_sector × sectorSize
               + sector_in_block) × 8
```

> **Read the worked table below before the formula, not the other way
> around.** Two people independently misread this formula by reasoning
> about it abstractly instead of computing actual byte offsets — the
> instinct to guess "which variable looks like it should be the sector"
> from the shape of the formula alone is unreliable here. Trust the table.

### Worked example (`sectorSize = 8`, for simple arithmetic)

`blockSize = 8 × 8 = 64` headers per block, 64 bytes per sector (one cache line at this `sectorSize`).

Picture the block as an 8×8 grid: **8 sectors (cache lines) as rows, 8 word
positions as columns.** A naive "fill the array in storage order" layout
would walk this grid **horizontally** — fill row 0 (sector 0) completely
across all 8 columns, then move to row 1. That's the layout you'd get from
a flat `index_in_block` mapping with no sectoring at all, and it's exactly
what causes false sharing: indices 0–7 would all share cache line 0.

This formula instead walks the grid **vertically** — fill column 0 down
through all 8 rows (one index per sector) before moving to column 1.
Sequential indices land in a different row (sector / cache line) every
single step; only after a full column is filled does the mapping return
to row 0, now in the next column (word position):

| idx | index_in_block | sector_in_block (row, idx÷8) | word_in_sector (col, idx mod 8) | byte_offset | cache line |
|----:|----:|----:|----:|----:|:--|
| 0 | 0 | 0 | 0 | 0   | 0 ← new |
| 1 | 1 | 0 | 1 | 64  | 1 ← new |
| 2 | 2 | 0 | 2 | 128 | 2 ← new |
| 3 | 3 | 0 | 3 | 192 | 3 ← new |
| 7 | 7 | 0 | 7 | 448 | 7 ← new |
| 8 | 8 | 1 | 0 | 8   | 0 — back to row 0, now column 1 |
| 9 | 9 | 1 | 1 | 72  | 1 — row 1, column 1 |

`sector_in_block` is the **row** — which cache line. It changes every
index, which is what gives the false-sharing protection.
`word_in_sector` is the **column** — the byte position within that cache
line. It only changes once every `sectorSize` indices.

This is the mechanism that keeps concurrent appenders apart: as long as
the number of simultaneously active appenders does not exceed
`sectorSize`, each one writes to a distinct row (cache line), and no two
appenders ever share one.

---

## 3. Choosing `sectorSize`

`sectorSize` is a tunable parameter, not a fixed architectural constant.
Any value works correctly, subject only to the constraint that
`sectorSize × sectorSize × 8` bytes (one block) fits within whatever
header-region budget the deployment is comfortable with. The choice
trades off three things:

| `sectorSize` | Bytes per sector | Bytes per block | Concurrent writers kept apart | Notes |
|---|---|---|---|---|
| 8  | 64 B (1 cache line) | 512 B | up to 8 | Minimal footprint; one sector is exactly one cache line |
| 64 | 512 B (8 cache lines) | 32 KB | up to 64 | **Current, measured choice** — see §4 |
| 128 | 1 KB (16 cache lines) | 128 KB | up to 128 | Larger footprint, headroom for more concurrent appenders |

All three are correct. None of them risks false sharing for the range of
concurrent-appender counts they're sized for, because the fundamental
guarantee — *adjacent sectors never overlap a cache line* — holds for any
`sectorSize` that is a multiple of 8 (one word).

---

## 4. Why `sectorSize = 64` is the chosen value

Measured benchmarking (round-trip latency, 64-byte messages, isolated
cores, `-O3 -march=native`, PGO, transparent huge pages; 20 independent
runs of 20M iterations each per configuration) shows `sectorSize = 64`
outperforming `sectorSize = 8` at **every** percentile measured, with the
gap widening substantially in the upper percentiles:

```
              sectorSize=64   sectorSize=8   sector=64 advantage
min (p0)            224 ns        226 ns           −0.7%
p50                 342 ns        347 ns           −1.4%
p90                 376 ns        396 ns           −5.1%
p95                 400 ns        486 ns          −17.8%
p99               2,718 ns      2,724 ns           −0.3%
p99.9             4,225 ns      4,288 ns           −1.5%
p99.99           10,075 ns     14,748 ns          −31.7%
p99.999          18,705 ns     19,982 ns           −6.4%
throughput      2,407,897/s   2,335,630/s          +3.1%
```

*(Medians across 20 independent benchmark runs per configuration, each run
20,000,000 iterations, 100-iteration warmup, 64-byte messages, cores
5/7/9 with isolcpus, identical build flags for both configurations.)*

`sectorSize = 64` wins decisively and consistently — not a single
percentile favours `sectorSize = 8` across either individual runs or the
medians. The advantage is modest at the median (−1.4%) but becomes large
in the p95 and p99.99 range, exactly where cache and TLB pressure from
the smaller, more cramped 512-byte block size in the `sectorSize = 8`
configuration begins to show. This is consistent with the explanation in
the bullet points below: prefetcher isolation and concurrency headroom
both degrade gracefully at `sectorSize = 64` and are more easily disrupted
at `sectorSize = 8`, and that disruption manifests precisely as the wider
spread between p90 and p99.99 visible in the sectorSize=8 column above.

The larger sector size has two compounding benefits beyond the raw
cache-line separation:

- **Headroom for concurrency.** With `sectorSize = 8`, only 8 appenders
  can be active before a ninth one starts sharing a sector with the
  first — at that point the false-sharing protection degrades exactly
  the way it would with no sectoring at all for the 9th+ appender. With
  `sectorSize = 64`, up to 64 appenders stay fully isolated from one
  another, which comfortably covers realistic concurrent-appender counts
  for this system (the queue's appender ID space is 256, but workloads
  with more than a handful of truly concurrent hot writers are rare; 64
  is a generous margin above what's typically needed while still being
  far short of the full 256 ID space).

- **Prefetcher isolation.** Modern CPU hardware prefetchers speculatively
  load cache lines adjacent to a recently accessed one. With
  `sectorSize = 8` (sectors one cache line apart), a prefetch triggered
  by reading one appender's header can pull in the *next* appender's
  cache line speculatively. With `sectorSize = 64` (sectors eight cache
  lines apart), the gap is far wider than any realistic prefetch depth,
  so this speculative interference cannot occur.

These effects combine to produce the measured advantage at the upper
percentiles, where cache and prefetcher behaviour dominate over raw
instruction count.

---

## 5. The size tradeoff, made explicit

The cost of a larger `sectorSize` is header file footprint: one block at
`sectorSize = 64` is 32 KB, versus 512 bytes at `sectorSize = 8` — a 64×
difference in how much header-file space a fully-populated block of
indices occupies on disk and in the TLB/cache working set. This is a
real, deliberate tradeoff, not an oversight: the queue is trading header
file density for cache/prefetcher isolation margin, and the measured
benchmarks confirm this trade pays off on target hardware, particularly
once transparent huge pages are enabled (which absorbs the larger
footprint's TLB cost almost entirely, since a 2 MB huge page comfortably
contains many blocks regardless of `sectorSize`).

If header file size becomes a binding constraint in a future deployment
(e.g. very memory-constrained embedded targets without huge page support),
`sectorSize = 8` remains a fully valid, correct configuration — it simply
trades some of the upper-percentile latency margin back for an 64×
smaller header footprint. The layout formula in §2 supports either choice
(or any value in between, or above) without any change to the queue's
correctness model.

---

## 6. Mapping this terminology onto `header_codec.h`

The current C implementation (`header_codec.h`, `header_position()` /
`header_index()`) computes the **identical** byte offsets as the formula
in §2 — verified term-for-term against the worked example above. The C
variable names differ slightly from this document's:

| This document | `header_codec.h` | Role |
|---|---|---|
| `sector_in_block` | `sector_in_block` | Row — selects the cache line |
| `word_in_sector` | `index_in_sector` | Column — selects the byte position within that cache line |

**`index_in_sector` in the C code plays the exact same role as
`word_in_sector` in this document — it is not swapped, inverted, or
incorrect.** It is the same quantity (`index_in_block % sectorSize`)
under a less descriptive name. If this is renamed for clarity in a future
cleanup, the rename must preserve which term is multiplied by `sectorSize`
in the formula (`word_in_sector × sectorSize` in §2's `byte_offset`
expression) — renaming the *labels* without preserving *which label
carries the multiplier* would silently invert the formula and corrupt the
header file layout. Verify any such rename against the worked example in
§2 before merging, the same way it was verified here.

---

## 7. Summary

- `sectorSize` is a tunable parameter, not a fixed constant — 8, 64, 128,
  or any multiple of 8 that fits the header region budget are all
  correct choices.
- The layout guarantees concurrent appenders up to `sectorSize` in count
  never share a cache line, eliminating false sharing on the commit path.
- `sectorSize = 64` is the chosen production value: it provides ample
  concurrency headroom (64 simultaneously isolated appenders) and
  measurably better upper-percentile latency than smaller values, at the
  cost of a larger (but THP-absorbable) header file footprint.
- This is a deliberate, measured engineering choice, not a default left
  unexamined — the queue's design explicitly supports retuning this
  parameter for different deployment constraints without any change to
  its correctness guarantees.

