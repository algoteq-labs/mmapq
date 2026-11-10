# 8. Performance and tuning

## The runtime: async, sync, mixed

An `MmapqRuntime` is a background thread that maps and unmaps file regions
ahead of the hot loop, absorbing `ftruncate`, page-zeroing and
`MAP_POPULATE` costs. A header- or payload-region wrap can still block the
caller up to the queue's mapping timeout — the runtime keeps ahead
normally; it is not a guarantee of zero stalls.

- **Async** (write side): the writer offloads file extension and pre-touch
  to the mapper. Writers trigger premap at the start of each region;
  readers at the halfway point.
- **Sync**: no background thread; mapping happens on the calling thread.
  Simplest, and the right choice for the read side when latency matters
  more than throughput.
- **Mixed**: separate runtimes per direction — async for the writer, sync
  for the reader — so the reader runs without background interference.

The async mapper needs a core it can occupy: on a 1-vCPU or oversubscribed
host, wait-for-mapper loops must `sched_yield()` or the waiter starves the
mapper thread. (Why: the Anti-Pattern Ledger in the
[maintainer guide](../contributing/maintainer_guide.md).)

## Benchmarks

*Measured 2026-11-11 (0.2.0, commit `800a0b4`), Intel i7-11800H,
isolcpus 5,7,9 + THP as below. Each figure is the median of 6 runs.*

Single-message queue — roundtrip to echo server in external process, 20M
iterations:

| Percentile | C (this library) |
|:-----------|------------------:|
| min (p0)   |        **187 ns** |
| p50        |        **287 ns** |
| p90        |        **336 ns** |
| p99        |        **394 ns** |
| p99.9      |       **1677 ns** |
| p99.99     |       **8430 ns** |
| p99.999    |      **16457 ns** |
| round trips/sec (closed loop) | **3.2 M** |

Batch queue — roundtrip to echo server in external process, 20M
iterations:

| Percentile |  C (this library) |
|:-----------|------------------:|
| min (p0)   |        **225 ns** |
| p50        |        **283 ns** |
| p90        |        **345 ns** |
| p95        |        **369 ns** |
| p99        |        **429 ns** |
| p99.9      |       **1637 ns** |
| p99.99     |       **6728 ns** |
| p99.999    |      **16773 ns** |
| round trips/sec (closed loop) | **3.1 M** |

The round-trips row is iterations divided by the wall-clock time of the
measured loop, which has one round trip in flight at a time — roughly
1 / mean round-trip latency (the loop's own per-iteration cost is
included). It is not the queue's capacity: a writer that does not wait
for a reply is not bounded by it.

## The benchmark method

```bash
# Full benchmark suite: single-message + batch, all modes
./scripts/bench.sh
./scripts/batch_bench.sh

# Or via make (runs the scripts above):
make bench-suite
make batch-bench-suite
```

Or run individual configurations manually:

```bash
# Single-message, internal server
rm -rf /tmp/request /tmp/response
sleep 3
./latency_bench 20000000 1000 64 5 7 9

# Single-message, external server
rm -rf /tmp/request /tmp/response
sleep 3
./echo_server /tmp request response 7 9 &
./latency_bench 20000000 1000 64 5 7 9 --external

# Batch queue, internal server
rm -rf /tmp/request /tmp/response
sleep 3
./batch_latency_bench 20000000 1000 64 5 7 9

# Batch queue, external server
rm -rf /tmp/request /tmp/response
sleep 3
./batch_echo_server /tmp request response 7 9 &
./batch_latency_bench 20000000 1000 64 5 7 9 --external
```

Arguments: `<iterations> <warmup> <msg_size> <client_cpu> <server_cpu> <mapper_cpu>`

Override defaults via environment variables:

```bash
BENCH_ITER=5000000 BENCH_MAPPER=11 ./scripts/bench.sh
```

## System configuration for best results

1. **Core isolation** — kernel boot parameter `isolcpus=5,7,9` (or
   equivalent) to reserve cores for the client loop, server loop, and
   region mapper.
2. **Transparent Huge Pages** — the library calls `madvise(MADV_HUGEPAGE)`
   on mapped regions to reduce TLB pressure.
3. **VDSO clock** — use `CLOCK_MONOTONIC` (the library default) to bypass
   the syscall overhead on timing.
4. **Thread affinity** — always spawn background threads *before* pinning
   the calling thread; see Anti-Pattern D in the
   [maintainer guide](../contributing/maintainer_guide.md).
