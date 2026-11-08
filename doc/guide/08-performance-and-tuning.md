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

Measured against the Java implementation the format derives from,
measured 2026-06-24 (commit `d3cfb28`), on the same machine — the one comparison where every variable
(format, framing, hardware, cores) is controlled. On isolated cores
(physical cores 5, 7, 9 on an Intel i7-12700H) with Transparent Huge Pages
enabled.

*Measured 2026-06-24 (commit `d3cfb28`, pre-0.1.0), Intel i7-12700H,
isolcpus 5,7,9 + THP as below. Refreshed at the 0.2.0 release gate.*

Single-message queue — roundtrip to echo server in external process, 20M
iterations:

| Percentile | C (this library) | Java (JMH, same machine) |
|:-----------|------------------:|-------------------------:|
| min (p0)   |        **182 ns** |                   265 ns |
| p50        |        **284 ns** |                   380 ns |
| p90        |        **333 ns** |                   429 ns |
| p99        |        **396 ns** |                   591 ns |
| p99.9      |       **1869 ns** |                 6,792 ns |
| p99.99     |       **7411 ns** |                16,533 ns |
| p99.999    |      **14984 ns** |                15,090 ns |
| round trips/sec (closed loop) | **3.2 M** | 2.41 M |

Batch queue — roundtrip to echo server in external process, 20M
iterations:

| Percentile |  C (this library) |
|:-----------|------------------:|
| min (p0)   |        **196 ns** |
| p50        |        **296 ns** |
| p90        |        **348 ns** |
| p95        |        **368 ns** |
| p99        |        **422 ns** |
| p99.9      |       **2030 ns** |
| p99.99     |       **7313 ns** |
| p99.999    |      **14895 ns** |
| round trips/sec (closed loop) | **3 M** |

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
