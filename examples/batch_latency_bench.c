#include "batch_queue_latency_probe.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "mmap_stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ITERATIONS 1000000
#define WARMUP     100000
#define MSG_SIZE   64

int main(int argc, char *argv[])
{
    int iterations = (argc > 1) ? atoi(argv[1]) : ITERATIONS;
    int warmup     = (argc > 2) ? atoi(argv[2]) : WARMUP;
    int msg_size   = (argc > 3) ? atoi(argv[3]) : MSG_SIZE;
    int client_cpu = (argc > 4) ? atoi(argv[4]) : -1;
    int mapper_cpu = (argc > 6) ? atoi(argv[6]) : -1;
    (void)argc;
    (void)argv;

    int use_sync = 0, use_mixed = 0;
    const char *dir = "/tmp";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--sync") == 0) use_sync = 1;
        if (strcmp(argv[i], "--mixed") == 0) use_mixed = 1;
        if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc) dir = argv[++i];
    }

    Runtime *write_rt = NULL, *read_rt = NULL;
    if (!use_sync) {
        Runtime *rt = runtime_region_mapper();
        if (mapper_cpu >= 0) runtime_set_affinity(rt, mapper_cpu);
        if (use_mixed) { write_rt = rt; read_rt = NULL; }
        else { write_rt = rt; read_rt = rt; }
    }

    BatchQueueLatencyProbe *probe = batch_queue_latency_probe_create(1, dir, "request", "response", msg_size, 0, 0, write_rt, read_rt);
    if (client_cpu >= 0) thread_set_affinity(client_cpu);

    printf("Starting Batch Round-Trip Latency Benchmark...\n");
    batch_queue_latency_probe_move_to_end(probe);
    for (int i = 0; i < warmup; i++) batch_queue_latency_probe_execute(probe);

    uint64_t *res = malloc(sizeof(uint64_t) * (size_t)iterations);
    uint64_t start = monotonic_nanos();
    for (int i = 0; i < iterations; i++) res[i] = batch_queue_latency_probe_execute(probe);
    uint64_t end = monotonic_nanos();

    mmap_stats_print(res, iterations, end - start, "Batch Round-Trip Latency Statistics:");
    batch_queue_latency_probe_close(probe);
    free(res);
    return 0;
}
