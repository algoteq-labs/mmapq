#include "queue_latency_probe.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "mmap_stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <unistd.h>

#define DEFAULT_ITERATIONS   1000000
#define DEFAULT_WARMUP       100000
#define DEFAULT_MSG_SIZE     64
#define DEFAULT_CLIENT_CPU   5
#define DEFAULT_SERVER_CPU   7
#define DEFAULT_MAPPER_CPU   9

int main(int argc, char *argv[])
{
    int iterations = (argc > 1) ? atoi(argv[1]) : DEFAULT_ITERATIONS;
    int warmup     = (argc > 2) ? atoi(argv[2]) : DEFAULT_WARMUP;
    int msg_size   = (argc > 3) ? atoi(argv[3]) : DEFAULT_MSG_SIZE;
    int client_cpu = (argc > 4) ? atoi(argv[4]) : DEFAULT_CLIENT_CPU;
    int server_cpu = (argc > 5) ? atoi(argv[5]) : DEFAULT_SERVER_CPU;
    int mapper_cpu = (argc > 6) ? atoi(argv[6]) : DEFAULT_MAPPER_CPU;
    int val_server = (argc > 7) ? atoi(argv[7]) : 0;
    int val_client = (argc > 8) ? atoi(argv[8]) : 0;

    printf("Starting Round-Trip Latency Benchmark...\n");
    printf("  Cores:  Client=%d, Server=%d, Mapper=%d\n", client_cpu, server_cpu, mapper_cpu);
    printf("  Config: Iterations=%d, Warmup=%d, MsgSize=%d\n", 
           iterations, warmup, msg_size);
    printf("  Validation: Server=%s, Client=%s\n", 
           val_server ? "ON" : "OFF", val_client ? "ON" : "OFF");

    /* Lock all current and future memory to prevent swapping and page faults */
#if defined(__linux__)
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        perror("mlockall failed (benchmark will continue without memory locking)");
    }
#endif

    Runtime *rt = runtime_region_mapper();
    if (mapper_cpu >= 0) {
        runtime_set_affinity(rt, mapper_cpu);
    }

    char name_req[256];
    char name_res[256];
    snprintf(name_req, sizeof(name_req), "req_%d", getpid());
    snprintf(name_res, sizeof(name_res), "res_%d", getpid());
    
    QueueLatencyProbe *probe = queue_latency_probe_create(
        1, "/tmp", name_req, name_res, msg_size, server_cpu, 
        val_server, val_client, rt);

    /* Client Pinning */
    if (client_cpu >= 0) {
        thread_set_affinity(client_cpu);
    }

    printf("Warming up...\n");
    for (int i = 0; i < warmup; i++) {
        queue_latency_probe_execute(probe);
    }

    printf("Measuring...\n");
    uint64_t *results = malloc(sizeof(uint64_t) * (size_t)iterations);
    
    uint64_t start_bench = monotonic_nanos();
    for (int i = 0; i < iterations; i++) {
        results[i] = queue_latency_probe_execute(probe);
    }
    uint64_t end_bench = monotonic_nanos();

    mmap_stats_print(results, iterations, end_bench - start_bench, "Round-Trip Latency Statistics (nanoseconds):");

    int64_t errors = queue_latency_probe_validation_errors(probe);
    if (errors > 0) {
        printf("!!! WARNING: %ld validation errors detected !!!\n\n", errors);
    }

    queue_latency_probe_close(probe);
    
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf /tmp/%s /tmp/%s", name_req, name_res);
    if (system(cmd) != 0) {}

    free(results);
    return 0;
}
