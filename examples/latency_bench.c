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
    
    (void)server_cpu; /* External server core is managed externally */

    printf("Starting Round-Trip Latency Benchmark (EXTERNAL SERVER MODE)...\n");
    printf("  Cores:  Client=%d, Mapper=%d (Server expected on external core)\n", client_cpu, mapper_cpu);
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

    /* Use predictable names for interoperability */
    const char *name_req = "req";
    const char *name_res = "res";
    const char *dir = "/tmp";
    
    printf("  Queues: %s/%s (request) -> %s/%s (response)\n", dir, name_req, dir, name_res);

    QueueLatencyProbe *probe = queue_latency_probe_create(
        1, dir, name_req, name_res, msg_size,
        val_server, val_client, rt);

    /* Client Pinning */
    if (client_cpu >= 0) {
        thread_set_affinity(client_cpu);
    }

    printf("Warming up...\n");
    queue_latency_probe_move_to_end(probe); /* Skip any stale messages */
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
    
    free(results);
    return 0;
}
