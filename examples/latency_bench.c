#include "queue_latency_probe.h"
#include "runtime.h"
#include "mmap_platform.h"
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

static int compare_u64(const void *a, const void *b)
{
    uint64_t v1 = *(const uint64_t *)a;
    uint64_t v2 = *(const uint64_t *)b;
    return (v1 < v2) ? -1 : (v1 > v2);
}

void print_stats(uint64_t *results, int count, uint64_t total_ns)
{
    qsort(results, (size_t)count, sizeof(uint64_t), compare_u64);
    
    double sum = 0;
    for (int i = 0; i < count; i++) sum += (double)results[i];
    double avg = sum / count;
    double ops_per_sec = (double)count / ((double)total_ns / 1e9);
    
    printf("\nRound-Trip Latency Statistics (nanoseconds):\n");
    printf("------------------------------------------\n");
    printf("Iterations: %10d\n", count);
    printf("Throughput: %10.2f ops/sec\n", ops_per_sec);
    printf("Average:    %10.2f\n", avg);
    printf("Min (p0):   %10lu\n", results[0]);
    printf("Max (p100): %10lu\n", results[count - 1]);
    printf("------------------------------------------\n");
    printf("P50:        %10lu\n", results[(int)(count * 0.50)]);
    printf("P90:        %10lu\n", results[(int)(count * 0.90)]);
    printf("P95:        %10lu\n", results[(int)(count * 0.95)]);
    printf("P99:        %10lu\n", results[(int)(count * 0.99)]);
    printf("P99.9:      %10lu\n", results[(int)(count * 0.999)]);
    printf("P99.99:     %10lu\n", results[(int)(count * 0.9999)]);
    printf("------------------------------------------\n\n");
}

int main(int argc, char *argv[])
{
    int iterations = (argc > 1) ? atoi(argv[1]) : DEFAULT_ITERATIONS;
    int warmup     = (argc > 2) ? atoi(argv[2]) : DEFAULT_WARMUP;
    int msg_size   = (argc > 3) ? atoi(argv[3]) : DEFAULT_MSG_SIZE;
    int client_cpu = (argc > 4) ? atoi(argv[4]) : DEFAULT_CLIENT_CPU;
    int server_cpu = (argc > 5) ? atoi(argv[5]) : DEFAULT_SERVER_CPU;
    int mapper_cpu = (argc > 6) ? atoi(argv[6]) : DEFAULT_MAPPER_CPU;

    printf("Starting Round-Trip Latency Benchmark...\n");
    printf("  Cores:  Client=%d, Server=%d, Mapper=%d\n", client_cpu, server_cpu, mapper_cpu);
    printf("  Config: Iterations=%d, Warmup=%d, MsgSize=%d\n", 
           iterations, warmup, msg_size);

    Runtime *rt = runtime_region_mapper();
    if (mapper_cpu >= 0) {
        runtime_set_affinity(rt, mapper_cpu);
    }

    char name_req[256];
    char name_res[256];
    snprintf(name_req, sizeof(name_req), "req_%d", getpid());
    snprintf(name_res, sizeof(name_res), "res_%d", getpid());
    
    QueueLatencyProbe *probe = queue_latency_probe_create(
        1, "/tmp", name_req, name_res, msg_size, server_cpu, rt);

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

    print_stats(results, iterations, end_bench - start_bench);

    queue_latency_probe_close(probe);
    
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf /tmp/%s /tmp/%s", name_req, name_res);
    if (system(cmd) != 0) {}

    free(results);
    return 0;
}
