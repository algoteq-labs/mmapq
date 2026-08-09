#include "queue_latency_probe.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "mmap_stats.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>

#define DEFAULT_ITERATIONS   1000000
#define DEFAULT_WARMUP       100000
#define DEFAULT_MSG_SIZE     64
#define DEFAULT_CLIENT_CPU   5
#define DEFAULT_SERVER_CPU   7
#define DEFAULT_MAPPER_CPU   9

#define SHUTDOWN_SEQ 0

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

typedef struct {
    IndexedAppender *response_appender;
    volatile int     shutdown_requested;
} InternalServerCtx;

static NextMove internal_echo_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    InternalServerCtx *s = (InternalServerCtx *)ctx;
    const LatencyHeader *hdr = (const LatencyHeader *)data;
    
    if (hdr->sequence == SHUTDOWN_SEQ) {
        s->shutdown_requested = 1;
    }
    /* Echo the entire payload back to the response queue */
    indexed_appender_append(s->response_appender, data, length);
    
    return NEXT_ADVANCE;
}

typedef struct {
    char dir[256];
    char req[256];
    char res[256];
    int cpu;
    int mapper_cpu;
} ServerThreadArgs;

static void *internal_server_thread_fn(void *arg)
{
    ServerThreadArgs *a = (ServerThreadArgs *)arg;
    
    if (a->cpu >= 0) {
        thread_set_affinity(a->cpu);
    }
    thread_set_name(pthread_self(), "echo-server");

    Runtime *rt = NULL;
    if (runtime_region_mapper(&rt) != 0) { fprintf(stderr, "runtime_region_mapper failed\n"); exit(1); }
    if (a->mapper_cpu >= 0) {
        runtime_set_affinity(rt, a->mapper_cpu);
    }
    
    IndexedQueue *q_req = NULL, *q_res = NULL;
    if (indexed_queue_create_default(a->dir, a->req, rt, rt, &q_req) != 0) { fprintf(stderr, "create request queue failed\n"); exit(1); }
    if (indexed_queue_create_default(a->dir, a->res, rt, rt, &q_res) != 0) { fprintf(stderr, "create response queue failed\n"); exit(1); }
    
    IndexedPoller *poller = NULL;
    assert(indexed_queue_create_poller(q_req, &poller) == 0);
    assert(poller != NULL);
    IndexedAppender *appender = NULL;
    assert(indexed_queue_create_appender(q_res, &appender) == 0);
    assert(appender != NULL);
    
    InternalServerCtx ctx = {
        .response_appender = appender,
        .shutdown_requested = 0
    };

    while (!ctx.shutdown_requested) {
        indexed_poller_poll(poller, internal_echo_handler, &ctx);
    }

    indexed_appender_close(appender);
    indexed_poller_close(poller);
    indexed_queue_close(q_req);
    indexed_queue_close(q_res);
    
    free(a);
    return NULL;
}

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
    
    int external_server = 0;
    int use_sync        = 0;
    int use_mixed       = 0;
    const char *dir     = "/tmp";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--external") == 0 || strcmp(argv[i], "-e") == 0) {
            external_server = 1;
        }
        if (strcmp(argv[i], "--sync") == 0 || strcmp(argv[i], "-s") == 0) {
            use_sync = 1;
        }
        if (strcmp(argv[i], "--mixed") == 0 || strcmp(argv[i], "-m") == 0) {
            use_mixed = 1;
        }
        if (strcmp(argv[i], "--dir") == 0 || strcmp(argv[i], "-d") == 0) {
            if (i + 1 < argc) {
                dir = argv[++i];
            }
        }
    }

    const char *mode_str = "ASYNCHRONOUS";
    if (use_sync) mode_str = "SYNCHRONOUS";
    if (use_mixed) mode_str = "MIXED (Async-Write, Sync-Read)";

    printf("Starting Round-Trip Latency Benchmark...\n");
    printf("  Cores:  Client=%d, Mapper=%d\n", client_cpu, mapper_cpu);
    printf("  Config: Iterations=%d, Warmup=%d, MsgSize=%d\n", 
           iterations, warmup, msg_size);
    printf("  Mode:   %s (%s)\n", external_server ? "EXTERNAL SERVER" : "INTERNAL SERVER",
                                   mode_str);
    printf("  Validation: Server=%s, Client=%s\n", 
           val_server ? "ON" : "OFF", val_client ? "ON" : "OFF");

    Runtime *write_rt = NULL;
    Runtime *read_rt  = NULL;

    if (use_sync) {
        /* Both NULL */
    } else if (use_mixed) {
        if (runtime_region_mapper(&write_rt) != 0) { fprintf(stderr, "runtime_region_mapper failed\n"); return 1; }
        read_rt  = NULL;
        if (mapper_cpu >= 0) runtime_set_affinity(write_rt, mapper_cpu);
    } else {
        if (runtime_region_mapper(&write_rt) != 0) { fprintf(stderr, "runtime_region_mapper failed\n"); return 1; }
        read_rt  = write_rt;
        if (mapper_cpu >= 0) runtime_set_affinity(write_rt, mapper_cpu);
    }

    /* Use predictable names for interoperability */
    const char *name_req = "request";
    const char *name_res = "response";
    
    printf("  Queues: %s/%s (request) -> %s/%s (response)\n", dir, name_req, dir, name_res);

    pthread_t server_thread;
    if (!external_server && server_cpu >= 0) {
        ServerThreadArgs *args = malloc(sizeof(ServerThreadArgs));
        strncpy(args->dir, dir, sizeof(args->dir)-1);
        strncpy(args->req, name_req, sizeof(args->req)-1);
        strncpy(args->res, name_res, sizeof(args->res)-1);
        args->cpu = server_cpu;
        args->mapper_cpu = mapper_cpu;
        
        pthread_create(&server_thread, NULL, internal_server_thread_fn, args);
        printf("  Internal Server started on Core %d\n", server_cpu);
    }

    QueueLatencyProbe *probe = NULL;
    if (queue_latency_probe_create(
        1, dir, name_req, name_res, msg_size,
        val_server, val_client, write_rt, read_rt, &probe) != 0) {
        fprintf(stderr, "probe create failed\n"); return 1;
    }

    /* Client Pinning */
    if (client_cpu >= 0) {
        thread_set_affinity(client_cpu);
    }

    printf("Warming up...\n");
    uint64_t *results = malloc(sizeof(uint64_t) * (size_t)iterations);
    if (results != NULL) {
        memset(results, 0, sizeof(uint64_t) * (size_t)iterations);
    }

    queue_latency_probe_move_to_end(probe); /* Skip any stale messages */
    for (int i = 0; i < warmup; i++) {
        queue_latency_probe_execute(probe);
    }

    printf("Measuring...\n");

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
    
    if (!external_server && server_cpu >= 0) {
        pthread_join(server_thread, NULL);
        printf("  Internal Server stopped.\n");
    }
    
    free(results);
    return 0;
}
