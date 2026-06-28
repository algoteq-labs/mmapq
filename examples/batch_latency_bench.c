#include "batch_queue_latency_probe.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "mmap_stats.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* ── Defaults ────────────────────────────────────────────────────────────── */

#define DEFAULT_ITERATIONS  20000000
#define DEFAULT_WARMUP      100000
#define DEFAULT_MSG_SIZE    64
#define DEFAULT_CLIENT_CPU  -1
#define DEFAULT_SERVER_CPU  -1
#define DEFAULT_MAPPER_CPU  -1

/* ── Internal server ─────────────────────────────────────────────────────── */

#define SHUTDOWN_SEQ 0

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} BatchLatencyHeader;

typedef struct {
    IndexedBatchAppender *response_appender;
    volatile int          shutdown_requested;
} BatchServerCtx;

static NextMove internal_echo_handler(
    int64_t              b_idx,
    BatchReadingContext *ctx,
    void                *user_ctx)
{
    (void)b_idx;
    BatchServerCtx *s = (BatchServerCtx *)user_ctx;

    if (!batch_iterator_advance(&ctx->batch_iterator))
        return NEXT_ADVANCE;
    const BatchLatencyHeader *hdr = batch_iterator_current_data(&ctx->batch_iterator);
    if (hdr->sequence == SHUTDOWN_SEQ)
        s->shutdown_requested = 1;

    MutableBatch *b = indexed_batch_appender_start_batch(s->response_appender);
    mutable_batch_append(b, batch_iterator_current_data(&ctx->batch_iterator),
                            batch_iterator_current_length(&ctx->batch_iterator));
    mutable_batch_append_remaining_from(b, &ctx->batch_iterator);
    indexed_batch_appender_commit_batch(s->response_appender, mutable_batch_complete(b));

    return NEXT_ADVANCE;
}

typedef struct {
    const char           *dir;
    int                   cpu;
    Runtime              *write_rt;
    Runtime              *read_rt;
    volatile int          ready;
    BatchServerCtx        ctx;
    IndexedBatchPoller   *poller;
    IndexedBatchAppender *appender;
    IndexedBatchQueue    *q_req;
    IndexedBatchQueue    *q_res;
} InternalServerArgs;

static void *internal_server_thread(void *arg)
{
    InternalServerArgs *a = (InternalServerArgs *)arg;
    if (a->cpu >= 0) thread_set_affinity(a->cpu);

    a->q_req   = indexed_batch_queue_create_default(a->dir, "request",  a->write_rt, a->read_rt);
    a->q_res   = indexed_batch_queue_create_default(a->dir, "response", a->write_rt, a->read_rt);
    a->poller  = indexed_batch_queue_create_poller(a->q_req);
    a->appender = indexed_batch_queue_create_appender(a->q_res);
    a->ctx.response_appender = a->appender;
    a->ctx.shutdown_requested = 0;

    __atomic_store_n(&a->ready, 1, __ATOMIC_RELEASE);

    while (!a->ctx.shutdown_requested)
        indexed_batch_poller_poll(a->poller, internal_echo_handler, &a->ctx);

    indexed_batch_appender_close(a->appender);
    indexed_batch_poller_close(a->poller);
    indexed_batch_queue_close(a->q_req);
    indexed_batch_queue_close(a->q_res);
    return NULL;
}

/* ── Main ─────────────────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr,
            "usage: %s <iterations> [warmup] [msg_size] [client_cpu] [server_cpu] [mapper_cpu]"
            " [--external] [--sync] [--mixed] [--validate-server] [--validate-client] [--dir <path>]\n",
            argv[0]);
        fprintf(stderr, "  defaults: iterations=%d warmup=%d msg_size=%d\n",
            DEFAULT_ITERATIONS, DEFAULT_WARMUP, DEFAULT_MSG_SIZE);
        return 1;
    }

    int iterations = (argc > 1) ? atoi(argv[1]) : DEFAULT_ITERATIONS;
    int warmup     = (argc > 2) ? atoi(argv[2]) : DEFAULT_WARMUP;
    int msg_size   = (argc > 3) ? atoi(argv[3]) : DEFAULT_MSG_SIZE;
    int client_cpu = (argc > 4) ? atoi(argv[4]) : DEFAULT_CLIENT_CPU;
    int server_cpu = (argc > 5) ? atoi(argv[5]) : DEFAULT_SERVER_CPU;
    int mapper_cpu = (argc > 6) ? atoi(argv[6]) : DEFAULT_MAPPER_CPU;

    int external_server = 0, use_sync = 0, use_mixed = 0;
    int validate_server = 0, validate_client = 0;
    const char *dir = "/tmp";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--external")         == 0) external_server  = 1;
        if (strcmp(argv[i], "--sync")             == 0) use_sync         = 1;
        if (strcmp(argv[i], "--mixed")            == 0) use_mixed        = 1;
        if (strcmp(argv[i], "--validate-server")  == 0) validate_server  = 1;
        if (strcmp(argv[i], "--validate-client")  == 0) validate_client  = 1;
        if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc) dir = argv[++i];
    }

    const char *mode_str = "ASYNCHRONOUS";
    if (use_sync)       mode_str = "SYNCHRONOUS";
    else if (use_mixed) mode_str = "MIXED";

    Runtime *write_rt = NULL, *read_rt = NULL;
    if (!use_sync) {
        Runtime *rt = runtime_region_mapper();
        if (mapper_cpu >= 0) runtime_set_affinity(rt, mapper_cpu);
        if (use_mixed) { write_rt = rt; read_rt = NULL; }
        else           { write_rt = rt; read_rt = rt;   }
    }

    printf("Starting Batch Round-Trip Latency Benchmark...\n");
    printf("  Cores:  Client=%d, Mapper=%d\n", client_cpu, mapper_cpu);
    printf("  Config: Iterations=%d, Warmup=%d, MsgSize=%d\n",
           iterations, warmup, msg_size);
    printf("  Mode:   %s (%s)\n",
           external_server ? "EXTERNAL SERVER" : "INTERNAL SERVER", mode_str);
    printf("  Validation: Server=%s, Client=%s\n",
           validate_server ? "ON" : "OFF", validate_client ? "ON" : "OFF");
    printf("  Queues: %s/request -> %s/response\n", dir, dir);

    pthread_t server_thread;
    InternalServerArgs server_args = {0};
    if (!external_server) {
        server_args.dir      = dir;
        server_args.cpu      = server_cpu;
        server_args.write_rt = write_rt;
        server_args.read_rt  = read_rt;
        server_args.ready    = 0;
        pthread_create(&server_thread, NULL, internal_server_thread, &server_args);
        while (!__atomic_load_n(&server_args.ready, __ATOMIC_ACQUIRE)) {}
        if (server_cpu >= 0)
            printf("  Internal Server started on Core %d\n", server_cpu);
    }

    if (client_cpu >= 0) thread_set_affinity(client_cpu);

    BatchQueueLatencyProbe *probe = batch_queue_latency_probe_create(
        1, dir, "request", "response", msg_size,
        validate_server, validate_client,
        write_rt, read_rt);

    printf("Warming up...\n");
    batch_queue_latency_probe_move_to_end(probe);
    for (int i = 0; i < warmup; i++)
        batch_queue_latency_probe_execute(probe);

    printf("Measuring...\n");
    uint64_t *results = malloc(sizeof(uint64_t) * (size_t)iterations);
    uint64_t start = monotonic_nanos();
    for (int i = 0; i < iterations; i++)
        results[i] = batch_queue_latency_probe_execute(probe);
    uint64_t elapsed = monotonic_nanos() - start;

    long errors = batch_queue_latency_probe_validation_errors(probe);
    if (errors > 0)
        printf("!!! WARNING: %ld validation errors detected !!!\n\n", errors);

    mmap_stats_print(results, iterations, elapsed, "Batch Round-Trip Latency Statistics (nanoseconds):");

    batch_queue_latency_probe_close(probe);
    free(results);

    if (!external_server) {
        pthread_join(server_thread, NULL);
        printf("  Internal Server stopped.\n");
    }

    return 0;
}
