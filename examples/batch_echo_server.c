#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "mmap_assert.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SHUTDOWN_SEQ 0

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

typedef struct {
    IndexedBatchAppender *response_appender;
    volatile int          shutdown_requested;
} ServerCtx;

static NextMove echo_handler(
    int64_t              b_idx,
    BatchReadingContext *ctx,
    void                *user_ctx)
{
    (void)b_idx;
    ServerCtx *s = (ServerCtx *)user_ctx;

    if (!batch_iterator_advance(&ctx->batch_iterator))
        return NEXT_ADVANCE;
    const LatencyHeader *hdr = batch_iterator_current_data(&ctx->batch_iterator);

    if (hdr->sequence == SHUTDOWN_SEQ)
        s->shutdown_requested = 1;

    MutableBatch *b = indexed_batch_appender_start_batch(s->response_appender);
    mutable_batch_append(b, batch_iterator_current_data(&ctx->batch_iterator),
                            batch_iterator_current_length(&ctx->batch_iterator));
    mutable_batch_append_remaining_from(b, &ctx->batch_iterator);
    indexed_batch_appender_commit_batch(s->response_appender, mutable_batch_complete(b));

    return NEXT_ADVANCE;
}

int main(int argc, char *argv[])
{
    if (argc < 4) {
        fprintf(stderr,
            "usage: %s <directory> <req_queue> <res_queue> [cpu_core] [mapper_cpu] [--sync] [--mixed]\n",
            argv[0]);
        return 1;
    }

    const char *dir    = argv[1];
    const char *req    = argv[2];
    const char *res    = argv[3];
    int cpu_core       = (argc >= 5) ? atoi(argv[4]) : -1;
    int mapper_cpu     = (argc >= 6) ? atoi(argv[5]) : -1;

    int use_sync = 0, use_mixed = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--sync")  == 0) use_sync  = 1;
        if (strcmp(argv[i], "--mixed") == 0) use_mixed = 1;
    }

    const char *mode_str = "ASYNCHRONOUS";
    if (use_sync)        mode_str = "SYNCHRONOUS";
    else if (use_mixed)  mode_str = "MIXED";

    Runtime *write_rt = NULL, *read_rt = NULL;
    if (!use_sync) {
        Runtime *rt = runtime_region_mapper();
        if (mapper_cpu >= 0) runtime_set_affinity(rt, mapper_cpu);
        if (use_mixed) { write_rt = rt; read_rt = NULL; }
        else           { write_rt = rt; read_rt = rt;   }
    }

    if (cpu_core >= 0) thread_set_affinity(cpu_core);

    IndexedBatchQueue    *q_req    = indexed_batch_queue_create_default(dir, req, write_rt, read_rt);
    IndexedBatchQueue    *q_res    = indexed_batch_queue_create_default(dir, res, write_rt, read_rt);
    IndexedBatchPoller   *poller   = indexed_batch_queue_create_poller(q_req);
    IndexedBatchAppender *appender = indexed_batch_queue_create_appender(q_res);

    ServerCtx ctx = { .response_appender = appender, .shutdown_requested = 0 };

    printf("Batch Echo Server started...\n");
    printf("  Queues: %s -> %s (%s)\n", req, res, mode_str);
    if (cpu_core   >= 0) printf("  Core:   %d\n", cpu_core);
    if (!use_sync && mapper_cpu >= 0) printf("  Mapper: %d\n", mapper_cpu);
    printf("Waiting for requests...\n");

    while (!ctx.shutdown_requested) {
        indexed_batch_poller_poll(poller, echo_handler, &ctx);
    }

    printf("Shutdown requested. Closing...\n");

    indexed_batch_appender_close(appender);
    indexed_batch_poller_close(poller);
    indexed_batch_queue_close(q_req);
    indexed_batch_queue_close(q_res);

    return 0;
}
