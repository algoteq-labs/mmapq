#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "internal/mmap_assert.h"
#include "internal/batch_payload_iterator_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SHUTDOWN_SEQ 0

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

typedef struct {
    MmapqIndexedBatchAppender *response_appender;
    volatile int          shutdown_requested;
} ServerCtx;

static MmapqNextMove echo_handler(
    int64_t              b_idx,
    MmapqBatchReadingContext *ctx,
    void                *user_ctx)
{
    (void)b_idx;
    ServerCtx *s = (ServerCtx *)user_ctx;

    if (!mmapq_batch_iterator_advance(ctx->iterator))
        return MMAPQ_NEXT_ADVANCE;
    const LatencyHeader *hdr = batch_iterator_current_data(ctx->iterator);

    if (hdr->sequence == SHUTDOWN_SEQ)
        s->shutdown_requested = 1;

    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(s->response_appender);
    mmapq_mutable_batch_append(b, batch_iterator_current_data(ctx->iterator),
                            batch_iterator_current_length(ctx->iterator));
    mmapq_mutable_batch_append_remaining_from(b, ctx->iterator);
    mmapq_indexed_batch_appender_commit_batch(s->response_appender, mmapq_mutable_batch_complete(b));

    return MMAPQ_NEXT_ADVANCE;
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

    MmapqRuntime *write_rt = NULL, *read_rt = NULL;
    if (!use_sync) {
        MmapqRuntime *rt = NULL;
        int rc_rt = mmapq_runtime_region_mapper(&rt);
        if (rc_rt != 0) { fprintf(stderr, "mmapq_runtime_region_mapper failed\n"); return 1; }
        if (mapper_cpu >= 0) mmapq_runtime_set_affinity(rt, mapper_cpu);
        if (use_mixed) { write_rt = rt; read_rt = NULL; }
        else           { write_rt = rt; read_rt = rt;   }
    }

    if (cpu_core >= 0) mmapq_thread_set_affinity(cpu_core);

    MmapqIndexedBatchQueue    *q_req    = NULL;
    MmapqIndexedBatchQueue    *q_res    = NULL;
    MmapqIndexedBatchPoller   *poller   = NULL;
    MmapqIndexedBatchAppender *appender = NULL;

    int rc_qr = mmapq_indexed_batch_queue_create_default(dir, req, write_rt, read_rt, &q_req);
    if (rc_qr != 0) { fprintf(stderr, "failed to create request queue\n"); return 1; }
    int rc_qs = mmapq_indexed_batch_queue_create_default(dir, res, write_rt, read_rt, &q_res);
    if (rc_qs != 0) { fprintf(stderr, "failed to create response queue\n"); return 1; }
    int rc_p = mmapq_indexed_batch_queue_create_poller(q_req, &poller);
    if (rc_p != 0) { fprintf(stderr, "failed to create poller\n"); return 1; }
    int rc_ap = mmapq_indexed_batch_queue_create_appender(q_res, &appender);
    if (rc_ap != 0) { fprintf(stderr, "failed to create appender\n"); return 1; }

    ServerCtx ctx = { .response_appender = appender, .shutdown_requested = 0 };

    printf("Batch Echo Server started...\n");
    printf("  Queues: %s -> %s (%s)\n", req, res, mode_str);
    if (cpu_core   >= 0) printf("  Core:   %d\n", cpu_core);
    if (!use_sync && mapper_cpu >= 0) printf("  Mapper: %d\n", mapper_cpu);
    printf("Waiting for requests...\n");

    while (!ctx.shutdown_requested) {
        mmapq_indexed_batch_poller_poll(poller, echo_handler, &ctx);
    }

    printf("Shutdown requested. Closing...\n");

    mmapq_indexed_batch_appender_close(appender);
    mmapq_indexed_batch_poller_close(poller);
    mmapq_indexed_batch_queue_close(q_req);
    mmapq_indexed_batch_queue_close(q_res);

    return 0;
}
