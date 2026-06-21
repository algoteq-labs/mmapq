#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_poller.h"
#include "runtime.h"
#include "mmap_platform.h"
#include "mmap_assert.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>

#define SHUTDOWN_SEQ 0

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

typedef struct {
    IndexedAppender *response_appender;
    volatile int     shutdown_requested;
} ServerCtx;

static MessageHandlerDirective echo_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    ServerCtx *s = (ServerCtx *)ctx;
    const LatencyHeader *hdr = (const LatencyHeader *)data;
    
    if (hdr->sequence == SHUTDOWN_SEQ) {
        s->shutdown_requested = 1;
    }
    /* Echo the entire payload back to the response queue */
    indexed_appender_append(s->response_appender, data, length);
    
    return HANDLER_ADVANCE;
}

int main(int argc, char *argv[])
{
    if (argc < 4 || argc > 6) {
        fprintf(stderr, "usage: %s <directory> <request_queue> <response_queue> [cpu_core] [mapper_cpu]\n", argv[0]);
        return 1;
    }

    const char *dir      = argv[1];
    const char *req_name = argv[2];
    const char *res_name = argv[3];
    int         cpu_core = (argc >= 5) ? atoi(argv[4]) : -1;
    int         mapper_cpu = (argc == 6) ? atoi(argv[5]) : -1;

    if (cpu_core >= 0) {
        if (thread_set_affinity(cpu_core) != 0) {
            perror("thread_set_affinity failed");
        }
    }

    printf("Echo Server started...\n");
    printf("  Queues: %s -> %s\n", req_name, res_name);
    if (cpu_core >= 0) printf("  Core:   %d\n", cpu_core);
    if (mapper_cpu >= 0) printf("  Mapper: %d\n", mapper_cpu);

    Runtime *rt = runtime_region_mapper();
    if (mapper_cpu >= 0) {
        runtime_set_affinity(rt, mapper_cpu);
    }
    
    IndexedQueue *q_req = indexed_queue_create_default(dir, req_name, rt);
    IndexedQueue *q_res = indexed_queue_create_default(dir, res_name, rt);
    
    IndexedPoller   *poller   = indexed_queue_create_poller(q_req);
    IndexedAppender *appender = indexed_queue_create_appender(q_res);
    
    ServerCtx ctx = {
        .response_appender = appender,
        .shutdown_requested = 0
    };

    printf("Waiting for requests...\n");

    while (!ctx.shutdown_requested) {
        indexed_poller_poll(poller, echo_handler, &ctx);
    }

    printf("Shutdown requested. Closing...\n");

    indexed_appender_close(appender);
    indexed_poller_close(poller);
    indexed_queue_close(q_req);
    indexed_queue_close(q_res);

    return 0;
}
