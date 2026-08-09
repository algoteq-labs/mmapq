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

static NextMove echo_handler(
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
    
    return NEXT_ADVANCE;
}

int main(int argc, char *argv[])
{
    if (argc < 4) {
        fprintf(stderr, "usage: %s <directory> <request_queue> <response_queue> [cpu_core] [mapper_cpu] [--sync]\n", argv[0]);
        return 1;
    }

    const char *dir      = argv[1];
    const char *req_name = argv[2];
    const char *res_name = argv[3];
    int         cpu_core = (argc >= 5) ? atoi(argv[4]) : -1;
    int         mapper_cpu = (argc >= 6) ? atoi(argv[5]) : -1;

    int use_sync = 0;
    int use_mixed = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--sync") == 0 || strcmp(argv[i], "-s") == 0) {
            use_sync = 1;
        }
        if (strcmp(argv[i], "--mixed") == 0 || strcmp(argv[i], "-m") == 0) {
            use_mixed = 1;
        }
    }

    const char *mode_str = "ASYNCHRONOUS";
    if (use_sync) mode_str = "SYNCHRONOUS";
    if (use_mixed) mode_str = "MIXED (Sync-Read, Async-Write)";

    printf("Echo Server started...\n");
    printf("  Queues: %s -> %s (%s)\n", req_name, res_name, mode_str);
    if (cpu_core >= 0) printf("  Core:   %d\n", cpu_core);
    if (!use_sync && mapper_cpu >= 0) printf("  Mapper: %d\n", mapper_cpu);

    Runtime *write_rt = NULL;
    Runtime *read_rt  = NULL;

    if (use_sync) {
        /* Both NULL */
    } else if (use_mixed) {
        /* Server: Read is Sync (NULL), Write is Async (rt) */
        write_rt = runtime_region_mapper();
        read_rt  = NULL;
        if (mapper_cpu >= 0) runtime_set_affinity(write_rt, mapper_cpu);
    } else {
        write_rt = runtime_region_mapper();
        read_rt  = write_rt;
        if (mapper_cpu >= 0) runtime_set_affinity(write_rt, mapper_cpu);
    }
    
    /* Pin main thread AFTER spawning the mapper to prevent affinity inheritance */
    if (cpu_core >= 0) {
        if (thread_set_affinity(cpu_core) != 0) {
            perror("thread_set_affinity failed");
        }
    }
    
    IndexedQueue *q_req = indexed_queue_create_default(dir, req_name, write_rt, read_rt);
    IndexedQueue *q_res = indexed_queue_create_default(dir, res_name, write_rt, read_rt);
    
    IndexedPoller *poller = NULL;
    int rc_poller = indexed_queue_create_poller(q_req, &poller);
    TEST_ASSERT_EQUAL_INT(0, rc_poller);
    TEST_ASSERT_NOT_NULL(poller);
    IndexedAppender *appender = NULL;
    int rc_appender = indexed_queue_create_appender(q_res, &appender);
    TEST_ASSERT_EQUAL_INT(0, rc_appender);
    TEST_ASSERT_NOT_NULL(appender);
    
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
