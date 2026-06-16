/*
 * producer.c — multiprocess test: producer side
 *
 * Writes N messages into the queue, then exits.
 * Usage: producer <queue_dir> <queue_name> <message_count> [appender_id]
 *
 * If appender_id is supplied, that specific ID is requested. This allows
 * two-producer tests to assign IDs explicitly so the consumer can track
 * per-appender sequences correctly.
 */

#include "indexed_queue.h"
#include "indexed_appender.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define REGION_SIZE   (256 * 1024)
#define RING_SIZE     4
#define MAX_FILE_SIZE ((int64_t)REGION_SIZE * 64)
#define TIMEOUT_NS    5000000000ULL

typedef struct {
    int64_t  sequence;
    uint32_t checksum;
    char     tag[20];
} TestMessage;

static uint32_t checksum_of(int64_t seq)
{
    return (uint32_t)((uint64_t)seq * 0xDEADBEEFULL);
}

int main(int argc, char *argv[])
{
    if (argc < 4 || argc > 5) {
        fprintf(stderr,
            "usage: %s <queue_dir> <queue_name> <message_count> [appender_id]\n",
            argv[0]);
        return 1;
    }

    const char *dir        = argv[1];
    const char *name       = argv[2];
    int64_t     count      = atoll(argv[3]);
    int         requested_id = (argc == 5) ? atoi(argv[4]) : -1;

    if (count <= 0) {
        fprintf(stderr, "message_count must be > 0\n");
        return 1;
    }

    Runtime      *rt = runtime_region_mapper();
    IndexedQueue *q  = indexed_queue_create(
        dir, name,
        REGION_SIZE, RING_SIZE, 1,
        MAX_FILE_SIZE, 0,
        TIMEOUT_NS, rt);

    IndexedAppender *a;
    if (requested_id >= 0) {
        a = indexed_queue_create_appender_with_id(q, (uint8_t)requested_id);
    } else {
        a = indexed_queue_create_appender(q);
    }

    fprintf(stderr, "producer: appender_id=%d writing %ld messages\n",
            indexed_appender_id(a), count);

    TestMessage msg;
    memset(msg.tag, 0, sizeof(msg.tag));
    strncpy(msg.tag, "mmap-c-producer", sizeof(msg.tag) - 1);

    for (int64_t i = 0; i < count; i++) {
        msg.sequence = i;
        msg.checksum = checksum_of(i);
        indexed_appender_append(a, &msg, sizeof(msg));
    }

    fprintf(stderr, "producer: done — wrote %ld messages, last index %ld\n",
            count, indexed_appender_current_index(a) - 1);

    indexed_appender_close(a);
    indexed_queue_close(q);
    return 0;
}
