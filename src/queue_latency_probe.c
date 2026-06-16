#include "queue_latency_probe.h"
#include "mmap_platform.h"
#include "mmap_assert.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>

#define SHUTDOWN_SEQ 0

typedef struct {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

struct QueueLatencyProbe {
    uint16_t         instance_id;
    int32_t          message_size;
    int              server_cpu;
    
    IndexedQueue    *request_queue;
    IndexedQueue    *response_queue;
    
    IndexedAppender *request_appender;
    IndexedPoller   *response_poller;
    
    /* Server-side state */
    IndexedAppender *response_appender;
    IndexedPoller   *request_poller;
    
    pthread_t        server_thread;
    volatile int     shutdown_requested;
    volatile int     response_received;
    uint32_t         current_request_seq;
    
    uint8_t         *msg_buffer;
};

/* ── Server Handler ──────────────────────────────────────────────────────── */

static MessageHandlerDirective server_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    QueueLatencyProbe *p = (QueueLatencyProbe *)ctx;
    const LatencyHeader *hdr = (const LatencyHeader *)data;
    
    if (hdr->instance_id == p->instance_id) {
        if (hdr->sequence == SHUTDOWN_SEQ) {
            p->shutdown_requested = 1;
        } else {
            /* Echo the request back to the response queue */
            indexed_appender_append(p->response_appender, data, length);
        }
    }
    
    return HANDLER_ADVANCE;
}

static void *server_thread_fn(void *arg)
{
    QueueLatencyProbe *p = (QueueLatencyProbe *)arg;
    
    thread_set_name(pthread_self(), "latency-server");
    if (p->server_cpu >= 0) {
        thread_set_affinity(p->server_cpu);
    }
    
    while (!p->shutdown_requested) {
        indexed_poller_poll(p->request_poller, server_handler, p);
    }
    
    return NULL;
}

/* ── Client Handler ──────────────────────────────────────────────────────── */

static MessageHandlerDirective client_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    QueueLatencyProbe *p = (QueueLatencyProbe *)ctx;
    const LatencyHeader *hdr = (const LatencyHeader *)data;
    
    if (hdr->instance_id == p->instance_id && 
        hdr->sequence == p->current_request_seq) {
        p->response_received = 1;
    }
    
    return HANDLER_ADVANCE;
}

/* ── Public API ──────────────────────────────────────────────────────────── */

QueueLatencyProbe *queue_latency_probe_create(
    uint16_t    instance_id,
    const char *directory,
    const char *request_name,
    const char *response_name,
    int32_t     message_size,
    int         server_cpu,
    Runtime    *runtime)
{
    MMAP_ASSERT(message_size >= (int32_t)sizeof(LatencyHeader), 
                "message_size too small for latency header");

    QueueLatencyProbe *p = calloc(1, sizeof(QueueLatencyProbe));
    MMAP_ASSERT(p != NULL, "out of memory");
    
    p->instance_id  = instance_id;
    p->message_size = message_size;
    p->server_cpu   = server_cpu;
    p->msg_buffer   = calloc(1, (size_t)message_size);
    
    p->request_queue = indexed_queue_create_default(directory, request_name, runtime);
    p->response_queue = indexed_queue_create_default(directory, response_name, runtime);
    
    p->request_appender = indexed_queue_create_appender(p->request_queue);
    p->response_poller  = indexed_queue_create_poller(p->response_queue);
    
    /* Server components */
    p->request_poller   = indexed_queue_create_poller(p->request_queue);
    p->response_appender = indexed_queue_create_appender(p->response_queue);
    
    pthread_create(&p->server_thread, NULL, server_thread_fn, p);
    
    return p;
}

void queue_latency_probe_move_to_end(QueueLatencyProbe *p)
{
    indexed_poller_move_to_end(p->response_poller);
}

uint64_t queue_latency_probe_execute(QueueLatencyProbe *p)
{
    p->response_received = 0;
    p->current_request_seq++;
    
    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence    = p->current_request_seq;
    
    uint64_t start = monotonic_nanos();
    
    indexed_appender_append(p->request_appender, p->msg_buffer, p->message_size);
    
    while (!p->response_received) {
        indexed_poller_poll(p->response_poller, client_handler, p);
    }
    
    return monotonic_nanos() - start;
}

void queue_latency_probe_close(QueueLatencyProbe *p)
{
    if (p == NULL) return;
    
    /* Signal server to shut down */
    LatencyHeader hdr = { .instance_id = p->instance_id, .sequence = SHUTDOWN_SEQ };
    indexed_appender_append(p->request_appender, &hdr, sizeof(hdr));
    
    pthread_join(p->server_thread, NULL);
    
    indexed_appender_close(p->request_appender);
    indexed_poller_close(p->response_poller);
    indexed_appender_close(p->response_appender);
    indexed_poller_close(p->request_poller);
    
    indexed_queue_close(p->request_queue);
    indexed_queue_close(p->response_queue);
    
    free(p->msg_buffer);
    free(p);
}
