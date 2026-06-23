#include "queue_latency_probe.h"
#include "mmap_platform.h"
#include "mmap_assert.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <stdatomic.h>

#define SHUTDOWN_SEQ 0  /* safe: current_request_seq starts at 0 and pre-increments */

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

struct QueueLatencyProbe {
    uint16_t         instance_id;
    int32_t          message_size;
    int              validate_on_server;
    int              validate_on_client;
    
    IndexedQueue    *request_queue;
    IndexedQueue    *response_queue;
    
    IndexedAppender *request_appender;
    IndexedPoller   *response_poller;
    
    atomic_int       response_received;
    uint32_t         current_request_seq;
    atomic_int_fast64_t validation_errors;
    
    uint8_t         *msg_buffer;
};

/* ── Payload Pattern ─────────────────────────────────────────────────────── */

static void fill_payload_pattern(uint8_t *data, int32_t length, uint32_t sequence)
{
    if (length <= (int32_t)sizeof(LatencyHeader)) return;
    for (int32_t i = (int32_t)sizeof(LatencyHeader); i < length; i++) {
        data[i] = (uint8_t)(sequence + i);
    }
}

static int verify_payload_pattern(const uint8_t *data, int32_t length, uint32_t sequence)
{
    if (length <= (int32_t)sizeof(LatencyHeader)) return 1;
    const uint8_t *payload = (const uint8_t *)data;
    for (int32_t i = (int32_t)sizeof(LatencyHeader); i < length; i++) {
        if (payload[i] != (uint8_t)(sequence + i)) return 0;
    }
    return 1;
}

/* ── Client Handler ──────────────────────────────────────────────────────── */

static NextMove client_handler(
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
        
        if (p->validate_on_client) {
            if (!verify_payload_pattern(data, length, hdr->sequence)) {
                atomic_fetch_add_explicit(&p->validation_errors, 1, memory_order_relaxed);
            }
        }
        
        atomic_store_explicit(&p->response_received, 1, memory_order_relaxed);
    }
    
    return NEXT_ADVANCE;
}

/* ── Public API ──────────────────────────────────────────────────────────── */

QueueLatencyProbe *queue_latency_probe_create(
    uint16_t    instance_id,
    const char *directory,
    const char *request_name,
    const char *response_name,
    int32_t     message_size,
    int         validate_on_server,
    int         validate_on_client,
    Runtime    *write_runtime,
    Runtime    *read_runtime,
    int         fixed_size_8bytes)
{
    MMAP_ASSERT(message_size >= (int32_t)sizeof(LatencyHeader), 
                "message_size too small for latency header");

    QueueLatencyProbe *p = calloc(1, sizeof(QueueLatencyProbe));
    MMAP_ASSERT(p != NULL, "out of memory");
    
    p->instance_id        = instance_id;
    p->message_size       = message_size;
    p->validate_on_server = validate_on_server;
    p->validate_on_client = validate_on_client;
    
    if (posix_memalign((void **)&p->msg_buffer, 64, (size_t)message_size) != 0) {
        MMAP_ASSERT(0, "out of memory (msg_buffer)");
    }
    memset(p->msg_buffer, 0, (size_t)message_size);
    
    p->request_queue = indexed_queue_create_default_opt(directory, request_name, write_runtime, read_runtime, fixed_size_8bytes);
    p->response_queue = indexed_queue_create_default_opt(directory, response_name, write_runtime, read_runtime, fixed_size_8bytes);
    
    p->request_appender = indexed_queue_create_appender(p->request_queue);
    p->response_poller  = indexed_queue_create_poller(p->response_queue);
    
    atomic_store(&p->validation_errors, 0);
    
    return p;
}

int64_t queue_latency_probe_validation_errors(QueueLatencyProbe *p)
{
    return (int64_t)atomic_load_explicit(&p->validation_errors, memory_order_relaxed);
}

void queue_latency_probe_move_to_end(QueueLatencyProbe *p)
{
    indexed_poller_move_to_end(p->response_poller);
}

uint64_t queue_latency_probe_execute(QueueLatencyProbe *p)
{
    atomic_store_explicit(&p->response_received, 0, memory_order_relaxed);
    p->current_request_seq++;
    
    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence    = p->current_request_seq;
    
    if (p->validate_on_server || p->validate_on_client) {
        fill_payload_pattern(p->msg_buffer, p->message_size, p->current_request_seq);
    }
    
    uint64_t start = monotonic_nanos();
    
    indexed_appender_append(p->request_appender, p->msg_buffer, p->message_size);
    
    while (!atomic_load_explicit(&p->response_received, memory_order_relaxed)) {
        indexed_poller_poll(p->response_poller, client_handler, p);
    }
    
    return monotonic_nanos() - start;
}

void queue_latency_probe_close(QueueLatencyProbe *p)
{
    if (p == NULL) return;
    
    /* Confirmed shutdown: send shutdown signal and wait for echo */
    atomic_store_explicit(&p->response_received, 0, memory_order_relaxed);
    uint32_t previous_seq = p->current_request_seq;
    p->current_request_seq = SHUTDOWN_SEQ;

    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence    = SHUTDOWN_SEQ;
    
    indexed_appender_append(p->request_appender, p->msg_buffer, p->message_size);
    
    uint64_t timeout_at = monotonic_nanos() + 2000000000ULL; /* 2 seconds */
    while (!atomic_load_explicit(&p->response_received, memory_order_relaxed) && 
           monotonic_nanos() < timeout_at) {
        indexed_poller_poll(p->response_poller, client_handler, p);
    }
    p->current_request_seq = previous_seq;

    indexed_appender_close(p->request_appender);
    indexed_poller_close(p->response_poller);
    
    indexed_queue_close(p->request_queue);
    indexed_queue_close(p->response_queue);
    
    free(p->msg_buffer);
    free(p);
}
