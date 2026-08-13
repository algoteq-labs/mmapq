#include "internal/queue_latency_probe.h"
#include "mmap_platform.h"
#include "internal/mmap_assert.h"
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
    
    MmapqIndexedQueue    *request_queue;
    MmapqIndexedQueue    *response_queue;
    
    MmapqIndexedAppender *request_appender;
    MmapqIndexedPoller   *response_poller;
    
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

static MmapqNextMove client_handler(
    int64_t     index,
    uint8_t     appender_id,
    const void *data,
    int32_t     length,
    void       *ctx)
{
    (void)index; (void)appender_id;
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
    
    return MMAPQ_NEXT_ADVANCE;
}

/* ── Public API ──────────────────────────────────────────────────────────── */

int mmapqi_queue_latency_probe_create(
    uint16_t    instance_id,
    const char *directory,
    const char *request_name,
    const char *response_name,
    int32_t     message_size,
    int         validate_on_server,
    int         validate_on_client,
    MmapqRuntime    *write_runtime,
    MmapqRuntime    *read_runtime,
    QueueLatencyProbe **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (message_size < (int32_t)sizeof(LatencyHeader)) return MMAPQ_EINVAL;

    QueueLatencyProbe *p = calloc(1, sizeof(QueueLatencyProbe));
    if (p == NULL) return MMAPQ_ENOMEM;
    
    p->instance_id        = instance_id;
    p->message_size       = message_size;
    p->validate_on_server = validate_on_server;
    p->validate_on_client = validate_on_client;
    
    if (posix_memalign((void **)&p->msg_buffer, 64, (size_t)message_size) != 0) {
        free(p); return MMAPQ_ENOMEM;
    }
    memset(p->msg_buffer, 0, (size_t)message_size);
    
    {
        int rc = mmapq_indexed_queue_create_default(directory, request_name, write_runtime, read_runtime, &p->request_queue);
        if (rc != 0) { free(p->msg_buffer); free(p); return rc; }
    }
    {
        int rc = mmapq_indexed_queue_create_default(directory, response_name, write_runtime, read_runtime, &p->response_queue);
        if (rc != 0) { mmapq_indexed_queue_close(p->request_queue); free(p->msg_buffer); free(p); return rc; }
    }
    
    p->request_appender = NULL;
    {
        int rc = mmapq_indexed_queue_create_appender(p->request_queue, &p->request_appender);
        if (rc != 0) { mmapq_indexed_queue_close(p->response_queue); mmapq_indexed_queue_close(p->request_queue); free(p->msg_buffer); free(p); return rc; }
    }
    p->response_poller = NULL;
    {
        int rc = mmapq_indexed_queue_create_poller(p->response_queue, &p->response_poller);
        if (rc != 0) { mmapq_indexed_appender_close(p->request_appender); mmapq_indexed_queue_close(p->response_queue); mmapq_indexed_queue_close(p->request_queue); free(p->msg_buffer); free(p); return rc; }
    }
    
    atomic_store(&p->validation_errors, 0);
    
    *out = p;
    return 0;
}

int64_t mmapqi_queue_latency_probe_validation_errors(QueueLatencyProbe *p)
{
    return (int64_t)atomic_load_explicit(&p->validation_errors, memory_order_relaxed);
}

void mmapqi_queue_latency_probe_move_to_end(QueueLatencyProbe *p)
{
    mmapq_indexed_poller_move_to_end(p->response_poller);
}

uint64_t mmapqi_queue_latency_probe_execute(QueueLatencyProbe *p)
{
    atomic_store_explicit(&p->response_received, 0, memory_order_relaxed);
    p->current_request_seq++;
    
    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence    = p->current_request_seq;
    
    if (p->validate_on_server || p->validate_on_client) {
        fill_payload_pattern(p->msg_buffer, p->message_size, p->current_request_seq);
    }
    
    uint64_t start = mmapq_monotonic_nanos();
    
    mmapq_indexed_appender_append(p->request_appender, p->msg_buffer, p->message_size);
    
    while (!atomic_load_explicit(&p->response_received, memory_order_relaxed)) {
        mmapq_indexed_poller_poll(p->response_poller, client_handler, p);
    }
    
    return mmapq_monotonic_nanos() - start;
}

void mmapqi_queue_latency_probe_close(QueueLatencyProbe *p)
{
    if (p == NULL) return;
    
    /* Confirmed shutdown: send shutdown signal and wait for echo */
    atomic_store_explicit(&p->response_received, 0, memory_order_relaxed);
    uint32_t previous_seq = p->current_request_seq;
    p->current_request_seq = SHUTDOWN_SEQ;

    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence    = SHUTDOWN_SEQ;
    
    mmapq_indexed_appender_append(p->request_appender, p->msg_buffer, p->message_size);
    
    uint64_t timeout_at = mmapq_monotonic_nanos() + 2000000000ULL; /* 2 seconds */
    while (!atomic_load_explicit(&p->response_received, memory_order_relaxed) && 
           mmapq_monotonic_nanos() < timeout_at) {
        mmapq_indexed_poller_poll(p->response_poller, client_handler, p);
    }
    p->current_request_seq = previous_seq;

    mmapq_indexed_appender_close(p->request_appender);
    mmapq_indexed_poller_close(p->response_poller);
    
    mmapq_indexed_queue_close(p->request_queue);
    mmapq_indexed_queue_close(p->response_queue);
    
    free(p->msg_buffer);
    free(p);
}
