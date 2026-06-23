#include "batch_queue_latency_probe.h"
#include "mmap_platform.h"
#include "mmap_assert.h"
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#define PRODUCER_ID_OFFSET 0
#define SEQUENCE_OFFSET    2
#define HEADER_LEN        6
#define SHUTDOWN_SEQ       0

typedef struct __attribute__((packed)) {
    uint16_t instance_id;
    uint32_t sequence;
} LatencyHeader;

struct BatchQueueLatencyProbe {
    uint16_t               instance_id;
    int32_t                message_size;
    int                    validate_on_server;
    int                    validate_on_client;

    IndexedBatchQueue     *request_queue;
    IndexedBatchQueue     *response_queue;

    IndexedBatchAppender  *request_appender;
    IndexedBatchPoller    *response_poller;

    atomic_int             response_received;
    uint32_t               current_request_seq;
    atomic_int_fast64_t    validation_errors;

    uint8_t               *msg_buffer;
};

static void fill_payload_pattern(uint8_t *data, int32_t length, uint32_t sequence)
{
    if (length <= HEADER_LEN) return;
    for (int32_t i = HEADER_LEN; i < length; i++) data[i] = (uint8_t)(sequence + i);
}

static int verify_payload_pattern(const uint8_t *data, int32_t length, uint32_t sequence)
{
    if (length <= HEADER_LEN) return 1;
    for (int32_t i = HEADER_LEN; i < length; i++) if (data[i] != (uint8_t)(sequence + i)) return 0;
    return 1;
}

static BatchNextMove client_handler(int64_t b_idx, int32_t m_idx, int32_t m_cnt, const void *data, int32_t length, void *ctx)
{
    BatchQueueLatencyProbe *p = (BatchQueueLatencyProbe *)ctx;
    LatencyHeader hdr;
    memcpy(&hdr, data, sizeof(LatencyHeader));

    if (hdr.instance_id == p->instance_id && hdr.sequence == p->current_request_seq) {
        if (p->validate_on_client && !verify_payload_pattern(data, length, hdr.sequence)) {
            atomic_fetch_add(&p->validation_errors, 1);
        }
        atomic_store(&p->response_received, 1);
    }
    return BATCH_ADVANCE;
}

BatchQueueLatencyProbe *batch_queue_latency_probe_create(
    uint16_t    instance_id,
    const char *directory,
    const char *request_name,
    const char *response_name,
    int32_t     message_size,
    int         validate_on_server,
    int         validate_on_client,
    Runtime    *write_runtime,
    Runtime    *read_runtime)
{
    BatchQueueLatencyProbe *p = calloc(1, sizeof(BatchQueueLatencyProbe));
    MMAP_ASSERT(p != NULL, "out of memory");

    p->instance_id = instance_id;
    p->message_size = message_size;
    p->validate_on_server = validate_on_server;
    p->validate_on_client = validate_on_client;

    p->msg_buffer = malloc((size_t)message_size);
    MMAP_ASSERT(p->msg_buffer != NULL, "out of memory");

    p->request_queue = indexed_batch_queue_create_default(directory, request_name, write_runtime, read_runtime);
    p->response_queue = indexed_batch_queue_create_default(directory, response_name, write_runtime, read_runtime);

    p->request_appender = indexed_batch_queue_create_appender(p->request_queue);
    p->response_poller = indexed_batch_queue_create_poller(p->response_queue);

    return p;
}

void batch_queue_latency_probe_move_to_end(BatchQueueLatencyProbe *p)
{
    indexed_batch_poller_move_to_end(p->response_poller);
}

uint64_t batch_queue_latency_probe_execute(BatchQueueLatencyProbe *p)
{
    atomic_store(&p->response_received, 0);
    p->current_request_seq++;

    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence = p->current_request_seq;

    if (p->validate_on_server || p->validate_on_client) fill_payload_pattern(p->msg_buffer, p->message_size, p->current_request_seq);

    uint64_t start = monotonic_nanos();

    MutableBatch *b = indexed_batch_appender_start_batch(p->request_appender);
    mutable_batch_append(b, p->msg_buffer, p->message_size);
    int64_t off = mutable_batch_complete(b);
    indexed_batch_appender_commit_batch(p->request_appender, off);

    while (!atomic_load(&p->response_received)) {
        indexed_batch_poller_poll(p->response_poller, client_handler, p);
    }

    return monotonic_nanos() - start;
}

int64_t batch_queue_latency_probe_validation_errors(BatchQueueLatencyProbe *p) { return atomic_load(&p->validation_errors); }

void batch_queue_latency_probe_send_shutdown(BatchQueueLatencyProbe *p)
{
    if (!p) return;

    atomic_store(&p->response_received, 0);
    uint32_t previous_seq   = p->current_request_seq;
    p->current_request_seq  = SHUTDOWN_SEQ;

    LatencyHeader *hdr = (LatencyHeader *)p->msg_buffer;
    hdr->instance_id = p->instance_id;
    hdr->sequence    = SHUTDOWN_SEQ;

    MutableBatch *b = indexed_batch_appender_start_batch(p->request_appender);
    mutable_batch_append(b, p->msg_buffer, p->message_size);
    indexed_batch_appender_commit_batch(p->request_appender, mutable_batch_complete(b));

    uint64_t timeout_at = monotonic_nanos() + 2000000000ULL; /* 2 seconds */
    while (!atomic_load(&p->response_received) && monotonic_nanos() < timeout_at)
        indexed_batch_poller_poll(p->response_poller, client_handler, p);

    p->current_request_seq = previous_seq;
}

void batch_queue_latency_probe_close(BatchQueueLatencyProbe *p)
{
    if (!p) return;
    indexed_batch_appender_close(p->request_appender);
    indexed_batch_poller_close(p->response_poller);
    indexed_batch_queue_close(p->request_queue);
    indexed_batch_queue_close(p->response_queue);
    free(p->msg_buffer);
    free(p);
}
