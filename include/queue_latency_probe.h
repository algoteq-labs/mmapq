#ifndef QUEUE_LATENCY_PROBE_H
#define QUEUE_LATENCY_PROBE_H

#include "indexed_queue.h"
#include <stdint.h>

/**
 * QueueLatencyProbe — high-performance request/response latency measurement tool.
 *
 * Encapsulates two queues (request and response).
 * The client appends to 'request' and polls 'response'.
 *
 * This component is a pure client. An external server (C echo_server or Java EchoServer)
 * must be running to receive requests and echo them back.
 */

typedef struct QueueLatencyProbe QueueLatencyProbe;

/**
 * Create a new QueueLatencyProbe.
 *
 * instance_id:    unique ID for this probe instance.
 * directory:      base directory for queues.
 * request_name:   name of the request queue.
 * response_name:  name of the response queue.
 * message_size:   size of messages in bytes (min 6 bytes for header).
 * validate_on_server: if 1, the client expects the server to verify pattern.
 * validate_on_client: if 1, the client verifies response payload pattern.
 * runtime:        region mapper runtime.
 */
QueueLatencyProbe *queue_latency_probe_create(
    uint16_t    instance_id,
    const char *directory,
    const char *request_name,
    const char *response_name,
    int32_t     message_size,
    int         validate_on_server,
    int         validate_on_client,
    Runtime    *runtime);

/**
 * Returns the total number of validation errors detected so far.
 */
int64_t queue_latency_probe_validation_errors(QueueLatencyProbe *probe);

/**
 * Synchronise the probe with the current state of the queues by
 * moving the response poller to the end.
 */
void queue_latency_probe_move_to_end(QueueLatencyProbe *probe);

/**
 * Execute a single round-trip message exchange.
 *
 * Appends a request, then busy-polls the response queue until the
 * corresponding response is received.
 *
 * Returns the round-trip latency in nanoseconds.
 */
uint64_t queue_latency_probe_execute(QueueLatencyProbe *probe);

/**
 * Close the probe by sending a shutdown signal to the external server
 * and waiting for a confirmed echo before closing resources.
 */
void queue_latency_probe_close(QueueLatencyProbe *probe);

#endif /* QUEUE_LATENCY_PROBE_H */
