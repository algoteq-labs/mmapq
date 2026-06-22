#ifndef BATCH_QUEUE_LATENCY_PROBE_H
#define BATCH_QUEUE_LATENCY_PROBE_H

#include <stdint.h>
#include "indexed_batch_queue.h"
#include "runtime.h"

typedef struct BatchQueueLatencyProbe BatchQueueLatencyProbe;

BatchQueueLatencyProbe *batch_queue_latency_probe_create(
    uint16_t    instance_id,
    const char *directory,
    const char *request_name,
    const char *response_name,
    int32_t     message_size,
    int         validate_on_server,
    int         validate_on_client,
    Runtime    *write_runtime,
    Runtime    *read_runtime);

void batch_queue_latency_probe_move_to_end(BatchQueueLatencyProbe *p);

uint64_t batch_queue_latency_probe_execute(BatchQueueLatencyProbe *p);

int64_t batch_queue_latency_probe_validation_errors(BatchQueueLatencyProbe *p);

void batch_queue_latency_probe_close(BatchQueueLatencyProbe *p);

#endif /* BATCH_QUEUE_LATENCY_PROBE_H */
