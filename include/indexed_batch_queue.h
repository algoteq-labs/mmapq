#ifndef INDEXED_BATCH_QUEUE_H
#define INDEXED_BATCH_QUEUE_H

#include <stdint.h>
#include "indexed_batch_appender.h"
#include "indexed_batch_poller.h"
#include "indexed_batch_reader.h"
#include "runtime.h"

/**
 * IndexedBatchQueue — factory for indexed batch queues.
 */

typedef struct IndexedBatchQueue IndexedBatchQueue;

IndexedBatchQueue *indexed_batch_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    Runtime    *write_runtime,
    Runtime    *read_runtime);

/**
 * indexed_batch_queue_create_default — create with default parameters.
 */
IndexedBatchQueue *indexed_batch_queue_create_default(
    const char *directory,
    const char *queue_name,
    Runtime    *write_runtime,
    Runtime    *read_runtime);

IndexedBatchAppender *indexed_batch_queue_create_appender(IndexedBatchQueue *q);

IndexedBatchPoller *indexed_batch_queue_create_poller(IndexedBatchQueue *q);

IndexedBatchReader *indexed_batch_queue_create_reader(IndexedBatchQueue *q);

void indexed_batch_queue_close(IndexedBatchQueue *q);

#endif /* INDEXED_BATCH_QUEUE_H */
