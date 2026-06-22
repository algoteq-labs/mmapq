#ifndef INDEXED_BATCH_READER_H
#define INDEXED_BATCH_READER_H

#include <stdint.h>
#include "region_accessor.h"
#include "runtime.h"
#include "indexed_reader.h"
#include "batch_payload_iterator.h"

/**
 * IndexedBatchReader — random-access reader for indexed batch queues.
 *
 * Provides read-by-index, read-first, and read-last semantics.
 * All state is zero-allocation on the read path — BatchReadingContext is
 * caller-owned and reused across calls.
 */

typedef struct IndexedBatchReader IndexedBatchReader;

typedef struct {
    int64_t              index;
    EntryStatus          status;
    int64_t              batch_offset;
    int32_t              message_count;
    BatchPayloadIterator batch_iterator;
} BatchReadingContext;

IndexedBatchReader *indexed_batch_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime);

BatchReadingContext *indexed_batch_reader_read(
    IndexedBatchReader  *r,
    int64_t              index,
    BatchReadingContext *ctx);

BatchReadingContext *indexed_batch_reader_read_first(
    IndexedBatchReader  *r,
    BatchReadingContext *ctx);

BatchReadingContext *indexed_batch_reader_read_last(
    IndexedBatchReader  *r,
    BatchReadingContext *ctx);

int64_t     indexed_batch_reader_last_index(IndexedBatchReader *r);
int64_t     indexed_batch_reader_start_index(IndexedBatchReader *r);
EntryStatus indexed_batch_reader_entry_status(IndexedBatchReader *r, int64_t index);

static inline int indexed_batch_reader_has_entry(IndexedBatchReader *r, int64_t index)
{
    return indexed_batch_reader_entry_status(r, index) == ENTRY_AVAILABLE;
}

void indexed_batch_reader_close(IndexedBatchReader *r);

#endif /* INDEXED_BATCH_READER_H */
