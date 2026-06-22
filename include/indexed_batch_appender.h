#ifndef INDEXED_BATCH_APPENDER_H
#define INDEXED_BATCH_APPENDER_H

#include <stdint.h>
#include "indexed_appender.h"
#include "batch_payload_iterator.h"
#include "runtime.h"

/**
 * IndexedBatchAppender — appends batches of messages to an indexed queue.
 *
 * A single uint64 header is committed per BATCH of messages.
 */

typedef struct IndexedBatchAppender IndexedBatchAppender;
typedef struct MutableBatch       MutableBatch;

struct MutableBatch {
    IndexedBatchAppender *appender;
    int64_t               batch_offset;
    int32_t               message_count;
};

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedBatchAppender *indexed_batch_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime);

/* ── Batch Lifecycle ─────────────────────────────────────────────────────── */

/**
 * start_batch — prepares a new batch. Returns handle to the mutable batch.
 * The handle is a pointer to a struct embedded in the appender (zero allocation).
 */
MutableBatch *indexed_batch_appender_start_batch(IndexedBatchAppender *a);

/**
 * mutable_batch_complete — finalises the batch on disk.
 * Returns the payload offset of the batch.
 */
int64_t mutable_batch_complete(MutableBatch *b);

/**
 * commit_batch — commits the batch to the header file.
 * Returns the queue index assigned to the batch.
 */
int64_t indexed_batch_appender_commit_batch(IndexedBatchAppender *a, int64_t batch_offset);

/* ── Appending to Batch ──────────────────────────────────────────────────── */

int32_t mutable_batch_append(MutableBatch *b, const void *data, int32_t length);

/**
 * append_reserved — reserve space for a message and return direct write-address.
 */
int32_t mutable_batch_append_reserved(MutableBatch *b, int32_t length, void **out_data);

/**
 * append_remaining_from — append all remaining messages from an iterator.
 */
int32_t mutable_batch_append_remaining_from(MutableBatch *b, BatchPayloadIterator *it);

/* ── Querying In-Progress Batch ──────────────────────────────────────────── */

int32_t mutable_batch_message_count(const MutableBatch *b);

const void *mutable_batch_get(MutableBatch *b, int32_t message_index, int32_t *out_length);

BatchPayloadIterator *mutable_batch_iterator(MutableBatch *b, int32_t start_message_index);

/* ── Metadata & Lifecycle ────────────────────────────────────────────────── */

uint8_t indexed_batch_appender_id(const IndexedBatchAppender *a);

void indexed_batch_appender_close(IndexedBatchAppender *a);

#endif /* INDEXED_BATCH_APPENDER_H */
