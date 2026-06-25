#include "indexed_batch_appender.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "appender_meta.h"
#include "appender_id_pool.h"
#include "header_reader.h"
#include "header_codec.h"
#include "batch_payload_iterator.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

struct IndexedBatchAppender {
    char                *directory;
    char                *queue_name;
    uint8_t              appender_id;
    AppenderIdPool      *id_pool;
    AppenderMeta        *appender_meta;
    QueueMeta           *queue_meta;
    QueueRegionAccessor *rw_acc;
    RegionAccessor      *header_acc;
    RegionAccessor      *payload_acc;

    int64_t current_index;
    int64_t current_header_position;
    int64_t current_payload_position;

    Buffer header_buf;
    Buffer payload_buf;

    MutableBatch         current_batch;
    BatchPayloadIterator batch_iterator;

    int closed;
};

/* ── Internal Helpers ────────────────────────────────────────────────────── */

static void wrap_payload_or_fail(IndexedBatchAppender *a, int64_t position)
{
    WrapResult r = region_accessor_wrap(a->payload_acc, position, &a->payload_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS, "indexed_batch_appender: failed to wrap payload");
}

static void ensure_space_for_batch_header(IndexedBatchAppender *a)
{
    wrap_payload_or_fail(a, a->current_payload_position);
    if (a->payload_buf.capacity < BATCH_MSG_COUNT_SIZE) {
        a->current_payload_position += a->payload_buf.capacity;
        wrap_payload_or_fail(a, a->current_payload_position);
    }
}

static void write_padding_if_needed(IndexedBatchAppender *a, int32_t length)
{
    wrap_payload_or_fail(a, a->current_payload_position);
    int32_t cap = a->payload_buf.capacity;
    int32_t total_needed = length + PAYLOAD_LENGTH_SIZE;

    if (cap >= total_needed) return;

    /* Write long padding if there is space for it */
    if (cap > PAYLOAD_LENGTH_SIZE) {
        int32_t pad = PAYLOAD_LENGTH_SIZE - cap;
        memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &pad, sizeof(int32_t));
    }

    a->current_payload_position += cap;
    wrap_payload_or_fail(a, a->current_payload_position);
}

static int64_t skip_to_end_of_stream(IndexedBatchAppender *a, int64_t offset)
{
    int64_t end_of_stream = offset;
    while (1) {
        int64_t batch_off = batch_iterator_probe_batch(&a->batch_iterator, end_of_stream);
        if (batch_off == BATCH_ITERATOR_INVALID_POSITION) break;

        batch_iterator_init(&a->batch_iterator, batch_off, -1, 0);
        batch_iterator_exhaust(&a->batch_iterator);
        end_of_stream = a->batch_iterator.next_message_position;
    }
    return end_of_stream;
}

static int64_t appender_start_index_fn(void *ctx) { (void)ctx; return 0; }

static void initialise_append_position(IndexedBatchAppender *a, int32_t region_size, int ring_size, int64_t max_file_size, uint64_t timeout_ns, Runtime *runtime)
{
    QueueRegionAccessor *ro_acc = queue_region_accessor_create_ro(
        a->directory, a->queue_name, region_size, ring_size, 0, max_file_size, timeout_ns, runtime);

    RegionAccessor *ro_header = queue_region_accessor_header(ro_acc);
    Buffer ro_buf = {0};

    int64_t last_index = header_search_last_index(0, appender_start_index_fn, NULL, ro_header, &ro_buf, a->queue_name);

    if (last_index == HEADER_NULL_INDEX) {
        a->current_index = 0;
        a->current_payload_position = appender_meta_start_offset(a->appender_meta);
    } else {
        a->current_index = last_index + 1;
        int64_t committed_payload_pos = -1;
        for (int64_t i = last_index; i >= 0; i--) {
            uint64_t hdr = header_read(ro_header, &ro_buf, i, a->queue_name);
            if (hdr != HEADER_NULL && header_appender_id(hdr) == a->appender_id) {
                committed_payload_pos = header_payload_position(hdr);
                break;
            }
        }

        if (committed_payload_pos != -1) {
            a->current_payload_position = skip_to_end_of_stream(a, committed_payload_pos);
        } else {
            a->current_payload_position = appender_meta_start_offset(a->appender_meta);
        }
    }

    queue_region_accessor_close(ro_acc);
    a->current_header_position = header_position(a->current_index);

    /* Eager wrap */
    WrapResult r;
    r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS, "batch_appender: failed to eager-wrap header");

    r = region_accessor_wrap(a->payload_acc, a->current_payload_position, &a->payload_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS, "batch_appender: failed to eager-wrap payload");
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedBatchAppender *indexed_batch_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    IndexedBatchAppender *a = calloc(1, sizeof(IndexedBatchAppender));
    MMAP_ASSERT(a != NULL, "out of memory");

    a->directory  = strdup(directory);
    a->queue_name = strdup(queue_name);
    a->id_pool    = appender_id_pool_open(directory, queue_name);
    a->appender_id = (uint8_t)appender_id_pool_acquire(a->id_pool);

    a->queue_meta = queue_meta_open_rw(directory, queue_name, 0, region_size, max_file_size, 0);
    int32_t rec_reg = queue_meta_region_size(a->queue_meta);
    int64_t rec_max = queue_meta_max_file_size(a->queue_meta);

    a->appender_meta = appender_meta_open_rw(directory, queue_name, a->appender_id);
    a->rw_acc = queue_region_accessor_create_rw(directory, queue_name, rec_reg, ring_size, 1, rec_max, timeout_ns, runtime, a->appender_id);
    a->header_acc = queue_region_accessor_header(a->rw_acc);
    a->payload_acc = queue_region_accessor_payload(a->rw_acc, a->appender_id);

    a->batch_iterator.queue_name  = a->queue_name;
    a->batch_iterator.payload_acc = a->payload_acc;

    initialise_append_position(a, rec_reg, ring_size, rec_max, timeout_ns, runtime);

    return a;
}

/* ── Batch Operations ────────────────────────────────────────────────────── */

MutableBatch *indexed_batch_appender_start_batch(IndexedBatchAppender *a)
{
    MMAP_ASSERT(!a->closed, "appender closed");
    ensure_space_for_batch_header(a);

    a->current_batch.appender      = a;
    a->current_batch.batch_offset  = a->current_payload_position;
    a->current_batch.message_count = 0;

    a->current_payload_position += BATCH_MSG_COUNT_SIZE;
    return &a->current_batch;
}

int64_t mutable_batch_complete(MutableBatch *b)
{
    IndexedBatchAppender *a = b->appender;
    wrap_payload_or_fail(a, b->batch_offset);
    memcpy(a->payload_buf.data + BATCH_MSG_COUNT_OFFSET, &b->message_count, sizeof(int32_t));
    return b->batch_offset;
}

static void move_to_last_header(IndexedBatchAppender *a)
{
    while (1) {
        a->current_index++;
        a->current_header_position = header_position(a->current_index);
        WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
        (void)r;
        MMAP_ASSERT(r == WRAP_SUCCESS, "batch_appender: failed to wrap header");

        _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
        if (atomic_load_explicit(hp, memory_order_acquire) == HEADER_NULL) return;
    }
}

int64_t indexed_batch_appender_commit_batch(IndexedBatchAppender *a, int64_t batch_offset)
{
    uint64_t header = header_encode(a->appender_id, batch_offset);
    WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS, "batch_appender: failed to wrap header");

    _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
    uint64_t expected = HEADER_NULL;

    while (__builtin_expect(!atomic_compare_exchange_strong_explicit(hp, &expected, header, memory_order_acq_rel, memory_order_acquire), 0)) {
        move_to_last_header(a);
        r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
        (void)r;
        MMAP_ASSERT(r == WRAP_SUCCESS, "batch_appender: failed to re-wrap header");
        hp = (_Atomic uint64_t *)a->header_buf.data;
        expected = HEADER_NULL;
    }

    int64_t idx = a->current_index;
    a->current_index++;
    a->current_header_position = header_position(a->current_index);
    return idx;
}

int32_t mutable_batch_append(MutableBatch *b, const void *data, int32_t length)
{
    IndexedBatchAppender *a = b->appender;
    write_padding_if_needed(a, length);
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    memcpy(a->payload_buf.data + PAYLOAD_DATA_OFFSET, data, (size_t)length);
    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;
    return b->message_count++;
}

int32_t mutable_batch_append_reserved(MutableBatch *b, int32_t length, void **out_data)
{
    IndexedBatchAppender *a = b->appender;
    write_padding_if_needed(a, length);
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    *out_data = a->payload_buf.data + PAYLOAD_DATA_OFFSET;
    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;
    return b->message_count++;
}

int32_t mutable_batch_append_remaining_from(MutableBatch *b, BatchPayloadIterator *it)
{
    int32_t count = 0;
    while (batch_iterator_advance(it)) {
        mutable_batch_append(b, batch_iterator_current_data(it), batch_iterator_current_length(it));
        count++;
    }
    return count;
}

int32_t mutable_batch_message_count(const MutableBatch *b) { return b->message_count; }

const void *mutable_batch_get(MutableBatch *b, int32_t message_index, int32_t *out_length)
{
    IndexedBatchAppender *a = b->appender;
    batch_iterator_init(&a->batch_iterator, b->batch_offset, b->message_count, message_index);
    if (batch_iterator_advance(&a->batch_iterator)) {
        *out_length = a->batch_iterator.current_msg_length;
        return batch_iterator_current_data(&a->batch_iterator);
    }
    return NULL;
}

BatchPayloadIterator *mutable_batch_iterator(MutableBatch *b, int32_t start_message_index)
{
    IndexedBatchAppender *a = b->appender;
    return batch_iterator_init(&a->batch_iterator, b->batch_offset, b->message_count, start_message_index);
}

uint8_t indexed_batch_appender_id(const IndexedBatchAppender *a) { return a->appender_id; }

void indexed_batch_appender_close(IndexedBatchAppender *a)
{
    if (!a || a->closed) return;
    a->closed = 1;
    appender_meta_close(a->appender_meta);
    queue_meta_close(a->queue_meta);
    queue_region_accessor_close(a->rw_acc);
    appender_id_pool_release(a->id_pool, a->appender_id);
    appender_id_pool_close(a->id_pool);
    free(a->directory);
    free(a->queue_name);
    free(a);
}
