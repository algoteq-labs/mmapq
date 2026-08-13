#include "indexed_batch_appender.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/appender_meta.h"
#include "internal/appender_id_pool.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* Bounded linear scan to avoid livelock when the appender is far behind
 * the queue head. Falls back to a jump-based search for efficient catch-up. */
#define MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH  100

struct MmapqIndexedBatchAppender {
    char                *directory;
    char                *queue_name;
    uint8_t              appender_id;
    AppenderIdPool      *id_pool;
    AppenderMeta        *appender_meta;
    QueueMeta           *queue_meta;
    QueueRegionAccessor *rw_acc;
    RegionAccessor      *header_acc;
    RegionAccessor      *payload_acc;

    /* Read-only accessor for jump-based header search fallback */
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *ro_header;
    Buffer               ro_buf;

    int64_t current_index;
    int64_t current_header_position;
    int64_t current_payload_position;

    Buffer header_buf;
    Buffer payload_buf;

    MmapqMutableBatch         current_batch;
    MmapqBatchPayloadIterator batch_iterator;

    int closed;
};

/* ── Internal Helpers ────────────────────────────────────────────────────── */

static int wrap_payload_or_fail(MmapqIndexedBatchAppender *a, int64_t position)
{
    WrapResult r = region_accessor_wrap(a->payload_acc, position, &a->payload_buf);
    if (r != WRAP_SUCCESS) return -1;
    return 0;
}

static int ensure_space_for_batch_header(MmapqIndexedBatchAppender *a)
{
    if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    if (a->payload_buf.capacity < MMAPQ_BATCH_MSG_COUNT_SIZE) {
        a->current_payload_position += a->payload_buf.capacity;
        if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    }
    return 0;
}

static int write_padding_if_needed(MmapqIndexedBatchAppender *a, int32_t length)
{
    if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    int32_t cap = a->payload_buf.capacity;
    int32_t total_needed = length + PAYLOAD_LENGTH_SIZE;

    if (cap >= total_needed) return 0;

    /* Write long padding if there is space for it */
    if (cap > PAYLOAD_LENGTH_SIZE) {
        int32_t pad = PAYLOAD_LENGTH_SIZE - cap;
        memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &pad, sizeof(int32_t));
    }

    a->current_payload_position += cap;
    if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    return 0;
}

static int64_t skip_to_end_of_stream(MmapqIndexedBatchAppender *a, int64_t offset)
{
    int64_t end_of_stream = offset;
    while (1) {
        int64_t batch_off = mmapq_batch_iterator_probe_batch(&a->batch_iterator, end_of_stream);
        if (batch_off == MMAPQ_BATCH_ITERATOR_INVALID_POSITION) break;

        mmapq_batch_iterator_init(&a->batch_iterator, batch_off, -1, 0);
        mmapq_batch_iterator_exhaust(&a->batch_iterator);
        end_of_stream = a->batch_iterator.next_message_position;
    }
    return end_of_stream;
}

static int64_t appender_start_index_from_meta(void *ctx)
{
    return queue_meta_start_index((const QueueMeta *)ctx);
}

static int initialise_append_position(MmapqIndexedBatchAppender *a, int64_t start_index)
{
    RegionAccessor *ro_header = a->ro_header;
    Buffer         *ro_buf   = &a->ro_buf;

    int64_t last_index = mmapqi_header_search_last_index(
        start_index,
        appender_start_index_from_meta, a->queue_meta,
        ro_header, ro_buf,
        a->queue_name);

    if (last_index == HEADER_NULL_INDEX) {
        a->current_index = start_index;
        a->current_payload_position = appender_meta_start_offset(a->appender_meta);
    } else {
        a->current_index = last_index + 1;
        int64_t committed_payload_pos = -1;
        for (int64_t i = last_index; i >= 0; i--) {
            uint64_t hdr = mmapqi_header_read(ro_header, ro_buf, i, a->queue_name);
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

    a->current_header_position = header_position(a->current_index);

    /* Eager wrap */
    if (region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf) != WRAP_SUCCESS) return -1;
    if (region_accessor_wrap(a->payload_acc, a->current_payload_position, &a->payload_buf) != WRAP_SUCCESS) return -1;

    return 0;
}


/* ── Factory ─────────────────────────────────────────────────────────────── */

static int batch_appender_create_internal(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    AppenderIdPool *pool,
    uint8_t     appender_id,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;

    MmapqIndexedBatchAppender *a = calloc(1, sizeof(MmapqIndexedBatchAppender));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory    = strdup(directory);
    if (a->directory == NULL) { free(a); return MMAPQ_ENOMEM; }
    a->queue_name   = strdup(queue_name);
    if (a->queue_name == NULL) { free(a->directory); free(a); return MMAPQ_ENOMEM; }
    a->id_pool      = pool;
    a->appender_id  = appender_id;

    int rc = mmapqi_queue_meta_open_rw(directory, queue_name, start_index, region_size, max_file_size, archivable, QUEUE_TYPE_BATCH, &a->queue_meta);
    if (rc != 0) {
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    int32_t rec_reg = queue_meta_region_size(a->queue_meta);
    int64_t rec_max = queue_meta_max_file_size(a->queue_meta);

    rc = mmapqi_appender_meta_open_rw(directory, queue_name, a->appender_id, &a->appender_meta);
    if (rc != 0) {
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    rc = mmapqi_queue_region_accessor_create_rw(directory, queue_name, rec_reg, ring_size, 1, rec_max, timeout_ns, runtime, a->appender_id, &a->rw_acc);
    if (rc != 0) {
        mmapqi_appender_meta_close(a->appender_meta);
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    a->header_acc = queue_region_accessor_header(a->rw_acc);
    a->payload_acc = mmapqi_queue_region_accessor_payload(a->rw_acc, a->appender_id);

    a->batch_iterator.queue_name  = a->queue_name;
    a->batch_iterator.payload_acc = a->payload_acc;

    rc = mmapqi_queue_region_accessor_create_ro(
        directory, queue_name,
        rec_reg, ring_size, 0,
        rec_max, timeout_ns, runtime,
        &a->ro_acc);
    if (rc != 0) {
        mmapqi_queue_region_accessor_close(a->rw_acc);
        mmapqi_appender_meta_close(a->appender_meta);
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    a->ro_header = queue_region_accessor_header(a->ro_acc);

    int init_rc = initialise_append_position(a, start_index);
    if (init_rc != 0) {
        mmapqi_queue_region_accessor_close(a->ro_acc);
        mmapqi_queue_region_accessor_close(a->rw_acc);
        mmapqi_appender_meta_close(a->appender_meta);
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return init_rc;
    }

    *out = a;
    return 0;
}

int mmapq_indexed_batch_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    AppenderIdPool *pool = NULL;
    int rc = mmapqi_appender_id_pool_open(directory, queue_name, &pool);
    if (rc != 0) return rc;

    int id = mmapqi_appender_id_pool_acquire(pool);
    if (id < 0) {
        mmapqi_appender_id_pool_close(pool);
        return MMAPQ_EIDCONFLICT;
    }

    return batch_appender_create_internal(directory, queue_name, region_size,
        ring_size, max_file_size, timeout_ns, archivable, runtime,
        pool, (uint8_t)id, start_index, out);
}

int mmapq_indexed_batch_appender_create_with_id(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    uint8_t     appender_id,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    AppenderIdPool *pool = NULL;
    int rc = mmapqi_appender_id_pool_open(directory, queue_name, &pool);
    if (rc != 0) return rc;

    int ok = mmapqi_appender_id_pool_try_acquire(pool, appender_id);
    if (!ok) {
        mmapqi_appender_id_pool_close(pool);
        return MMAPQ_EIDCONFLICT;
    }

    return batch_appender_create_internal(directory, queue_name, region_size,
        ring_size, max_file_size, timeout_ns, archivable, runtime,
        pool, appender_id, start_index, out);
}

/* ── Batch Operations ────────────────────────────────────────────────────── */

MmapqMutableBatch *mmapq_indexed_batch_appender_start_batch(MmapqIndexedBatchAppender *a)
{
    if (a->closed) return NULL;
    if (ensure_space_for_batch_header(a)) return NULL;

    a->current_batch.appender      = a;
    a->current_batch.batch_offset  = a->current_payload_position;
    a->current_batch.message_count = 0;

    a->current_payload_position += MMAPQ_BATCH_MSG_COUNT_SIZE;
    return &a->current_batch;
}

int64_t mmapq_mutable_batch_complete(MmapqMutableBatch *b)
{
    if (b == NULL) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (wrap_payload_or_fail(a, b->batch_offset)) return -1;
    memcpy(a->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, &b->message_count, sizeof(int32_t));
    return b->batch_offset;
}

static int move_to_last_header(MmapqIndexedBatchAppender *a)
{
    int attempts = 0;
    while (attempts < MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH) {
        attempts++;
        a->current_index++;
        a->current_header_position = header_position(a->current_index);
        WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
        if (r != WRAP_SUCCESS) return -1;

        _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
        if (atomic_load_explicit(hp, memory_order_acquire) == HEADER_NULL) return 0;
    }

    int64_t last_index = mmapqi_header_search_last_index(
        a->current_index + 1,
        appender_start_index_from_meta, a->queue_meta,
        a->ro_header, &a->ro_buf,
        a->queue_name);

    if (last_index == HEADER_NULL_INDEX) {
        a->current_index++;
    } else {
        a->current_index = last_index + 1;
    }

    int64_t start_index = queue_meta_start_index(a->queue_meta);
    if (a->current_index < start_index) {
        a->current_index = start_index;
    }

    a->current_header_position = header_position(a->current_index);

    WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    return 0;
}

int64_t mmapq_indexed_batch_appender_commit_batch(MmapqIndexedBatchAppender *a, int64_t batch_offset)
{
    uint64_t header = header_encode(a->appender_id, batch_offset);
    WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
    uint64_t expected = HEADER_NULL;

    while (__builtin_expect(!atomic_compare_exchange_strong_explicit(hp, &expected, header, memory_order_acq_rel, memory_order_acquire), 0)) {
        if (move_to_last_header(a)) return -1;
        r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
        if (r != WRAP_SUCCESS) return -1;
        hp = (_Atomic uint64_t *)a->header_buf.data;
        expected = HEADER_NULL;
    }

    int64_t idx = a->current_index;
    a->current_index++;
    a->current_header_position = header_position(a->current_index);
    return idx;
}

void mmapq_indexed_batch_appender_discard_batch(MmapqIndexedBatchAppender *a, int64_t batch_offset)
{
    WrapResult r = region_accessor_wrap(a->payload_acc, batch_offset, &a->payload_buf);
    if (r != WRAP_SUCCESS) return;  /* cannot rollback on timeout */

    int32_t zero = 0;
    memcpy(a->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, &zero, sizeof(int32_t));
    a->current_payload_position = batch_offset;
}

int64_t mmapq_indexed_batch_appender_append_and_commit(MmapqIndexedBatchAppender *a, const void *data, int32_t length)
{
    MmapqMutableBatch *b = mmapq_indexed_batch_appender_start_batch(a);
    if (!b) return -1;
    int append_rc = mmapq_mutable_batch_append(b, data, length);
    if (append_rc < 0) return append_rc;
    int64_t off = mmapq_mutable_batch_complete(b);
    if (off < 0) return -1;
    return mmapq_indexed_batch_appender_commit_batch(a, off);
}

int32_t mmapq_mutable_batch_append(MmapqMutableBatch *b, const void *data, int32_t length)
{
    if (b == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
    if (write_padding_if_needed(a, length)) return -1;
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    memcpy(a->payload_buf.data + PAYLOAD_DATA_OFFSET, data, (size_t)length);
    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;
    return b->message_count++;
}

int32_t mmapq_mutable_batch_append_reserved(MmapqMutableBatch *b, int32_t length, void **out_data)
{
    if (b == NULL || out_data == NULL || length <= 0) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
    if (write_padding_if_needed(a, length)) return -1;
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    *out_data = a->payload_buf.data + PAYLOAD_DATA_OFFSET;
    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;
    return b->message_count++;
}

int32_t mmapq_mutable_batch_append_remaining_from(MmapqMutableBatch *b, MmapqBatchPayloadIterator *it)
{
    if (b == NULL) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    int32_t region_sz = queue_meta_region_size(a->queue_meta);
    int32_t count = 0;
    while (mmapq_batch_iterator_advance(it)) {
        int32_t msg_len = batch_iterator_current_length(it);
        if (msg_len > region_sz - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
        int rc = mmapq_mutable_batch_append(b, batch_iterator_current_data(it), msg_len);
        if (rc < 0) return rc;
        count++;
    }
    return count;
}

int32_t mmapq_mutable_batch_message_count(const MmapqMutableBatch *b) { return b->message_count; }

const void *mmapq_mutable_batch_get(MmapqMutableBatch *b, int32_t message_index, int32_t *out_length)
{
    MmapqIndexedBatchAppender *a = b->appender;
    mmapq_batch_iterator_init(&a->batch_iterator, b->batch_offset, b->message_count, message_index);
    if (mmapq_batch_iterator_advance(&a->batch_iterator)) {
        *out_length = a->batch_iterator.current_msg_length;
        return batch_iterator_current_data(&a->batch_iterator);
    }
    return NULL;
}

MmapqBatchPayloadIterator *mmapq_mutable_batch_iterator(MmapqMutableBatch *b, int32_t start_message_index)
{
    MmapqIndexedBatchAppender *a = b->appender;
    return mmapq_batch_iterator_init(&a->batch_iterator, b->batch_offset, b->message_count, start_message_index);
}

uint8_t mmapq_indexed_batch_appender_id(const MmapqIndexedBatchAppender *a) { return a->appender_id; }

void mmapq_indexed_batch_appender_close(MmapqIndexedBatchAppender *a)
{
    if (!a || a->closed) return;
    a->closed = 1;
    mmapqi_appender_meta_close(a->appender_meta);
    mmapqi_queue_meta_close(a->queue_meta);
    if (a->rw_acc) {
        mmapqi_queue_region_accessor_close(a->rw_acc);
        a->rw_acc = NULL;
    }
    if (a->ro_acc) {
        /* ro_header is owned by ro_acc — no separate close */
        mmapqi_queue_region_accessor_close(a->ro_acc);
        a->ro_acc    = NULL;
        a->ro_header = NULL;
    }
    mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
    mmapqi_appender_id_pool_close(a->id_pool);
    free(a->directory);
    free(a->queue_name);
    free(a);
}
