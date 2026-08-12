#include "indexed_batch_reader.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/batch_payload_iterator_internal.h"

#include <stdlib.h>
#include <string.h>

struct IndexedBatchReader {
    char                *directory;
    char                *queue_name;
    int                  ring_size;
    int                  regions_to_map_ahead;
    uint64_t             timeout_ns;
    Runtime             *runtime;

    /* Lazy-init */
    QueueMeta           *queue_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;

    /*
     * Reusable buffer — must be a struct member, not a pointer.
     * header_read() and header_search_last_index() take &r->header_buf.
     * Using the context's payload_buf here would be incorrect since
     * batch_iterator_init() immediately re-wraps it for payload access.
     */
    Buffer               header_buf;

    /* Reusable iterator storage — owned by this reader, borrowed by the
     * BatchReadingContext during read calls. Valid only until the next
     * read call on this reader. */
    BatchPayloadIterator  batch_iterator;

    int64_t cached_last_index;
    int     closed;
    int     meta_init_failed;
    int     init_result;
    int     last_error;       /* MMAPQ_* code from last BATCH_ENTRY_ERROR, 0 otherwise */
};

static int64_t reader_start_index_fn(void *ctx)
{
    IndexedBatchReader *r = (IndexedBatchReader *)ctx;
    if (r->queue_meta == NULL) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(r->queue_meta);
}

static int reader_init(IndexedBatchReader *r)
{
    if (r->queue_meta != NULL) return 1;
    if (r->meta_init_failed) return 0;

    int rc = queue_meta_open_ro(r->directory, r->queue_name, QUEUE_TYPE_BATCH, &r->queue_meta);
    if (rc != 0) {
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            r->meta_init_failed = 1;
        r->init_result = rc;
        return 0;
    }

    int acc_rc = queue_region_accessor_create_ro(
        r->directory, r->queue_name,
        queue_meta_region_size(r->queue_meta),
        r->ring_size,
        r->regions_to_map_ahead,
        queue_meta_max_file_size(r->queue_meta),
        r->timeout_ns, r->runtime,
        &r->ro_acc);
    if (acc_rc != 0) {
        queue_meta_close(r->queue_meta);
        r->queue_meta = NULL;
        r->init_result = acc_rc;
        return 0;
    }

    r->header_acc = queue_region_accessor_header(r->ro_acc);
    return 1;
}

int indexed_batch_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    IndexedBatchReader **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    IndexedBatchReader *r = calloc(1, sizeof(IndexedBatchReader));
    if (r == NULL) return MMAPQ_ENOMEM;

    r->directory            = strdup(directory);
    r->queue_name           = strdup(queue_name);
    r->ring_size            = ring_size;
    r->regions_to_map_ahead = REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD;
    r->timeout_ns           = timeout_ns;
    r->runtime              = runtime;
    r->cached_last_index    = HEADER_NULL_INDEX;
    r->closed               = 0;
    /* meta_init_failed and init_result are zero-initialised by calloc */

    *out = r;
    return 0;
}

static void reader_fill_context(IndexedBatchReader *r, int64_t index, BatchReadingContext *ctx)
{
    r->last_error = 0;
    ctx->index         = index;
    ctx->batch_offset  = -1;
    ctx->message_count = 0;
    ctx->iterator      = NULL;

    if (!reader_init(r)) {
        ctx->status = (r->init_result == MMAPQ_ENOENT
                    || r->init_result == MMAPQ_ENOTREADY)
            ? BATCH_ENTRY_END_OF_STREAM
            : BATCH_ENTRY_ERROR;
        if (ctx->status == BATCH_ENTRY_ERROR) r->last_error = r->init_result;
        return;
    }

    int64_t start = queue_meta_start_index(r->queue_meta);
    if (index < start) {
        ctx->status = BATCH_ENTRY_ARCHIVED;
        return;
    }

    /*
     * Use the reader's own header_buf, not the r->batch_iterator's
     * payload_buf. The payload_buf is immediately re-wrapped by
     * batch_iterator_init for payload access — using it here would
     * corrupt the header read.
     */
    uint64_t header = header_read(r->header_acc, &r->header_buf, index, r->queue_name);
    if (header == HEADER_NULL) {
        ctx->status = BATCH_ENTRY_END_OF_STREAM;
        return;
    }

    uint8_t  appender_id      = header_appender_id(header);
    int64_t  payload_position = header_payload_position(header);

    RegionAccessor *payload_acc =
        queue_region_accessor_payload(r->ro_acc, appender_id);

    r->batch_iterator.queue_name  = r->queue_name;
    r->batch_iterator.payload_acc = payload_acc;

    int32_t message_count = batch_iterator_read_message_count(&r->batch_iterator, payload_position);

    batch_iterator_init(&r->batch_iterator, payload_position, message_count, 0);
    ctx->iterator = &r->batch_iterator;

    ctx->batch_offset  = payload_position;
    ctx->message_count = message_count;
    ctx->appender_id   = appender_id;
    ctx->status        = BATCH_ENTRY_AVAILABLE;
}

BatchReadingContext *indexed_batch_reader_read(
    IndexedBatchReader  *r,
    int64_t              index,
    BatchReadingContext *ctx)
{
    if (r == NULL)   return NULL;
    if (ctx == NULL) return NULL;
    if (r->closed)   { r->last_error = MMAPQ_ECLOSED; ctx->status = BATCH_ENTRY_ERROR; return ctx; }
    if (index < 0)   { r->last_error = MMAPQ_EINVAL; ctx->status = BATCH_ENTRY_ERROR; return ctx; }

    reader_fill_context(r, index, ctx);
    return ctx;
}

BatchReadingContext *indexed_batch_reader_read_first(
    IndexedBatchReader  *r,
    BatchReadingContext *ctx)
{
    if (r == NULL)   return NULL;
    if (ctx == NULL) return NULL;
    if (r->closed)   { r->last_error = MMAPQ_ECLOSED; ctx->status = BATCH_ENTRY_ERROR; return ctx; }

    int64_t start = reader_init(r)
        ? queue_meta_start_index(r->queue_meta)
        : QUEUE_META_INITIAL_START_INDEX;

    reader_fill_context(r, start, ctx);
    return ctx;
}

BatchReadingContext *indexed_batch_reader_read_last(
    IndexedBatchReader  *r,
    BatchReadingContext *ctx)
{
    if (r == NULL)   return NULL;
    if (ctx == NULL) return NULL;
    if (r->closed)   { r->last_error = MMAPQ_ECLOSED; ctx->status = BATCH_ENTRY_ERROR; return ctx; }

    int64_t last = indexed_batch_reader_last_index(r);
    if (last == HEADER_NULL_INDEX) {
        ctx->index         = HEADER_NULL_INDEX;
        ctx->batch_offset  = -1;
        ctx->message_count = 0;
        ctx->iterator      = NULL;
        ctx->status        = BATCH_ENTRY_END_OF_STREAM;
        return ctx;
    }

    reader_fill_context(r, last, ctx);
    return ctx;
}

int64_t indexed_batch_reader_last_index(IndexedBatchReader *r)
{
    if (r == NULL)  return HEADER_NULL_INDEX;
    if (r->closed)  return HEADER_NULL_INDEX;

    if (!reader_init(r)) return HEADER_NULL_INDEX;

    int64_t from = r->cached_last_index == HEADER_NULL_INDEX
        ? queue_meta_start_index(r->queue_meta)
        : r->cached_last_index;

    int64_t last = header_search_last_index(
        from, reader_start_index_fn, r,
        r->header_acc, &r->header_buf, r->queue_name);

    if (last != HEADER_NULL_INDEX) {
        r->cached_last_index = last;
    }

    return r->cached_last_index;
}

int64_t indexed_batch_reader_start_index(IndexedBatchReader *r)
{
    if (r == NULL) return QUEUE_META_INITIAL_START_INDEX;
    if (!reader_init(r)) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(r->queue_meta);
}

BatchEntryStatus indexed_batch_reader_entry_status(IndexedBatchReader *r, int64_t index)
{
    if (r == NULL)  return BATCH_ENTRY_ERROR;
    if (r->closed)  return BATCH_ENTRY_ERROR;

    if (!reader_init(r)) return (r->init_result == MMAPQ_ENOENT || r->init_result == MMAPQ_ENOTREADY) ? BATCH_ENTRY_END_OF_STREAM : BATCH_ENTRY_ERROR;
    if (index < queue_meta_start_index(r->queue_meta)) return BATCH_ENTRY_ARCHIVED;

    return header_found(r->header_acc, &r->header_buf, index, r->queue_name)
        ? BATCH_ENTRY_AVAILABLE
        : BATCH_ENTRY_END_OF_STREAM;
}

/* ── Error accessor ────────────────────────────────────────────────────────── */

int indexed_batch_reader_last_error(const IndexedBatchReader *r)
{
    return r == NULL ? 0 : r->last_error;
}

/* ── Accessors ─────────────────────────────────────────────────────────────── */

QueueMeta *indexed_batch_reader_queue_meta(IndexedBatchReader *r)
{
    if (!reader_init(r)) return NULL;
    return r->queue_meta;
}

const char *indexed_batch_reader_directory(IndexedBatchReader *r)
{
    return r->directory;
}

const char *indexed_batch_reader_queue_name(IndexedBatchReader *r)
{
    return r->queue_name;
}

void indexed_batch_reader_close(IndexedBatchReader *r)
{
    if (r == NULL || r->closed) return;
    r->closed = 1;

    if (r->ro_acc != NULL) {
        queue_region_accessor_close(r->ro_acc);
        r->ro_acc = NULL;
    }
    if (r->queue_meta != NULL) {
        queue_meta_close(r->queue_meta);
        r->queue_meta = NULL;
    }

    free(r->directory);
    free(r->queue_name);
    free(r);
}
