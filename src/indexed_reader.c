#include "indexed_reader.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

struct MmapqIndexedReader {
    char                *directory;
    char                *queue_name;
    int                  ring_size;
    int                  regions_to_map_ahead;
    uint64_t             timeout_ns;
    MmapqRuntime             *runtime;

    /* Lazy-init */
    QueueMeta           *queue_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;

    /* Cached last index for efficient repeated calls */
    int64_t cached_last_index;

    /* Reusable buffers */
    Buffer header_buf;
    Buffer payload_buf;

    int closed;
    int meta_init_failed;
    int init_result;
    int last_error;       /* MMAPQ_* code from last MMAPQ_ENTRY_ERROR, 0 otherwise */
};

/* ── Start index supplier ────────────────────────────────────────────────── */

static int64_t reader_start_index_fn(void *ctx)
{
    MmapqIndexedReader *r = (MmapqIndexedReader *)ctx;
    if (r->queue_meta == NULL) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(r->queue_meta);
}

/* ── Lazy init ───────────────────────────────────────────────────────────── */

static int reader_init(MmapqIndexedReader *r)
{
    if (r->queue_meta != NULL) return 1;
    if (r->meta_init_failed) return 0;

    int rc = mmapqi_queue_meta_open_ro(r->directory, r->queue_name, QUEUE_TYPE_NORMAL, &r->queue_meta);
    if (rc != 0) {
        /* Latch terminal errors only — ENOENT, ENOTREADY, ETIMEDOUT are transient */
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            r->meta_init_failed = 1;
        r->init_result = rc;
        return 0;
    }

    int acc_rc = mmapqi_queue_region_accessor_create_ro(
        r->directory, r->queue_name,
        queue_meta_region_size(r->queue_meta),
        r->ring_size,
        r->regions_to_map_ahead,
        queue_meta_max_file_size(r->queue_meta),
        r->timeout_ns, r->runtime,
        &r->ro_acc);
    if (acc_rc != 0) {
        mmapqi_queue_meta_close(r->queue_meta);
        r->queue_meta = NULL;
        r->init_result = acc_rc;
        return 0;
    }

    r->header_acc = queue_region_accessor_header(r->ro_acc);
    return 1;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapq_indexed_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedReader **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    MmapqIndexedReader *r = calloc(1, sizeof(MmapqIndexedReader));
    if (r == NULL) return MMAPQ_ENOMEM;

    r->directory            = strdup(directory);
    if (r->directory == NULL) { free(r); return MMAPQ_ENOMEM; }
    r->queue_name           = strdup(queue_name);
    if (r->queue_name == NULL) { free(r->directory); free(r); return MMAPQ_ENOMEM; }
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

/* ── Internal read helper ────────────────────────────────────────────────── */

static void reader_fill_context(MmapqIndexedReader *r, int64_t index, MmapqReadingContext *ctx)
{
    r->last_error = 0;
    ctx->index       = index;
    ctx->appender_id = 0;
    ctx->msg_offset  = -1;
    ctx->data        = NULL;
    ctx->length      = 0;

    if (!reader_init(r)) {
        ctx->status = (r->init_result == MMAPQ_ENOENT
                    || r->init_result == MMAPQ_ENOTREADY)
            ? MMAPQ_ENTRY_END_OF_STREAM
            : MMAPQ_ENTRY_ERROR;
        if (ctx->status == MMAPQ_ENTRY_ERROR) r->last_error = r->init_result;
        return;
    }

    int64_t start = queue_meta_start_index(r->queue_meta);
    if (index < start) {
        ctx->status = MMAPQ_ENTRY_ARCHIVED;
        return;
    }

    uint64_t header;
    r->last_error = mmapqi_header_read_checked(r->header_acc, &r->header_buf, index, &header);
    if (r->last_error != 0) {
        ctx->status = MMAPQ_ENTRY_ERROR;
        return;
    }
    if (header == HEADER_NULL) {
        ctx->status = MMAPQ_ENTRY_END_OF_STREAM;
        return;
    }

    uint8_t  appender_id     = header_appender_id(header);
    int64_t  payload_position = header_payload_position(header);

    RegionAccessor *payload_acc =
        mmapqi_queue_region_accessor_payload(r->ro_acc, appender_id);

    WrapResult wr = region_accessor_wrap(payload_acc, payload_position, &r->payload_buf);
    if (wr != WRAP_SUCCESS) { ctx->status = MMAPQ_ENTRY_ERROR; r->last_error = MMAPQ_ETIMEDOUT; return; }

    if (r->payload_buf.capacity <= PAYLOAD_LENGTH_SIZE) {
        ctx->status = MMAPQ_ENTRY_ERROR;
        r->last_error = MMAPQ_ECORRUPT;
        return;
    }
    int32_t length;
    memcpy(&length, r->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));
    if (length <= 0 || length > r->payload_buf.capacity - PAYLOAD_LENGTH_SIZE) {
        ctx->status = MMAPQ_ENTRY_ERROR;
        r->last_error = MMAPQ_ECORRUPT;
        return;
    }

    ctx->appender_id = appender_id;
    ctx->msg_offset  = payload_position;
    ctx->data        = r->payload_buf.data + PAYLOAD_DATA_OFFSET;
    ctx->length      = length;
    ctx->status      = MMAPQ_ENTRY_AVAILABLE;
}

/* ── Read operations ─────────────────────────────────────────────────────── */

MmapqReadingContext *mmapq_indexed_reader_read(
    MmapqIndexedReader  *r,
    int64_t         index,
    MmapqReadingContext *ctx)
{
    if (r == NULL)    return NULL;
    if (ctx == NULL)  return NULL;
    if (r->closed)    { r->last_error = MMAPQ_ECLOSED; ctx->status = MMAPQ_ENTRY_ERROR; return ctx; }
    if (!header_index_in_range(index)) { r->last_error = MMAPQ_EINVAL; ctx->status = MMAPQ_ENTRY_ERROR; return ctx; }

    reader_fill_context(r, index, ctx);
    return ctx;
}

MmapqReadingContext *mmapq_indexed_reader_read_first(
    MmapqIndexedReader  *r,
    MmapqReadingContext *ctx)
{
    if (r == NULL)    return NULL;
    if (ctx == NULL)  return NULL;
    if (r->closed)    { r->last_error = MMAPQ_ECLOSED; ctx->status = MMAPQ_ENTRY_ERROR; return ctx; }

    int64_t start = reader_init(r)
        ? queue_meta_start_index(r->queue_meta)
        : QUEUE_META_INITIAL_START_INDEX;

    reader_fill_context(r, start, ctx);
    return ctx;
}

MmapqReadingContext *mmapq_indexed_reader_read_last(
    MmapqIndexedReader  *r,
    MmapqReadingContext *ctx)
{
    if (r == NULL)    return NULL;
    if (ctx == NULL)  return NULL;
    if (r->closed)    { r->last_error = MMAPQ_ECLOSED; ctx->status = MMAPQ_ENTRY_ERROR; return ctx; }

    int64_t last = mmapq_indexed_reader_last_index(r);
    if (last == HEADER_NULL_INDEX) {
        ctx->index   = HEADER_NULL_INDEX;
        ctx->data    = NULL;
        ctx->length  = 0;
        ctx->status = r->last_error != 0 && r->last_error != MMAPQ_ENOENT && r->last_error != MMAPQ_ENOTREADY
            ? MMAPQ_ENTRY_ERROR : MMAPQ_ENTRY_END_OF_STREAM;
        return ctx;
    }

    reader_fill_context(r, last, ctx);
    return ctx;
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

int64_t mmapq_indexed_reader_last_index(MmapqIndexedReader *r)
{
    if (r == NULL)  return HEADER_NULL_INDEX;
    r->last_error = 0;
    if (r->closed) { r->last_error = MMAPQ_ECLOSED; return HEADER_NULL_INDEX; }

    if (!reader_init(r)) { r->last_error = r->init_result; return HEADER_NULL_INDEX; }

    /* Use cached_last_index as starting point — search grows monotonically */
    int64_t from = r->cached_last_index == HEADER_NULL_INDEX
        ? queue_meta_start_index(r->queue_meta)
        : r->cached_last_index;

    int64_t last = mmapqi_header_search_last_index_checked(
        from, reader_start_index_fn, r,
        r->header_acc, &r->header_buf, r->queue_name, &r->last_error);
    if (r->last_error != 0) return HEADER_NULL_INDEX;

    if (last != HEADER_NULL_INDEX) {
        r->cached_last_index = last;
    }

    return r->cached_last_index;
}

int64_t mmapq_indexed_reader_start_index(MmapqIndexedReader *r)
{
    if (r == NULL) return 0;
    if (!reader_init(r)) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(r->queue_meta);
}

MmapqEntryStatus mmapq_indexed_reader_entry_status(MmapqIndexedReader *r, int64_t index)
{
    if (r == NULL)  return MMAPQ_ENTRY_ERROR;
    if (r->closed)  return MMAPQ_ENTRY_ERROR;

    if (!reader_init(r)) return (r->init_result == MMAPQ_ENOENT || r->init_result == MMAPQ_ENOTREADY) ? MMAPQ_ENTRY_END_OF_STREAM : MMAPQ_ENTRY_ERROR;
    /* Only the upper bound is rejected; a negative index keeps the legacy
     * fall-through to ARCHIVED below. Above MMAPQI_MAX_INDEX the header
     * position would alias an already-written slot. */
    if (index > MMAPQI_MAX_INDEX) { r->last_error = MMAPQ_EINVAL; return MMAPQ_ENTRY_ERROR; }
    if (index < queue_meta_start_index(r->queue_meta)) return MMAPQ_ENTRY_ARCHIVED;

    return header_found(r->header_acc, &r->header_buf, index, r->queue_name)
        ? MMAPQ_ENTRY_AVAILABLE
        : MMAPQ_ENTRY_END_OF_STREAM;
}

/* ── Accessors ─────────────────────────────────────────────────────────────── */

QueueMeta *mmapqi_indexed_reader_queue_meta(MmapqIndexedReader *r)
{
    if (r == NULL) return NULL;
    if (!reader_init(r)) r->last_error = r->init_result;
    return r->queue_meta;
}

/* ── Error accessor ────────────────────────────────────────────────────────── */

int mmapq_indexed_reader_last_error(const MmapqIndexedReader *r)
{
    return r == NULL ? 0 : r->last_error;
}

/* ── mmapq_indexed_reader_close ────────────────────────────────────────────────── */

void mmapq_indexed_reader_close(MmapqIndexedReader *r)
{
    if (r == NULL || r->closed) return;
    r->closed = 1;

    if (r->ro_acc != NULL) {
        mmapqi_queue_region_accessor_close(r->ro_acc);
        r->ro_acc = NULL;
    }
    if (r->queue_meta != NULL) {
        mmapqi_queue_meta_close(r->queue_meta);
        r->queue_meta = NULL;
    }

    free(r->directory);
    free(r->queue_name);
    free(r);
}
