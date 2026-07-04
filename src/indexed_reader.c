#include "indexed_reader.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "header_reader.h"
#include "header_codec.h"
#include "mmap_assert.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

struct IndexedReader {
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

    /* Cached last index for efficient repeated calls */
    int64_t cached_last_index;

    /* Reusable buffers */
    Buffer header_buf;
    Buffer payload_buf;

    int closed;
};

/* ── Start index supplier ────────────────────────────────────────────────── */

static int64_t reader_start_index_fn(void *ctx)
{
    IndexedReader *r = (IndexedReader *)ctx;
    if (r->queue_meta == NULL) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(r->queue_meta);
}

/* ── Lazy init ───────────────────────────────────────────────────────────── */

static int reader_init(IndexedReader *r)
{
    if (r->queue_meta != NULL) return 1;

    r->queue_meta = queue_meta_open_ro(r->directory, r->queue_name, QUEUE_TYPE_NORMAL);
    if (r->queue_meta == NULL) return 0;

    r->ro_acc = queue_region_accessor_create_ro(
        r->directory, r->queue_name,
        queue_meta_region_size(r->queue_meta),
        r->ring_size,
        r->regions_to_map_ahead,
        queue_meta_max_file_size(r->queue_meta),
        r->timeout_ns, r->runtime);

    r->header_acc = queue_region_accessor_header(r->ro_acc);
    return 1;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedReader *indexed_reader_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    MMAP_ASSERT(directory  != NULL, "indexed_reader_create: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "indexed_reader_create: queue_name is NULL");

    IndexedReader *r = calloc(1, sizeof(IndexedReader));
    MMAP_ASSERT(r != NULL, "indexed_reader_create: out of memory");

    r->directory            = strdup(directory);
    r->queue_name           = strdup(queue_name);
    r->ring_size            = ring_size;
    r->regions_to_map_ahead = REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD;
    r->timeout_ns           = timeout_ns;
    r->runtime              = runtime;
    r->cached_last_index    = HEADER_NULL_INDEX;
    r->closed               = 0;

    return r;
}

/* ── Internal read helper ────────────────────────────────────────────────── */

static void reader_fill_context(IndexedReader *r, int64_t index, ReadingContext *ctx)
{
    ctx->index       = index;
    ctx->appender_id = 0;
    ctx->msg_offset  = -1;
    ctx->data        = NULL;
    ctx->length      = 0;

    if (!reader_init(r)) {
        ctx->status = ENTRY_END_OF_STREAM;
        return;
    }

    int64_t start = queue_meta_start_index(r->queue_meta);
    if (index < start) {
        ctx->status = ENTRY_ARCHIVED;
        return;
    }

    uint64_t header = header_read(r->header_acc, &r->header_buf,
                                   index, r->queue_name);
    if (header == HEADER_NULL) {
        ctx->status = ENTRY_END_OF_STREAM;
        return;
    }

    uint8_t  appender_id     = header_appender_id(header);
    int64_t  payload_position = header_payload_position(header);

    RegionAccessor *payload_acc =
        queue_region_accessor_payload(r->ro_acc, appender_id);

    WrapResult wr = region_accessor_wrap(payload_acc, payload_position, &r->payload_buf);
    if (wr != WRAP_SUCCESS) { ctx->status = ENTRY_ERROR; return; }

    int32_t length;
    memcpy(&length, r->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));
    MMAP_ASSERT(length > 0,
        "indexed_reader: invalid payload length at committed header");

    ctx->appender_id = appender_id;
    ctx->msg_offset  = payload_position;
    ctx->data        = r->payload_buf.data + PAYLOAD_DATA_OFFSET;
    ctx->length      = length;
    ctx->status      = ENTRY_AVAILABLE;
}

/* ── Read operations ─────────────────────────────────────────────────────── */

ReadingContext *indexed_reader_read(
    IndexedReader  *r,
    int64_t         index,
    ReadingContext *ctx)
{
    MMAP_ASSERT(r   != NULL, "indexed_reader_read: reader is NULL");
    MMAP_ASSERT(ctx != NULL, "indexed_reader_read: ctx is NULL");
    MMAP_ASSERT(!r->closed,  "indexed_reader_read: reader is closed");
    MMAP_ASSERT(index >= 0,  "indexed_reader_read: index must be >= 0");

    reader_fill_context(r, index, ctx);
    return ctx;
}

ReadingContext *indexed_reader_read_first(
    IndexedReader  *r,
    ReadingContext *ctx)
{
    MMAP_ASSERT(r   != NULL, "indexed_reader_read_first: reader is NULL");
    MMAP_ASSERT(ctx != NULL, "indexed_reader_read_first: ctx is NULL");
    MMAP_ASSERT(!r->closed,  "indexed_reader_read_first: reader is closed");

    int64_t start = reader_init(r)
        ? queue_meta_start_index(r->queue_meta)
        : QUEUE_META_INITIAL_START_INDEX;

    reader_fill_context(r, start, ctx);
    return ctx;
}

ReadingContext *indexed_reader_read_last(
    IndexedReader  *r,
    ReadingContext *ctx)
{
    MMAP_ASSERT(r   != NULL, "indexed_reader_read_last: reader is NULL");
    MMAP_ASSERT(ctx != NULL, "indexed_reader_read_last: ctx is NULL");
    MMAP_ASSERT(!r->closed,  "indexed_reader_read_last: reader is closed");

    int64_t last = indexed_reader_last_index(r);
    if (last == HEADER_NULL_INDEX) {
        ctx->index   = HEADER_NULL_INDEX;
        ctx->data    = NULL;
        ctx->length  = 0;
        ctx->status  = ENTRY_END_OF_STREAM;
        return ctx;
    }

    reader_fill_context(r, last, ctx);
    return ctx;
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

int64_t indexed_reader_last_index(IndexedReader *r)
{
    MMAP_ASSERT(r != NULL,  "indexed_reader_last_index: reader is NULL");
    MMAP_ASSERT(!r->closed, "indexed_reader_last_index: reader is closed");

    if (!reader_init(r)) return HEADER_NULL_INDEX;

    /* Use cached_last_index as starting point — search grows monotonically */
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

int64_t indexed_reader_start_index(IndexedReader *r)
{
    MMAP_ASSERT(r != NULL, "indexed_reader_start_index: reader is NULL");
    if (!reader_init(r)) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(r->queue_meta);
}

EntryStatus indexed_reader_entry_status(IndexedReader *r, int64_t index)
{
    MMAP_ASSERT(r != NULL,  "indexed_reader_entry_status: reader is NULL");
    MMAP_ASSERT(!r->closed, "indexed_reader_entry_status: reader is closed");

    if (!reader_init(r)) return ENTRY_END_OF_STREAM;
    if (index < queue_meta_start_index(r->queue_meta)) return ENTRY_ARCHIVED;

    return header_found(r->header_acc, &r->header_buf, index, r->queue_name)
        ? ENTRY_AVAILABLE
        : ENTRY_END_OF_STREAM;
}

/* ── Accessors ─────────────────────────────────────────────────────────────── */

QueueMeta *indexed_reader_queue_meta(IndexedReader *r)
{
    MMAP_ASSERT(r != NULL, "indexed_reader_queue_meta: reader is NULL");
    reader_init(r);
    return r->queue_meta;
}

const char *indexed_reader_directory(IndexedReader *r)
{
    MMAP_ASSERT(r != NULL, "indexed_reader_directory: reader is NULL");
    return r->directory;
}

const char *indexed_reader_queue_name(IndexedReader *r)
{
    MMAP_ASSERT(r != NULL, "indexed_reader_queue_name: reader is NULL");
    return r->queue_name;
}

/* ── indexed_reader_close ────────────────────────────────────────────────── */

void indexed_reader_close(IndexedReader *r)
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
