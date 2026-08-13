#include "indexed_batch_poller.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "batch_payload_iterator.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/batch_payload_iterator_internal.h"

#include <stdlib.h>
#include <string.h>

struct MmapqIndexedBatchPoller {
    char                *directory;
    char                *queue_name;
    int                  ring_size;
    uint64_t             timeout_ns;
    MmapqRuntime             *runtime;

    QueueMeta           *queue_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;

    int64_t              current_batch_index;
    MmapqBatchPayloadIterator *iterators[MAX_APPENDERS];

    Buffer               header_buf;
    int                  closed;
    int                  meta_init_failed;
    int                  init_result;
    int                  last_error;       /* MMAPQ_* code from last MMAPQ_POLL_ERROR, 0 otherwise */
};

/* ── Internal Helpers ────────────────────────────────────────────────────── */

static int poller_init(MmapqIndexedBatchPoller *p)
{
    if (p->queue_meta) return 1;
    if (p->meta_init_failed) return 0;
    int rc = mmapqi_queue_meta_open_ro(p->directory, p->queue_name, QUEUE_TYPE_BATCH, &p->queue_meta);
    if (rc != 0) {
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            p->meta_init_failed = 1;
        p->init_result = rc;
        return 0;
    }

    int acc_rc = mmapqi_queue_region_accessor_create_ro(p->directory, p->queue_name, queue_meta_region_size(p->queue_meta), p->ring_size, 1, queue_meta_max_file_size(p->queue_meta), p->timeout_ns, p->runtime, &p->ro_acc);
    if (acc_rc != 0) {
        mmapqi_queue_meta_close(p->queue_meta);
        p->queue_meta = NULL;
        p->init_result = acc_rc;
        return 0;
    }
    p->header_acc = queue_region_accessor_header(p->ro_acc);
    p->current_batch_index = queue_meta_start_index(p->queue_meta);
    return 1;
}

static MmapqBatchPayloadIterator *get_iterator(MmapqIndexedBatchPoller *p, uint8_t appender_id)
{
    MmapqBatchPayloadIterator *it = p->iterators[appender_id];
    if (it) return it;

    it = calloc(1, sizeof(MmapqBatchPayloadIterator));
    if (it == NULL) return NULL;
    it->queue_name  = p->queue_name;
    it->payload_acc = mmapqi_queue_region_accessor_payload(p->ro_acc, appender_id);
    p->iterators[appender_id] = it;
    return it;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapq_indexed_batch_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedBatchPoller **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    MmapqIndexedBatchPoller *p = calloc(1, sizeof(MmapqIndexedBatchPoller));
    if (p == NULL) return MMAPQ_ENOMEM;

    p->directory  = strdup(directory);
    if (p->directory == NULL) { free(p); return MMAPQ_ENOMEM; }
    p->queue_name = strdup(queue_name);
    if (p->queue_name == NULL) { free(p->directory); free(p); return MMAPQ_ENOMEM; }
    p->ring_size  = ring_size;
    p->timeout_ns = timeout_ns;
    p->runtime    = runtime;
    p->closed           = 0;
    /* meta_init_failed and init_result are zero-initialised by calloc */

    *out = p;
    return 0;
}

/* ── Polling ─────────────────────────────────────────────────────────────── */

BatchPollResult mmapq_indexed_batch_poller_poll(MmapqIndexedBatchPoller *p, MmapqBatchHandler handler, void *ctx)
{
    if (p == NULL)    return MMAPQ_POLL_ERROR;
    p->last_error = 0;

    if (p->closed)     { p->last_error = MMAPQ_ECLOSED; return MMAPQ_POLL_ERROR; }
    if (handler == NULL) { p->last_error = MMAPQ_EINVAL; return MMAPQ_POLL_ERROR; }

    if (__builtin_expect(!poller_init(p), 0)) {
        if (p->init_result == MMAPQ_ENOENT || p->init_result == MMAPQ_ENOTREADY)
            return MMAPQ_POLL_END_OF_STREAM;
        p->last_error = p->init_result;
        return MMAPQ_POLL_ERROR;
    }

    long start_idx = queue_meta_start_index(p->queue_meta);
    if (p->current_batch_index < start_idx) return MMAPQ_POLL_ARCHIVED;

    uint64_t header = mmapqi_header_read_sequential(p->header_acc, &p->header_buf, p->current_batch_index, p->queue_name);
    if (header == HEADER_NULL) return MMAPQ_POLL_END_OF_STREAM;

    uint8_t aid = header_appender_id(header);
    int64_t pos = header_payload_position(header);

    MmapqBatchPayloadIterator *it = get_iterator(p, aid);
    if (it == NULL) { p->last_error = MMAPQ_ENOMEM; return MMAPQ_POLL_ERROR; }
    int32_t msg_count = mmapq_batch_iterator_read_message_count(it, pos);
    if (msg_count <= 0) return MMAPQ_POLL_CORRUPT;
    mmapq_batch_iterator_init(it, pos, msg_count, 0);

    MmapqBatchReadingContext batch_ctx;
    batch_ctx.index         = p->current_batch_index;
    batch_ctx.status        = MMAPQ_BATCH_ENTRY_AVAILABLE;
    batch_ctx.batch_offset  = pos;
    batch_ctx.message_count = msg_count;
    batch_ctx.iterator = it;

    MmapqNextMove move = handler(p->current_batch_index, &batch_ctx, ctx);

    switch (move) {
        case MMAPQ_NEXT_ADVANCE:
            p->current_batch_index++;
            return MMAPQ_POLL_ADVANCED;

        case MMAPQ_NEXT_STOP:
            return MMAPQ_POLL_STOPPED;

        case MMAPQ_NEXT_RETREAT:
            if (p->current_batch_index > start_idx) {
                p->current_batch_index--;
                return MMAPQ_POLL_RETREATED;
            }
            return MMAPQ_POLL_STOPPED;

        default:
            return MMAPQ_POLL_STOPPED;
    }
}

/* ── Positioning ─────────────────────────────────────────────────────────── */

static int64_t poller_start_index_fn(void *ctx) { return mmapq_indexed_batch_poller_start_index((MmapqIndexedBatchPoller *)ctx); }

int mmapq_indexed_batch_poller_move_to_index(MmapqIndexedBatchPoller *p, int64_t index)
{
    if (!poller_init(p)) {
        if (index == 0) { p->current_batch_index = 0; return 1; }
        return 0;
    }
    long start = queue_meta_start_index(p->queue_meta);
    if (index < start) return 0;

    if (p->current_batch_index == index) return 1;

    if (index == start || header_found(p->header_acc, &p->header_buf, index, p->queue_name) ||
        (index > 0 && header_found(p->header_acc, &p->header_buf, index - 1, p->queue_name))) {
        p->current_batch_index = index;
        return 1;
    }
    return 0;
}

int mmapq_indexed_batch_poller_move_to_end(MmapqIndexedBatchPoller *p)
{
    if (!poller_init(p)) return 1;
    long from = p->current_batch_index > queue_meta_start_index(p->queue_meta) ? p->current_batch_index : queue_meta_start_index(p->queue_meta);
    int64_t last = mmapqi_header_search_last_index(from, poller_start_index_fn, p, p->header_acc, &p->header_buf, p->queue_name);
    p->current_batch_index = (last == HEADER_NULL_INDEX) ? queue_meta_start_index(p->queue_meta) : last + 1;
    return 1;
}

int mmapq_indexed_batch_poller_move_to_last_existing_batch(MmapqIndexedBatchPoller *p)
{
    if (!poller_init(p)) return 0;
    long from = p->current_batch_index > 0 ? p->current_batch_index - 1 : 0;
    from = from > queue_meta_start_index(p->queue_meta) ? from : queue_meta_start_index(p->queue_meta);
    int64_t last = mmapqi_header_search_last_index(from, poller_start_index_fn, p, p->header_acc, &p->header_buf, p->queue_name);
    if (last == HEADER_NULL_INDEX) return 0;
    p->current_batch_index = last;
    return 1;
}

int mmapq_indexed_batch_poller_move_to_start(MmapqIndexedBatchPoller *p)
{
    p->current_batch_index = mmapq_indexed_batch_poller_start_index(p);
    return 1;
}

int64_t mmapq_indexed_batch_poller_current_batch_index(const MmapqIndexedBatchPoller *p) { return p->current_batch_index; }
int64_t mmapq_indexed_batch_poller_start_index(MmapqIndexedBatchPoller *p) { if (!poller_init(p)) return 0; return queue_meta_start_index(p->queue_meta); }

int mmapq_indexed_batch_poller_exists(MmapqIndexedBatchPoller *p, int64_t batch_index)
{
    if (!poller_init(p)) return 0;
    if (batch_index < queue_meta_start_index(p->queue_meta)) return 0;
    return header_found(p->header_acc, &p->header_buf, batch_index, p->queue_name) ? 1 : 0;
}

/* ── Error accessor ────────────────────────────────────────────────────────── */

int mmapq_indexed_batch_poller_last_error(const MmapqIndexedBatchPoller *p)
{
    return p == NULL ? 0 : p->last_error;
}

void mmapq_indexed_batch_poller_close(MmapqIndexedBatchPoller *p)
{
    if (!p || p->closed) return;
    p->closed = 1;
    for (int i = 0; i < MAX_APPENDERS; i++) if (p->iterators[i]) { free(p->iterators[i]); p->iterators[i] = NULL; }
    if (p->ro_acc) mmapqi_queue_region_accessor_close(p->ro_acc);
    if (p->queue_meta) mmapqi_queue_meta_close(p->queue_meta);
    free(p->directory);
    free(p->queue_name);
    free(p);
}
