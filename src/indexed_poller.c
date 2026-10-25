#include "indexed_poller.h"
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

struct MmapqIndexedPoller {
    char                *directory;
    char                *queue_name;
    int                  ring_size;
    int                  regions_to_map_ahead;
    uint64_t             timeout_ns;
    MmapqRuntime             *runtime;

    /* Lazy-init state */
    QueueMeta           *queue_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;

    /* Cursor */
    int64_t current_index;

    /* Reusable buffers */
    Buffer header_buf;
    Buffer payload_buf;

    int closed;
    int meta_init_failed;
    int init_result;
    int last_error;       /* MMAPQ_* code from last MMAPQ_POLL_ERROR, 0 otherwise */
};

/* ── Start index supplier ────────────────────────────────────────────────── */

static int64_t poller_start_index_fn(void *ctx)
{
    MmapqIndexedPoller *p = (MmapqIndexedPoller *)ctx;
    if (p->queue_meta == NULL) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(p->queue_meta);
}

/* ── Lazy init ───────────────────────────────────────────────────────────── */

static int poller_init(MmapqIndexedPoller *p)
{
    if (p->queue_meta != NULL) return 1;
    if (p->meta_init_failed) return 0;

    int rc = mmapqi_queue_meta_open_ro(p->directory, p->queue_name, QUEUE_TYPE_NORMAL, &p->queue_meta);
    if (rc != 0) {
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            p->meta_init_failed = 1;
        p->init_result = rc;
        return 0;
    }

    int acc_rc = mmapqi_queue_region_accessor_create_ro(
        p->directory, p->queue_name,
        queue_meta_region_size(p->queue_meta),
        p->ring_size,
        p->regions_to_map_ahead,
        queue_meta_max_file_size(p->queue_meta),
        p->timeout_ns, p->runtime,
        &p->ro_acc);
    if (acc_rc != 0) {
        mmapqi_queue_meta_close(p->queue_meta);
        p->queue_meta = NULL;
        p->init_result = acc_rc;
        return 0;
    }

    p->header_acc = queue_region_accessor_header(p->ro_acc);

    /* Start cursor at queue's start_index */
    p->current_index = queue_meta_start_index(p->queue_meta);

    return 1;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapq_indexed_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    MmapqIndexedPoller **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    MmapqIndexedPoller *p = calloc(1, sizeof(MmapqIndexedPoller));
    if (p == NULL) return MMAPQ_ENOMEM;

    p->directory           = strdup(directory);
    if (p->directory == NULL) { free(p); return MMAPQ_ENOMEM; }
    p->queue_name          = strdup(queue_name);
    if (p->queue_name == NULL) { free(p->directory); free(p); return MMAPQ_ENOMEM; }
    p->ring_size           = ring_size;
    p->regions_to_map_ahead = REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD;
    p->timeout_ns          = timeout_ns;
    p->runtime             = runtime;
    p->current_index       = QUEUE_META_INITIAL_START_INDEX;
    p->closed              = 0;
    /* meta_init_failed and init_result are zero-initialised by calloc */

    *out = p;
    return 0;
}

/* ── mmapq_indexed_poller_poll ─────────────────────────────────────────────────── */

MmapqPollResult mmapq_indexed_poller_poll(
    MmapqIndexedPoller *p,
    MmapqMessageHandler handler,
    void          *ctx)
{
    if (p == NULL)    return MMAPQ_POLL_ERROR;
    p->last_error = 0;
    if (p->closed)    { p->last_error = MMAPQ_ECLOSED; return MMAPQ_POLL_ERROR; }
    if (handler == NULL) { p->last_error = MMAPQ_EINVAL; return MMAPQ_POLL_ERROR; }

    if (!poller_init(p)) {
        if (p->init_result == MMAPQ_ENOENT || p->init_result == MMAPQ_ENOTREADY)
            return MMAPQ_POLL_END_OF_STREAM;
        p->last_error = p->init_result;
        return MMAPQ_POLL_ERROR;
    }

    /*
     * Optimisation: a non-archivable queue's start_index is fixed at
     * creation (it may be non-zero, e.g. a PRESERVE-mode transfer
     * destination), so for them this check reads a value that never
     * moves — caching it saves overhead in the hot loop.
     */
    if (p->queue_meta->archivable) {
        int64_t start_index = queue_meta_start_index(p->queue_meta);
        if (p->current_index < start_index) {
            return MMAPQ_POLL_ARCHIVED;
        }
    }

    /* Read header at current index — sequential wrap to trigger premap.
     * A mapping timeout is MMAPQ_POLL_ERROR + last_error ETIMEDOUT, exactly
     * like the payload wrap below (CAND-17; Java throws in the same spot).
     * The cursor does not advance. Only a genuinely absent entry is
     * END_OF_STREAM. herr stays 0 unless wrap() itself failed — a
     * successful wrap of an unwritten slot does not touch it. */
    int herr = 0;
    uint64_t header = mmapqi_header_read_sequential(p->header_acc, &p->header_buf,
                                                    p->current_index, &herr);

    if (header == HEADER_NULL) {
        if (herr != 0) { p->last_error = herr; return MMAPQ_POLL_ERROR; }
        return MMAPQ_POLL_END_OF_STREAM;
    }

    /* Prefetch the next header slot (512 bytes ahead due to vertical walk) */
    __builtin_prefetch(p->header_buf.data + 512, 0, 3);

    /* Decode and read payload */
    uint8_t  appender_id     = header_appender_id(header);
    int64_t  payload_position = header_payload_position(header);

    RegionAccessor *payload_acc =
        mmapqi_queue_region_accessor_payload(p->ro_acc, appender_id);

    WrapResult r = region_accessor_wrap(payload_acc, payload_position, &p->payload_buf);
    if (r != WRAP_SUCCESS) { p->last_error = MMAPQ_ETIMEDOUT; return MMAPQ_POLL_ERROR; }

    if (p->payload_buf.capacity <= PAYLOAD_LENGTH_SIZE) {
        p->last_error = MMAPQ_ECORRUPT;
        return MMAPQ_POLL_CORRUPT;
    }
    int32_t length;
    memcpy(&length, p->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));

    if (length <= 0 || length > p->payload_buf.capacity - PAYLOAD_LENGTH_SIZE) { p->last_error = MMAPQ_ECORRUPT; return MMAPQ_POLL_CORRUPT; }

    const void *data = p->payload_buf.data + PAYLOAD_DATA_OFFSET;

    /* Call handler */
    MmapqNextMove directive =
        handler(p->current_index, appender_id, data, length, ctx);

    /* Apply directive */
    switch (directive) {
    case MMAPQ_NEXT_ADVANCE:
        p->current_index++;
        return MMAPQ_POLL_ADVANCED;

    case MMAPQ_NEXT_RETREAT: {
        int64_t start_index = queue_meta_start_index(p->queue_meta);
        if (p->current_index > start_index) {
            p->current_index--;
            return MMAPQ_POLL_RETREATED;
        }
        return MMAPQ_POLL_STOPPED;
    }

    case MMAPQ_NEXT_STOP:
        return MMAPQ_POLL_STOPPED;

    default:
        MMAP_ASSERT(0, "mmapq_indexed_poller_poll: unknown handler directive");
        return MMAPQ_POLL_STOPPED;
    }
}

/* ── Cursor positioning ──────────────────────────────────────────────────── */

int mmapq_indexed_poller_move_to_index(MmapqIndexedPoller *p, int64_t index)
{
    if (p == NULL)   return 0;
    if (p->closed)   return 0;
    if (!header_index_in_range(index)) return 0;

    if (!poller_init(p)) {
        if (index == 0) { p->current_index = 0; return 1; }
        return 0;
    }

    int64_t start = queue_meta_start_index(p->queue_meta);
    if (index < start) return 0;  /* archived */

    /* Valid if: entry exists at index, OR previous entry exists (one past end), OR is start */
    if (index == start ||
        header_found(p->header_acc, &p->header_buf, index, p->queue_name) ||
        (index > 0 && header_found(p->header_acc, &p->header_buf, index - 1, p->queue_name))) {
        p->current_index = index;
        return 1;
    }
    return 0;
}

int mmapq_indexed_poller_move_to_end(MmapqIndexedPoller *p)
{
    if (p == NULL)  return 0;
    if (p->closed)  return 0;

    if (!poller_init(p)) return 1;

    int64_t from = p->current_index > queue_meta_start_index(p->queue_meta)
        ? p->current_index
        : queue_meta_start_index(p->queue_meta);

    int64_t last = mmapqi_header_search_last_index(
        from, poller_start_index_fn, p,
        p->header_acc, &p->header_buf, p->queue_name);

    p->current_index = (last == HEADER_NULL_INDEX)
        ? queue_meta_start_index(p->queue_meta)
        : last + 1;

    return 1;
}

int mmapq_indexed_poller_move_to_last(MmapqIndexedPoller *p)
{
    if (p == NULL)  return 0;
    if (p->closed)  return 0;

    if (!poller_init(p)) return 0;

    int64_t from = p->current_index > 0 ? p->current_index - 1 : 0;
    from = from > queue_meta_start_index(p->queue_meta)
        ? from
        : queue_meta_start_index(p->queue_meta);

    int64_t last = mmapqi_header_search_last_index(
        from, poller_start_index_fn, p,
        p->header_acc, &p->header_buf, p->queue_name);

    if (last == HEADER_NULL_INDEX) return 0;

    p->current_index = last;
    return 1;
}

int mmapq_indexed_poller_move_to_start(MmapqIndexedPoller *p)
{
    if (p == NULL)  return 0;
    if (p->closed)  return 0;

    p->current_index = mmapq_indexed_poller_start_index(p);
    return 1;
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

int64_t mmapq_indexed_poller_current_index(const MmapqIndexedPoller *p)
{
    if (p == NULL) return 0;
    return p->current_index;
}

int64_t mmapq_indexed_poller_start_index(MmapqIndexedPoller *p)
{
    if (p == NULL) return 0;
    if (!poller_init(p)) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(p->queue_meta);
}

int mmapq_indexed_poller_exists(MmapqIndexedPoller *p, int64_t index)
{
    if (p == NULL) return 0;
    if (!header_index_in_range(index)) return 0;
    if (!poller_init(p)) return 0;
    if (index < queue_meta_start_index(p->queue_meta)) return 0;
    return header_found(p->header_acc, &p->header_buf, index, p->queue_name);
}

/* ── Error accessor ────────────────────────────────────────────────────────── */

int mmapq_indexed_poller_last_error(const MmapqIndexedPoller *p)
{
    return p == NULL ? 0 : p->last_error;
}

/* ── mmapq_indexed_poller_close ────────────────────────────────────────────────── */

void mmapq_indexed_poller_close(MmapqIndexedPoller *p)
{
    if (p == NULL || p->closed) return;
    p->closed = 1;

    if (p->ro_acc != NULL) {
        mmapqi_queue_region_accessor_close(p->ro_acc);
        p->ro_acc = NULL;
    }
    if (p->queue_meta != NULL) {
        mmapqi_queue_meta_close(p->queue_meta);
        p->queue_meta = NULL;
    }

    free(p->directory);
    free(p->queue_name);
    free(p);
}
