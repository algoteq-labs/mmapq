#include "indexed_poller.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "header_reader.h"
#include "header_codec.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

struct IndexedPoller {
    char                *directory;
    char                *queue_name;
    int                  ring_size;
    int                  regions_to_map_ahead;
    uint64_t             timeout_ns;
    Runtime             *runtime;

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
};

/* ── Start index supplier ────────────────────────────────────────────────── */

static int64_t poller_start_index_fn(void *ctx)
{
    IndexedPoller *p = (IndexedPoller *)ctx;
    if (p->queue_meta == NULL) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(p->queue_meta);
}

/* ── Lazy init ───────────────────────────────────────────────────────────── */

static int poller_init(IndexedPoller *p)
{
    if (p->queue_meta != NULL) return 1;

    p->queue_meta = queue_meta_open_ro(p->directory, p->queue_name);
    if (p->queue_meta == NULL) return 0;  /* queue not created yet */

    p->ro_acc = queue_region_accessor_create_ro(
        p->directory, p->queue_name,
        queue_meta_region_size(p->queue_meta),
        p->ring_size,
        p->regions_to_map_ahead,
        queue_meta_max_file_size(p->queue_meta),
        p->timeout_ns, p->runtime);

    p->header_acc = queue_region_accessor_header(p->ro_acc);

    /* Start cursor at queue's start_index */
    p->current_index = queue_meta_start_index(p->queue_meta);

    return 1;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedPoller *indexed_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    MMAP_ASSERT(directory  != NULL, "indexed_poller_create: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "indexed_poller_create: queue_name is NULL");
    MMAP_ASSERT(runtime    != NULL, "indexed_poller_create: runtime is NULL");

    IndexedPoller *p = calloc(1, sizeof(IndexedPoller));
    MMAP_ASSERT(p != NULL, "indexed_poller_create: out of memory");

    p->directory           = strdup(directory);
    p->queue_name          = strdup(queue_name);
    p->ring_size           = ring_size;
    p->regions_to_map_ahead = REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD;
    p->timeout_ns          = timeout_ns;
    p->runtime             = runtime;
    p->current_index       = QUEUE_META_INITIAL_START_INDEX;
    p->closed              = 0;

    return p;
}

/* ── indexed_poller_poll ─────────────────────────────────────────────────── */

PollResult indexed_poller_poll(
    IndexedPoller *p,
    MessageHandler handler,
    void          *ctx)
{
    MMAP_ASSERT(p        != NULL, "indexed_poller_poll: poller is NULL");
    MMAP_ASSERT(!p->closed,       "indexed_poller_poll: poller is closed");
    MMAP_ASSERT(handler  != NULL, "indexed_poller_poll: handler is NULL");

    if (!poller_init(p)) return POLL_END_OF_STREAM;

    /* 
     * Optimisation: non-archivable queues always start at 0.
     * Caching this check saves overhead in the hot loop.
     */
    if (p->queue_meta->archivable) {
        int64_t start_index = queue_meta_start_index(p->queue_meta);
        if (p->current_index < start_index) {
            return POLL_ARCHIVED;
        }
    }

    /* Read header at current index — use sequential wrap to trigger premap */
    uint64_t header = header_read_sequential(p->header_acc, &p->header_buf,
                                              p->current_index, p->queue_name);

    if (header == HEADER_NULL) {
        return POLL_END_OF_STREAM;
    }

    /* Prefetch the next header slot (512 bytes ahead due to vertical walk) */
    __builtin_prefetch(p->header_buf.data + 512, 0, 3);

    /* Decode and read payload */
    uint8_t  appender_id     = header_appender_id(header);
    int64_t  payload_position = header_payload_position(header);

    RegionAccessor *payload_acc =
        queue_region_accessor_payload(p->ro_acc, appender_id);

    WrapResult r = region_accessor_wrap(payload_acc, payload_position, &p->payload_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS,
        "indexed_poller_poll: failed to wrap payload — mapping timeout or corruption");

    int32_t length;
    memcpy(&length, p->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));

    MMAP_ASSERT(length > 0,
        "indexed_poller_poll: invalid payload length at committed header");

    const void *data = p->payload_buf.data + PAYLOAD_DATA_OFFSET;

    /* Call handler */
    NextMove directive =
        handler(p->current_index, appender_id, data, length, ctx);

    /* Apply directive */
    switch (directive) {
    case NEXT_ADVANCE:
        p->current_index++;
        return POLL_ADVANCED;

    case NEXT_RETREAT: {
        int64_t start_index = queue_meta_start_index(p->queue_meta);
        if (p->current_index > start_index) {
            p->current_index--;
            return POLL_RETREATED;
        }
        return POLL_STOPPED;
    }

    case NEXT_STOP:
        return POLL_STOPPED;

    default:
        MMAP_ASSERT(0, "indexed_poller_poll: unknown handler directive");
        return POLL_STOPPED;
    }
}

/* ── Cursor positioning ──────────────────────────────────────────────────── */

int indexed_poller_move_to_index(IndexedPoller *p, int64_t index)
{
    MMAP_ASSERT(p != NULL,   "indexed_poller_move_to_index: poller is NULL");
    MMAP_ASSERT(!p->closed,  "indexed_poller_move_to_index: poller is closed");
    MMAP_ASSERT(index >= 0,  "indexed_poller_move_to_index: index must be >= 0");

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

int indexed_poller_move_to_end(IndexedPoller *p)
{
    MMAP_ASSERT(p != NULL,  "indexed_poller_move_to_end: poller is NULL");
    MMAP_ASSERT(!p->closed, "indexed_poller_move_to_end: poller is closed");

    if (!poller_init(p)) return 1;

    int64_t from = p->current_index > queue_meta_start_index(p->queue_meta)
        ? p->current_index
        : queue_meta_start_index(p->queue_meta);

    int64_t last = header_search_last_index(
        from, poller_start_index_fn, p,
        p->header_acc, &p->header_buf, p->queue_name);

    p->current_index = (last == HEADER_NULL_INDEX)
        ? queue_meta_start_index(p->queue_meta)
        : last + 1;

    return 1;
}

int indexed_poller_move_to_last(IndexedPoller *p)
{
    MMAP_ASSERT(p != NULL,  "indexed_poller_move_to_last: poller is NULL");
    MMAP_ASSERT(!p->closed, "indexed_poller_move_to_last: poller is closed");

    if (!poller_init(p)) return 0;

    int64_t from = p->current_index > 0 ? p->current_index - 1 : 0;
    from = from > queue_meta_start_index(p->queue_meta)
        ? from
        : queue_meta_start_index(p->queue_meta);

    int64_t last = header_search_last_index(
        from, poller_start_index_fn, p,
        p->header_acc, &p->header_buf, p->queue_name);

    if (last == HEADER_NULL_INDEX) return 0;

    p->current_index = last;
    return 1;
}

int indexed_poller_move_to_start(IndexedPoller *p)
{
    MMAP_ASSERT(p != NULL,  "indexed_poller_move_to_start: poller is NULL");
    MMAP_ASSERT(!p->closed, "indexed_poller_move_to_start: poller is closed");

    p->current_index = indexed_poller_start_index(p);
    return 1;
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

int64_t indexed_poller_current_index(const IndexedPoller *p)
{
    MMAP_ASSERT(p != NULL, "indexed_poller_current_index: poller is NULL");
    return p->current_index;
}

int64_t indexed_poller_start_index(IndexedPoller *p)
{
    MMAP_ASSERT(p != NULL, "indexed_poller_start_index: poller is NULL");
    if (!poller_init(p)) return QUEUE_META_INITIAL_START_INDEX;
    return queue_meta_start_index(p->queue_meta);
}

int indexed_poller_exists(IndexedPoller *p, int64_t index)
{
    MMAP_ASSERT(p != NULL, "indexed_poller_exists: poller is NULL");
    if (!poller_init(p)) return 0;
    if (index < queue_meta_start_index(p->queue_meta)) return 0;
    return header_found(p->header_acc, &p->header_buf, index, p->queue_name);
}

/* ── indexed_poller_close ────────────────────────────────────────────────── */

void indexed_poller_close(IndexedPoller *p)
{
    if (p == NULL || p->closed) return;
    p->closed = 1;

    if (p->ro_acc != NULL) {
        queue_region_accessor_close(p->ro_acc);
        p->ro_acc = NULL;
    }
    if (p->queue_meta != NULL) {
        queue_meta_close(p->queue_meta);
        p->queue_meta = NULL;
    }

    free(p->directory);
    free(p->queue_name);
    free(p);
}
