#include "indexed_batch_poller.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "header_reader.h"
#include "header_codec.h"
#include "batch_payload_iterator.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>

struct IndexedBatchPoller {
    char                *directory;
    char                *queue_name;
    int                  ring_size;
    uint64_t             timeout_ns;
    Runtime             *runtime;

    QueueMeta           *queue_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;

    int64_t              current_batch_index;
    BatchPayloadIterator *iterators[MAX_APPENDERS];
    BatchPayloadIterator *current_iterator;

    Buffer               header_buf;
    int                  closed;
};

/* ── Internal Helpers ────────────────────────────────────────────────────── */

static int poller_init(IndexedBatchPoller *p)
{
    if (p->queue_meta) return 1;
    p->queue_meta = queue_meta_open_ro(p->directory, p->queue_name);
    if (!p->queue_meta) return 0;

    p->ro_acc = queue_region_accessor_create_ro(p->directory, p->queue_name, queue_meta_region_size(p->queue_meta), p->ring_size, 1, queue_meta_max_file_size(p->queue_meta), p->timeout_ns, p->runtime);
    p->header_acc = queue_region_accessor_header(p->ro_acc);
    p->current_batch_index = queue_meta_start_index(p->queue_meta);
    return 1;
}

static BatchPayloadIterator *get_iterator(IndexedBatchPoller *p, uint8_t appender_id)
{
    if (p->iterators[appender_id]) return p->iterators[appender_id];

    BatchPayloadIterator *it = calloc(1, sizeof(BatchPayloadIterator));
    MMAP_ASSERT(it != NULL, "out of memory");
    it->queue_name  = p->queue_name;
    it->payload_acc = queue_region_accessor_payload(p->ro_acc, appender_id);
    p->iterators[appender_id] = it;
    return it;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedBatchPoller *indexed_batch_poller_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    IndexedBatchPoller *p = calloc(1, sizeof(IndexedBatchPoller));
    MMAP_ASSERT(p != NULL, "out of memory");

    p->directory  = strdup(directory);
    p->queue_name = strdup(queue_name);
    p->ring_size  = ring_size;
    p->timeout_ns = timeout_ns;
    p->runtime    = runtime;
    p->closed     = 0;

    return p;
}

/* ── Polling ─────────────────────────────────────────────────────────────── */

BatchPollResult indexed_batch_poller_poll(IndexedBatchPoller *p, BatchMessageHandler handler, void *ctx)
{
    if (__builtin_expect(!poller_init(p), 0)) return POLL_BATCH_END_OF_STREAM;

    long start_idx = queue_meta_start_index(p->queue_meta);
    if (p->current_batch_index < start_idx) return POLL_BATCH_ARCHIVED;

    if (p->current_iterator == NULL) {
        uint64_t header = header_read_sequential(p->header_acc, &p->header_buf, p->current_batch_index, p->queue_name);
        if (header == HEADER_NULL) return POLL_BATCH_END_OF_STREAM;

        uint8_t  aid = header_appender_id(header);
        int64_t  pos = header_payload_position(header);

        p->current_iterator = get_iterator(p, aid);
        batch_iterator_init(p->current_iterator, pos, -1, 0);
        batch_iterator_advance(p->current_iterator);
    }

    if (!batch_iterator_has_current(p->current_iterator)) {
        MMAP_ASSERT(0, "batch_poller: iterator should have current message");
    }

    MessageHandlerDirective move = handler(
        p->current_batch_index,
        p->current_iterator->current_msg_index,
        p->current_iterator->message_count,
        batch_iterator_current_data(p->current_iterator),
        batch_iterator_current_length(p->current_iterator),
        ctx);

    switch (move) {
        case HANDLER_ADVANCE:
            if (!batch_iterator_advance(p->current_iterator)) {
                p->current_batch_index++;
                p->current_iterator = NULL;
                return POLL_BATCH_ADVANCED_BATCH;
            }
            return POLL_BATCH_ADVANCED_MESSAGE;

        case HANDLER_ADVANCE_BATCH:
            p->current_batch_index++;
            p->current_iterator = NULL;
            return POLL_BATCH_ADVANCED_BATCH;

        case HANDLER_RETREAT:
        case HANDLER_RETREAT_BATCH:
            if (p->current_batch_index > start_idx) {
                p->current_batch_index--;
                p->current_iterator = NULL;
                return POLL_BATCH_RETREATED_BATCH;
            }
            return POLL_BATCH_STOPPED;

        case HANDLER_STOP:
            return POLL_BATCH_STOPPED;

        default:
            return POLL_BATCH_STOPPED;
    }
}

/* ── Positioning ─────────────────────────────────────────────────────────── */

static int64_t poller_start_index_fn(void *ctx) { return indexed_batch_poller_start_index((IndexedBatchPoller *)ctx); }

int indexed_batch_poller_move_to_index(IndexedBatchPoller *p, int64_t index)
{
    if (!poller_init(p)) {
        if (index == 0) { p->current_batch_index = 0; p->current_iterator = NULL; return 1; }
        return 0;
    }
    long start = queue_meta_start_index(p->queue_meta);
    if (index < start) return 0;

    if (p->current_batch_index == index) {
        if (p->current_iterator) batch_iterator_init(p->current_iterator, p->current_iterator->batch_offset, -1, 0);
        return 1;
    }

    if (index == start || header_found(p->header_acc, &p->header_buf, index, p->queue_name) ||
        (index > 0 && header_found(p->header_acc, &p->header_buf, index - 1, p->queue_name))) {
        p->current_batch_index = index;
        p->current_iterator = NULL;
        return 1;
    }
    return 0;
}

int indexed_batch_poller_move_to(IndexedBatchPoller *p, int64_t batch_index, int32_t message_index)
{
    if (!poller_init(p)) {
        if (batch_index == 0 && message_index == 0) { p->current_batch_index = 0; p->current_iterator = NULL; return 1; }
        return 0;
    }

    if (p->current_batch_index == batch_index && p->current_iterator) {
        if (p->current_iterator->current_msg_index == message_index) return 1;
        if (message_index < p->current_iterator->message_count) {
            batch_iterator_init(p->current_iterator, p->current_iterator->batch_offset, p->current_iterator->message_count, message_index);
            batch_iterator_advance(p->current_iterator);
            return 1;
        }
        return 0;
    }

    uint64_t hdr = header_read(p->header_acc, &p->header_buf, batch_index, p->queue_name);
    if (hdr != HEADER_NULL) {
        uint8_t  aid = header_appender_id(hdr);
        int64_t  pos = header_payload_position(hdr);
        BatchPayloadIterator *it = get_iterator(p, aid);
        int32_t count = batch_iterator_read_message_count(it, pos);
        if (message_index < count) {
            p->current_iterator = it;
            batch_iterator_init(it, pos, count, message_index);
            batch_iterator_advance(it);
            p->current_batch_index = batch_index;
            return 1;
        }
    } else if (message_index == 0 && (batch_index == queue_meta_start_index(p->queue_meta) || 
               (batch_index > 0 && header_found(p->header_acc, &p->header_buf, batch_index - 1, p->queue_name)))) {
        p->current_batch_index = batch_index;
        p->current_iterator = NULL;
        return 1;
    }

    return 0;
}

int indexed_batch_poller_move_to_end(IndexedBatchPoller *p)
{
    if (!poller_init(p)) return 1;
    long from = p->current_batch_index > queue_meta_start_index(p->queue_meta) ? p->current_batch_index : queue_meta_start_index(p->queue_meta);
    int64_t last = header_search_last_index(from, poller_start_index_fn, p, p->header_acc, &p->header_buf, p->queue_name);
    p->current_batch_index = (last == HEADER_NULL_INDEX) ? queue_meta_start_index(p->queue_meta) : last + 1;
    p->current_iterator = NULL;
    return 1;
}

int indexed_batch_poller_move_to_last_existing_batch(IndexedBatchPoller *p)
{
    if (!poller_init(p)) return 0;
    long from = p->current_batch_index > 0 ? p->current_batch_index - 1 : 0;
    from = from > queue_meta_start_index(p->queue_meta) ? from : queue_meta_start_index(p->queue_meta);
    int64_t last = header_search_last_index(from, poller_start_index_fn, p, p->header_acc, &p->header_buf, p->queue_name);
    if (last == HEADER_NULL_INDEX) return 0;
    p->current_batch_index = last;
    p->current_iterator = NULL;
    return 1;
}

int indexed_batch_poller_move_to_start(IndexedBatchPoller *p)
{
    p->current_batch_index = indexed_batch_poller_start_index(p);
    p->current_iterator = NULL;
    return 1;
}

int64_t indexed_batch_poller_current_batch_index(const IndexedBatchPoller *p) { return p->current_batch_index; }
int32_t indexed_batch_poller_current_message_index(const IndexedBatchPoller *p) { return p->current_iterator ? p->current_iterator->current_msg_index : 0; }
int64_t indexed_batch_poller_start_index(IndexedBatchPoller *p) { if (!poller_init(p)) return 0; return queue_meta_start_index(p->queue_meta); }

int indexed_batch_poller_exists(IndexedBatchPoller *p, int64_t batch_index, int32_t message_index)
{
    if (!poller_init(p)) return 0;
    if (batch_index < queue_meta_start_index(p->queue_meta)) return 0;
    if (p->current_batch_index == batch_index && p->current_iterator) return message_index < p->current_iterator->message_count;
    uint64_t hdr = header_read(p->header_acc, &p->header_buf, batch_index, p->queue_name);
    if (hdr == HEADER_NULL) return 0;
    BatchPayloadIterator *it = get_iterator(p, header_appender_id(hdr));
    return message_index < batch_iterator_read_message_count(it, header_payload_position(hdr));
}

void indexed_batch_poller_close(IndexedBatchPoller *p)
{
    if (!p || p->closed) return;
    p->closed = 1;
    for (int i = 0; i < MAX_APPENDERS; i++) if (p->iterators[i]) { free(p->iterators[i]); p->iterators[i] = NULL; }
    if (p->ro_acc) queue_region_accessor_close(p->ro_acc);
    if (p->queue_meta) queue_meta_close(p->queue_meta);
    free(p->directory);
    free(p->queue_name);
    free(p);
}
