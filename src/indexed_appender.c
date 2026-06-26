#include "indexed_appender.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "appender_meta.h"
#include "appender_id_pool.h"
#include "header_reader.h"
#include "header_codec.h"
#include "payload_walk.h"
#include "payload_iterator.h"
#include "uncommitted_payload_iterator.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

struct IndexedAppender {
    char                *directory;
    char                *queue_name;
    uint8_t              appender_id;
    AppenderIdPool      *id_pool;
    AppenderMeta        *appender_meta;
    QueueMeta           *queue_meta;   /* Reconciled meta */
    QueueRegionAccessor *rw_acc;   /* RW header + RW payload (this appender) */
    RegionAccessor      *header_acc;
    RegionAccessor      *payload_acc;

    /* Append position state */
    int64_t current_index;
    int64_t current_header_position;
    int64_t current_payload_position;

    /* Reusable buffers (flyweight) */
    Buffer header_buf;
    Buffer payload_buf;

    /* Iterators — created lazily on first iterator() call */
    PayloadIterator            *payload_iter;
    PayloadIterator            *uncommitted_inner;
    UncommittedPayloadIterator *uncommitted_iter;

    int closed;
};

/* ── Stream offset callbacks ─────────────────────────────────────────────── */

static int64_t appender_start_of_stream(void *ctx)
{
    IndexedAppender *a = (IndexedAppender *)ctx;
    return appender_meta_start_offset(a->appender_meta);
}

static int64_t appender_end_of_stream(void *ctx)
{
    IndexedAppender *a = (IndexedAppender *)ctx;
    return a->current_payload_position;
}

/* ── Start index supplier for header search ──────────────────────────────── */

static int64_t appender_start_index_fn(void *ctx)
{
    /* Appenders don't archive — start index is always 0 */
    (void)ctx;
    return QUEUE_META_INITIAL_START_INDEX;
}

/* ── Write padding at region boundary ───────────────────────────────────── */

static int write_padding_if_needed(IndexedAppender *a, int32_t length)
{
    WrapResult r = region_accessor_wrap(a->payload_acc,
                                         a->current_payload_position,
                                         &a->payload_buf);
    if (r != WRAP_SUCCESS) return -1;

    int32_t capacity = a->payload_buf.capacity;

    if (capacity >= length + PAYLOAD_LENGTH_SIZE) {
        return 0;  /* enough space — no padding needed */
    }

    /*
     * Not enough space. Write padding so the payload_walk can skip past
     * this region boundary during recovery.
     *
     * Short padding: capacity <= 4 bytes — don't write anything, just
     *   skip. The walk detects this by seeing capacity <= PAYLOAD_LENGTH_SIZE.
     *
     * Long padding: capacity > 4 bytes — write a negative length field.
     *   Value = -(capacity - PAYLOAD_LENGTH_SIZE), so skip = abs(value) + 4 = capacity.
     */
    if (capacity > PAYLOAD_LENGTH_SIZE) {
        int32_t pad_value = PAYLOAD_LENGTH_SIZE - capacity;  /* negative */
        memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &pad_value, sizeof(int32_t));
    }

    a->current_payload_position += capacity;

    /* Re-wrap at next region */
    r = region_accessor_wrap(a->payload_acc,
                              a->current_payload_position,
                              &a->payload_buf);
    if (r != WRAP_SUCCESS) return -1;
    return 0;
}

/* ── Write payload bytes ─────────────────────────────────────────────────── */

static int write_payload(IndexedAppender *a, const void *data, int32_t length,
                          int64_t *out_offset)
{
    MMAP_ASSERT(length + PAYLOAD_LENGTH_SIZE <= queue_meta_region_size(a->queue_meta),
        "write_payload: message too large — must fit within one region. "
        "Increase region_size or reduce message size.");
    if (write_padding_if_needed(a, length)) return -1;

    int64_t payload_offset = a->current_payload_position;

    /* Write length prefix */
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));

    /* Write payload bytes */
    memcpy(a->payload_buf.data + PAYLOAD_DATA_OFFSET, data, (size_t)length);

    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;

    *out_offset = payload_offset;
    return 0;
}

/* ── CAS header commit ───────────────────────────────────────────────────── */

static int move_to_last_header(IndexedAppender *a)
{
    while (1) {
        a->current_index++;
        a->current_header_position = header_position(a->current_index);

        WrapResult r = region_accessor_wrap(a->header_acc,
                                             a->current_header_position,
                                             &a->header_buf);
        if (r != WRAP_SUCCESS) return -1;

        _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
        if (atomic_load_explicit(hp, memory_order_acquire) == HEADER_NULL) {
            return 0;
        }
    }
}

static int commit_header(IndexedAppender *a, int64_t payload_offset, int64_t *out_index)
{
    uint64_t header = header_encode(a->appender_id, payload_offset);

    WrapResult r = region_accessor_wrap(a->header_acc,
                                         a->current_header_position,
                                         &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
    ASSERT_HEADER_ALIGNED(hp);
    uint64_t expected    = HEADER_NULL;

    while (__builtin_expect(!atomic_compare_exchange_strong_explicit(
               hp, &expected, header,
               memory_order_acq_rel, memory_order_acquire), 0)) {
        if (move_to_last_header(a)) return -1;

        r = region_accessor_wrap(a->header_acc,
                                  a->current_header_position,
                                  &a->header_buf);
        if (r != WRAP_SUCCESS) return -1;

        hp       = (_Atomic uint64_t *)a->header_buf.data;
        expected = HEADER_NULL;
    }

    *out_index = a->current_index;
    a->current_index++;
    a->current_header_position = header_position(a->current_index);

    return 0;
}

/* ── Initialise append position after (re)start ──────────────────────────── */

static void initialise_append_position(IndexedAppender *a,
                                        const char *directory,
                                        const char *queue_name,
                                        int32_t region_size,
                                        int ring_size,
                                        int64_t max_file_size,
                                        uint64_t timeout_ns,
                                        Runtime *runtime)
{
    /*
     * Step 1: Use a temporary RO accessor to find lastIndex via
     *         exponential+binary search. RO accessor allows non-sequential
     *         random access jumps without polluting RW region mapping state.
     */
    QueueRegionAccessor *ro_acc = queue_region_accessor_create_ro(
        directory, queue_name,
        region_size, ring_size,
        0,           /* regions_to_map_ahead = 0 for recovery scan */
        max_file_size, timeout_ns, runtime);

    RegionAccessor *ro_header = queue_region_accessor_header(ro_acc);
    Buffer ro_buf = {0};

    int64_t last_index = header_search_last_index(
        QUEUE_META_INITIAL_START_INDEX,
        appender_start_index_fn, NULL,
        ro_header, &ro_buf,
        a->queue_name);

    if (last_index == HEADER_NULL_INDEX) {
        /* Queue is empty */
        a->current_index           = 0;
        a->current_payload_position = appender_meta_start_offset(a->appender_meta);
    } else {
        a->current_index = last_index + 1;

        /*
         * Step 2: Scan backwards from lastIndex to find this appender's
         *         last committed header. Linear scan — no jumping —
         *         since we need to walk the payload stream forward from
         *         that point.
         */
        int64_t committed_payload_pos = PAYLOAD_ITERATOR_NOT_SET;

        for (int64_t i = last_index; i >= 0; i--) {
            uint64_t hdr = header_read(ro_header, &ro_buf, i, a->queue_name);
            if (hdr == HEADER_NULL) break;
            if (header_appender_id(hdr) == a->appender_id) {
                committed_payload_pos = header_payload_position(hdr);
                break;
            }
        }

        if (committed_payload_pos == PAYLOAD_ITERATOR_NOT_SET) {
            /* No committed entry found for this appender */
            a->current_payload_position = appender_meta_start_offset(a->appender_meta);
        } else {
            /*
             * Step 3: Walk payload forward past the committed message
             *         and any uncommitted tail, to find the true end
             *         of the payload stream.
             */
            RegionAccessor *ro_payload =
                queue_region_accessor_payload(ro_acc, a->appender_id);

            int64_t probing = committed_payload_pos;
            int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;

            /* Walk past the committed message itself first */
            int32_t len = payload_walk_advance(a->queue_name,
                                                &probing, &msg_off,
                                                ro_payload, &ro_buf);
            (void)len;
            MMAP_ASSERT(len > 0,
                "initialise_append_position: committed message has zero length");

            /* Walk past any uncommitted tail */
            while (1) {
                int64_t next_probing = probing;
                int64_t next_msg_off = PAYLOAD_ITERATOR_NOT_SET;
                int32_t next_len = payload_walk_advance(a->queue_name,
                                                         &next_probing,
                                                         &next_msg_off,
                                                         ro_payload, &ro_buf);
                if (next_len == 0) break;
                probing = next_probing;
            }

            a->current_payload_position = probing;
        }
    }

    queue_region_accessor_close(ro_acc);

    a->current_header_position = header_position(a->current_index);

    /*
     * Step 4: Eager-wrap RW regions to trigger file creation and initial
     *         mapping before the first append. Eliminates first-message
     *         latency spike.
     */
    WrapResult r;
    r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS,
        "initialise_append_position: failed to eager-wrap header region");

    r = region_accessor_wrap(a->payload_acc, a->current_payload_position, &a->payload_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS,
        "initialise_append_position: failed to eager-wrap payload region");
}

/* ── Internal constructor ────────────────────────────────────────────────── */

static IndexedAppender *appender_create_internal(
    const char     *directory,
    const char     *queue_name,
    int32_t         region_size,
    int             ring_size,
    int64_t         max_file_size,
    uint64_t        timeout_ns,
    Runtime        *runtime,
    AppenderIdPool *id_pool,
    uint8_t         appender_id)
{
    IndexedAppender *a = calloc(1, sizeof(IndexedAppender));
    MMAP_ASSERT(a != NULL, "indexed_appender: out of memory");

    a->directory    = strdup(directory);
    a->queue_name   = strdup(queue_name);
    a->appender_id  = appender_id;
    a->id_pool      = id_pool;
    a->closed       = 0;

    a->current_index           = INDEXED_APPENDER_NOT_INITIALISED;
    a->current_header_position = INDEXED_APPENDER_NOT_INITIALISED;
    a->current_payload_position = INDEXED_APPENDER_NOT_INITIALISED;

    /*
     * Open (or create) the queue meta file.
     * Appender is responsible for creating the meta file if it doesn't exist.
     * reconciliation logic:
     *   - If file is new: use parameters passed to create().
     *   - If file exists: adopt its parameters and IGNORE create() args.
     */
    a->queue_meta = queue_meta_open_rw(directory, queue_name, 
                                        QUEUE_META_INITIAL_START_INDEX,
                                        region_size, max_file_size,
                                        /* archivable = */ 0);
    MMAP_ASSERT(a->queue_meta != NULL, "indexed_appender: failed to open/create queue meta");

    /* Adopt contractual parameters from meta */
    int32_t reconciled_region_size   = queue_meta_region_size(a->queue_meta);
    int64_t reconciled_max_file_size = queue_meta_max_file_size(a->queue_meta);

    /* Open appender meta (creates file if new) */
    a->appender_meta = appender_meta_open_rw(directory, queue_name, appender_id);
    MMAP_ASSERT(a->appender_meta != NULL, "indexed_appender: failed to open appender meta");

    /* Create RW region accessors */
    a->rw_acc = queue_region_accessor_create_rw(
        directory, queue_name, reconciled_region_size, ring_size,
        REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD,
        reconciled_max_file_size, timeout_ns, runtime, appender_id);

    a->header_acc  = queue_region_accessor_header(a->rw_acc);
    a->payload_acc = queue_region_accessor_payload(a->rw_acc, appender_id);

    /* Find where to resume appending */
    initialise_append_position(a, directory, queue_name,
                                 reconciled_region_size, ring_size,
                                 reconciled_max_file_size, timeout_ns, runtime);

    return a;
}

/* ── Factory functions ───────────────────────────────────────────────────── */

IndexedAppender *indexed_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    MMAP_ASSERT(directory  != NULL, "indexed_appender_create: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "indexed_appender_create: queue_name is NULL");

    /* Acquire an ID from the pool — pool is opened here and owned by the appender */
    AppenderIdPool *pool = appender_id_pool_open(directory, queue_name);
    int id = appender_id_pool_acquire(pool);
    MMAP_ASSERT(id >= 0, "indexed_appender_create: appender ID pool exhausted");

    return appender_create_internal(directory, queue_name, region_size, ring_size,
                                      max_file_size, timeout_ns, runtime,
                                      pool, (uint8_t)id);
}

IndexedAppender *indexed_appender_create_with_id(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    uint8_t     appender_id)
{
    MMAP_ASSERT(directory  != NULL, "indexed_appender_create_with_id: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "indexed_appender_create_with_id: queue_name is NULL");

    AppenderIdPool *pool = appender_id_pool_open(directory, queue_name);
    int ok = appender_id_pool_try_acquire(pool, appender_id);
    (void)ok;
    MMAP_ASSERT(ok, "indexed_appender_create_with_id: requested appender_id already in use");

    return appender_create_internal(directory, queue_name, region_size, ring_size,
                                      max_file_size, timeout_ns, runtime,
                                      pool, appender_id);
}

/* ── indexed_appender_append ─────────────────────────────────────────────── */

int64_t indexed_appender_append(
    IndexedAppender *a,
    const void      *data,
    int32_t          length)
{
    MMAP_ASSERT(a      != NULL, "indexed_appender_append: appender is NULL");
    MMAP_ASSERT(!a->closed,     "indexed_appender_append: appender is closed");
    MMAP_ASSERT(data   != NULL, "indexed_appender_append: data is NULL");
    MMAP_ASSERT(length > 0,     "indexed_appender_append: length must be > 0");

    int64_t payload_offset;
    if (write_payload(a, data, length, &payload_offset)) return -1;

    int64_t committed;
    if (commit_header(a, payload_offset, &committed)) return -1;
    return committed;
}

/* ── indexed_appender_append_uncommitted ─────────────────────────────────── */

int64_t indexed_appender_append_uncommitted(
    IndexedAppender *a,
    const void      *data,
    int32_t          length)
{
    MMAP_ASSERT(a      != NULL, "indexed_appender_append_uncommitted: appender is NULL");
    MMAP_ASSERT(!a->closed,     "indexed_appender_append_uncommitted: appender is closed");
    MMAP_ASSERT(data   != NULL, "indexed_appender_append_uncommitted: data is NULL");
    MMAP_ASSERT(length > 0,     "indexed_appender_append_uncommitted: length must be > 0");

    int64_t payload_offset;
    if (write_payload(a, data, length, &payload_offset)) return -1;
    return payload_offset;
}

/* ── indexed_appender_commit ─────────────────────────────────────────────── */

int64_t indexed_appender_commit(
    IndexedAppender *a,
    int64_t          payload_offset)
{
    MMAP_ASSERT(a != NULL,  "indexed_appender_commit: appender is NULL");
    MMAP_ASSERT(!a->closed, "indexed_appender_commit: appender is closed");
    MMAP_ASSERT(payload_offset > 0,
        "indexed_appender_commit: payload_offset must be > 0");

    int64_t committed;
    if (commit_header(a, payload_offset, &committed)) return -1;
    return committed;
}

/* ── Iterators ───────────────────────────────────────────────────────────── */

PayloadIterator *indexed_appender_iterator(
    IndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset)
{
    MMAP_ASSERT(a != NULL, "indexed_appender_iterator: appender is NULL");

    if (a->payload_iter == NULL) {
        a->payload_iter = payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            0 /* live end-of-stream */);
    }

    return payload_iterator_init(a->payload_iter, start_offset, end_offset);
}

UncommittedPayloadIterator *indexed_appender_uncommitted_iterator(
    IndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset)
{
    MMAP_ASSERT(a != NULL, "indexed_appender_uncommitted_iterator: appender is NULL");

    /* Create inner iterator with fixed end-of-stream */
    if (a->uncommitted_inner == NULL) {
        a->uncommitted_inner = payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            1 /* fix end-of-stream at init time */);
    }

    if (a->uncommitted_iter == NULL) {
        a->uncommitted_iter = uncommitted_iterator_create(
            a->queue_name,
            a->appender_id,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            a->header_acc,
            a->uncommitted_inner);
    }

    return uncommitted_iterator_init(a->uncommitted_iter, start_offset, end_offset);
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

uint8_t indexed_appender_id(const IndexedAppender *a)
{
    MMAP_ASSERT(a != NULL, "indexed_appender_id: appender is NULL");
    return a->appender_id;
}

int64_t indexed_appender_current_index(const IndexedAppender *a)
{
    MMAP_ASSERT(a != NULL, "indexed_appender_current_index: appender is NULL");
    return a->current_index;
}

int64_t indexed_appender_start_offset(const IndexedAppender *a)
{
    MMAP_ASSERT(a != NULL, "indexed_appender_start_offset: appender is NULL");
    return appender_meta_start_offset(a->appender_meta);
}

/* ── indexed_appender_close ──────────────────────────────────────────────── */

void indexed_appender_close(IndexedAppender *a)
{
    if (a == NULL || a->closed) return;
    a->closed = 1;

    if (a->uncommitted_iter != NULL) {
        uncommitted_iterator_destroy(a->uncommitted_iter);
        a->uncommitted_iter = NULL;
    }
    if (a->uncommitted_inner != NULL) {
        payload_iterator_destroy(a->uncommitted_inner);
        a->uncommitted_inner = NULL;
    }
    if (a->payload_iter != NULL) {
        payload_iterator_destroy(a->payload_iter);
        a->payload_iter = NULL;
    }

    if (a->appender_meta != NULL) {
        appender_meta_close(a->appender_meta);
        a->appender_meta = NULL;
    }

    if (a->queue_meta != NULL) {
        queue_meta_close(a->queue_meta);
        a->queue_meta = NULL;
    }

    if (a->rw_acc != NULL) {
        queue_region_accessor_close(a->rw_acc);
        a->rw_acc = NULL;
    }

    if (a->id_pool != NULL) {
        appender_id_pool_release(a->id_pool, a->appender_id);
        appender_id_pool_close(a->id_pool);
        a->id_pool = NULL;
    }

    free(a->directory);
    free(a->queue_name);
    free(a);
}
