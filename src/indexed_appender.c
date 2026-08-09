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

/* Bounded linear scan to avoid livelock when the appender is far behind
 * the queue head. Falls back to a jump-based search for efficient catch-up. */
#define MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH  100

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

    /* Read-only accessor for jump-based header search fallback */
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *ro_header;
    Buffer               ro_buf;

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

/* ── Start index suppliers for header search ──────────────────────────────── */

static int64_t appender_start_index_from_meta(void *ctx)
{
    return queue_meta_start_index((const QueueMeta *)ctx);
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
    int attempts = 0;
    while (attempts < MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH) {
        attempts++;
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

    /* Fall back to jump-based search — the appender is far behind the head */
    int64_t last_index = header_search_last_index(
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

    WrapResult r = region_accessor_wrap(a->header_acc,
                                         a->current_header_position,
                                         &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    return 0;
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

static int initialise_append_position(IndexedAppender *a, int64_t start_index)
{
    /*
     * Step 1: Use the stored RO accessor to find lastIndex via
     *         exponential+binary search. RO accessor allows non-sequential
     *         random access jumps without polluting RW region mapping state.
     */
    RegionAccessor *ro_header = a->ro_header;
    Buffer         *ro_buf   = &a->ro_buf;

    int64_t last_index = header_search_last_index(
        start_index,
        appender_start_index_from_meta, a->queue_meta,
        ro_header, ro_buf,
        a->queue_name);

    if (last_index == HEADER_NULL_INDEX) {
        /* Queue is empty — start at the requested start_index */
        a->current_index           = start_index;
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
            uint64_t hdr = header_read(ro_header, ro_buf, i, a->queue_name);
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
                queue_region_accessor_payload(a->ro_acc, a->appender_id);

            int64_t probing = committed_payload_pos;
            int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;

            /* Walk past the committed message itself first */
            int32_t len = payload_walk_advance(a->queue_name,
                                                &probing, &msg_off,
                                                ro_payload, ro_buf);
            if (len <= 0) {
                return MMAPQ_ECORRUPT;
            }

            /* Walk past any uncommitted tail */
            while (1) {
                int64_t next_probing = probing;
                int64_t next_msg_off = PAYLOAD_ITERATOR_NOT_SET;
                int32_t next_len = payload_walk_advance(a->queue_name,
                                                          &next_probing,
                                                          &next_msg_off,
                                                          ro_payload, ro_buf);
                if (next_len == 0) break;
                probing = next_probing;
            }

            a->current_payload_position = probing;
        }
    }

    a->current_header_position = header_position(a->current_index);

    /*
     * Step 4: Eager-wrap RW regions to trigger file creation and initial
     *         mapping before the first append. Eliminates first-message
     *         latency spike.
     */
    if (region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf) != WRAP_SUCCESS) return -1;
    if (region_accessor_wrap(a->payload_acc, a->current_payload_position, &a->payload_buf) != WRAP_SUCCESS) return -1;
    return 0;
}

/* ── Internal constructor ────────────────────────────────────────────────── */

static int appender_create_internal(
    const char     *directory,
    const char     *queue_name,
    int32_t         region_size,
    int             ring_size,
    int64_t         max_file_size,
    uint64_t        timeout_ns,
    int             archivable,
    Runtime        *runtime,
    AppenderIdPool *id_pool,
    uint8_t         appender_id,
    int64_t         start_index,
    IndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;

    IndexedAppender *a = calloc(1, sizeof(IndexedAppender));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory    = strdup(directory);
    if (a->directory == NULL) { free(a); return MMAPQ_ENOMEM; }
    a->queue_name   = strdup(queue_name);
    if (a->queue_name == NULL) { free(a->directory); free(a); return MMAPQ_ENOMEM; }
    a->appender_id  = appender_id;
    a->id_pool      = id_pool;
    a->closed       = 0;

    a->current_index           = INDEXED_APPENDER_NOT_INITIALISED;
    a->current_header_position = INDEXED_APPENDER_NOT_INITIALISED;
    a->current_payload_position = INDEXED_APPENDER_NOT_INITIALISED;

    /*
     * Open (or create) the queue meta file.
     */
    int rc = queue_meta_open_rw(directory, queue_name,
                                 start_index,
                                 region_size, max_file_size,
                                 archivable,
                                 QUEUE_TYPE_NORMAL,
                                 &a->queue_meta);
    if (rc != 0) {
        appender_id_pool_release(a->id_pool, a->appender_id);
        appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }

    /* Adopt contractual parameters from meta */
    int32_t reconciled_region_size   = queue_meta_region_size(a->queue_meta);
    int64_t reconciled_max_file_size = queue_meta_max_file_size(a->queue_meta);

    /* Open appender meta (creates file if new) */
    rc = appender_meta_open_rw(directory, queue_name, appender_id, &a->appender_meta);
    if (rc != 0) {
        if (a->queue_meta) queue_meta_close(a->queue_meta);
        appender_id_pool_release(a->id_pool, a->appender_id);
        appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }

    /* Create RW region accessors */
    rc = queue_region_accessor_create_rw(
        directory, queue_name, reconciled_region_size, ring_size,
        REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD,
        reconciled_max_file_size, timeout_ns, runtime, appender_id,
        &a->rw_acc);
    if (rc != 0) {
        appender_meta_close(a->appender_meta);
        if (a->queue_meta) queue_meta_close(a->queue_meta);
        appender_id_pool_release(a->id_pool, a->appender_id);
        appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }

    a->header_acc  = queue_region_accessor_header(a->rw_acc);
    a->payload_acc = queue_region_accessor_payload(a->rw_acc, appender_id);

    /* Create RO accessor for header search fallback */
    rc = queue_region_accessor_create_ro(
        directory, queue_name,
        reconciled_region_size, ring_size,
        0,
        reconciled_max_file_size, timeout_ns, runtime,
        &a->ro_acc);
    if (rc != 0) {
        queue_region_accessor_close(a->rw_acc);
        appender_meta_close(a->appender_meta);
        if (a->queue_meta) queue_meta_close(a->queue_meta);
        appender_id_pool_release(a->id_pool, a->appender_id);
        appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    a->ro_header = queue_region_accessor_header(a->ro_acc);

    /* Find where to resume appending */
    if (initialise_append_position(a, start_index)) {
        queue_region_accessor_close(a->ro_acc);
        queue_region_accessor_close(a->rw_acc);
        appender_meta_close(a->appender_meta);
        if (a->queue_meta) queue_meta_close(a->queue_meta);
        appender_id_pool_release(a->id_pool, a->appender_id);
        appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return MMAPQ_ECLOSED;
    }

    *out = a;
    return 0;
}

/* ── Factory functions ───────────────────────────────────────────────────── */

int indexed_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    Runtime    *runtime,
    int64_t     start_index,
    IndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    /* Acquire an ID from the pool — pool is opened here and owned by the appender */
    AppenderIdPool *pool = NULL;
    int rc = appender_id_pool_open(directory, queue_name, &pool);
    if (rc != 0) return rc;

    int id = appender_id_pool_acquire(pool);
    if (id < 0) {
        appender_id_pool_close(pool);
        return MMAPQ_EIDCONFLICT;
    }

    return appender_create_internal(directory, queue_name, region_size, ring_size,
                                      max_file_size, timeout_ns, archivable, runtime,
                                      pool, (uint8_t)id, start_index, out);
}

int indexed_appender_create_with_id(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    Runtime    *runtime,
    uint8_t     appender_id,
    int64_t     start_index,
    IndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    AppenderIdPool *pool = NULL;
    int rc = appender_id_pool_open(directory, queue_name, &pool);
    if (rc != 0) return rc;

    int ok = appender_id_pool_try_acquire(pool, appender_id);
    if (!ok) {
        appender_id_pool_close(pool);
        return MMAPQ_EIDCONFLICT;
    }

    return appender_create_internal(directory, queue_name, region_size, ring_size,
                                      max_file_size, timeout_ns, archivable, runtime,
                                      pool, appender_id, start_index, out);
}

/* ── indexed_appender_append ─────────────────────────────────────────────── */

int64_t indexed_appender_append(
    IndexedAppender *a,
    const void      *data,
    int32_t          length)
{
    if (a == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;

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
    if (a == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;

    int64_t payload_offset;
    if (write_payload(a, data, length, &payload_offset)) return -1;
    return payload_offset;
}

/* ── indexed_appender_commit ─────────────────────────────────────────────── */

int64_t indexed_appender_commit(
    IndexedAppender *a,
    int64_t          payload_offset)
{
    if (a == NULL) return -1;
    if (a->closed) return -1;
    if (payload_offset <= 0) return -1;

    int64_t committed;
    if (commit_header(a, payload_offset, &committed)) return -1;
    return committed;
}

PayloadIterator *indexed_appender_iterator(
    IndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset)
{
    if (a == NULL) return NULL;

    if (a->payload_iter == NULL) {
        payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            0, /* live end-of-stream */
            &a->payload_iter);
    }

    return payload_iterator_init(a->payload_iter, start_offset, end_offset);
}

UncommittedPayloadIterator *indexed_appender_uncommitted_iterator(
    IndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset)
{
    if (a == NULL) return NULL;

    /* Create inner iterator with fixed end-of-stream */
    if (a->uncommitted_inner == NULL) {
        payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            1, /* fix end-of-stream at init time */
            &a->uncommitted_inner);
    }

    if (a->uncommitted_iter == NULL) {
        uncommitted_iterator_create(
            a->queue_name,
            a->appender_id,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            a->header_acc,
            a->uncommitted_inner,
            &a->uncommitted_iter);
    }

    return uncommitted_iterator_init(a->uncommitted_iter, start_offset, end_offset);
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

uint8_t indexed_appender_id(const IndexedAppender *a)
{
    if (a == NULL) return 0;
    return a->appender_id;
}

int64_t indexed_appender_current_index(const IndexedAppender *a)
{
    if (a == NULL) return 0;
    return a->current_index;
}

int64_t indexed_appender_start_offset(const IndexedAppender *a)
{
    if (a == NULL) return 0;
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

    if (a->ro_acc != NULL) {
        /* ro_header is owned by ro_acc — no separate close */
        queue_region_accessor_close(a->ro_acc);
        a->ro_acc    = NULL;
        a->ro_header = NULL;
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
