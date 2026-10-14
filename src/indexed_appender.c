#include "indexed_appender.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/appender_meta.h"
#include "internal/appender_id_pool.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/payload_walk.h"
#include "internal/payload_iterator.h"
#include "internal/uncommitted_payload_iterator.h"
#include "internal/test_hooks.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

/* Bounded linear scan to avoid livelock when the appender is far behind
 * the queue head. Falls back to a jump-based search for efficient catch-up. */
#define MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH  100

struct MmapqIndexedAppender {
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
    int64_t last_appended;   /* payload offset of the most recent append —
                              * commit() fast-path key (CAND-30)            */

    /* Reusable buffers (flyweight) */
    Buffer header_buf;
    Buffer payload_buf;

    /* Iterators — created lazily on first iterator() call */
    MmapqPayloadIterator            *payload_iter;
    MmapqPayloadIterator            *uncommitted_inner;
    MmapqUncommittedPayloadIterator *uncommitted_iter;

    int closed;
};

/* ── Stream offset callbacks ─────────────────────────────────────────────── */

static int64_t appender_start_of_stream(void *ctx)
{
    MmapqIndexedAppender *a = (MmapqIndexedAppender *)ctx;
    return appender_meta_start_offset(a->appender_meta);
}

static int64_t appender_end_of_stream(void *ctx)
{
    MmapqIndexedAppender *a = (MmapqIndexedAppender *)ctx;
    return a->current_payload_position;
}

/* ── Start index suppliers for header search ──────────────────────────────── */

static int64_t appender_start_index_from_meta(void *ctx)
{
    return queue_meta_start_index((const QueueMeta *)ctx);
}

/* ── Write padding at region boundary ───────────────────────────────────── */

static int write_padding_if_needed(MmapqIndexedAppender *a, int32_t length)
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

static int write_payload(MmapqIndexedAppender *a, const void *data, int32_t length,
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
    a->last_appended           = payload_offset;

    *out_offset = payload_offset;
    return 0;
}

/* ── CAS header commit ───────────────────────────────────────────────────── */

static int move_to_last_header(MmapqIndexedAppender *a)
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

    WrapResult r = region_accessor_wrap(a->header_acc,
                                         a->current_header_position,
                                         &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    return 0;
}

static int commit_header(MmapqIndexedAppender *a, int64_t payload_offset, int64_t *out_index)
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

/* Walk forward from `from` to the end of the payload stream, read-only and
 * never creating files (CAND-30, I5: a joining appender never writes at
 * start_offset — bytes there may belong to a dead payload, and writing a
 * shorter message over a longer one leaves stale bytes a later recovery
 * would misread). A position with no file yet, or beyond a file's end, is
 * the end of the stream. The pre-wrap is required: payload_walk collapses
 * "no file" into -1 (D8). */
static int walk_payload_to_end(MmapqIndexedAppender *a, int64_t from,
                               int64_t *out_end)
{
    RegionAccessor *ro_payload =
        mmapqi_queue_region_accessor_payload(a->ro_acc, a->appender_id);
    int64_t probing = from;
    while (1) {
        WrapResult wr = region_accessor_wrap_no_premap(ro_payload, probing,
                                                       &a->ro_buf);
        if (wr == WRAP_FILE_NOT_FOUND || wr == WRAP_BEYOND_FILE_SIZE)
            break;   /* end of stream */
        if (wr == WRAP_MAP_TIMED_OUT)
            return MMAPQ_ETIMEDOUT;
        if (wr != WRAP_SUCCESS)
            return MMAPQ_ECORRUPT;
        int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;
        int32_t len = mmapqi_payload_walk_advance(a->queue_name, &probing,
                                                  &msg_off,
                                                  ro_payload, &a->ro_buf);
        if (len == 0) break;
        if (len < 0) return MMAPQ_ECORRUPT;
    }
    *out_end = probing;
    return 0;
}

static int initialise_append_position(MmapqIndexedAppender *a, int64_t start_index)
{
    /*
     * Step 1: Use the stored RO accessor to find lastIndex via
     *         exponential+binary search. RO accessor allows non-sequential
     *         random access jumps without polluting RW region mapping state.
     */
    RegionAccessor *ro_header = a->ro_header;
    Buffer         *ro_buf   = &a->ro_buf;

    int64_t last_index = mmapqi_header_search_last_index(
        start_index,
        appender_start_index_from_meta, a->queue_meta,
        ro_header, ro_buf,
        a->queue_name);

    /* I5: never read headers below start_index — those files may have been
     * deleted (P12). One acquire load; a concurrent archive advancing it
     * further is covered by the deletion protocol (archiving-design.md). */
    int64_t scan_floor = queue_meta_start_index(a->queue_meta);

    if (last_index == HEADER_NULL_INDEX) {
        /* Queue is empty — start at the requested start_index; walk from
         * start_offset to the end of stream (no file yet = the offset) */
        a->current_index = start_index;
        int wrc = walk_payload_to_end(a, appender_meta_start_offset(a->appender_meta),
                                      &a->current_payload_position);
        if (wrc != 0) return wrc;
    } else {
        a->current_index = last_index + 1;

        /*
         * Step 2: Scan backwards from lastIndex to find this appender's
         *         last committed header, bounded at start_index. Linear
         *         scan — no jumping — since we need to walk the payload
         *         stream forward from that point.
         */
        int64_t committed_payload_pos = PAYLOAD_ITERATOR_NOT_SET;

        for (int64_t i = last_index; i >= scan_floor; i--) {
            uint64_t hdr = mmapqi_header_read(ro_header, ro_buf, i, a->queue_name);
            if (hdr == HEADER_NULL) break;
            if (header_appender_id(hdr) == a->appender_id) {
                committed_payload_pos = header_payload_position(hdr);
                break;
            }
        }

        if (committed_payload_pos == PAYLOAD_ITERATOR_NOT_SET) {
            /* No committed entry in the live range for this appender —
             * walk forward from start_offset to the end of the stream */
            int wrc = walk_payload_to_end(a, appender_meta_start_offset(a->appender_meta),
                                          &a->current_payload_position);
            if (wrc != 0) return wrc;
        } else {
            /*
             * Step 3: Walk payload forward past the committed message
             *         and any uncommitted tail, to find the true end
             *         of the payload stream.
             */
            RegionAccessor *ro_payload =
                mmapqi_queue_region_accessor_payload(a->ro_acc, a->appender_id);

            int64_t probing = committed_payload_pos;
            int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;

            /* Walk past the committed message itself first */
            int32_t len = mmapqi_payload_walk_advance(a->queue_name,
                                                &probing, &msg_off,
                                                ro_payload, ro_buf);
            if (len <= 0) {
                return MMAPQ_ECORRUPT;
            }

            /* Walk past any uncommitted tail */
            while (1) {
                int64_t next_probing = probing;
                int64_t next_msg_off = PAYLOAD_ITERATOR_NOT_SET;
                int32_t next_len = mmapqi_payload_walk_advance(a->queue_name,
                                                          &next_probing,
                                                          &next_msg_off,
                                                          ro_payload, ro_buf);
                if (next_len == 0) break;
                /* A walk error leaves probing unadvanced; without this the loop
                 * would spin forever on a corrupt tail. Matches the first walk's
                 * len <= 0 mapping at MMAPQ_ECORRUPT above. */
                if (next_len < 0) return MMAPQ_ECORRUPT;
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
    MmapqRuntime        *runtime,
    AppenderIdPool *id_pool,
    uint8_t         appender_id,
    int64_t         start_index,
    MmapqIndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;

    MmapqIndexedAppender *a = calloc(1, sizeof(MmapqIndexedAppender));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory    = strdup(directory);
    if (a->directory == NULL) { free(a); return MMAPQ_ENOMEM; }
    a->queue_name   = strdup(queue_name);
    if (a->queue_name == NULL) { free(a->directory); free(a); return MMAPQ_ENOMEM; }
    a->appender_id  = appender_id;
    a->id_pool      = id_pool;
    a->closed       = 0;

    a->current_index           = MMAPQ_INDEXED_APPENDER_NOT_INITIALISED;
    a->current_header_position = MMAPQ_INDEXED_APPENDER_NOT_INITIALISED;
    a->current_payload_position = MMAPQ_INDEXED_APPENDER_NOT_INITIALISED;

    /*
     * Open (or create) the queue meta file.
     */
    int rc = mmapqi_queue_meta_open_rw(directory, queue_name,
                                 start_index,
                                 region_size, max_file_size,
                                 archivable,
                                 QUEUE_TYPE_NORMAL,
                                 &a->queue_meta);
    if (rc != 0) {
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }

    /* Adopt contractual parameters from meta */
    int32_t reconciled_region_size   = queue_meta_region_size(a->queue_meta);
    int64_t reconciled_max_file_size = queue_meta_max_file_size(a->queue_meta);

    /* Open appender meta (creates file if new) */
    rc = mmapqi_appender_meta_open_rw(directory, queue_name, appender_id, &a->appender_meta);
    if (rc != 0) {
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }

    /* Create RW region accessors */
    rc = mmapqi_queue_region_accessor_create_rw(
        directory, queue_name, reconciled_region_size, ring_size,
        REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD,
        reconciled_max_file_size, timeout_ns, runtime, appender_id,
        &a->rw_acc);
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

    a->header_acc  = queue_region_accessor_header(a->rw_acc);
    a->payload_acc = mmapqi_queue_region_accessor_payload(a->rw_acc, appender_id);

    /* Create RO accessor for header search fallback */
    rc = mmapqi_queue_region_accessor_create_ro(
        directory, queue_name,
        reconciled_region_size, ring_size,
        0,
        reconciled_max_file_size, timeout_ns, runtime,
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

    /* Find where to resume appending */
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

/* ── Factory functions ───────────────────────────────────────────────────── */

int mmapq_indexed_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    int64_t     start_index,
    MmapqIndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    /* Acquire an ID from the pool — pool is opened here and owned by the appender */
    AppenderIdPool *pool = NULL;
    int rc = mmapqi_appender_id_pool_open(directory, queue_name, &pool);
    if (rc != 0) return rc;

    int id = mmapqi_appender_id_pool_acquire(pool);
    if (id < 0) {
        mmapqi_appender_id_pool_close(pool);
        return MMAPQ_EIDCONFLICT;
    }

    return appender_create_internal(directory, queue_name, region_size, ring_size,
                                      max_file_size, timeout_ns, archivable, runtime,
                                      pool, (uint8_t)id, start_index, out);
}

int mmapq_indexed_appender_create_with_id(
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
    MmapqIndexedAppender **out)
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

    return appender_create_internal(directory, queue_name, region_size, ring_size,
                                      max_file_size, timeout_ns, archivable, runtime,
                                      pool, appender_id, start_index, out);
}

/* ── mmapq_indexed_appender_append ─────────────────────────────────────────────── */

int64_t mmapq_indexed_appender_append(
    MmapqIndexedAppender *a,
    const void      *data,
    int32_t          length)
{
    if (a == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;

    int64_t payload_offset;
    if (write_payload(a, data, length, &payload_offset)) return -1;

    int64_t committed;
    if (commit_header(a, payload_offset, &committed)) return -1;
    return committed;
}

/* ── mmapq_indexed_appender_append_uncommitted ─────────────────────────────────── */

int64_t mmapq_indexed_appender_append_uncommitted(
    MmapqIndexedAppender *a,
    const void      *data,
    int32_t          length)
{
    if (a == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;

    int64_t payload_offset;
    if (write_payload(a, data, length, &payload_offset)) return -1;
    return payload_offset;
}

/* ── mmapq_indexed_appender_commit ─────────────────────────────────────────────── */

#ifdef MMAPQ_TEST_HOOKS
static void (*commit_pre_hook)(void);
void mmapq_test_hook_set_commit_pre(void (*fn)(void)) { commit_pre_hook = fn; }
#endif

int64_t mmapq_indexed_appender_commit(
    MmapqIndexedAppender *a,
    int64_t          payload_offset)
{
    if (a == NULL) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;
    if (payload_offset <= 0) return MMAPQ_EINVAL;

    /* Fast path — the payload this appender appended most recently (the
     * normal in-sequence case): no shared loads. Keyed on the last APPENDED
     * offset; "greater than the last committed" is not safe under
     * resequencing (a gap-fill payload below the last committed one would
     * skip the check entirely). */
    if (payload_offset != a->last_appended) {
        /* Slow path (gap fill). CAND-30: a payload overtaken by archiving
         * can never become live — its file may already be listed. */
        if (payload_offset < appender_meta_start_offset(a->appender_meta))
            return MMAPQ_EINVAL;
#ifdef MMAPQ_TEST_HOOKS
        if (commit_pre_hook) commit_pre_hook();
#endif
        int64_t committed;
        if (commit_header(a, payload_offset, &committed)) return -1;
        /* Race with an archive that published a higher boundary between the
         * pre-check and the CAS: the archiver stores start_offset, fences,
         * then reads headers; this commit stored its header, fences, then
         * reads start_offset — with seq_cst fences on both sides at least
         * one side observes the other, so one of them lowers the boundary
         * below this payload. */
        atomic_thread_fence(memory_order_seq_cst);
        if (payload_offset < appender_meta_start_offset(a->appender_meta))
            appender_meta_start_offset_min(a->appender_meta, payload_offset);
        return committed;
    }

    int64_t committed;
    if (commit_header(a, payload_offset, &committed)) return -1;
    return committed;
}

MmapqPayloadIterator *mmapq_indexed_appender_iterator(
    MmapqIndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset)
{
    if (a == NULL) return NULL;

    /* Reject out-of-range caller offsets at the boundary, before the
     * appender-owned iterator is created or touched. */
    int64_t sos = appender_start_of_stream(a);
    if ((start_offset != PAYLOAD_ITERATOR_NOT_SET && start_offset < sos) ||
        (end_offset   != PAYLOAD_ITERATOR_NOT_SET && end_offset   < sos))
        return NULL;

    if (a->payload_iter == NULL) {
        mmapqi_payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            0, /* live end-of-stream */
            &a->payload_iter);
    }

    if (mmapqi_payload_iterator_init(a->payload_iter, start_offset, end_offset) != 0)
        return NULL;
    return a->payload_iter;
}

MmapqUncommittedPayloadIterator *mmapq_indexed_appender_uncommitted_iterator(
    MmapqIndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset)
{
    if (a == NULL) return NULL;

    /* Create inner iterator with fixed end-of-stream */
    if (a->uncommitted_inner == NULL) {
        mmapqi_payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            1, /* fix end-of-stream at init time */
            &a->uncommitted_inner);
    }

    if (a->uncommitted_iter == NULL) {
        mmapqi_uncommitted_iterator_create(
            a->queue_name,
            a->appender_id,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            a->header_acc,
            a->uncommitted_inner,
            &a->uncommitted_iter);
    }

    if (mmapqi_uncommitted_iterator_init(a->uncommitted_iter, start_offset, end_offset) != 0)
        return NULL;
    return a->uncommitted_iter;
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

uint8_t mmapq_indexed_appender_id(const MmapqIndexedAppender *a)
{
    if (a == NULL) return 0;
    return a->appender_id;
}

int64_t mmapq_indexed_appender_current_index(const MmapqIndexedAppender *a)
{
    if (a == NULL) return 0;
    return a->current_index;
}

int64_t mmapq_indexed_appender_start_offset(const MmapqIndexedAppender *a)
{
    if (a == NULL) return 0;
    return appender_meta_start_offset(a->appender_meta);
}

/* ── mmapq_indexed_appender_close ──────────────────────────────────────────────── */

void mmapq_indexed_appender_close(MmapqIndexedAppender *a)
{
    if (a == NULL || a->closed) return;
    a->closed = 1;

    if (a->uncommitted_iter != NULL) {
        mmapqi_uncommitted_iterator_destroy(a->uncommitted_iter);
        a->uncommitted_iter = NULL;
    }
    if (a->uncommitted_inner != NULL) {
        mmapqi_payload_iterator_destroy(a->uncommitted_inner);
        a->uncommitted_inner = NULL;
    }
    if (a->payload_iter != NULL) {
        mmapqi_payload_iterator_destroy(a->payload_iter);
        a->payload_iter = NULL;
    }

    if (a->appender_meta != NULL) {
        mmapqi_appender_meta_close(a->appender_meta);
        a->appender_meta = NULL;
    }

    if (a->queue_meta != NULL) {
        mmapqi_queue_meta_close(a->queue_meta);
        a->queue_meta = NULL;
    }

    if (a->rw_acc != NULL) {
        mmapqi_queue_region_accessor_close(a->rw_acc);
        a->rw_acc = NULL;
    }

    if (a->ro_acc != NULL) {
        /* ro_header is owned by ro_acc — no separate close */
        mmapqi_queue_region_accessor_close(a->ro_acc);
        a->ro_acc    = NULL;
        a->ro_header = NULL;
    }

    if (a->id_pool != NULL) {
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        a->id_pool = NULL;
    }

    free(a->directory);
    free(a->queue_name);
    free(a);
}
