#include "indexed_batch_appender.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/appender_meta.h"
#include "internal/appender_id_pool.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/payload_iterator.h"
#include "internal/payload_walk.h"
#include "internal/offset_set.h"
#include "internal/uncommitted_batch_iterator.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* Bounded linear scan to avoid livelock when the appender is far behind
 * the queue head. Falls back to a jump-based search for efficient catch-up. */
#define MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH  100

struct MmapqIndexedBatchAppender {
    char                *directory;
    char                *queue_name;
    uint8_t              appender_id;
    AppenderIdPool      *id_pool;
    AppenderMeta        *appender_meta;
    QueueMeta           *queue_meta;
    QueueRegionAccessor *rw_acc;
    RegionAccessor      *header_acc;
    RegionAccessor      *payload_acc;

    /* Read-only accessor for jump-based header search fallback */
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *ro_header;
    Buffer               ro_buf;

    int64_t current_index;
    int64_t current_header_position;
    int64_t current_payload_position;

    Buffer header_buf;
    Buffer payload_buf;

    MmapqMutableBatch         current_batch;
    MmapqBatchPayloadIterator batch_iterator;

    int closed;

    /* CAND-49 (§3.3/§3.5/§3.7) — at the END of the struct so existing field
     * offsets (and the hot paths' object code) are unchanged:
     *  - outstanding: the commit_batch validation gate; complete_uncommitted
     *    and the uncommitted-batches walker (refill) add batch offsets.
     *  - batch_state: NONE / OPEN (after start_batch) / ORPHANED (set ONLY by
     *    discard's pre-store failure; start_batch recovers it, §3.7).
     *  - join completion: a trailing open batch completed to staged at rejoin
     *    or by the start_batch orphan recovery (§3.7), read-and-clear via
     *    mmapq_indexed_batch_appender_join_completed_open_batch().
     *  - uncommitted_batches: the §3.6 walker, created lazily. */
    OffsetSet            outstanding;
    int                  batch_state;
    int                  join_completed_flag;
    int64_t              join_completed_offset;
    MmapqUncommittedBatchIterator *uncommitted_batches;
};

/* §3.7 batch state machine values (batch_state). */
#define BATCH_STATE_NONE      0
#define BATCH_STATE_OPEN      1
#define BATCH_STATE_ORPHANED  2

/* ── Internal Helpers ────────────────────────────────────────────────────── */

static int wrap_payload_or_fail(MmapqIndexedBatchAppender *a, int64_t position)
{
    WrapResult r = region_accessor_wrap(a->payload_acc, position, &a->payload_buf);
    if (r != WRAP_SUCCESS) return -1;
    return 0;
}

static int ensure_space_for_batch_header(MmapqIndexedBatchAppender *a)
{
    if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    if (a->payload_buf.capacity < MMAPQ_BATCH_MSG_COUNT_SIZE) {
        a->current_payload_position += a->payload_buf.capacity;
        if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    }
    return 0;
}

/* always_inline: the CAND-45 rationale (a helper flipping to outlined is a
 * silent hot-path change). The CAND-51c signal fence in the append bodies
 * flipped this helper to outlined (append_reserved 60.0 → 70.0 Ir/msg);
 * the attribute restores the shipped shape. */
static __attribute__((always_inline)) inline int write_padding_if_needed(MmapqIndexedBatchAppender *a, int32_t length)
{
    if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    int32_t cap = a->payload_buf.capacity;
    int32_t total_needed = length + PAYLOAD_LENGTH_SIZE;

    if (cap >= total_needed) return 0;

    /* Write long padding if there is space for it */
    if (cap > PAYLOAD_LENGTH_SIZE) {
        int32_t pad = PAYLOAD_LENGTH_SIZE - cap;
        memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &pad, sizeof(int32_t));
    }

    a->current_payload_position += cap;
    if (wrap_payload_or_fail(a, a->current_payload_position)) return -1;
    return 0;
}

/* §3.7 join completion of a trailing open batch (D-E fix), shared by rejoin
 * and the start_batch ORPHANED recovery. Walks messages from just past the
 * count slot at `p` until a zero length (end of stream), applying the
 * padding/region-skip rules (payload_walk; a misfit positive length is
 * external corruption — ECORRUPT, never a crash artifact: append rejects
 * length > region_size − 4 before storing anything and pads first, so a
 * crash can only leave a length that fits).
 *
 * If at least one message was walked: the count is stored into the mapped
 * region as the LAST step (idempotent if interrupted), the position moves
 * to EOS, join_completed_open_batch is set with the batch offset, and the
 * offset joins the outstanding set (commit_batch works before any scan; a
 * later refill is an idempotent no-op). If the first length is already 0
 * (empty open batch): nothing is written; the position stays at `p` — the
 * next batch overwrites the slot. */
static int recover_open_batch(MmapqIndexedBatchAppender *a, int64_t p, int64_t *out_pos)
{
    int64_t probing = p + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int32_t walked = 0;
    while (1) {
        int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;
        int32_t len = mmapqi_payload_walk_advance(a->queue_name, &probing, &msg_off,
                                                  a->payload_acc, &a->payload_buf);
        if (len < 0) return len;   /* D8: timeout (retryable) vs corruption propagates */
        if (len == 0) break;       /* end of stream */
        walked++;
    }

    if (walked == 0) {
        *out_pos = p;
        return 0;
    }

    if (wrap_payload_or_fail(a, p) != 0) return -1;
    memcpy(a->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, &walked, sizeof(int32_t));
    a->join_completed_flag   = 1;
    a->join_completed_offset = p;
    (void)mmapqi_offset_set_add(&a->outstanding, p);  /* the standing spare: never allocates */
    *out_pos = probing;
    return 0;
}

/* Forward-only end-of-stream walk. probe_batch is NOT used: its single
 * INVALID return cannot distinguish a wrap failure from a count-0 record,
 * and the §3.7 join completion needs that distinction. The §3.8 boundary
 * padding-marker rule (D-F) applies at every count slot here. */
static int skip_to_end_of_stream(MmapqIndexedBatchAppender *a, int64_t offset,
                                 int64_t *out_end)
{
    int64_t end_of_stream = offset;
    while (1) {
        WrapResult wr = region_accessor_wrap(a->payload_acc, end_of_stream,
                                             &a->payload_buf);
        /* A position with no file yet, or beyond a file's end, is the end
         * of the stream; a timeout is transient (retryable) and corruption
         * is not — the D8 join contract, never collapsed. */
        if (wr == WRAP_FILE_NOT_FOUND || wr == WRAP_BEYOND_FILE_SIZE) break;
        if (wr == WRAP_MAP_TIMED_OUT) return MMAPQ_ETIMEDOUT;
        if (wr != WRAP_SUCCESS) return MMAPQ_ECORRUPT;

        int32_t cap = a->payload_buf.capacity;
        /* Batch headers never occupy a < 4-byte region tail
         * (ensure_space_for_batch_header rule) — skip to the next region. */
        if (cap < MMAPQ_BATCH_MSG_COUNT_SIZE) {
            end_of_stream += cap;
            continue;
        }

        int32_t count;
        memcpy(&count, a->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));

        if (count < 0) {
            /* §3.8 boundary padding marker (D-F): legal region-skip iff its
             * extent is exactly the rest of the region, else ECORRUPT. */
            int64_t skip = -(int64_t)count + MMAPQ_BATCH_MSG_COUNT_SIZE;
            if (skip != cap) return MMAPQ_ECORRUPT;
            end_of_stream += cap;
            continue;
        }

        if (count == 0) {
            /* Trailing open batch (a crash between start_batch and
             * complete) — complete it to staged (§3.7). */
            int rc = recover_open_batch(a, end_of_stream, &end_of_stream);
            if (rc != 0) return rc;
            break;
        }

        /* A real batch: walk past it. A partial scan is not an end of
         * stream. Accepting it would place the append position inside
         * committed data (short) or past it in unreachable space (long).
         * Matches indexed_batch_archiver.c and indexed_appender.c, both of
         * which refuse here. */
        mmapq_batch_iterator_init(&a->batch_iterator, end_of_stream, count, 0);
        mmapq_batch_iterator_exhaust(&a->batch_iterator);
        if (a->batch_iterator.last_error != 0) return a->batch_iterator.last_error;
        end_of_stream = a->batch_iterator.next_message_position;
    }
    *out_end = end_of_stream;
    return 0;
}

static int64_t appender_start_index_from_meta(void *ctx)
{
    return queue_meta_start_index((const QueueMeta *)ctx);
}

/* ── Uncommitted-batches walker callbacks (CAND-49 §3.6) ─────────────────── */

static int64_t batch_appender_start_of_stream(void *ctx)
{
    MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)ctx;
    return appender_meta_start_offset(a->appender_meta);
}

static int64_t batch_appender_end_of_stream(void *ctx)
{
    MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)ctx;
    return a->current_payload_position;
}

/* Archive line supplier (CAND-49 §3.9). */
static int64_t batch_appender_archive_offset(void *ctx)
{
    MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)ctx;
    return appender_meta_archive_offset(a->appender_meta);
}

/* Refill helper (§3.3): make a walker-yielded batch offset committable.
 * Batch has no flag-tracked tail — every staged batch is a set member. */
static int batch_appender_refill(void *ctx, int64_t off)
{
    MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)ctx;
    return mmapqi_offset_set_add(&a->outstanding, off);
}

static int initialise_append_position(MmapqIndexedBatchAppender *a, int64_t start_index)
{
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
        a->current_index = start_index;
        /* No live entry possible — skip from start_offset to the end of the
         * stream rather than writing at it (CAND-30) */
        int rc_skip = skip_to_end_of_stream(a,
            appender_meta_start_offset(a->appender_meta),
            &a->current_payload_position);
        if (rc_skip != 0) return rc_skip;
    } else {
        a->current_index = last_index + 1;
        int64_t committed_payload_pos = -1;
        for (int64_t i = last_index; i >= scan_floor; i--) {
            uint64_t hdr = mmapqi_header_read(ro_header, ro_buf, i, a->queue_name);
            if (hdr != HEADER_NULL && header_appender_id(hdr) == a->appender_id) {
                committed_payload_pos = header_payload_position(hdr);
                break;
            }
        }

        if (committed_payload_pos != -1) {
            int rc_scan = skip_to_end_of_stream(a, committed_payload_pos,
                                                &a->current_payload_position);
            if (rc_scan != 0) return rc_scan;
        } else {
            /* No committed entry in the live range for this appender —
             * skip forward from start_offset to the end of the stream */
            int rc_skip = skip_to_end_of_stream(a,
                appender_meta_start_offset(a->appender_meta),
                &a->current_payload_position);
            if (rc_skip != 0) return rc_skip;
        }
    }

    a->current_header_position = header_position(a->current_index);

    /* Eager wrap */
    if (region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf) != WRAP_SUCCESS) return -1;
    if (region_accessor_wrap(a->payload_acc, a->current_payload_position, &a->payload_buf) != WRAP_SUCCESS) return -1;

    return 0;
}


/* ── Factory ─────────────────────────────────────────────────────────────── */

static int batch_appender_create_internal(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    AppenderIdPool *pool,
    uint8_t     appender_id,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;

    MmapqIndexedBatchAppender *a = calloc(1, sizeof(MmapqIndexedBatchAppender));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory    = strdup(directory);
    if (a->directory == NULL) { free(a); return MMAPQ_ENOMEM; }
    a->queue_name   = strdup(queue_name);
    if (a->queue_name == NULL) { free(a->directory); free(a); return MMAPQ_ENOMEM; }
    a->id_pool      = pool;
    a->appender_id  = appender_id;

    /* CAND-49 §3.3: the outstanding-offset set is allocated at create
     * (initial capacity 16 slots = 15 + the standing spare), so the
     * deferred-path adds never allocate. Pure memory — unwind before the
     * queue meta open keeps the existing error paths unchanged. */
    mmapqi_offset_set_init(&a->outstanding);
    if (mmapqi_offset_set_reserve(&a->outstanding, OFFSET_SET_INITIAL_CAPACITY - 1) != 0) {
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return MMAPQ_ENOMEM;
    }

    int rc = mmapqi_queue_meta_open_rw(directory, queue_name, start_index, region_size, max_file_size, archivable, QUEUE_TYPE_BATCH, &a->queue_meta);
    if (rc != 0) {
        mmapqi_offset_set_destroy(&a->outstanding);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    int32_t rec_reg = queue_meta_region_size(a->queue_meta);
    int64_t rec_max = queue_meta_max_file_size(a->queue_meta);

    rc = mmapqi_appender_meta_open_rw(directory, queue_name, a->appender_id, &a->appender_meta);
    if (rc != 0) {
        mmapqi_offset_set_destroy(&a->outstanding);
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    rc = mmapqi_queue_region_accessor_create_rw(directory, queue_name, rec_reg, ring_size, 1, rec_max, timeout_ns, runtime, a->appender_id, &a->rw_acc);
    if (rc != 0) {
        mmapqi_offset_set_destroy(&a->outstanding);
        mmapqi_appender_meta_close(a->appender_meta);
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return rc;
    }
    a->header_acc = queue_region_accessor_header(a->rw_acc);
    a->payload_acc = mmapqi_queue_region_accessor_payload(a->rw_acc, a->appender_id);

    a->batch_iterator.queue_name  = a->queue_name;
    a->batch_iterator.payload_acc = a->payload_acc;

    rc = mmapqi_queue_region_accessor_create_ro(
        directory, queue_name,
        rec_reg, ring_size, 0,
        rec_max, timeout_ns, runtime,
        &a->ro_acc);
    if (rc != 0) {
        mmapqi_offset_set_destroy(&a->outstanding);
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

    int init_rc = initialise_append_position(a, start_index);
    if (init_rc != 0) {
        mmapqi_offset_set_destroy(&a->outstanding);
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

int mmapq_indexed_batch_appender_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *runtime,
    int64_t     start_index,
    MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    AppenderIdPool *pool = NULL;
    int rc = mmapqi_appender_id_pool_open(directory, queue_name, &pool);
    if (rc != 0) return rc;

    int id = mmapqi_appender_id_pool_acquire(pool);
    if (id < 0) {
        mmapqi_appender_id_pool_close(pool);
        return MMAPQ_EIDCONFLICT;
    }

    return batch_appender_create_internal(directory, queue_name, region_size,
        ring_size, max_file_size, timeout_ns, archivable, runtime,
        pool, (uint8_t)id, start_index, out);
}

int mmapq_indexed_batch_appender_create_with_id(
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
    MmapqIndexedBatchAppender **out)
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

    return batch_appender_create_internal(directory, queue_name, region_size,
        ring_size, max_file_size, timeout_ns, archivable, runtime,
        pool, appender_id, start_index, out);
}

/* ── Batch Operations ────────────────────────────────────────────────────── */

int mmapq_indexed_batch_appender_start_batch_checked(
    MmapqIndexedBatchAppender *appender, MmapqMutableBatch **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (appender == NULL) return MMAPQ_EINVAL;
    if (appender->closed) return MMAPQ_ECLOSED;

    /* §3.7 batch state machine: an OPEN batch means caller misuse (a C
     * caller dropped an open handle without discarding; Rust/Python cannot
     * reach this); an ORPHANED batch (discard's pre-store failure) is
     * recovered to staged first. */
    if (appender->batch_state == BATCH_STATE_OPEN) return MMAPQ_EINVAL;
    if (appender->batch_state == BATCH_STATE_ORPHANED) {
        int64_t new_pos;
        int rc = recover_open_batch(appender, appender->current_batch.batch_offset,
                                    &new_pos);
        if (rc != 0) return rc;   /* starts nothing */
        appender->current_payload_position = new_pos;
        appender->batch_state = BATCH_STATE_NONE;
    }

    int result = ensure_space_for_batch_header(appender);
    if (result != 0) return result;
    appender->current_batch.appender = appender;
    appender->current_batch.batch_offset = appender->current_payload_position;
    appender->current_batch.message_count = 0;
    appender->current_batch.consumed = 0;
    appender->current_payload_position += MMAPQ_BATCH_MSG_COUNT_SIZE;
    appender->batch_state = BATCH_STATE_OPEN;
    *out = &appender->current_batch;
    return 0;
}

MmapqMutableBatch *mmapq_indexed_batch_appender_start_batch(MmapqIndexedBatchAppender *a)
{
    MmapqMutableBatch *batch = NULL;
    mmapq_indexed_batch_appender_start_batch_checked(a, &batch);
    return batch;
}

static int move_to_last_header(MmapqIndexedBatchAppender *a)
{
    int attempts = 0;
    while (attempts < MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH) {
        attempts++;
        a->current_index++;
        a->current_header_position = header_position(a->current_index);
        WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
        if (r != WRAP_SUCCESS) return -1;

        _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
        if (atomic_load_explicit(hp, memory_order_acquire) == HEADER_NULL) return 0;
    }

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

    WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    return 0;
}

#ifdef MMAPQ_TEST_HOOKS
static void (*batch_commit_pre_publish_hook)(void);
void mmapq_test_hook_set_batch_commit_pre_publish(void (*fn)(void)) { batch_commit_pre_publish_hook = fn; }
#endif

/* batch_publish — the encode + wrap + CAS + move_to_last_header body,
 * extracted verbatim from the pre-reshape commit_batch (incl. the 56-bit
 * ECORRUPT guard). Shared by the complete() hot path (membership by
 * construction — never touches the set on the success path) and by the
 * validated commit_batch(). */
static int64_t batch_publish(MmapqIndexedBatchAppender *a, int64_t batch_offset)
{
    /* Encoded positions are STARTS — limited to the 56-bit field (2^56−1),
     * one tighter than the start_offset open check (CAND-32 spec §2). */
    if (batch_offset < INITIAL_PAYLOAD_POSITION ||
        batch_offset > (int64_t)HEADER_PAYLOAD_MASK)
        return MMAPQ_ECORRUPT;

    uint64_t header = header_encode(a->appender_id, batch_offset);
    WrapResult r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
    if (r != WRAP_SUCCESS) return -1;

    _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
    uint64_t expected = HEADER_NULL;

    while (__builtin_expect(!atomic_compare_exchange_strong_explicit(hp, &expected, header, memory_order_acq_rel, memory_order_acquire), 0)) {
        if (move_to_last_header(a)) return -1;
        r = region_accessor_wrap(a->header_acc, a->current_header_position, &a->header_buf);
        if (r != WRAP_SUCCESS) return -1;
        hp = (_Atomic uint64_t *)a->header_buf.data;
        expected = HEADER_NULL;
    }

    int64_t idx = a->current_index;
    a->current_index++;
    a->current_header_position = header_position(a->current_index);
    return idx;
}

/* Shared finalize step: store the count into the mapped region. The count
 * store is what consumes the handle (§3.5) and PRECEDES any header CAS
 * (acq_rel) — invariant. Returns 0 with the handle consumed and the
 * appender state back to NONE; pre-store failures leave the handle open. */
static int batch_store_count(MmapqMutableBatch *b)
{
    MmapqIndexedBatchAppender *a = b->appender;
    if (wrap_payload_or_fail(a, b->batch_offset) != 0) return -1;
    memcpy(a->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, &b->message_count, sizeof(int32_t));
    b->consumed = 1;
    a->batch_state = BATCH_STATE_NONE;
    return 0;
}

int64_t mmapq_mutable_batch_complete(MmapqMutableBatch *b)
{
    if (b == NULL) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (a->closed) return MMAPQ_ECLOSED;
    /* Consumed handle / empty batch: EINVAL and the handle STAYS OPEN
     * (an empty complete publishes nothing — discard instead). */
    if (b->consumed) return MMAPQ_EINVAL;
    if (b->message_count == 0) return MMAPQ_EINVAL;

    if (batch_store_count(b) != 0) return -1;

    /* complete = finalize + PUBLISH (CAND-49 §3.5). */
    int64_t idx = batch_publish(a, b->batch_offset);
    if (idx < 0) {
        /* Publish failed after the count store: the batch is staged and its
         * offset joins the outstanding set immediately — the spare-slot
         * invariant (§3.3) guarantees this add needs no allocation, so
         * complete performs no reserve and no set work on its success
         * path. */
        (void)mmapqi_offset_set_add(&a->outstanding, b->batch_offset);
    }
    return idx;
}

int64_t mmapq_mutable_batch_complete_uncommitted(MmapqMutableBatch *b)
{
    if (b == NULL) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (a->closed) return MMAPQ_ECLOSED;
    if (b->consumed) return MMAPQ_EINVAL;
    if (b->message_count == 0) return MMAPQ_EINVAL;   /* handle stays open */

    /* Stage (CAND-49 §3.5): ensure the set's spare slot BEFORE the count
     * store — ENOMEM means nothing is stored and the handle stays open
     * (ruling 5). One compare when the spare exists (the normal case). */
    if (mmapqi_offset_set_reserve(&a->outstanding, 1) != 0) return MMAPQ_ENOMEM;

    if (batch_store_count(b) != 0) return -1;

    (void)mmapqi_offset_set_add(&a->outstanding, b->batch_offset);  /* spare ensured */
    return b->batch_offset;
}

int mmapq_mutable_batch_discard(MmapqMutableBatch *b)
{
    if (b == NULL) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (a->closed) return MMAPQ_ECLOSED;
    if (b->consumed) return MMAPQ_EINVAL;

    if (b->message_count > 0) {
        /* Option A (owner ruling 8): store the count — the batch becomes a
         * staged-record-that-is-never-committed. The position is already
         * past it: NO reset, NO set add — in-session commit_batch of its
         * offset returns EINVAL; discard is final. After a restart the
         * walker reports it and refill makes it committable; the app
         * decides. */
        if (batch_store_count(b) != 0) {
            /* Pre-store failure: the handle stays OPEN-ORPHANED (§3.7) —
             * the next start_batch recovers it to staged. */
            a->batch_state = BATCH_STATE_ORPHANED;
            return -1;
        }
        return 0;
    }

    /* Empty batch: nothing but the untouched count slot lies between —
     * reset the write position to the batch start; the next batch
     * overwrites the slot. */
    a->current_payload_position = b->batch_offset;
    b->consumed = 1;
    a->batch_state = BATCH_STATE_NONE;
    return 0;
}

int64_t mmapq_indexed_batch_appender_commit_batch(MmapqIndexedBatchAppender *a, int64_t batch_offset)
{
    if (a == NULL) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;

    /* Membership is the gate (CAND-49 §3.5; supersedes the CAND-32 C3 line,
     * which is removed): double commit, stale-after-discard,
     * never-completed, above-last-started are all non-members. Members are
     * staged batches (complete_uncommitted / a failed publish in complete /
     * join completion / a walker refill). */
    if (!mmapqi_offset_set_contains(&a->outstanding, batch_offset))
        return MMAPQ_EINVAL;

    /* A batch overtaken by archiving can never become live (CAND-30 batch
     * analogue). The most recently started batch is exempt: it is the
     * lane's tail — every committed batch sits below it, so the archiver's
     * boundary can never reach it (the §3.4 fast-path proof's batch form).
     * A batch commit racing an archive past the check is caught by the
     * re-check below (CAND-51b F-REC-1 — the CAND-30 protocol's batch form,
     * retiring the CAND-49 §4-item-5 carve-out); commit_batch is the cold
     * path (§3.5), so complete and batch_publish stay fence-free. */
    if (batch_offset != a->current_batch.batch_offset &&
        batch_offset < appender_meta_start_offset(a->appender_meta)) {
        mmapqi_offset_set_remove(&a->outstanding, batch_offset);
        return MMAPQ_EINVAL;   /* overtaken — final */
    }

#ifdef MMAPQ_TEST_HOOKS
    /* CAND-51b pin: park between the overtaken pre-check and the publish */
    if (batch_commit_pre_publish_hook) batch_commit_pre_publish_hook();
#endif
    int64_t idx = batch_publish(a, batch_offset);
    if (idx < 0) return idx;   /* publish failure — member retained, retryable */
    /* Race with an archive that published a higher boundary between the
     * pre-check and the CAS: the archiver stores start_offset, fences, then
     * reads headers; this commit stored its header, fences, then reads
     * start_offset — with seq_cst fences on both sides at least one side
     * observes the other, so one of them lowers the boundary below this
     * batch (the CAND-30 store-buffering argument, batch form). Lowers
     * start_offset only; never archive_offset (CAND-49 §3.9). */
    atomic_thread_fence(memory_order_seq_cst);
    if (batch_offset < appender_meta_start_offset(a->appender_meta))
        appender_meta_start_offset_min(a->appender_meta, batch_offset);
    mmapqi_offset_set_remove(&a->outstanding, batch_offset);
    return idx;
}

int mmapq_indexed_batch_appender_join_completed_open_batch(
    MmapqIndexedBatchAppender *a, int64_t *offset)
{
    if (a == NULL || offset == NULL) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;
    /* Read-and-clear, once per join-completion event. The batch was
     * completed by the queue, not the app: it may lack messages the app
     * intended; the last counted message may have partial content. */
    if (!a->join_completed_flag) return 0;
    *offset = a->join_completed_offset;
    a->join_completed_flag = 0;
    return 1;
}

int64_t mmapq_indexed_batch_appender_append_and_commit(MmapqIndexedBatchAppender *a, const void *data, int32_t length)
{
    /* CAND-51a (W): validate BEFORE start_batch — a rejected call leaves no
     * open batch behind. Any later append failure discards (rc ignored —
     * best-effort rollback). The checked start preserves the failure code
     * (ECLOSED/EINVAL/ENOMEM/timeout) — never the -1 collapse. */
    if (a == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
    MmapqMutableBatch *b = NULL;
    int started = mmapq_indexed_batch_appender_start_batch_checked(a, &b);
    if (started != 0) return started;
    int append_rc = mmapq_mutable_batch_append(b, data, length);
    if (append_rc < 0) { (void)mmapq_mutable_batch_discard(b); return append_rc; }
    return mmapq_mutable_batch_complete(b);   /* complete = publish (§3.5) */
}

/* ── Uncommitted batches (CAND-49 §3.6/§5.2) ─────────────────────────────── */

int mmapq_indexed_batch_appender_uncommitted_batches_checked(
    MmapqIndexedBatchAppender *a,
    MmapqUncommittedBatchIterator **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (a == NULL) return MMAPQ_EINVAL;

    if (a->uncommitted_batches == NULL) {
        int rc = mmapqi_uncommitted_batch_iterator_create(
            a->queue_name,
            a->appender_id,
            batch_appender_start_of_stream,
            batch_appender_end_of_stream,
            a,
            batch_appender_archive_offset,   /* §3.9: the archive line */
            a->header_acc,
            a->payload_acc,
            appender_start_index_from_meta,  /* D8/S3: header scan floor */
            a->queue_meta,
            batch_appender_refill,           /* §3.3: refill on yield */
            a,
            &a->uncommitted_batches);
        if (rc != 0) return rc;
    }

    int rc = mmapqi_uncommitted_batch_iterator_init(a->uncommitted_batches);
    if (rc != 0) return rc;
    *out = a->uncommitted_batches;
    return 0;
}

int mmapq_uncommitted_batch_next(
    MmapqUncommittedBatchIterator *it,
    int64_t     *batch_offset,
    int32_t     *message_count,
    MmapqBatchPayloadIterator **msgs)
{
    if (it == NULL || batch_offset == NULL || message_count == NULL || msgs == NULL)
        return MMAPQ_EINVAL;

    int r = mmapqi_uncommitted_batch_iterator_next(it, batch_offset, message_count, msgs);
    if (r != 1) {
        /* Exhausted (0) or a negative MMAPQ_E* — outputs cleared either way. */
        *batch_offset  = PAYLOAD_ITERATOR_NOT_SET;
        *message_count = 0;
        *msgs          = NULL;
        return r;
    }
    return 1;
}

int32_t mmapq_mutable_batch_append(MmapqMutableBatch *b, const void *data, int32_t length)
{
    if (b == NULL || data == NULL || length <= 0) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
    if (write_padding_if_needed(a, length)) return -1;
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    /* Length store precedes payload bytes in program order — the
     * no-leftover argument (CAND-49 §3.7, CAND-51c §0; the twin comment in
     * indexed_appender.c has the full text). Compiler barrier only — no
     * instructions emitted; cross-observer visibility is carried by the
     * header CAS at publish. */
    atomic_signal_fence(memory_order_release);
    memcpy(a->payload_buf.data + PAYLOAD_DATA_OFFSET, data, (size_t)length);
    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;
    return b->message_count++;
}

int32_t mmapq_mutable_batch_append_reserved(MmapqMutableBatch *b, int32_t length, void **out_data)
{
    if (b == NULL || out_data == NULL || length <= 0) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    if (length > queue_meta_region_size(a->queue_meta) - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
    if (write_padding_if_needed(a, length)) return -1;
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    /* Length store precedes the (caller-written) payload bytes in program
     * order — the no-leftover argument (CAND-49 §3.7, CAND-51c §0; the
     * twin comment in indexed_appender.c has the full text). Compiler
     * barrier only — no instructions emitted. */
    atomic_signal_fence(memory_order_release);
    *out_data = a->payload_buf.data + PAYLOAD_DATA_OFFSET;
    a->current_payload_position += PAYLOAD_LENGTH_SIZE + length;
    return b->message_count++;
}

int32_t mmapq_mutable_batch_append_remaining_from(MmapqMutableBatch *b, MmapqBatchPayloadIterator *it)
{
    if (b == NULL || it == NULL) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    int32_t region_sz = queue_meta_region_size(a->queue_meta);
    int32_t count = 0;
    while (mmapq_batch_iterator_advance(it)) {
        int32_t msg_len = batch_iterator_current_length(it);
        if (msg_len > region_sz - PAYLOAD_LENGTH_SIZE) return MMAPQ_EINVAL;
        int rc = mmapq_mutable_batch_append(b, batch_iterator_current_data(it), msg_len);
        if (rc < 0) return rc;
        count++;
    }
    return it->last_error != 0 ? it->last_error : count;
}

int32_t mmapq_mutable_batch_message_count(const MmapqMutableBatch *b) { return b->message_count; }

const void *mmapq_mutable_batch_get(MmapqMutableBatch *b, int32_t message_index, int32_t *out_length)
{
    if (b == NULL || out_length == NULL) return NULL;
    MmapqIndexedBatchAppender *a = b->appender;
    mmapq_batch_iterator_init(&a->batch_iterator, b->batch_offset, b->message_count, message_index);
    if (mmapq_batch_iterator_advance(&a->batch_iterator)) {
        *out_length = a->batch_iterator.current_msg_length;
        return batch_iterator_current_data(&a->batch_iterator);
    }
    return NULL;
}

MmapqBatchPayloadIterator *mmapq_mutable_batch_iterator(MmapqMutableBatch *b, int32_t start_message_index)
{
    MmapqIndexedBatchAppender *a = b->appender;
    return mmapq_batch_iterator_init(&a->batch_iterator, b->batch_offset, b->message_count, start_message_index);
}

uint8_t mmapq_indexed_batch_appender_id(const MmapqIndexedBatchAppender *a) { return a->appender_id; }

void mmapq_indexed_batch_appender_close(MmapqIndexedBatchAppender *a)
{
    if (!a || a->closed) return;
    a->closed = 1;
    mmapqi_offset_set_destroy(&a->outstanding);
    if (a->uncommitted_batches != NULL) {
        mmapqi_uncommitted_batch_iterator_destroy(a->uncommitted_batches);
        a->uncommitted_batches = NULL;
    }
    mmapqi_appender_meta_close(a->appender_meta);
    mmapqi_queue_meta_close(a->queue_meta);
    if (a->rw_acc) {
        mmapqi_queue_region_accessor_close(a->rw_acc);
        a->rw_acc = NULL;
    }
    if (a->ro_acc) {
        /* ro_header is owned by ro_acc — no separate close */
        mmapqi_queue_region_accessor_close(a->ro_acc);
        a->ro_acc    = NULL;
        a->ro_header = NULL;
    }
    mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
    mmapqi_appender_id_pool_close(a->id_pool);
    free(a->directory);
    free(a->queue_name);
    free(a);
}
