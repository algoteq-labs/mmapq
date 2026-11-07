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
#include "internal/walk_classify.h"
#include "internal/test_hooks.h"
#include "internal/test_reach.h"
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

    /* CAND-58 (spec §4.1): the walk accessor — RO + size-checked; the batch
     * twins of the single-message walk sites map through it (see the twin
     * comment in indexed_appender.c). */
    QueueRegionAccessor *walk_acc;
    RegionAccessor      *walk_header;
    RegionAccessor      *walk_payload;

    /* The floor that classified the join walk's current overtaken failure. */
    int64_t             walk_ovt_floor;

    int64_t current_index;
    int64_t current_header_position;
    int64_t current_payload_position;

    Buffer header_buf;
    Buffer payload_buf;

    MmapqMutableBatch         current_batch;
    /* batch_iterator is shared with the PUBLIC mutable-batch read path
     * (mmapq_mutable_batch_iterator) — it stays on the RW payload accessor.
     * walk_batch_iterator is the CAND-58 join walk's own (walk accessor +
     * walk_mode). */
    MmapqBatchPayloadIterator batch_iterator;
    MmapqBatchPayloadIterator walk_batch_iterator;

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

    /* CAND-54: the queue's on-disk archivable flag, cached at open from
     * the queue meta — NEVER the factory argument (see the single-message
     * twin in indexed_appender.c). */
    int                  archivable;

    /* APPEND-HOTPATH-01 R2: region_size − PAYLOAD_LENGTH_SIZE, cached once
     * at create from the reconciled meta — see the single-message twin in
     * indexed_appender.c (region_size is immutable after creation; the
     * struct-end placement follows the CAND-49 offset discipline). */
    int32_t              max_message_length;
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
    /* CAND-51e unified tail rule — one rule for every item: with <= 4
     * bytes left in a region, skip to the next region. A message keeps its
     * length and payload in one region; a batch count could fit in exactly
     * 4 bytes but deliberately does not use them — four zero bytes at a
     * region end must mean only padding, because an open batch's count
     * slot is also zero and recovery could not tell them apart (the rejoin
     * wedge found by the crash-composition suite). */
    if (a->payload_buf.capacity <= MMAPQ_BATCH_MSG_COUNT_SIZE) {
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

#ifdef MMAPQ_TEST_HOOKS
/* CAND-51e P8: fires per walked message in recover_open_batch below, before
 * the count store — the mid-join-completion kill point. Declared here, not
 * with the other batch hooks, because the call site precedes them. */
static void (*batch_recover_walk_step_hook)(int);
void mmapq_test_hook_set_batch_recover_walk_step(void (*fn)(int)) { batch_recover_walk_step_hook = fn; }
#endif

/* CAND-58: the classification's payload floor — the archive line re-read
 * seq_cst AFTER a walk failure (spec §4.2; never start_offset, L(iv)). */
static int64_t batch_appender_archive_offset_sc(void *ctx)
{
    MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)ctx;
    return appender_meta_archive_offset_sc(a->appender_meta);
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
    /* CAND-58: the message walk maps through the walk accessor (RO,
     * size-checked); a map miss classifies — below the re-read line →
     * MMAPQI_WALK_RESTART (the open batch is uncommittable there, L(i));
     * at/above → EOS (the §4.5 hand-deleted-staged limitation, unchanged
     * from today) — the count slot at p is known-written, the messages past
     * it are not. The count store below stays RW (the append path). */
    int64_t probing = p + MMAPQ_BATCH_MSG_COUNT_SIZE;
    int32_t walked = 0;
    while (1) {
        int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;
        C58_HOOK_JOIN_WALK_STEP(probing);
        int32_t len = mmapqi_payload_walk_advance(a->queue_name, &probing, &msg_off,
                                                  a->walk_payload, &a->payload_buf);
        if (len == MMAPQI_WALK_MAP_MISS) {
            int64_t f;
            MmapqiWalkClass c = mmapqi_walk_classify(probing,
                                                     p + MMAPQ_BATCH_MSG_COUNT_SIZE,
                                                     batch_appender_archive_offset_sc,
                                                     a, &f);
            if (c == MMAPQI_WALK_OVERTAKEN) {
                a->walk_ovt_floor = f;
                return MMAPQI_WALK_RESTART;
            }
            if (c == MMAPQI_WALK_CORRUPT) return MMAPQ_ECORRUPT;
            break;   /* EOS: the frontier */
        }
        if (len < 0) return len;   /* D8: timeout (retryable) vs corruption propagates */
        if (len == 0) break;       /* end of stream */
        walked++;
#ifdef MMAPQ_TEST_HOOKS
        /* CAND-51e P8: mid-completion kill point — after the s-th walked
         * message, before the count store (the store stays the last step). */
        if (batch_recover_walk_step_hook) batch_recover_walk_step_hook(walked);
#endif
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
/* CAND-58: the batch join walk's classification of a map miss at
 * `probing` — below the re-read line → MMAPQI_WALK_RESTART; at/above → EOS
 * or, at a known-written position (a committed batch's span start), ECORRUPT. */
static int batch_join_walk_classify(MmapqIndexedBatchAppender *a, int64_t probing,
                                    int64_t known_written_end)
{
    int64_t f;
    MmapqiWalkClass c = mmapqi_walk_classify(probing, known_written_end,
                                             batch_appender_archive_offset_sc, a, &f);
    if (c == MMAPQI_WALK_OVERTAKEN) {
        a->walk_ovt_floor = f;
        return MMAPQI_WALK_RESTART;
    }
    if (c == MMAPQI_WALK_EOS) return 0;
    return MMAPQ_ECORRUPT;
}

static int skip_to_end_of_stream(MmapqIndexedBatchAppender *a, int64_t offset,
                                 int64_t known_written_end, int64_t *out_end)
{
    /* CAND-58: maps through the walk accessor (RO, size-checked) — never
     * creates; a position with no file yet, or beyond a file's end, is no
     * longer automatically the end of the stream: it classifies first.
     * known_written_end: offset + 1 when walking from a committed batch
     * (its start is known-written — a miss there at/above the line is
     * ECORRUPT); offset from the join anchor (a fresh anchor's file may
     * legitimately not exist — EOS). */
    int64_t end_of_stream = offset;
    while (1) {
        C58_HOOK_JOIN_WALK_STEP(end_of_stream);
        WrapResult wr = region_accessor_wrap(a->walk_payload, end_of_stream,
                                             &a->payload_buf);
        if (wr == WRAP_FILE_NOT_FOUND || wr == WRAP_BEYOND_FILE_SIZE) {
            int crc = batch_join_walk_classify(a, end_of_stream, known_written_end);
            if (crc == MMAPQI_WALK_RESTART) return MMAPQI_WALK_RESTART;
            if (crc != 0) return crc;
            break;   /* EOS: the frontier (positions past `offset` are past
                      * everything this walk knows written) */
        }
        if (wr == WRAP_MAP_TIMED_OUT) return MMAPQ_ETIMEDOUT;
        if (wr != WRAP_SUCCESS) return MMAPQ_ECORRUPT;

        int32_t cap = a->payload_buf.capacity;
        /* CAND-51e unified tail rule: <= 4 bytes left means padding —
         * skip to the next region (the ensure_space_for_batch_header
         * rule; the batch count slot never occupies a region tail). */
        if (cap <= MMAPQ_BATCH_MSG_COUNT_SIZE) {
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
        mmapq_batch_iterator_init(&a->walk_batch_iterator, end_of_stream, count, 0);
        mmapq_batch_iterator_exhaust(&a->walk_batch_iterator);
        /* CAND-58: a map miss walking the committed batch classifies — its
         * span start is known-written (header evidence): below the line →
         * the join restart; at/above → ECORRUPT. */
        if (a->walk_batch_iterator.last_error == MMAPQI_WALK_MAP_MISS) {
            int crc = batch_join_walk_classify(a, end_of_stream, end_of_stream + 1);
            if (crc != 0) return crc;
            return MMAPQ_ECORRUPT;   /* EOS is impossible: the span start is
                                      * known-written, so the miss classified
                                      * CORRUPT */
        }
        if (a->walk_batch_iterator.last_error != 0) return a->walk_batch_iterator.last_error;
        end_of_stream = a->walk_batch_iterator.next_message_position;
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

/* CAND-59 (spec v1.1 §3.2/§3.3): forward declarations — the join's eager
 * wrap and the create-time floor-check install precede the definitions
 * (which sit above batch_publish, per the spec's placement). */
static int commit_header_below_floor(void *ctx, int64_t position);
static __attribute__((always_inline)) inline int
header_wrap_frontier(MmapqIndexedBatchAppender *a);

/* walk_floor (CAND-58 §4.4): 0 on the first pass; on a restart, the floor
 * that classified the failure — the restart-floor rule (see the
 * single-message twin in indexed_appender.c). The walks map through the
 * walk accessor; a map miss classifies via batch_join_walk_classify —
 * OVERTAKEN returns MMAPQI_WALK_RESTART to the create-time restart loop. */
static int initialise_append_position(MmapqIndexedBatchAppender *a, int64_t start_index,
                                      int64_t walk_floor)
{
    /* CAND-54 ruling 2: the join clears a stale commit intent (see the
     * single-message twin in indexed_appender.c). */
    appender_meta_intent_clear(a->appender_meta);

    RegionAccessor *ro_header = a->ro_header;
    Buffer         *ro_buf   = &a->ro_buf;

    int64_t last_index = mmapqi_header_search_last_index(
        start_index,
        appender_start_index_from_meta, a->queue_meta,
        ro_header, ro_buf,
        a->queue_name);

    /* I5: never read headers below start_index — those files may have been
     * deleted (P12). One acquire load; a concurrent archive advancing it
     * further is covered by the deletion protocol (doc/design/archiving-design.md). */
    int64_t scan_floor = queue_meta_start_index(a->queue_meta);

    if (last_index == HEADER_NULL_INDEX) {
        a->current_index = start_index;
        /* No live entry possible — skip from the join anchor to the end
         * of the stream rather than writing at it (CAND-30; CAND-54
         * L(ii)) */
        int64_t anchor = appender_join_anchor(a->appender_meta);
        if (anchor < walk_floor) anchor = walk_floor;   /* the restart floor */
        int rc_skip = skip_to_end_of_stream(a, anchor, anchor,
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
            /* The restart-floor rule: when the last live header sits below
             * the classifying floor, its batch is archived (the line never
             * moves back) — walk from the floor instead. */
            int64_t start = committed_payload_pos < walk_floor
                            ? walk_floor : committed_payload_pos;
            int rc_scan = skip_to_end_of_stream(a, start,
                /* known-written iff walking from the committed batch itself */
                start == committed_payload_pos ? start + 1 : start,
                &a->current_payload_position);
            if (rc_scan != 0) return rc_scan;
        } else {
            /* No committed entry in the live range for this appender —
             * skip forward from the join anchor (CAND-54 L(ii)) to the
             * end of the stream */
            int64_t anchor = appender_join_anchor(a->appender_meta);
            if (anchor < walk_floor) anchor = walk_floor;   /* the restart floor */
            int rc_skip = skip_to_end_of_stream(a, anchor, anchor,
                &a->current_payload_position);
            if (rc_skip != 0) return rc_skip;
        }
    }

    a->current_header_position = header_position(a->current_index);

    /* Eager wrap */
    int join_rc = header_wrap_frontier(a);
    if (join_rc != 0) return join_rc;
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
    if (rc == 0)
        a->archivable = queue_meta_archivable(a->queue_meta);   /* CAND-54: on-disk value, never the argument */
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
    a->max_message_length = rec_reg - PAYLOAD_LENGTH_SIZE;   /* APPEND-HOTPATH-01 R2 */

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

    /* CAND-59 items 2+4: install the commit path's adoption-time floor
     * check on the RW header accessor, before the join's eager wrap. */
    mmapqi_region_ring_accessor_set_floor_check(a->header_acc,
                                                commit_header_below_floor,
                                                a->queue_meta);

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

    /* CAND-58: the walk accessor (RO + size-checked) for the walks/scans */
    rc = mmapqi_queue_region_accessor_create_walk(
        directory, queue_name, rec_reg, ring_size, 1, rec_max, timeout_ns,
        runtime, &a->walk_acc);
    if (rc != 0) {
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
        return rc;
    }
    a->walk_header  = queue_region_accessor_header(a->walk_acc);
    a->walk_payload = mmapqi_queue_region_accessor_payload(a->walk_acc, a->appender_id);
    a->batch_iterator.queue_name  = a->queue_name;
    a->batch_iterator.payload_acc = a->payload_acc;
    /* CAND-58: the join's committed-batch walk (skip_to_end_of_stream) maps
     * through the walk accessor and reports misses for classification */
    a->walk_batch_iterator.queue_name  = a->queue_name;
    a->walk_batch_iterator.payload_acc = a->walk_payload;
    a->walk_batch_iterator.walk_mode   = 1;
    if (a->walk_payload == NULL) {
        mmapqi_offset_set_destroy(&a->outstanding);
        mmapqi_queue_region_accessor_close(a->walk_acc);
        mmapqi_queue_region_accessor_close(a->ro_acc);
        mmapqi_queue_region_accessor_close(a->rw_acc);
        mmapqi_appender_meta_close(a->appender_meta);
        if (a->queue_meta) mmapqi_queue_meta_close(a->queue_meta);
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return MMAPQ_ETIMEDOUT;   /* the lazy payload creation's only
                                   * failure channel */
    }

    /* CAND-58 §4.4: an overtaken join walk recomputes JoinBase — a fresh
     * pass floored at the classifying line — never surfaced; bounded by the
     * no-progress detector + the last-resort cap. */
    int64_t walk_floor = 0, last_line_floor = INT64_MIN;
    int     restarts = 0;
    int     init_rc;
    for (;;) {
        init_rc = initialise_append_position(a, start_index, walk_floor);
        if (init_rc != MMAPQI_WALK_RESTART) break;
        if (mmapqi_walk_restart_permitted(a->walk_ovt_floor, &last_line_floor,
                                          &restarts) != 0) {
            init_rc = MMAPQ_ECORRUPT;
            break;
        }
        walk_floor = a->walk_ovt_floor;
    }
    if (init_rc != 0) {
        mmapqi_offset_set_destroy(&a->outstanding);
        mmapqi_queue_region_accessor_close(a->walk_acc);
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

/* Cold half of start_batch: recover an ORPHANED batch to staged.
 * noinline (RS-BATCH-LAT-01): inlined, this path's address-taken local cost
 * every start_batch a stack frame, callee-saved spills and a stack-protector
 * canary although it runs only after a discard's pre-store failure. */
__attribute__((noinline)) static int
start_batch_recover_orphan(MmapqIndexedBatchAppender *appender)
{
    int64_t new_pos;
    int rc = recover_open_batch(appender, appender->current_batch.batch_offset,
                                &new_pos);
    /* CAND-58: MMAPQI_WALK_RESTART is unreachable here — the orphaned
     * batch sits at this incarnation's live frontier, which the archive
     * line never passes (the line advances only over committed
     * records) — and the restart is a create-time concept; map the
     * unreachable case loudly rather than leaking the internal code. */
    if (rc == MMAPQI_WALK_RESTART) return MMAPQ_ECORRUPT;
    if (rc != 0) return rc;   /* starts nothing */
    appender->current_payload_position = new_pos;
    appender->batch_state = BATCH_STATE_NONE;
    return 0;
}

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
        int rc = start_batch_recover_orphan(appender);
        if (rc != 0) return rc;   /* starts nothing */
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

/* CAND-59 test hooks — see internal/test_hooks.h. */
#ifdef MMAPQ_TEST_HOOKS
static void (*mtlh_entry_batch_hook)(uint8_t);
void mmapq_test_hook_set_mtlh_entry_batch(void (*fn)(uint8_t lane)) { mtlh_entry_batch_hook = fn; }
#define MTLH_ENTRY_BATCH() do { if (mtlh_entry_batch_hook != NULL) mtlh_entry_batch_hook(a->appender_id); } while (0)
#else
#define MTLH_ENTRY_BATCH() do { } while (0)
#endif

/* CAND-59 (spec v1.1 §3.2): the commit path's floor check, installed on
 * this appender's RW header ring accessor — fires only on the cold/adoption
 * seam (ring_accessor_wrap), never on the in-region fast path. */
static int commit_header_below_floor(void *ctx, int64_t position)
{
    return header_index(position) < queue_meta_start_index((const QueueMeta *)ctx);
}

/* CAND-59 items 1+2 (spec v1.1 §3.3), cold half: the NOT_FOUND recovery
 * loop (start_index re-read, explicit header create, 1024-retry cap,
 * ECORRUPT diagnostics, prev_floor). noinline (APPEND-HOTPATH-01 R1):
 * always_inline'd into batch_publish, the loop's register pressure and
 * its fprintf stack alignment are paid statically in the publisher's
 * prologue on EVERY call (one extra callee-saved push/pop + sub/add —
 * the 4 Ir/publish CAND-59 regression), although the loop never runs on
 * the success path.
 * C59_HOOK_HEADER_NOT_FOUND_NOTE() (mutation-3 witness) fires once per
 * ANOMALOUS entry only: a below-si jump (the stale lane probed below
 * start_index) or a no-progress/cap failure. A clean frontier create —
 * the ordinary boundary crossing — does NOT count. */
__attribute__((noinline)) static int
header_wrap_frontier_cold(MmapqIndexedBatchAppender *a)
{
    WrapResult r = WRAP_FILE_NOT_FOUND;
    int noted = 0;
    int64_t prev_floor = -1;
    for (int iter = 0; ; iter++) {
        int64_t si = queue_meta_start_index(a->queue_meta);
        if (a->current_index < si) {
            if (!noted) { C59_HOOK_HEADER_NOT_FOUND_NOTE(); noted = 1; }
            a->current_index = si;
            a->current_header_position = header_position(si);
        } else if (mmapqi_queue_region_accessor_header_create(
                       a->rw_acc, a->current_header_position) != 0) {
            return -1;   /* create I/O error (not EEXIST): fail, retryable */
        }
        r = region_accessor_wrap(a->header_acc,
                                 a->current_header_position, &a->header_buf);
        if (r == WRAP_SUCCESS) return 0;
        if (r != WRAP_FILE_NOT_FOUND) return -1;
        if (si <= prev_floor) {
            if (!noted) { C59_HOOK_HEADER_NOT_FOUND_NOTE(); noted = 1; }
            fprintf(stderr, "mmapq: commit-path header wrap made no progress "
                            "(floor did not advance) — MMAPQ_ECORRUPT\n");
            return MMAPQ_ECORRUPT;
        }
        if (iter >= 1024) {
            if (!noted) { C59_HOOK_HEADER_NOT_FOUND_NOTE(); noted = 1; }
            fprintf(stderr, "mmapq: commit-path header wrap exceeded the "
                            "1024-retry cap — MMAPQ_ECORRUPT\n");
            return MMAPQ_ECORRUPT;
        }
        prev_floor = si;
    }
}

/* CAND-59 items 1+2 (spec v1.1 §3.3): the commit path's header wrap.
 * In-region identical to a bare region_accessor_wrap (always_inline,
 * CAND-45): the fast path is the wrap + one predicted branch; every
 * non-SUCCESS outcome falls through to the cold half above. On
 * WRAP_FILE_NOT_FOUND — a genuinely missing frontier file or the item-2
 * floor rejection — the cold half re-reads start_index: slot below si →
 * jump to si; at/above si → create the file (O_CREAT|O_EXCL; EEXIST =
 * another lane created it) and re-wrap. A repeat NOT_FOUND whose
 * classifying floor did not advance is a bug, not a timeout: ECORRUPT +
 * a distinct stderr line (the region_ring.c convention); the 1024 cap has
 * a second line. Returns 0 on WRAP_SUCCESS, MMAPQ_ECORRUPT on the two
 * corruption exits, -1 on every other failure — the corrupt flag rides
 * the return value instead of an out-parameter (APPEND-HOTPATH-01 R1):
 * an address-taken local lives on the stack, forcing a frame on callers
 * that inline this. The call sites map the result exactly as they mapped
 * the flag pair. */
static __attribute__((always_inline)) inline int
header_wrap_frontier(MmapqIndexedBatchAppender *a)
{
    WrapResult r = region_accessor_wrap(a->header_acc,
                                        a->current_header_position,
                                        &a->header_buf);
    if (__builtin_expect(r == WRAP_SUCCESS, 1)) return 0;
    if (r != WRAP_FILE_NOT_FOUND) return -1;
    return header_wrap_frontier_cold(a);
}

static int move_to_last_header(MmapqIndexedBatchAppender *a)
{
    MTLH_ENTRY_BATCH();
    /* CAND-59 item 3: one start_index read at entry; jump if behind — no
     * 100-slot below-si probe, no cold wrap into a reclaimed file. */
    int64_t si = queue_meta_start_index(a->queue_meta);
    if (a->current_index < si) a->current_index = si - 1;  /* the loop's ++ lands on si */
    int attempts = 0;
    while (attempts < MAX_LINEAR_ATTEMPTS_BEFORE_SEARCH) {
        attempts++;
        a->current_index++;
        a->current_header_position = header_position(a->current_index);
        int rc = header_wrap_frontier(a);
        if (rc != 0) return rc;

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

    int rc = header_wrap_frontier(a);
    if (rc != 0) return rc;

    return 0;
}

#ifdef MMAPQ_TEST_HOOKS
static void (*batch_commit_pre_publish_hook)(void);
void mmapq_test_hook_set_batch_commit_pre_publish(void (*fn)(void)) { batch_commit_pre_publish_hook = fn; }
static void (*batch_commit_post_cas_hook)(void);
void mmapq_test_hook_set_batch_commit_post_cas(void (*fn)(void)) { batch_commit_post_cas_hook = fn; }
static void (*batch_commit_min_pre_hook)(void);
void mmapq_test_hook_set_batch_commit_min_pre(void (*fn)(void)) { batch_commit_min_pre_hook = fn; }
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
    int rc = header_wrap_frontier(a);
    if (rc != 0) return rc;

    _Atomic uint64_t *hp = (_Atomic uint64_t *)a->header_buf.data;
    uint64_t expected = HEADER_NULL;

    while (__builtin_expect(!atomic_compare_exchange_strong_explicit(hp, &expected, header, memory_order_acq_rel, memory_order_acquire), 0)) {
        int mrc = move_to_last_header(a);
        if (mrc) return mrc;
        rc = header_wrap_frontier(a);
        if (rc != 0) return rc;
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
     * path (§3.5), so complete and batch_publish stay fence-free.
     *
     * CAND-54 (spec §2.2, the batch form by the same-protocol argument):
     * archivable queues run the commit-intent protocol — intent store
     * (seq_cst), pre-check against max(start_offset, archive line) (L(i)),
     * publish CAS, fence, re-check, min (L(iv): unchanged), intent clear
     * (release, LAST). archivable=0: membership gate + overtaken pre-check
     * + CAS only (identical error behaviour; nothing can raise the
     * boundary or delete files). The branch is on the on-disk flag cached
     * at open — never the factory argument. */
    int exempt = (batch_offset == a->current_batch.batch_offset);
    if (!exempt) {
        if (a->archivable) {
            appender_meta_intent_store(a->appender_meta, batch_offset);
            int64_t so   = appender_meta_start_offset_sc(a->appender_meta);
            int64_t line = appender_meta_archive_offset_sc(a->appender_meta);
            int64_t floor = so > line ? so : line;
            if (batch_offset < floor) {
                /* overtaken — final; clear the intent first (release): a
                 * refused commit holds no claim */
                appender_meta_intent_clear(a->appender_meta);
                mmapqi_offset_set_remove(&a->outstanding, batch_offset);
                return MMAPQ_EINVAL;
            }
        } else if (batch_offset < appender_meta_start_offset(a->appender_meta)) {
            mmapqi_offset_set_remove(&a->outstanding, batch_offset);
            return MMAPQ_EINVAL;   /* overtaken — final */
        }
    }

#ifdef MMAPQ_TEST_HOOKS
    /* CAND-51b pin: park between the overtaken pre-check and the publish */
    if (batch_commit_pre_publish_hook) batch_commit_pre_publish_hook();
#endif
    int64_t idx = batch_publish(a, batch_offset);
    if (idx < 0) {
        /* Publish failure — member retained, retryable. Nothing was
         * published (every batch_publish failure precedes a successful
         * CAS), so clear the intent (release): leaving it set would pin
         * the lane's reclaim for a process that abandons the batch after
         * a timeout (ruling 2026-10-25). A retry stores it again. The
         * exempt current batch never stored one. */
        if (a->archivable && !exempt)
            appender_meta_intent_clear(a->appender_meta);
        return idx;
    }
#ifdef MMAPQ_TEST_HOOKS
    /* CAND-54 pin: kill -9 between the publish CAS and the fence +
     * re-check (the single-message commit_post_cas pin's batch twin) */
    if (batch_commit_post_cas_hook) batch_commit_post_cas_hook();
#endif
    if (a->archivable) {
        /* Race with an archive that published a higher boundary between the
         * pre-check and the CAS: the archiver stores start_offset, fences,
         * then reads headers; this commit stored its header, fences, then
         * reads start_offset — with seq_cst fences on both sides at least
         * one side observes the other, so one of them lowers the boundary
         * below this batch (the CAND-30 store-buffering argument, batch
         * form). Lowers start_offset only; never archive_offset
         * (CAND-49 §3.9). */
        atomic_thread_fence(memory_order_seq_cst);
        if (batch_offset < appender_meta_start_offset(a->appender_meta)) {
#ifdef MMAPQ_TEST_HOOKS
            /* CAND-54 pin: hold the commit just before the lowering min
             * (the single-message commit_min_pre pin's batch twin) */
            if (batch_commit_min_pre_hook) batch_commit_min_pre_hook();
#endif
            appender_meta_start_offset_min(a->appender_meta, batch_offset);
        }
        /* clear the intent (release) LAST — after the publish and the
         * boundary repair (spec §2.2) */
        if (!exempt)
            appender_meta_intent_clear(a->appender_meta);
    }
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
    if (length > a->max_message_length) return MMAPQ_EINVAL;
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
        /* CAND-58: the walk accessor's RO header side + RO size-checked
         * payload side; the classification floor is the seq_cst line */
        int rc = mmapqi_uncommitted_batch_iterator_create(
            a->queue_name,
            a->appender_id,
            batch_appender_start_of_stream,
            batch_appender_end_of_stream,
            a,
            batch_appender_archive_offset,   /* §3.9: the archive line */
            batch_appender_archive_offset_sc,/* CAND-58 §4.2: the classification floor */
            a->walk_header,
            a->walk_payload,
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
    if (length > a->max_message_length) return MMAPQ_EINVAL;
    /* Order the previous record's payload bytes before this record's
     * length prefix (and before any padding marker below) — the batch
     * side of the torn-tail rule (CAND-50 item 2; the full text is on the
     * leading fence in write_payload, indexed_appender.c). Compiler
     * barrier only — no instructions emitted. */
    atomic_signal_fence(memory_order_release);
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
    if (length > a->max_message_length) return MMAPQ_EINVAL;
    /* Order the previous record's payload bytes before this record's
     * length prefix (and before any padding marker below) — the batch
     * side of the torn-tail rule (CAND-50 item 2; the full text is on the
     * leading fence in write_payload, indexed_appender.c). Also covers
     * the caller-written payload bytes of a preceding append_reserved:
     * they are program-order-before this fence. Compiler barrier only —
     * no instructions emitted. */
    atomic_signal_fence(memory_order_release);
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

int mmapq_mutable_batch_get_checked(MmapqMutableBatch *b, int32_t message_index,
                                    const void **out_data, int32_t *out_length)
{
    if (out_data != NULL) *out_data = NULL;
    if (out_length != NULL) *out_length = 0;
    if (b == NULL || out_data == NULL || out_length == NULL) return MMAPQ_EINVAL;
    /* CAND-50 item 3: an out-of-range index is an error, never message 0 —
     * the negative-index clamp was a positioning-loop artifact, never
     * deliberate (the D3.0 rule-13 record; Java moveTo has always
     * thrown). */
    if (message_index < 0 || message_index >= b->message_count) return MMAPQ_EINVAL;
    MmapqIndexedBatchAppender *a = b->appender;
    mmapq_batch_iterator_init(&a->batch_iterator, b->batch_offset,
                              b->message_count, message_index);
    if (a->batch_iterator.last_error != 0) return a->batch_iterator.last_error;
    if (!mmapq_batch_iterator_advance(&a->batch_iterator))
        /* the range check above makes advance-failure here a walk failure
         * (timeout/corrupt in last_error); the ECORRUPT fallback is
         * unreachable but keeps the failure causes distinct */
        return a->batch_iterator.last_error != 0
                   ? a->batch_iterator.last_error : MMAPQ_ECORRUPT;
    *out_data   = batch_iterator_current_data(&a->batch_iterator);
    *out_length = batch_iterator_current_length(&a->batch_iterator);
    return 0;
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
    if (a->walk_acc) {
        /* walk_header/walk_payload are owned by walk_acc — no separate close */
        mmapqi_queue_region_accessor_close(a->walk_acc);
        a->walk_acc     = NULL;
        a->walk_header  = NULL;
        a->walk_payload = NULL;
    }
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

/* Test-only reach (see internal/test_reach.h). */
RegionAccessor *mmapqi_batch_appender_header_acc(MmapqIndexedBatchAppender *a)
{
    return a == NULL ? NULL : a->header_acc;
}
RegionAccessor *mmapqi_batch_appender_payload_acc(MmapqIndexedBatchAppender *a)
{
    return a == NULL ? NULL : a->payload_acc;
}
