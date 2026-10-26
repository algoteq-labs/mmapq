#include "indexed_appender.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/appender_meta.h"
#include "internal/appender_id_pool.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/payload_walk.h"
#include "internal/payload_iterator.h"
#include "internal/offset_set.h"
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

    /* Iterators — created lazily on first iterator call */
    MmapqPayloadIterator            *uncommitted_inner;
    MmapqUncommittedPayloadIterator *uncommitted_iter;

    int closed;

    /* Outstanding-offset tracking (CAND-49 §3.3): the commit-validation
     * gate. The latest uncommitted offset is flag-tracked in
     * last_pending_offset (never in the set); earlier ones live in the set,
     * refilled by the uncommitted iterator on yield. At the END of the
     * struct so existing field offsets (and the hot paths' object code)
     * are unchanged. */
    OffsetSet            outstanding;
    int64_t              last_pending_offset;  /* PAYLOAD_ITERATOR_NOT_SET when none */

    /* CAND-54: the queue's on-disk archivable flag, cached at open from
     * the queue meta — NEVER the factory argument (a reopened queue keeps
     * its on-disk value; the commit-intent protocol's branch must follow
     * the disk, or a mismatched reopen silently drops the intent). */
    int                  archivable;
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

/* Archive line supplier for the uncommitted iterator (CAND-49 §3.9). */
static int64_t appender_archive_offset(void *ctx)
{
    MmapqIndexedAppender *a = (MmapqIndexedAppender *)ctx;
    return appender_meta_archive_offset(a->appender_meta);
}

/* Refill helper (CAND-49 §3.3): make an iterator-yielded offset
 * committable. Skips the flag-tracked tail (already committable via the
 * fast path) — the skip rule; idempotent add is the backstop for
 * everything else. */
static int appender_refill_uncommitted(void *ctx, int64_t off)
{
    MmapqIndexedAppender *a = (MmapqIndexedAppender *)ctx;
    if (off == a->last_pending_offset) return 0;
    return mmapqi_offset_set_add(&a->outstanding, off);
}

/* ── Start index suppliers for header search ──────────────────────────────── */

static int64_t appender_start_index_from_meta(void *ctx)
{
    return queue_meta_start_index((const QueueMeta *)ctx);
}

/* ── Write padding at region boundary ───────────────────────────────────── */

/* always_inline: same §9.1 anchor as write_payload — keep the shipped
 * hot-path inlining shape under the CAND-49 TU growth. */
static __attribute__((always_inline)) inline int write_padding_if_needed(
    MmapqIndexedAppender *a, int32_t length)
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

/* always_inline: preserves the shipped hot-path object code exactly — with
 * the CAND-49 append_uncommitted growth in the same TU, gcc -O3 otherwise
 * flips write_payload from inlined (into append/append_uncommitted) to
 * outlined, a silent hot-path change (§9.1 byte identity). Semantics
 * unchanged. */
static __attribute__((always_inline)) inline int write_payload(
    MmapqIndexedAppender *a, const void *data, int32_t length,
    int64_t *out_offset)
{
    /* Order the previous record's payload bytes before this record's
     * length prefix (and before any padding marker below): the torn-tail
     * rule — after a crash only the lane's highest-offset uncommitted
     * record can be torn; every earlier uncommitted record is byte-intact
     * (CAND-50 item 2) — needs the prior append's payload memcpy complete
     * in program order before this record's length store. With LTO=1
     * consecutive appends can be inlined into one stream and plain
     * memcpys carry no ordering guarantee (the CAND-51c §0 reasoning).
     * Compiler barrier only — a signal fence emits no instructions. */
    atomic_signal_fence(memory_order_release);
    MMAP_ASSERT(length + PAYLOAD_LENGTH_SIZE <= queue_meta_region_size(a->queue_meta),
        "write_payload: message too large — must fit within one region. "
        "Increase region_size or reduce message size.");
    if (write_padding_if_needed(a, length)) return -1;

    int64_t payload_offset = a->current_payload_position;

    /* Write length prefix */
    memcpy(a->payload_buf.data + PAYLOAD_LENGTH_OFFSET, &length, sizeof(int32_t));
    /* The length store must precede the payload bytes in program order —
     * the no-leftover argument (CAND-49 §3.7, CAND-51c §0): a kill -9 may
     * leave a full length with partial content (the rejoin walk jumps its
     * declared extent) or a zero length (EOS), but never payload bytes
     * past EOS without their length. Plain memcpys carry no ordering
     * guarantee; the -O3 build happens to emit them in order today
     * (CAND-51c objdump evidence). Compiler barrier only — a signal fence
     * emits no instructions; cross-observer visibility is carried by the
     * header CAS (acq_rel) at commit. */
    atomic_signal_fence(memory_order_release);

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

/* always_inline (CAND-45): GCC stopped inlining commit_header during the
 * hardening campaign — append went 99 → 137 Ir/msg (reviewer pack
 * audit/claude-hotpath-drift/, callgrind exclusive, release flags). The
 * attribute restores the shipped shape: 102. Same flip mechanism as the
 * CAND-49 anchors on write_payload / write_padding_if_needed above. */
static __attribute__((always_inline)) inline int commit_header(MmapqIndexedAppender *a, int64_t payload_offset, int64_t *out_index)
{
    /* Encoded positions are STARTS — limited to the 56-bit field (2^56−1),
     * one tighter than the start_offset open check (CAND-32 spec §2). */
    if (payload_offset < INITIAL_PAYLOAD_POSITION ||
        payload_offset > (int64_t)HEADER_PAYLOAD_MASK)
        return MMAPQ_ECORRUPT;

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

/* Walk forward from `from` to the end of the payload stream. The walk
 * uses the writer's RW payload accessor (CAND-51e, below): it may extend
 * a region file at a wrap site, but it never RE-CREATES a reclaimed file,
 * because it starts at or above the readability floor
 * max(start_offset, archive_offset) (CAND-54 L(ii)) — files below the
 * floor are never probed (pinned by test_c54_hole1_single and
 * test_doc01_rejoin_after_reclaim_recreates_nothing). It never writes at
 * start_offset (CAND-30, I5: bytes there may belong to a dead payload,
 * and writing a shorter message over a longer one leaves stale bytes a
 * later recovery would misread). A position with no file yet, or beyond a
 * file's end, is the end of the stream. The pre-wrap is required:
 * post-D8 the walk maps a missing or too-short file at a wrap site to
 * MMAPQ_ECORRUPT (F1) — only this pre-wrap makes "no file" the end of the
 * stream here. */
#ifdef MMAPQ_TEST_HOOKS
static int     appender_walk_site = -1;
static int32_t appender_walk_rc;
static int     appender_walk_fired;
void mmapq_test_hook_set_appender_walk_result(int site, int32_t rc)
{ appender_walk_site = site; appender_walk_rc = rc; appender_walk_fired = 0; }
int mmapq_test_hook_appender_walk_fired(void) { return appender_walk_fired; }
#define WALK_HOOK(site, v) do { if (appender_walk_site == (site)) { (v) = appender_walk_rc; appender_walk_fired++; } } while (0)
#else
#define WALK_HOOK(site, v) do { } while (0)
#endif

static int walk_payload_to_end(MmapqIndexedAppender *a, int64_t from,
                               int64_t *out_end)
{
    /* CAND-51e: the rejoin walk uses the WRITER'S RW payload accessor, not
     * the RO one. Anti-Pattern E (no file-size check on the RO payload path)
     * is correct for header-driven readers, which only map where a committed
     * header points; this walk PROBES past committed data, where that
     * premise fails. The RO accessor neither extends nor size-checks, so a
     * probe at a region the writer never touched SIGBUSed (a message ending
     * exactly at an in-file region end) and a tail marker whose skip target
     * file was never created read as ECORRUPT instead of EOS. The RW wrap is
     * size-checked and creates/extends the region (zeros -> EOS), matching
     * the batch twin's walk. It can never re-create a reclaimed file: the
     * walk starts at this lane's last live header or at the join anchor
     * max(start_offset, archive_line) (CAND-54 L(ii)) — reclaimed files
     * are wholly below the archive line, and the line never moves back.
     * (The pre-L argument anchored at start_offset; hole 1 of spec §7.1 —
     * a commit min lowering start_offset into the reclaimed region after
     * the reclaim — broke it.) */
    RegionAccessor *rw_payload = a->payload_acc;
    int64_t probing = from;
    while (1) {
        WrapResult wr = region_accessor_wrap_no_premap(rw_payload, probing,
                                                       &a->payload_buf);
        if (wr == WRAP_FILE_NOT_FOUND || wr == WRAP_BEYOND_FILE_SIZE)
            break;   /* end of stream */
        if (wr == WRAP_MAP_TIMED_OUT)
            return MMAPQ_ETIMEDOUT;
        if (wr != WRAP_SUCCESS)
            return MMAPQ_ECORRUPT;
        int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;
        int32_t len = mmapqi_payload_walk_advance(a->queue_name, &probing,
                                                  &msg_off,
                                                  rw_payload, &a->payload_buf);
        WALK_HOOK(0, len);
        if (len == 0) break;
        if (len < 0) return len;   /* D8: timeout vs corruption propagates */
    }
    *out_end = probing;
    return 0;
}

static int initialise_append_position(MmapqIndexedAppender *a, int64_t start_index)
{
    /* CAND-54 ruling 2: the join clears a stale commit intent. A writer
     * killed after its intent store would otherwise pin this lane's
     * reclaim from the intent's file upward forever (the stated liveness
     * cost). Safe here: one lane, one writer — the previous incarnation
     * is gone by the appender-id-pool rule, so nothing in flight owns the
     * slot. Harmless on non-archivable queues (the slot is never set). */
    appender_meta_intent_clear(a->appender_meta);

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
         * the join anchor to the end of stream (no file yet = the
         * offset). */
        a->current_index = start_index;
        int wrc = walk_payload_to_end(a, appender_join_anchor(a->appender_meta),
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
             * walk forward from the join anchor (CAND-54 L(ii)) to the
             * end of the stream */
            int wrc = walk_payload_to_end(a, appender_join_anchor(a->appender_meta),
                                          &a->current_payload_position);
            if (wrc != 0) return wrc;
        } else {
            /*
             * Step 3: Walk payload forward past the committed message
             *         and any uncommitted tail, to find the true end
             *         of the payload stream. CAND-51e: the WRITER'S RW
             *         payload accessor, like walk_payload_to_end — this
             *         walk probes past committed data, where the RO path's
             *         no-size-check premise (Anti-Pattern E is for
             *         header-driven readers) does not hold.
             */
            RegionAccessor *rw_payload = a->payload_acc;
            Buffer         *rw_buf     = &a->payload_buf;

            int64_t probing = committed_payload_pos;
            int64_t msg_off = PAYLOAD_ITERATOR_NOT_SET;

            /* Walk past the committed message itself first */
            int32_t len = mmapqi_payload_walk_advance(a->queue_name,
                                                &probing, &msg_off,
                                                rw_payload, rw_buf);
            WALK_HOOK(1, len);
            if (len < 0) return len;   /* D8: timeout vs corruption propagates */
            if (len == 0) {
                /* End of stream where a committed message must exist */
                return MMAPQ_ECORRUPT;
            }

            /* Walk past any uncommitted tail */
            while (1) {
                int64_t next_probing = probing;
                int64_t next_msg_off = PAYLOAD_ITERATOR_NOT_SET;
                int32_t next_len = mmapqi_payload_walk_advance(a->queue_name,
                                                          &next_probing,
                                                          &next_msg_off,
                                                          rw_payload, rw_buf);
                WALK_HOOK(2, next_len);
                /* CAND-51e: adopt the final advance's position at EOS too —
                 * payload_walk sets it past a terminal padding skip (short
                 * or marker), and the walker must apply the writer's rule at
                 * each position: the next write belongs past the padding
                 * (walk_payload_to_end adopts via its direct probing, and
                 * the batch twin's recover/skip adopt likewise). Dropping it
                 * here placed the next write ON a terminal marker when the
                 * rejoin walked from a committed base — the two rejoin paths
                 * disagreed by the skip. */
                if (next_len == 0) { probing = next_probing; break; }
                /* A walk error leaves probing unadvanced; without this the loop
                 * would spin forever on a corrupt tail. D8: the walk's code
                 * propagates verbatim (timeout is retryable, corruption is
                 * not), matching the first walk's propagation above. */
                if (next_len < 0) return next_len;
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

    /* CAND-49 §3.3: the outstanding-offset set is allocated at create
     * (initial capacity 16 slots = 15 + the standing spare), so the
     * deferred-path adds never allocate. Pure memory — unwind before the
     * queue meta open keeps the existing error paths unchanged. */
    mmapqi_offset_set_init(&a->outstanding);
    a->last_pending_offset = PAYLOAD_ITERATOR_NOT_SET;
    if (mmapqi_offset_set_reserve(&a->outstanding, OFFSET_SET_INITIAL_CAPACITY - 1) != 0) {
        mmapqi_appender_id_pool_release(a->id_pool, a->appender_id);
        mmapqi_appender_id_pool_close(a->id_pool);
        free(a->directory);
        free(a->queue_name);
        free(a);
        return MMAPQ_ENOMEM;
    }

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
        mmapqi_offset_set_destroy(&a->outstanding);
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
    a->archivable = queue_meta_archivable(a->queue_meta);   /* CAND-54: on-disk value, never the argument */

    /* Open appender meta (creates file if new) */
    rc = mmapqi_appender_meta_open_rw(directory, queue_name, appender_id, &a->appender_meta);
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

    /* Create RW region accessors */
    rc = mmapqi_queue_region_accessor_create_rw(
        directory, queue_name, reconciled_region_size, ring_size,
        REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD,
        reconciled_max_file_size, timeout_ns, runtime, appender_id,
        &a->rw_acc);
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

    /* Find where to resume appending */
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
    int crc = commit_header(a, payload_offset, &committed);
    if (crc != 0) return crc;
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

    /* CAND-49 §3.3: the latest uncommitted offset is flag-tracked; the
     * previous flag-tracked offset (if any) moves into the outstanding set.
     * Ensure the set's spare slot BEFORE the write — ENOMEM means nothing
     * is written (ruling 5). One compare when the spare exists (the normal
     * case), a grow attempt otherwise. */
    int64_t prev = a->last_pending_offset;
    if (prev != PAYLOAD_ITERATOR_NOT_SET &&
        mmapqi_offset_set_reserve(&a->outstanding, 1) != 0)
        return MMAPQ_ENOMEM;

    int64_t payload_offset;
    if (write_payload(a, data, length, &payload_offset)) return -1;

    if (prev != PAYLOAD_ITERATOR_NOT_SET)
        (void)mmapqi_offset_set_add(&a->outstanding, prev);  /* cannot allocate: spare ensured above */
    a->last_pending_offset = payload_offset;
    return payload_offset;
}

/* ── mmapq_indexed_appender_commit ─────────────────────────────────────────────── */

#ifdef MMAPQ_TEST_HOOKS
static void (*commit_pre_hook)(void);
void mmapq_test_hook_set_commit_pre(void (*fn)(void)) { commit_pre_hook = fn; }
static void (*commit_min_pre_hook)(void);
void mmapq_test_hook_set_commit_min_pre(void (*fn)(void)) { commit_min_pre_hook = fn; }
static void (*commit_post_cas_hook)(void);
void mmapq_test_hook_set_commit_post_cas(void (*fn)(void)) { commit_post_cas_hook = fn; }
#endif

int64_t mmapq_indexed_appender_commit(
    MmapqIndexedAppender *a,
    int64_t          payload_offset)
{
    if (a == NULL) return MMAPQ_EINVAL;
    if (a->closed) return MMAPQ_ECLOSED;
    if (payload_offset <= 0) return MMAPQ_EINVAL;

    /* Fast path — the flag-tracked tail, no set access, no shared loads.
     * Proof obligation (CAND-49 §3.4): the flag-tracked offset was written
     * by this incarnation's most recent append_uncommitted and is the
     * lane's tail; every committed payload of the lane sits below it, so
     * the archiver's boundary (end of the highest-position ARCHIVED =
     * committed message of the lane) can never reach it. The CAND-30
     * check is unreachable-unnecessary here, not skipped. Keyed on the last
     * APPENDED offset; "greater than the last committed" is not safe under
     * resequencing (a gap-fill payload below the last committed one would
     * skip the check entirely). */
    if (payload_offset == a->last_appended &&
        payload_offset == a->last_pending_offset) {
        int64_t committed;
        int crc = commit_header(a, payload_offset, &committed);
        if (crc != 0) return crc;   /* flag unchanged — retryable */
        a->last_pending_offset = PAYLOAD_ITERATOR_NOT_SET;
        return committed;
    }

    /* Slow path (gap fill / iterated offsets). Membership is the gate
     * (CAND-49 §3.4, supersedes the CAND-32 C3 line, which is REMOVED):
     * above-last-written, double commit, mid-message, never-written are all
     * non-members. Members: the flag-tracked tail or a set offset (staged
     * by append_uncommitted's move or refilled by the uncommitted
     * iterator). */
    int flagged = (payload_offset == a->last_pending_offset);
    if (!flagged &&
        !mmapqi_offset_set_contains(&a->outstanding, payload_offset))
        return MMAPQ_EINVAL;

    /* CAND-54 commit-intent protocol (spec §2.2), archivable queues only.
     * archivable=0: membership gate + overtaken pre-check + CAS — nothing
     * can raise the boundary or delete files, so the intent, fence,
     * re-check and min are skipped (identical error behaviour; the
     * pre-check keeps its single acquire load). The branch is on the flag
     * cached at open from the queue meta — NEVER the factory argument
     * (the mismatched-flag reopen pin). */
    if (a->archivable) {
        /* 1. intent store (seq_cst) — before the pre-check, so the
         *    listing's inflight term covers the whole window */
        appender_meta_intent_store(a->appender_meta, payload_offset);
        /* 2. overtaken pre-check against max(start_offset, archive line)
         *    (L(i): the line floors the pre-check — a commit min can lower
         *    start_offset below the line, so start_offset alone is not
         *    the floor); both loads seq_cst (spec §2.5) */
        int64_t so   = appender_meta_start_offset_sc(a->appender_meta);
        int64_t line = appender_meta_archive_offset_sc(a->appender_meta);
        int64_t floor = so > line ? so : line;
        if (payload_offset < floor) {
            /* overtaken — final. Clear the intent first (release): a
             * refused commit holds no claim, and a lingering intent would
             * pin the lane's reclaim until the next slow commit or a
             * join. */
            appender_meta_intent_clear(a->appender_meta);
            if (flagged) a->last_pending_offset = PAYLOAD_ITERATOR_NOT_SET;
            else mmapqi_offset_set_remove(&a->outstanding, payload_offset);
            return MMAPQ_EINVAL;
        }
    } else if (payload_offset < appender_meta_start_offset(a->appender_meta)) {
        /* overtaken — final: drop tracking (flag or set) */
        if (flagged) a->last_pending_offset = PAYLOAD_ITERATOR_NOT_SET;
        else mmapqi_offset_set_remove(&a->outstanding, payload_offset);
        return MMAPQ_EINVAL;
    }
#ifdef MMAPQ_TEST_HOOKS
    if (commit_pre_hook) commit_pre_hook();
#endif
    int64_t committed;
    int crc = commit_header(a, payload_offset, &committed);
    if (crc != 0) {
        /* Publish failure: every failure return in commit_header precedes
         * a successful CAS, so nothing was published — clear the intent
         * (release). Leaving it set would pin the lane's reclaim for the
         * life of a process that abandons the payload after a timeout —
         * new behaviour versus today (ruling 2026-10-25). A retry stores
         * it again. flag and set UNCHANGED — a transient (ETIMEDOUT)
         * failure is retryable (CAND-49 delta-review fix 1: clearing the
         * flag before the CAS would make the retry EINVAL — neither
         * flagged nor a member — and the payload committable again only
         * after a re-scan). */
        if (a->archivable) appender_meta_intent_clear(a->appender_meta);
        return crc;
    }
#ifdef MMAPQ_TEST_HOOKS
    /* CAND-35 pin: kill -9 between the CAS and the fence + re-check */
    if (commit_post_cas_hook) commit_post_cas_hook();
#endif
    if (a->archivable) {
        /* Race with an archive that published a higher boundary between the
         * pre-check and the CAS: the archiver stores start_offset, fences,
         * then reads headers; this commit stored its header, fences, then
         * reads start_offset — with seq_cst fences on both sides at least
         * one side observes the other, so one of them lowers the boundary
         * below this payload. */
        atomic_thread_fence(memory_order_seq_cst);
        if (payload_offset < appender_meta_start_offset(a->appender_meta)) {
#ifdef MMAPQ_TEST_HOOKS
            /* CAND-37 pin: hold the commit between the archiver's snapshot
             * and its publish, just before the lowering min */
            if (commit_min_pre_hook) commit_min_pre_hook();
#endif
            /* lowers start_offset only; never archive_offset (CAND-49 §3.9;
             * CAND-54 L(iv): the min is UNCHANGED — it may lower
             * start_offset below the line; the line is the floor) */
            appender_meta_start_offset_min(a->appender_meta, payload_offset);
        }
        /* clear the intent (release) LAST — after the header is published
         * and the boundary repair is done (spec §2.2) */
        appender_meta_intent_clear(a->appender_meta);
    }
    if (flagged) a->last_pending_offset = PAYLOAD_ITERATOR_NOT_SET;
    else mmapqi_offset_set_remove(&a->outstanding, payload_offset);
    return committed;
}

int mmapq_indexed_appender_uncommitted_iterator_checked(
    MmapqIndexedAppender *a,
    int64_t          start_offset,
    int64_t          end_offset,
    MmapqUncommittedPayloadIterator **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (a == NULL) return MMAPQ_EINVAL;

    /* Create inner iterator with fixed end-of-stream */
    if (a->uncommitted_inner == NULL) {
        int rc = mmapqi_payload_iterator_create(
            a->queue_name,
            a->payload_acc,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            1, /* fix end-of-stream at init time */
            &a->uncommitted_inner);
        if (rc != 0) return rc;
    }

    if (a->uncommitted_iter == NULL) {
        int rc = mmapqi_uncommitted_iterator_create(
            a->queue_name,
            a->appender_id,
            appender_start_of_stream,
            appender_end_of_stream,
            a,
            appender_archive_offset,          /* CAND-49 §3.9: the archive line */
            a->header_acc,
            a->uncommitted_inner,
            appender_start_index_from_meta,   /* D8/S3: header scan floor */
            a->queue_meta,
            appender_refill_uncommitted,      /* CAND-49 §3.3: refill on yield */
            a,
            &a->uncommitted_iter);
        if (rc != 0) return rc;
    }

    int rc = mmapqi_uncommitted_iterator_init(a->uncommitted_iter, start_offset, end_offset);
    if (rc != 0) return rc;
    *out = a->uncommitted_iter;
    return 0;
}

int mmapq_uncommitted_iterator_next(
    MmapqUncommittedPayloadIterator *it,
    const void **data,
    int32_t     *length,
    int64_t     *offset)
{
    if (it == NULL || data == NULL || length == NULL || offset == NULL)
        return MMAPQ_EINVAL;

    int r = mmapqi_uncommitted_iterator_advance(it);
    if (r != 1) {
        /* Exhausted (0) or a negative MMAPQ_E* — outputs cleared either way. */
        *data   = NULL;
        *length = 0;
        *offset = MMAPQ_PAYLOAD_ITERATOR_NOT_SET;
        return r;
    }

    Buffer *b = mmapqi_uncommitted_iterator_current(it);
    *data   = b->data;
    *length = b->capacity;
    *offset = it->inner->current_message_offset;   /* record start (length-prefix position) */
    return 1;
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

    mmapqi_offset_set_destroy(&a->outstanding);

    if (a->uncommitted_iter != NULL) {
        mmapqi_uncommitted_iterator_destroy(a->uncommitted_iter);
        a->uncommitted_iter = NULL;
    }
    if (a->uncommitted_inner != NULL) {
        mmapqi_payload_iterator_destroy(a->uncommitted_inner);
        a->uncommitted_inner = NULL;
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
