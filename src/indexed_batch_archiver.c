#include "indexed_batch_archiver.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/queue_files.h"
#include "internal/appender_meta.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/test_hooks.h"
#include "batch_payload_iterator.h"
#include "internal/batch_payload_iterator_internal.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <unistd.h>

#define OFFSET_NOT_SET ((int64_t)-1)

/* ── Internal struct ─────────────────────────────────────────────────────── */

struct MmapqIndexedBatchArchiver {
    char        *directory;
    char        *queue_name;
    int          ring_size;
    MmapqRuntime     *runtime;

    QueueMeta           *ro_meta;
    QueueMeta           *rw_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;
    Buffer               header_buf;

    AppenderMeta         *appender_metas[MAX_APPENDERS];
    MmapqBatchPayloadIterator *iterators[MAX_APPENDERS]; /* reused across archive calls */

    int meta_init_failed;
};

/* ── Lazy initialisers ───────────────────────────────────────────────────── */

static QueueMeta *ro_init(MmapqIndexedBatchArchiver *a)
{
    if (a->ro_meta) return a->ro_meta;
    if (a->meta_init_failed) return NULL;
    int rc = mmapqi_queue_meta_open_ro(a->directory, a->queue_name, QUEUE_TYPE_BATCH, &a->ro_meta);
    if (rc != 0) {
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            a->meta_init_failed = 1;
        return NULL;
    }
    /* Sync mapping: archiver does a single sequential pass, no premap benefit */
    int acc_rc = mmapqi_queue_region_accessor_create_ro(
        a->directory, a->queue_name,
        queue_meta_region_size(a->ro_meta),
        a->ring_size,
        0,   /* regions_to_map_ahead = 0 — sync, no background mapper needed */
        queue_meta_max_file_size(a->ro_meta),
        10000000000ULL, a->runtime,
        &a->ro_acc);
    if (acc_rc != 0) {
        mmapqi_queue_meta_close(a->ro_meta);
        a->ro_meta = NULL;
        return NULL;
    }
    a->header_acc = queue_region_accessor_header(a->ro_acc);
    return a->ro_meta;
}

/* Note: start_index arg is only used when creating a brand-new meta file.
 * Since the meta already exists when rw_meta_open() is called, all fields
 * are adopted from disk; the passed value is ignored.
 *
 * Dual mapping: ro_meta and rw_meta are two MAP_SHARED views of the same file.
 * Linux guarantees coherence across MAP_SHARED mappings of the same page — a
 * write through rw_meta is immediately visible via ro_meta on all cores.
 * queue_meta_set_start_index(rwm, index) writes atomically; the next
 * queue_meta_start_index() read through ro_meta will see the new value. */
static QueueMeta *rw_meta_open(MmapqIndexedBatchArchiver *a)
{
    if (a->rw_meta) return a->rw_meta;
    if (mmapqi_queue_meta_open_rw(a->directory, a->queue_name,
        queue_meta_start_index(a->ro_meta),
        queue_meta_region_size(a->ro_meta),
        queue_meta_max_file_size(a->ro_meta),
        queue_meta_archivable(a->ro_meta),
        QUEUE_TYPE_BATCH,
        &a->rw_meta) != 0) { a->rw_meta = NULL; }
    return a->rw_meta;
}

static AppenderMeta *get_appender_meta(MmapqIndexedBatchArchiver *a, uint8_t id)
{
    if (!a->appender_metas[id]) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/appender_meta_%u",
                 a->directory, a->queue_name, (unsigned)id);
        if (!mmapqi_queue_file_exists(path)) return NULL;
        if (mmapqi_appender_meta_open_rw(
                a->directory, a->queue_name, id, &a->appender_metas[id]) != 0) return NULL;
    }
    return a->appender_metas[id];
}

static MmapqBatchPayloadIterator *get_iterator(MmapqIndexedBatchArchiver *a, uint8_t id)
{
    if (!a->iterators[id]) {
        MmapqBatchPayloadIterator *it = calloc(1, sizeof(MmapqBatchPayloadIterator));
        if (it == NULL) return NULL;
        it->queue_name  = a->queue_name;
        it->payload_acc = mmapqi_queue_region_accessor_payload(a->ro_acc, id);
        a->iterators[id] = it;
    }
    return a->iterators[id];
}

/* ── Test hook (P15) — never compiled into release builds ────────────────── */

#ifdef MMAPQ_TEST_HOOKS
static int (*live_pass_hook)(void);
void mmapq_test_hook_set_batch_archiver_live_pass(int (*fn)(void)) { live_pass_hook = fn; }
#endif

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapq_indexed_batch_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    MmapqRuntime    *runtime,
    MmapqIndexedBatchArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    MmapqIndexedBatchArchiver *a = calloc(1, sizeof(MmapqIndexedBatchArchiver));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory  = strdup(directory);
    if (a->directory == NULL) { free(a); return MMAPQ_ENOMEM; }
    a->queue_name = strdup(queue_name);
    if (a->queue_name == NULL) { free(a->directory); free(a); return MMAPQ_ENOMEM; }
    a->ring_size  = ring_size > 0 ? ring_size : 4;
    a->runtime    = runtime;
    *out = a;
    return 0;
}

/* ── Archiving ───────────────────────────────────────────────────────────── */

int mmapq_indexed_batch_archiver_archivable(MmapqIndexedBatchArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_archivable(m);
}

/* Lower every published start_offset back to its pre-call value after a
 * live-pass failure. atomic-min can only lower, so a commit that raced in
 * below old[] survives. old[] satisfied I1 over a superset of the new live
 * range, so this is always safe; the logical archive (start_index) stands —
 * only reclamation does not advance. Retrying the same index is a no-op
 * (index == current); the next forward archive past it recomputes the
 * boundary. */
static void rollback_start_offsets(MmapqIndexedBatchArchiver *a, const int64_t *old)
{
    for (int aid = 0; aid < MAX_APPENDERS; aid++) {
        if (old[aid] == OFFSET_NOT_SET) continue;
        appender_meta_start_offset_min(get_appender_meta(a, (uint8_t)aid),
                                       old[aid]);
    }
}

/* ── Forward archiving (CAND-30) ───────────────────────────────────────────
 *
 * The deletion boundary is computed from headers, not write order — see
 * indexed_archiver.c's archive_forward_variable for the full rationale and
 * the store-buffering argument. Batch twin: the candidate is the position
 * after the archived batch with the MAXIMUM payload position; the live pass
 * lowers the boundary below any live batch sitting under it. */
static int archive_forward(MmapqIndexedBatchArchiver *a,
                           int64_t current, int64_t index)
{
    int64_t max_pos[MAX_APPENDERS];  /* highest archived batch position     */
    int32_t counts[MAX_APPENDERS];   /* message count of that batch         */
    int64_t ends[MAX_APPENDERS];     /* position after that batch           */
    int64_t old[MAX_APPENDERS];      /* boundary before this call           */
    for (int i = 0; i < MAX_APPENDERS; i++) {
        max_pos[i] = OFFSET_NOT_SET;
        counts[i]  = 0;
        ends[i]    = OFFSET_NOT_SET;
        old[i]     = OFFSET_NOT_SET;
    }

    /* Read and validate every header in [current, index) (I4) */
    for (int64_t i = current; i < index; i++) {
        uint64_t hdr = mmapqi_header_read(a->header_acc, &a->header_buf,
                                          i, a->queue_name);
        if (hdr == HEADER_NULL)
            return MMAPQ_EINVAL;
        uint8_t aid = header_appender_id(hdr);
        int64_t pos = header_payload_position(hdr);

        MmapqBatchPayloadIterator *it = get_iterator(a, aid);
        if (it == NULL) return MMAPQ_ENOMEM;
        int32_t msg_count = mmapq_batch_iterator_read_message_count(it, pos);
        if (msg_count < 0) return msg_count;
        if (msg_count == 0)
            return MMAPQ_ECORRUPT;

        if (pos > max_pos[aid]) {
            max_pos[aid] = pos;
            counts[aid]  = msg_count;
        }
    }

    /* Exhaust the max-position batch per appender for the candidate end */
    for (int aid = 0; aid < MAX_APPENDERS; aid++) {
        if (max_pos[aid] == OFFSET_NOT_SET) continue;
        MmapqBatchPayloadIterator *it = get_iterator(a, (uint8_t)aid);
        if (it == NULL) return MMAPQ_ENOMEM;
        mmapq_batch_iterator_init(it, max_pos[aid], counts[aid], 0);
        mmapq_batch_iterator_exhaust(it);
        /* Exhaustion also signals failure; a partial scan is not an
         * archive boundary and must not reach the metadata updates. */
        if (it->last_error != 0) return it->last_error;
        ends[aid] = batch_iterator_next_payload_position(it);
    }

    /* Validate all appender metas are accessible before mutating; record the
     * pre-call boundary for the live-pass failure rollback. */
    for (int aid = 0; aid < MAX_APPENDERS; aid++) {
        if (ends[aid] == OFFSET_NOT_SET) continue;
        AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
        if (m == NULL) return MMAPQ_ECORRUPT;
        old[aid] = appender_meta_start_offset(m);
    }

    /* Open rw_meta before publishing: no failure point between the boundary
     * stores and the start_index store. */
    QueueMeta *rwm = rw_meta_open(a);
    if (rwm == NULL) return MMAPQ_EIO;

    /* Publish the candidate boundary (never lowered here), then start_index.
     * CAND-37 twin: CAS from the snapshot, as in indexed_archiver.c. Here
     * the CAS cannot fail — batch commits never touch start_offset and
     * archive.lock EX excludes other archivers — mirrored for symmetry. */
    for (int aid = 0; aid < MAX_APPENDERS; aid++) {
        if (ends[aid] == OFFSET_NOT_SET) continue;
        AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
        if (m == NULL) return MMAPQ_ECORRUPT;
        if (ends[aid] > old[aid])
            (void)appender_meta_start_offset_cas(m, old[aid], ends[aid]);
    }
    queue_meta_set_start_index(rwm, index);

    /* One pass over the live headers, lowering below any live batch. The
     * checked read is required — the plain read maps a mapping timeout to
     * HEADER_NULL and would end the pass early, silently leaving the
     * boundary too high. */
    atomic_thread_fence(memory_order_seq_cst);
    for (int64_t i = index; ; i++) {
        uint64_t hdr;
#ifdef MMAPQ_TEST_HOOKS
        /* P15 twin: inject at the wrap-result level (see indexed_archiver.c) */
        if (live_pass_hook != NULL && live_pass_hook() != 0)
            mmapq_test_hook_header_wrap_inject(WRAP_MAP_TIMED_OUT);
#endif
        int hrc = mmapqi_header_read_checked(a->header_acc, &a->header_buf,
                                             i, &hdr);
#ifdef MMAPQ_TEST_HOOKS
        mmapq_test_hook_header_wrap_inject(0);
#endif
        if (hrc != 0) {
            rollback_start_offsets(a, old);
            return hrc;
        }
        if (hdr == HEADER_NULL) break;   /* end of stream */
        uint8_t aid = header_appender_id(hdr);
        int64_t pos = header_payload_position(hdr);
        AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
        if (m == NULL) {                 /* live entry's meta must exist */
            rollback_start_offsets(a, old);
            return MMAPQ_ECORRUPT;
        }
        appender_meta_start_offset_min(m, pos);
    }
    return 0;
}

static int archive_up_to_index_locked(MmapqIndexedBatchArchiver *a, int64_t index)
{
    QueueMeta *rom = ro_init(a);
    if (rom == NULL || !queue_meta_archivable(rom)) return MMAPQ_EINVAL;

    int64_t current       = queue_meta_start_index(rom);
    if (index == current) return 0;

    int64_t max_file_size   = queue_meta_max_file_size(rom);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;

    if (index > current) {
        /* ── Forward archiving ─────────────────────────────────────────── */
        return archive_forward(a, current, index);

    } else {
        /* ── Backward archiving (undo) ─────────────────────────────────── */

        /* Collect file indices using O(1) bitsets — no heap allocation needed.
         * ~128 KB stack allocation (256 * 512 byte FileIndexSet + 256 * 8 byte positions).
         * This function is called infrequently (archiving, never on hot path), so the
         * large frame is acceptable. */
        FileIndexSet hdr_files;
        FileIndexSet pay_files[MAX_APPENDERS]; /* 256 * 512 bytes = 128 KB stack */
        int64_t first_positions[MAX_APPENDERS];

        file_index_set_clear(&hdr_files);
        for (int i = 0; i < MAX_APPENDERS; i++) {
            file_index_set_clear(&pay_files[i]);
            first_positions[i] = OFFSET_NOT_SET;
        }

        for (int64_t i = index; i < current; i++) {
            uint64_t hdr = mmapqi_header_read(a->header_acc, &a->header_buf,
                                       i, a->queue_name);
            if (hdr == HEADER_NULL)
                return MMAPQ_EINVAL;

            /* The set records any window of FILE_INDEX_SET_MAX consecutive
             * file numbers, whatever order they arrive in — payload file
             * order is not contractually monotonic. A 0 means the undo
             * genuinely spans more than FILE_INDEX_SET_MAX files — refuse
             * the undo as posed with EINVAL, not ECORRUPT: the data is
             * fine, the request is unservable. Remedy: undo in steps
             * spanning < FILE_INDEX_SET_MAX files each. */
            if (!file_index_set_set(&hdr_files, i / headers_in_file))
                return MMAPQ_EINVAL;

            uint8_t aid = header_appender_id(hdr);
            int64_t pos = header_payload_position(hdr);

            /* The boundary must cover the LOWEST restored position */
            if (first_positions[aid] == OFFSET_NOT_SET ||
                pos < first_positions[aid])
                first_positions[aid] = pos;

            /* Track all payload files spanned by this batch's messages */
            MmapqBatchPayloadIterator *it = get_iterator(a, aid);
            if (it == NULL) return MMAPQ_ENOMEM;
            int32_t msg_count = mmapq_batch_iterator_read_message_count(it, pos);
            if (msg_count < 0) return msg_count;
            if (msg_count == 0)
                return MMAPQ_ECORRUPT;
            mmapq_batch_iterator_init(it, pos, msg_count, 0);
            /* Record the file containing the batch header itself */
            if (!file_index_set_set(&pay_files[aid], pos / max_file_size))
                return MMAPQ_EINVAL;  /* payload span too wide — undo in smaller steps */
            /* Record files spanned by each message's end offset */
            while (mmapq_batch_iterator_advance(it)) {
                int64_t msg_end = batch_iterator_next_payload_position(it);
                if (!file_index_set_set(&pay_files[aid],
                                        msg_end / max_file_size))
                    return MMAPQ_EINVAL;  /* payload span too wide — undo in smaller steps */
            }
            /* An incomplete file scan cannot validate an undo operation. */
            if (it->last_error != 0) return it->last_error;
        }

        /* Validate all required header files exist */
        for (int64_t fi = mmapqi_file_index_set_first(&hdr_files); fi >= 0;
             fi = mmapqi_file_index_set_next(&hdr_files, fi)) {
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s/header_%ld",
                     a->directory, a->queue_name, (long)fi);
            if (!mmapqi_queue_file_exists(path))
                return MMAPQ_ECORRUPT;
        }

        /* Validate all required payload files exist */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            if (first_positions[aid] < INITIAL_PAYLOAD_POSITION)
                return MMAPQ_ECORRUPT;
            for (int64_t fi = mmapqi_file_index_set_first(&pay_files[aid]); fi >= 0;
                 fi = mmapqi_file_index_set_next(&pay_files[aid], fi)) {
                char path[4096];
                snprintf(path, sizeof(path), "%s/%s/payload_%d_%ld",
                         a->directory, a->queue_name, aid, (long)fi);
                if (!mmapqi_queue_file_exists(path))
                    return MMAPQ_ECORRUPT;
            }
        }

        /* Validate all appender metas are accessible before mutating */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            if (get_appender_meta(a, (uint8_t)aid) == NULL)
                return MMAPQ_ECORRUPT;
        }

        /* All files present — lower appender boundaries; undo never raises */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
            if (m == NULL) return MMAPQ_ECORRUPT;
            appender_meta_start_offset_min(m, first_positions[aid]);
        }
    }

    /* Commit new start_index atomically — visible to all readers immediately.
     * rw_meta_open can fail from external I/O, which is returned rather than
     * asserted. Undo lowers boundaries via atomic-min, so a retry recomputes
     * the same values and the min-writes are idempotent. */
    QueueMeta *rwm = rw_meta_open(a);
    if (rwm == NULL) return MMAPQ_EIO;
    queue_meta_set_start_index(rwm, index);
    return 0;
}

int mmapq_indexed_batch_archiver_archive_up_to_index(MmapqIndexedBatchArchiver *a, int64_t index)
{
    if (a == NULL || !header_index_in_range(index)) return MMAPQ_EINVAL;

    /* I6: one archive operation at a time per queue */
    int lock_fd;
    int lrc = mmapqi_archive_lock_acquire(a->directory, a->queue_name,
                                          1, &lock_fd);
    if (lrc != 0) return lrc;

    int rc = archive_up_to_index_locked(a, index);
    close(lock_fd);
    return rc;
}

/* ── Query ───────────────────────────────────────────────────────────────── */

int64_t mmapq_indexed_batch_archiver_start_index(MmapqIndexedBatchArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_start_index(m);
}

int mmapq_indexed_batch_archiver_start_offset(
    MmapqIndexedBatchArchiver *a, uint8_t appender_id, int64_t *out_offset)
{
    if (a == NULL || out_offset == NULL) return 0;

    if (!ro_init(a)) return 0;

    AppenderMeta *am = get_appender_meta(a, appender_id);
    if (!am) return 0;

    *out_offset = appender_meta_start_offset(am);
    return 1;
}

static int archivable_files_locked(
    MmapqIndexedBatchArchiver *a,
    char                **paths_out,
    int                   max_paths)
{

    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;

    int64_t start = queue_meta_start_index(m);
    if (start <= QUEUE_META_INITIAL_START_INDEX) return 0;

    int64_t max_file_size   = queue_meta_max_file_size(m);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;
    int     count = 0;

    /* Fully covered header files */
    int64_t full_header_files = (start - 1) / headers_in_file;
    for (int64_t i = 0; i < full_header_files && count < max_paths; i++) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/header_%ld",
                 a->directory, a->queue_name, (long)i);
        /* Caller must free each returned string */
        if (mmapqi_queue_file_exists(path)) {
            paths_out[count] = strdup(path);
            if (paths_out[count] == NULL) {
                for (int j = 0; j < count; j++) free(paths_out[j]);
                return MMAPQ_ENOMEM;
            }
            count++;
        }
    }

    /* Fully covered payload files per appender */
    uint8_t ids[MAX_APPENDERS];
    int n_ids = mmapqi_queue_files_get_appender_ids(
        a->directory, a->queue_name, ids, MAX_APPENDERS);
    for (int k = 0; k < n_ids && count < max_paths; k++) {
        AppenderMeta *am = get_appender_meta(a, ids[k]);
        if (!am) continue;
        int64_t start_off    = appender_meta_start_offset(am);
        int64_t full_payload = start_off / max_file_size;
        for (int64_t fi = 0; fi < full_payload && count < max_paths; fi++) {
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s/payload_%u_%ld",
                     a->directory, a->queue_name, (unsigned)ids[k], (long)fi);
            if (mmapqi_queue_file_exists(path)) {
                paths_out[count] = strdup(path);
                if (paths_out[count] == NULL) {
                    for (int j = 0; j < count; j++) free(paths_out[j]);
                    return MMAPQ_ENOMEM;
                }
                count++;
            }
        }
    }

    return count;
}

int mmapq_indexed_batch_archiver_archivable_files(
    MmapqIndexedBatchArchiver *a,
    char                **paths_out,
    int                   max_paths)
{
    if (a == NULL || paths_out == NULL) return 0;

    /* Shared lock: a listing never observes an archive in progress (I6).
     * A missing queue directory yields an empty listing, matching the
     * uninitialised-queue outcome below. */
    int lock_fd;
    int lrc = mmapqi_archive_lock_acquire(a->directory, a->queue_name,
                                          0, &lock_fd);
    if (lrc == MMAPQ_EINVAL) return 0;
    if (lrc != 0) return lrc;

    int n = archivable_files_locked(a, paths_out, max_paths);
    close(lock_fd);
    return n;
}

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

void mmapq_indexed_batch_archiver_close(MmapqIndexedBatchArchiver *a)
{
    if (!a) return;

    for (int i = 0; i < MAX_APPENDERS; i++) {
        if (a->iterators[i])      { free(a->iterators[i]); }
        if (a->appender_metas[i]) { mmapqi_appender_meta_close(a->appender_metas[i]); }
    }
    if (a->ro_acc)  mmapqi_queue_region_accessor_close(a->ro_acc);
    if (a->ro_meta) mmapqi_queue_meta_close(a->ro_meta);
    if (a->rw_meta) mmapqi_queue_meta_close(a->rw_meta);

    free(a->directory);
    free(a->queue_name);
    free(a);
}
