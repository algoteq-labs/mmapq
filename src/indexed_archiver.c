#include "indexed_archiver.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/queue_files.h"
#include "internal/appender_meta.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/payload_walk.h"
#include "internal/test_hooks.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <unistd.h>
#include <errno.h>

#define OFFSET_NOT_SET ((int64_t)-1)

/* ── Internal struct ─────────────────────────────────────────────────────── */

struct MmapqIndexedArchiver {
    char        *directory;
    char        *queue_name;
    int          ring_size;
    MmapqRuntime     *runtime;

    /* Lazy-initialised on first use */
    QueueMeta           *ro_meta;
    QueueMeta           *rw_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;
    Buffer               header_buf;

    /* Per-appender meta — opened lazily, keyed by appender_id (0–255) */
    AppenderMeta *appender_metas[MAX_APPENDERS];

    int meta_init_failed;
};

/* ── Lazy initialisers ───────────────────────────────────────────────────── */

static QueueMeta *ro_init(MmapqIndexedArchiver *a)
{
    if (a->ro_meta) return a->ro_meta;
    if (a->meta_init_failed) return NULL;

    int rc = mmapqi_queue_meta_open_ro(a->directory, a->queue_name, QUEUE_TYPE_NORMAL, &a->ro_meta);
    if (rc != 0) {
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            a->meta_init_failed = 1;
        return NULL;
    }

    /* Sync mapping (regions_to_map_ahead = 0): the archiver does a single
     * sequential pass over headers and never benefits from background premap. */
    int acc_rc = mmapqi_queue_region_accessor_create_ro(
        a->directory, a->queue_name,
        queue_meta_region_size(a->ro_meta),
        a->ring_size,
        0,   /* regions_to_map_ahead — sync, no background mapper needed */
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

/* Note: rw_meta is opened with the current on-disk values — the start_index
 * argument is only used when creating a brand-new file (st.st_size == 0).
 * Since the meta file already exists by the time rw_meta() is called,
 * all fields are adopted from disk.
 *
 * Dual mapping: ro_meta and rw_meta are two MAP_SHARED views of the same file.
 * Linux guarantees coherence across MAP_SHARED mappings of the same page — a
 * write through rw_meta is immediately visible via ro_meta on all cores.
 * queue_meta_set_start_index(rwm, index) writes atomically; the next
 * queue_meta_start_index() read through ro_meta will see the new value. */
static QueueMeta *rw_meta_open(MmapqIndexedArchiver *a)
{
    if (a->rw_meta) return a->rw_meta;
    if (mmapqi_queue_meta_open_rw(a->directory, a->queue_name,
        queue_meta_start_index(a->ro_meta),
        queue_meta_region_size(a->ro_meta),
        queue_meta_max_file_size(a->ro_meta),
        queue_meta_archivable(a->ro_meta),
        QUEUE_TYPE_NORMAL,
        &a->rw_meta) != 0) { a->rw_meta = NULL; }
    return a->rw_meta;
}

static AppenderMeta *get_appender_meta(MmapqIndexedArchiver *a, uint8_t id)
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

/* ── Test hook (P15) — never compiled into release builds ────────────────── */

#ifdef MMAPQ_TEST_HOOKS
static int (*live_pass_hook)(void);
void mmapq_test_hook_set_archiver_live_pass(int (*fn)(void)) { live_pass_hook = fn; }
static void (*publish_pre_hook)(void);
void mmapq_test_hook_set_archiver_publish_pre(void (*fn)(void)) { publish_pre_hook = fn; }
static void (*reclaim_pre_unlink_hook)(void);
void mmapq_test_hook_set_reclaim_pre_unlink(void (*fn)(void)) { reclaim_pre_unlink_hook = fn; }
#endif

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapq_indexed_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    MmapqRuntime    *runtime,
    MmapqIndexedArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (ring_size <= 0) return MMAPQ_EINVAL;

    MmapqIndexedArchiver *a = calloc(1, sizeof(MmapqIndexedArchiver));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory  = strdup(directory);
    if (a->directory == NULL) { free(a); return MMAPQ_ENOMEM; }
    a->queue_name = strdup(queue_name);
    if (a->queue_name == NULL) { free(a->directory); free(a); return MMAPQ_ENOMEM; }
    a->ring_size  = ring_size;
    a->runtime    = runtime;
    *out = a;
    return 0;
}

/* ── Archiving ───────────────────────────────────────────────────────────── */

int mmapq_indexed_archiver_archivable(MmapqIndexedArchiver *a)
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
static void rollback_start_offsets(MmapqIndexedArchiver *a, const int64_t *old)
{
    for (int aid = 0; aid < MAX_APPENDERS; aid++) {
        if (old[aid] == OFFSET_NOT_SET) continue;
        appender_meta_start_offset_min(get_appender_meta(a, (uint8_t)aid),
                                       old[aid]);
    }
}

/* ── Forward archiving, variable-size payloads (CAND-30) ───────────────────
 *
 * The deletion boundary is computed from headers, not write order:
 * resequenced commits (append_uncommitted + commit) can place a live payload
 * below an archived one. The candidate is the end of the archived entry with
 * the MAXIMUM payload position; after publishing, one pass over the live
 * headers lowers the boundary below any live payload sitting under it.
 *
 * Ordering: the archiver raises start_offset by CAS from the snapshot
 * (acq_rel), seq_cst fence, then loads headers (acquire); the appender
 * stores its header (acq_rel CAS), seq_cst fence, then loads start_offset
 * (acquire). The two fences carry the store-buffering argument: at least
 * one side observes the other, so every live payload ends at or above
 * start_offset once the archive returns. The CAS from the snapshot is
 * what keeps a commit's lowering that lands between the snapshot and the
 * publish from being erased by the publish (CAND-37). The residual window
 * — a commit between its CAS and its re-check — is covered by the caller's
 * deletion re-validation (doc/archiving-design.md). */
static int archive_forward_variable(MmapqIndexedArchiver *a,
                                    int64_t current, int64_t index)
{
    int64_t max_pos[MAX_APPENDERS];  /* highest archived payload position   */
    int64_t ends[MAX_APPENDERS];     /* end of that entry's message         */
    int64_t old[MAX_APPENDERS];      /* boundary before this call           */
    for (int i = 0; i < MAX_APPENDERS; i++) {
        max_pos[i] = OFFSET_NOT_SET;
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

        RegionAccessor *pay_acc =
            mmapqi_queue_region_accessor_payload(a->ro_acc, aid);
        Buffer pay_buf = {0};
        WrapResult wr = region_accessor_wrap(pay_acc, pos, &pay_buf);
        if (wr == WRAP_MAP_TIMED_OUT)
            return MMAPQ_ETIMEDOUT;
        if (wr != WRAP_SUCCESS)
            return MMAPQ_ECORRUPT;

        /* Headers must reference a complete message in this mapping.
         * A bogus length must not advance the deletion boundary. */
        if (pay_buf.capacity < PAYLOAD_LENGTH_SIZE)
            return MMAPQ_ECORRUPT;
        int32_t len;
        memcpy(&len, pay_buf.data + PAYLOAD_LENGTH_OFFSET, PAYLOAD_LENGTH_SIZE);
        if (len <= 0 || len > pay_buf.capacity - PAYLOAD_LENGTH_SIZE)
            return MMAPQ_ECORRUPT;

        if (pos > max_pos[aid]) {
            max_pos[aid] = pos;
            ends[aid]    = pos + PAYLOAD_LENGTH_SIZE + len;
        }
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
#ifdef MMAPQ_TEST_HOOKS
    /* CAND-37 pin: park between the start_offset snapshot and the publish */
    if (publish_pre_hook) publish_pre_hook();
#endif

    /* Publish the candidate boundary (never lowered here), then start_index.
     * CAND-37: raise by CAS from the snapshot — a commit re-check that
     * lowered start_offset between the snapshot and this publish must not
     * be erased; the CAS fails then and the lower value stands (the live
     * pass and rollback only lower further). */
    for (int aid = 0; aid < MAX_APPENDERS; aid++) {
        if (ends[aid] == OFFSET_NOT_SET) continue;
        AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
        if (m == NULL) return MMAPQ_ECORRUPT;
        if (ends[aid] > old[aid])
            (void)appender_meta_start_offset_cas(m, old[aid], ends[aid]);
    }
    queue_meta_set_start_index(rwm, index);

    /* One pass over the live headers: lower the boundary below any live
     * payload sitting under it. The checked read is required — the plain
     * read maps a mapping timeout to HEADER_NULL and would end the pass
     * early, silently leaving the boundary too high. */
    atomic_thread_fence(memory_order_seq_cst);
    for (int64_t i = index; ; i++) {
        uint64_t hdr;
#ifdef MMAPQ_TEST_HOOKS
        /* P15: inject at the wrap-result level so the plain read (mutation
         * M11b) collapses the timeout to end-of-stream while the checked
         * read reports MMAPQ_ETIMEDOUT */
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

static int archive_up_to_index_locked(MmapqIndexedArchiver *a, int64_t index)
{
    QueueMeta *rom = ro_init(a);
    if (rom == NULL || !queue_meta_archivable(rom)) return MMAPQ_EINVAL;

    int64_t current   = queue_meta_start_index(rom);
    if (index == current) return 0; /* no-op */

    int64_t max_file_size = queue_meta_max_file_size(rom);
    int     fixed_size    = queue_meta_fixed_size_8bytes(rom);

    if (index > current) {
        /* ── Forward archiving ─────────────────────────────────────────── */

        if (!fixed_size)
            return archive_forward_variable(a, current, index);

        /* Fixed-size queue: just validate the boundary entry exists */
        if (!header_found(a->header_acc, &a->header_buf,
                          index - 1, a->queue_name))
            return MMAPQ_EINVAL;

    } else {
        /* ── Backward archiving (undo) ─────────────────────────────────── */

        int64_t headers_in_file = max_file_size / HEADER_SIZE;

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
             * file numbers, whatever order they arrive in — resequenced
             * commits (append_uncommitted + commit) make payload file order
             * non-monotonic. A 0 means the undo genuinely spans more than
             * FILE_INDEX_SET_MAX files — refuse the undo as posed with
             * EINVAL, not ECORRUPT: the data is fine, the request is
             * unservable. Remedy: undo in steps spanning
             * < FILE_INDEX_SET_MAX files each. */
            if (!file_index_set_set(&hdr_files, i / headers_in_file))
                return MMAPQ_EINVAL;

            if (!fixed_size) {
                uint8_t aid = header_appender_id(hdr);
                int64_t pos = header_payload_position(hdr);
                /* Resequenced commits make payload order non-monotonic; the
                 * boundary must cover the LOWEST restored position. */
                if (first_positions[aid] == OFFSET_NOT_SET ||
                    pos < first_positions[aid])
                    first_positions[aid] = pos;
                if (!file_index_set_set(&pay_files[aid], pos / max_file_size))
                    return MMAPQ_EINVAL;  /* payload span too wide — undo in smaller steps */
            }
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

int mmapq_indexed_archiver_archive_up_to_index(MmapqIndexedArchiver *a, int64_t index)
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

int64_t mmapq_indexed_archiver_start_index(MmapqIndexedArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_start_index(m);
}

int mmapq_indexed_archiver_start_offset(
    MmapqIndexedArchiver *a, uint8_t appender_id, int64_t *out_offset)
{
    if (a == NULL || out_offset == NULL) return 0;

    if (!ro_init(a)) return 0;

    AppenderMeta *am = get_appender_meta(a, appender_id);
    if (!am) return 0;

    *out_offset = appender_meta_start_offset(am);
    return 1;
}

static int archivable_files_locked(
    MmapqIndexedArchiver  *a,
    char            **paths_out,
    int               max_paths)
{

    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;

    int64_t start = queue_meta_start_index(m);
    if (start <= QUEUE_META_INITIAL_START_INDEX) return 0;

    int64_t max_file_size   = queue_meta_max_file_size(m);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;
    int     count = 0;

    /* Fully covered header files: files whose entire index range < start */
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

    /* Fully covered payload files: files whose byte range < start_offset */
    if (!queue_meta_fixed_size_8bytes(m)) {
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
    }

    return count;
}

int mmapq_indexed_archiver_archivable_files(
    MmapqIndexedArchiver  *a,
    char            **paths_out,
    int               max_paths)
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

int mmapq_indexed_archiver_reclaim_files(
    MmapqIndexedArchiver *a,
    char                **marked_paths,
    int                   n_marked)
{
    if (a == NULL || n_marked < 0 ||
        (marked_paths == NULL && n_marked > 0))
        return MMAPQ_EINVAL;

    /* CAND-36: the re-list AND the unlink happen in ONE shared hold of
     * archive.lock (the model's AtomicDelete). Undo takes the lock
     * exclusively, so it cannot slip between the two steps and have its
     * restored files deleted — the interleaving that made the hand
     * protocol lose live data. SH (not EX): only the archiver must be
     * excluded; concurrent listers/reclaimers are admitted, which is why
     * ENOENT below is tolerated. The hold is bounded: a complete listing
     * (meta reads + directory stats — nothing that blocks on another
     * actor) plus at most |listed ∩ marked| unlink() calls. */
    int lock_fd;
    int lrc = mmapqi_archive_lock_acquire(a->directory, a->queue_name,
                                          0, &lock_fd);
    if (lrc == MMAPQ_EINVAL) return 0;
    if (lrc != 0) return lrc;

    if (n_marked == 0) {
        close(lock_fd);
        return 0;
    }

    /* The re-list must be COMPLETE: archivable_files_locked enumerates
     * header files first, then payload lanes in appender-id order, and
     * truncates at capacity — a listing capped at the mark count can
     * starve a mark whose files sort after other lanes' (B1). Grow and
     * re-list until nothing was truncated. Terminates: under this SH hold
     * the listing can only shrink (commit re-checks lower start_offset
     * without the lock; every raiser holds EX, excluded here). */
    int cap = n_marked;
    char **list = calloc((size_t)cap, sizeof *list);
    if (list == NULL) {
        close(lock_fd);
        return MMAPQ_ENOMEM;
    }
    int n;
    for (;;) {
        n = archivable_files_locked(a, list, cap);
        if (n < 0) {          /* listing frees its own partial strings */
            free(list);
            close(lock_fd);
            return n;
        }
        if (n < cap) break;   /* complete */
        for (int i = 0; i < n; i++) free(list[i]);
        cap *= 2;
        char **nl = realloc(list, (size_t)cap * sizeof *nl);
        if (nl == NULL) {
            free(list);
            close(lock_fd);
            return MMAPQ_ENOMEM;
        }
        list = nl;
    }

#ifdef MMAPQ_TEST_HOOKS
    if (reclaim_pre_unlink_hook != NULL) reclaim_pre_unlink_hook();
#endif

    /* Unlink the overlap. Every unlink was validated by the in-hold
     * re-list, so partial progress is safe: a retry re-lists and skips
     * what is already gone. ENOENT means a concurrent reclaimer (SH)
     * beat us to it — the goal is met; not counted, not an error. */
    int deleted = 0;
    int rc = 0;
    for (int i = 0; i < n; i++) {
        int marked = 0;
        for (int k = 0; k < n_marked; k++)
            if (strcmp(list[i], marked_paths[k]) == 0) { marked = 1; break; }
        if (!marked) continue;
        if (unlink(list[i]) == 0) { deleted++; continue; }
        if (errno == ENOENT) continue;
        rc = MMAPQ_EIO;
        break;
    }

    for (int i = 0; i < n; i++) free(list[i]);
    free(list);
    close(lock_fd);
    return rc != 0 ? rc : deleted;
}

static int64_t archiver_start_index_fn(void *ctx)
{
    return queue_meta_start_index((QueueMeta *)ctx);
}

int mmapq_indexed_archiver_print_summary(MmapqIndexedArchiver *a, char *sb, int n)
{
    if (a == NULL || sb == NULL || n <= 0) return 0;

    QueueMeta *m = ro_init(a);
    if (!m) {
        return snprintf(sb, (size_t)n, "Queue %s/%s is not initialised.\n",
                        a->directory, a->queue_name);
    }

#define APPEND(...) do { \
    if (written < n) \
        written += snprintf(sb + written, (size_t)(n - written), __VA_ARGS__); \
} while (0)

    int written = 0;
    APPEND("Queue Summary: %s\n"
           "Directory:     %s\n"
           "Start Index:   %ld\n"
           "Fixed Size 8B: %s\n"
           "--------------------------------------------------\n",
           a->queue_name, a->directory,
           (long)queue_meta_start_index(m),
           queue_meta_fixed_size_8bytes(m) ? "yes" : "no");

    int64_t max_file_size   = queue_meta_max_file_size(m);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;
    int64_t last_index      = mmapqi_header_search_last_index(
        0, archiver_start_index_fn, m,
        a->header_acc, &a->header_buf, a->queue_name);

    if (last_index == HEADER_NULL_INDEX) {
        APPEND("Queue is empty.\n");
        return written;
    }

    APPEND("End Index: %ld\n"
           "--------------------------------------------------\n"
           "Header Files:\n", (long)last_index);

    for (int64_t fi = 0; fi * headers_in_file <= last_index; fi++) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/header_%ld",
                 a->directory, a->queue_name, (long)fi);
        if (!mmapqi_queue_file_exists(path)) continue;
        int64_t s = fi * headers_in_file;
        int64_t e = (fi + 1) * headers_in_file - 1;
        if (e > last_index) e = last_index;
        APPEND("  header_%ld: [%ld, %ld]\n", (long)fi, (long)s, (long)e);
    }

    if (!queue_meta_fixed_size_8bytes(m)) {
        APPEND("\nPayload Files:\n");
        uint8_t ids[MAX_APPENDERS];
        int n_ids = mmapqi_queue_files_get_appender_ids(
            a->directory, a->queue_name, ids, MAX_APPENDERS);
        for (int k = 0; k < n_ids; k++) {
            APPEND("  Appender %u:\n", (unsigned)ids[k]);
            for (int64_t fi = 0; ; fi++) {
                char path[4096];
                snprintf(path, sizeof(path), "%s/%s/payload_%u_%ld",
                         a->directory, a->queue_name, (unsigned)ids[k], (long)fi);
                if (!mmapqi_queue_file_exists(path)) break;
                APPEND("    payload_%u_%ld\n", (unsigned)ids[k], (long)fi);
            }
        }
    }

#undef APPEND

    return written;
}

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

void mmapq_indexed_archiver_close(MmapqIndexedArchiver *a)
{
    if (!a) return;

    for (int i = 0; i < MAX_APPENDERS; i++) {
        if (a->appender_metas[i])
            mmapqi_appender_meta_close(a->appender_metas[i]);
    }
    if (a->ro_acc)  mmapqi_queue_region_accessor_close(a->ro_acc);
    if (a->ro_meta) mmapqi_queue_meta_close(a->ro_meta);
    if (a->rw_meta) mmapqi_queue_meta_close(a->rw_meta);

    free(a->directory);
    free(a->queue_name);
    free(a);
}
