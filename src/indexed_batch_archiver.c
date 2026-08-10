#include "indexed_batch_archiver.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "queue_files.h"
#include "appender_meta.h"
#include "header_reader.h"
#include "header_codec.h"
#include "batch_payload_iterator.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define OFFSET_NOT_SET ((int64_t)-1)

/* ── Internal struct ─────────────────────────────────────────────────────── */

struct IndexedBatchArchiver {
    char        *directory;
    char        *queue_name;
    int          ring_size;
    Runtime     *runtime;

    QueueMeta           *ro_meta;
    QueueMeta           *rw_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;
    Buffer               header_buf;

    AppenderMeta         *appender_metas[MAX_APPENDERS];
    BatchPayloadIterator *iterators[MAX_APPENDERS]; /* reused across archive calls */
};

/* ── Lazy initialisers ───────────────────────────────────────────────────── */

static QueueMeta *ro_init(IndexedBatchArchiver *a)
{
    if (a->ro_meta) return a->ro_meta;
    if (queue_meta_open_ro(a->directory, a->queue_name, QUEUE_TYPE_BATCH, &a->ro_meta) != 0)
        return NULL;
    /* Sync mapping: archiver does a single sequential pass, no premap benefit */
    if (queue_region_accessor_create_ro(
        a->directory, a->queue_name,
        queue_meta_region_size(a->ro_meta),
        a->ring_size,
        0,   /* regions_to_map_ahead = 0 — sync, no background mapper needed */
        queue_meta_max_file_size(a->ro_meta),
        10000000000ULL, a->runtime,
        &a->ro_acc) != 0) { return NULL; }
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
static QueueMeta *rw_meta_open(IndexedBatchArchiver *a)
{
    if (a->rw_meta) return a->rw_meta;
    if (queue_meta_open_rw(a->directory, a->queue_name,
        queue_meta_start_index(a->ro_meta),
        queue_meta_region_size(a->ro_meta),
        queue_meta_max_file_size(a->ro_meta),
        queue_meta_archivable(a->ro_meta),
        QUEUE_TYPE_BATCH,
        &a->rw_meta) != 0) { a->rw_meta = NULL; }
    return a->rw_meta;
}

static AppenderMeta *get_appender_meta(IndexedBatchArchiver *a, uint8_t id)
{
    if (!a->appender_metas[id]) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/appender_meta_%u",
                 a->directory, a->queue_name, (unsigned)id);
        if (!queue_file_exists(path)) return NULL;
        if (appender_meta_open_rw(
                a->directory, a->queue_name, id, &a->appender_metas[id]) != 0) return NULL;
    }
    return a->appender_metas[id];
}

static BatchPayloadIterator *get_iterator(IndexedBatchArchiver *a, uint8_t id)
{
    if (!a->iterators[id]) {
        BatchPayloadIterator *it = calloc(1, sizeof(BatchPayloadIterator));
        if (it == NULL) return NULL;
        it->queue_name  = a->queue_name;
        it->payload_acc = queue_region_accessor_payload(a->ro_acc, id);
        a->iterators[id] = it;
    }
    return a->iterators[id];
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int indexed_batch_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    Runtime    *runtime,
    IndexedBatchArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    IndexedBatchArchiver *a = calloc(1, sizeof(IndexedBatchArchiver));
    if (a == NULL) return MMAPQ_ENOMEM;

    a->directory  = strdup(directory);
    a->queue_name = strdup(queue_name);
    a->ring_size  = ring_size > 0 ? ring_size : 4;
    a->runtime    = runtime;
    *out = a;
    return 0;
}

/* ── Archiving ───────────────────────────────────────────────────────────── */

int indexed_batch_archiver_archivable(IndexedBatchArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_archivable(m);
}

int indexed_batch_archiver_archive_up_to_index(IndexedBatchArchiver *a, int64_t index)
{
    if (a == NULL || index < 0) return MMAPQ_EINVAL;

    QueueMeta *rom = ro_init(a);
    if (rom == NULL || !queue_meta_archivable(rom)) return MMAPQ_EINVAL;

    int64_t current       = queue_meta_start_index(rom);
    if (index == current) return 0;

    int64_t max_file_size   = queue_meta_max_file_size(rom);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;

    if (index > current) {
        /* ── Forward archiving ─────────────────────────────────────────── */

        /* Walk backwards: putIfAbsent — captures the highest-index (most
         * recent) committed batch payload position per appender. */
        int64_t last_positions[MAX_APPENDERS];
        int64_t next_offsets[MAX_APPENDERS];
        for (int i = 0; i < MAX_APPENDERS; i++) {
            last_positions[i] = OFFSET_NOT_SET;
            next_offsets[i]   = OFFSET_NOT_SET;
        }

        for (int64_t i = index - 1; i >= current; i--) {
            uint64_t hdr = header_read(a->header_acc, &a->header_buf,
                                       i, a->queue_name);
            if (hdr == HEADER_NULL)
                return MMAPQ_EINVAL;
            uint8_t aid = header_appender_id(hdr);
            int64_t pos = header_payload_position(hdr);
            if (last_positions[aid] == OFFSET_NOT_SET)
                last_positions[aid] = pos;
        }

        /* Exhaust the batch at last_position to find nextPayloadPosition */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            int64_t pos = last_positions[aid];
            if (pos == OFFSET_NOT_SET) continue;

            BatchPayloadIterator *it = get_iterator(a, (uint8_t)aid);
            if (it == NULL) return MMAPQ_ENOMEM;
            int32_t msg_count = batch_iterator_read_message_count(it, pos);
            if (msg_count <= 0)
                return MMAPQ_ECORRUPT;

            batch_iterator_init(it, pos, msg_count, 0);
            batch_iterator_exhaust(it);
            next_offsets[aid] = batch_iterator_next_payload_position(it);
        }

        /* Validate all appender metas are accessible before mutating */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (next_offsets[aid] == OFFSET_NOT_SET) continue;
            if (get_appender_meta(a, (uint8_t)aid) == NULL)
                return MMAPQ_ECORRUPT;
        }

        /* Update appender metas */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (next_offsets[aid] == OFFSET_NOT_SET) continue;
            AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
            appender_meta_set_start_offset(m, next_offsets[aid]);
        }

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
            uint64_t hdr = header_read(a->header_acc, &a->header_buf,
                                       i, a->queue_name);
            if (hdr == HEADER_NULL)
                return MMAPQ_EINVAL;

            file_index_set_set(&hdr_files, (int)(i / headers_in_file));

            uint8_t aid = header_appender_id(hdr);
            int64_t pos = header_payload_position(hdr);

            if (first_positions[aid] == OFFSET_NOT_SET)
                first_positions[aid] = pos;

            /* Track all payload files spanned by this batch's messages */
            BatchPayloadIterator *it = get_iterator(a, aid);
            if (it == NULL) return MMAPQ_ENOMEM;
            int32_t msg_count = batch_iterator_read_message_count(it, pos);
            if (msg_count <= 0)
                return MMAPQ_ECORRUPT;
            batch_iterator_init(it, pos, msg_count, 0);
            /* Record the file containing the batch header itself */
            file_index_set_set(&pay_files[aid], (int)(pos / max_file_size));
            /* Record files spanned by each message's end offset */
            while (batch_iterator_advance(it)) {
                int64_t msg_end = batch_iterator_next_payload_position(it);
                file_index_set_set(&pay_files[aid],
                                   (int)(msg_end / max_file_size));
            }
        }

        /* Validate all required header files exist */
        for (int fi = file_index_set_first(&hdr_files); fi >= 0;
             fi = file_index_set_next(&hdr_files, fi)) {
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s/header_%d",
                     a->directory, a->queue_name, fi);
            if (!queue_file_exists(path))
                return MMAPQ_ECORRUPT;
        }

        /* Validate all required payload files exist */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            if (first_positions[aid] < INITIAL_PAYLOAD_POSITION)
                return MMAPQ_ECORRUPT;
            for (int fi = file_index_set_first(&pay_files[aid]); fi >= 0;
                 fi = file_index_set_next(&pay_files[aid], fi)) {
                char path[4096];
                snprintf(path, sizeof(path), "%s/%s/payload_%d_%d",
                         a->directory, a->queue_name, aid, fi);
                if (!queue_file_exists(path))
                    return MMAPQ_ECORRUPT;
            }
        }

        /* Validate all appender metas are accessible before mutating */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            if (get_appender_meta(a, (uint8_t)aid) == NULL)
                return MMAPQ_ECORRUPT;
        }

        /* All files present — update appender metas */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
            appender_meta_set_start_offset(m, first_positions[aid]);
        }
    }

    /* Commit new start_index atomically — visible to all readers immediately.
     * rw_meta_open can fail from external I/O, which is returned rather than
     * asserted. If it fails after appender meta updates above, the metas are
     * already committed but the start_index is not — on retry the function
     * recomputes identical offsets, making the meta writes idempotent. */
    QueueMeta *rwm = rw_meta_open(a);
    if (rwm == NULL) return MMAPQ_EIO;
    queue_meta_set_start_index(rwm, index);
    return 0;
}

/* ── Query ───────────────────────────────────────────────────────────────── */

int64_t indexed_batch_archiver_start_index(IndexedBatchArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_start_index(m);
}

int indexed_batch_archiver_start_offset(
    IndexedBatchArchiver *a, uint8_t appender_id, int64_t *out_offset)
{
    if (a == NULL || out_offset == NULL) return 0;

    if (!ro_init(a)) return 0;

    AppenderMeta *am = get_appender_meta(a, appender_id);
    if (!am) return 0;

    *out_offset = appender_meta_start_offset(am);
    return 1;
}

int indexed_batch_archiver_archivable_files(
    IndexedBatchArchiver *a,
    char                **paths_out,
    int                   max_paths)
{
    if (a == NULL || paths_out == NULL) return 0;

    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;

    int64_t start = queue_meta_start_index(m);
    if (start <= QUEUE_META_INITIAL_START_INDEX) return 0;

    int64_t max_file_size   = queue_meta_max_file_size(m);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;
    int     count = 0;

    /* Fully covered header files */
    int full_header_files = (int)((start - 1) / headers_in_file);
    for (int i = 0; i < full_header_files && count < max_paths; i++) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/header_%d",
                 a->directory, a->queue_name, i);
        /* Caller must free each returned string */
        if (queue_file_exists(path))
            paths_out[count++] = strdup(path);
    }

    /* Fully covered payload files per appender */
    uint8_t ids[MAX_APPENDERS];
    int n_ids = queue_files_get_appender_ids(
        a->directory, a->queue_name, ids, MAX_APPENDERS);
    for (int k = 0; k < n_ids && count < max_paths; k++) {
        AppenderMeta *am = get_appender_meta(a, ids[k]);
        if (!am) continue;
        int64_t start_off    = appender_meta_start_offset(am);
        int     full_payload = (int)(start_off / max_file_size);
        for (int fi = 0; fi < full_payload && count < max_paths; fi++) {
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s/payload_%u_%d",
                     a->directory, a->queue_name, (unsigned)ids[k], fi);
            if (queue_file_exists(path))
                paths_out[count++] = strdup(path);
        }
    }

    return count;
}

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

void indexed_batch_archiver_close(IndexedBatchArchiver *a)
{
    if (!a) return;

    for (int i = 0; i < MAX_APPENDERS; i++) {
        if (a->iterators[i])      { free(a->iterators[i]); }
        if (a->appender_metas[i]) { appender_meta_close(a->appender_metas[i]); }
    }
    if (a->ro_acc)  queue_region_accessor_close(a->ro_acc);
    if (a->ro_meta) queue_meta_close(a->ro_meta);
    if (a->rw_meta) queue_meta_close(a->rw_meta);

    free(a->directory);
    free(a->queue_name);
    free(a);
}
