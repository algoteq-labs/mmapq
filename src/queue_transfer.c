#include "queue_transfer.h"
#include "internal/queue_meta.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "internal/export_meta.h"
#include "internal/appender_cache.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

/* Internal accessors — declared in src/internal/queue_meta.h */
QueueMeta *mmapqi_indexed_reader_queue_meta(struct MmapqIndexedReader *r);
QueueMeta *mmapqi_indexed_batch_reader_queue_meta(struct MmapqIndexedBatchReader *r);

/* ── Internal helpers ────────────────────────────────────────────────────── */

/* Create a directory (and parents).  Returns 0 on success, -1 on error. */

static int create_dir(const char *path)
{
    if (mkdir(path, 0775) != 0 && errno != EEXIST) return -1;
    return 0;
}

static int write_all(FILE *f, const void *buf, size_t n)
{
    return fwrite(buf, 1, n, f) == n ? 0 : -1;
}

static int read_all(FILE *f, void *buf, size_t n)
{
    return fread(buf, 1, n, f) == n ? 0 : -1;
}

/* ── Typed bridge functions for AppenderCache (avoids UB casts) ──────────── */

static int appender_create_bridge(void *q, void **out)
{
    return mmapq_indexed_queue_create_appender((MmapqIndexedQueue *)q, (MmapqIndexedAppender **)out);
}

static int appender_create_with_id_bridge(void *q, uint8_t id, void **out)
{
    return mmapq_indexed_queue_create_appender_with_id((MmapqIndexedQueue *)q, id, (MmapqIndexedAppender **)out);
}

static void appender_close_bridge(void *a)
{
    mmapq_indexed_appender_close((MmapqIndexedAppender *)a);
}

static void queue_close_bridge(void *q)
{
    mmapq_indexed_queue_close((MmapqIndexedQueue *)q);
}

static int write_framed_entry(FILE *out, uint16_t aid, const void *data, int32_t len)
{
    if (write_all(out, &aid, sizeof(uint16_t))) return MMAPQ_EIO;
    if (write_all(out, &len,  sizeof(int32_t)))  return MMAPQ_EIO;
    if (len > 0 && write_all(out, data, (size_t)len)) return MMAPQ_EIO;
    return 0;
}
typedef struct {
    int64_t start_idx;
    int64_t end_idx;
    int64_t count;
} ChunkDesc;

#define MAX_CHUNKS 4096

/* ── Export ───────────────────────────────────────────────────────────────── */

int64_t mmapq_queue_transfer_export(
    MmapqIndexedReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    MmapqQueueFilterFn include, MmapqQueueFilterFn exclude, void *ctx)
{
    if (!reader || !out) return MMAPQ_EINVAL;

    QueueMeta *qm = mmapqi_indexed_reader_queue_meta(reader);
    if (!qm) return mmapq_indexed_reader_last_error(reader);

    int64_t qstart = mmapq_indexed_reader_start_index(reader);
    int64_t qend   = mmapq_indexed_reader_last_index(reader);
    int discovery_error = mmapq_indexed_reader_last_error(reader);
    if (discovery_error != 0) return discovery_error;
    if (qend < 0) {
        /* Empty queue: write a valid header with 0 entries */
        ExportMeta meta;
        export_meta_init(&meta);
        meta.schema_version    = 1;
        meta.region_size       = queue_meta_region_size(qm);
        meta.max_file_size     = queue_meta_max_file_size(qm);
        meta.start_index       = (start_index_mode == MMAPQ_QUEUE_TRANSFER_REBASE) ? 0 : start;
        meta.total_entries     = 0;
        meta.archivable        = (uint8_t)queue_meta_archivable(qm);
        meta.queue_type = (uint8_t)queue_meta_queue_type(qm);
        uint8_t hdr[EXPORT_META_SIZE];
        export_meta_encode(&meta, hdr);
        if (write_all(out, hdr, EXPORT_META_SIZE)) return MMAPQ_EIO;
        return 0;
    }
    if (start > end) return MMAPQ_EINVAL;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;

    int no_filter = (!include && !exclude);

    /* Count matching entries */
    int64_t count = 0;
    if (no_filter) {
        count = start <= end ? end - start + 1 : 0;
    } else {
        MmapqReadingContext rc;
        for (int64_t i = start; i <= end; i++) {
            mmapq_indexed_reader_read(reader, i, &rc);
            if (rc.status == MMAPQ_ENTRY_ERROR) { int error = mmapq_indexed_reader_last_error(reader);  return error; }
            if (rc.status != MMAPQ_ENTRY_AVAILABLE) continue;
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (in && !ex) count++;
        }
    }

    /* Write ExportMeta header */
    ExportMeta meta;
    export_meta_init(&meta);
    meta.schema_version    = 1;
    meta.region_size       = queue_meta_region_size(qm);
    meta.max_file_size     = queue_meta_max_file_size(qm);
    meta.start_index       = (start_index_mode == MMAPQ_QUEUE_TRANSFER_REBASE) ? 0 : start;
    meta.total_entries     = count;
    meta.archivable        = (uint8_t)queue_meta_archivable(qm);
    meta.queue_type = (uint8_t)queue_meta_queue_type(qm);

    uint8_t hdr[EXPORT_META_SIZE];
    export_meta_encode(&meta, hdr);
    if (write_all(out, hdr, EXPORT_META_SIZE)) return MMAPQ_EIO;

    /* Write matching entries */
    MmapqReadingContext rc;
    int64_t written = 0;
    for (int64_t i = start; i <= end; i++) {
        mmapq_indexed_reader_read(reader, i, &rc);
        if (rc.status == MMAPQ_ENTRY_ERROR) { int error = mmapq_indexed_reader_last_error(reader);  return error; }
        if (rc.status != MMAPQ_ENTRY_AVAILABLE) continue;
        if (!no_filter) {
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
        }
        if (write_framed_entry(out, rc.appender_id, rc.data, rc.length)) return MMAPQ_EIO;
        written++;
    }
    return written == count ? written : MMAPQ_ECORRUPT;
}

/* ── Chunked export ──────────────────────────────────────────────────────── */

int64_t mmapq_queue_transfer_export_chunked(
    MmapqIndexedReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    MmapqQueueFilterFn include, MmapqQueueFilterFn exclude, void *ctx)
{
    if (!reader || !base_path || chunk_size_mb <= 0) return MMAPQ_EINVAL;
    if (start > end) return MMAPQ_EINVAL;

    QueueMeta *qm = mmapqi_indexed_reader_queue_meta(reader);
    if (!qm) return mmapq_indexed_reader_last_error(reader);

    int64_t qstart = mmapq_indexed_reader_start_index(reader);
    int64_t qend   = mmapq_indexed_reader_last_index(reader);
    int discovery_error = mmapq_indexed_reader_last_error(reader);
    if (discovery_error != 0) return discovery_error;
    if (qend < 0) return 0;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;
    if (start > end) return 0;

    int64_t chunk_max = (int64_t)chunk_size_mb * 1024 * 1024;

    /* First pass: count matching entries and determine chunk boundaries */
    ChunkDesc chunks[MAX_CHUNKS];
    int nchunks = 0;
    int64_t chunk_start = start;
    int64_t chunk_cnt   = 0;
    int64_t chunk_bytes = EXPORT_META_SIZE;

    MmapqReadingContext rc;
    for (int64_t i = start; i <= end; i++) {
        mmapq_indexed_reader_read(reader, i, &rc);
        if (rc.status == MMAPQ_ENTRY_ERROR) { int error = mmapq_indexed_reader_last_error(reader);  return error; }
        if (rc.status != MMAPQ_ENTRY_AVAILABLE) continue;
        int in = include ? include(i, rc.data, rc.length, ctx) : 1;
        int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
        if (!in || ex) continue;

        int64_t esize = (int64_t)sizeof(uint16_t) + sizeof(int32_t) + rc.length;
        if (chunk_cnt > 0 && chunk_bytes + esize > chunk_max) {
            if (nchunks >= MAX_CHUNKS) return MMAPQ_EINVAL;
            {
                chunks[nchunks].start_idx = chunk_start;
                chunks[nchunks].end_idx   = i - 1;
                chunks[nchunks].count     = chunk_cnt;
                nchunks++;
            }
            chunk_start = i;
            chunk_cnt   = 0;
            chunk_bytes = EXPORT_META_SIZE;
        }
        chunk_bytes += esize;
        chunk_cnt++;
    }
    /* Flush final chunk */
    if (chunk_cnt > 0) {
        if (nchunks >= MAX_CHUNKS) return MMAPQ_EINVAL;
        chunks[nchunks].start_idx = chunk_start;
        chunks[nchunks].end_idx   = end;
        chunks[nchunks].count     = chunk_cnt;
        nchunks++;
    }

    /* Second pass: write each chunk to its own file */
    int64_t cum_exported = 0;
    int64_t total_written = 0;

    for (int c = 0; c < nchunks; c++) {
        char fpath[4096];
        snprintf(fpath, sizeof(fpath), "%s_%ld-%ld",
                 base_path, (long)chunks[c].start_idx, (long)chunks[c].end_idx);

        FILE *cf = fopen(fpath, "wb");
        if (!cf) return MMAPQ_EIO;

        /* Write header */
        ExportMeta meta;
        export_meta_init(&meta);
        meta.schema_version    = 1;
        meta.region_size       = queue_meta_region_size(qm);
        meta.max_file_size     = queue_meta_max_file_size(qm);
        meta.start_index       = (start_index_mode == MMAPQ_QUEUE_TRANSFER_REBASE) ? cum_exported : chunks[c].start_idx;
        meta.total_entries     = chunks[c].count;
        meta.archivable        = (uint8_t)queue_meta_archivable(qm);
        meta.queue_type = (uint8_t)queue_meta_queue_type(qm);

        uint8_t hdr[EXPORT_META_SIZE];
        export_meta_encode(&meta, hdr);
        if (write_all(cf, hdr, EXPORT_META_SIZE)) { fclose(cf); return MMAPQ_EIO; }

        /* Write entries */
        int64_t written = 0;
        for (int64_t i = chunks[c].start_idx; i <= chunks[c].end_idx; i++) {
            mmapq_indexed_reader_read(reader, i, &rc);
            if (rc.status == MMAPQ_ENTRY_ERROR) { int error = mmapq_indexed_reader_last_error(reader); fclose(cf); return error; }
            if (rc.status != MMAPQ_ENTRY_AVAILABLE) continue;
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
            if (write_framed_entry(cf, rc.appender_id, rc.data, rc.length)) { fclose(cf); return MMAPQ_EIO; }
            written++;
        }
        if (written != chunks[c].count) { fclose(cf); return MMAPQ_ECORRUPT; }
        if (fclose(cf)) return MMAPQ_EIO;

        cum_exported  += chunks[c].count;
        total_written += chunks[c].count;
    }

    return total_written;
}

/* ── Import helpers ──────────────────────────────────────────────────────── */

/**
 * Internal import — reads framed entries from @p in and appends them to a
 * new (or existing) normal queue at @p directory / @p queue_name.
 *
 * When @p is_batch_stream is non-zero, the input is parsed as a batched
 * (batch queue) stream and flattened — each message becomes its own entry.
 * Otherwise the input is parsed as a flat (single-message) stream.
 *
 * Large payloads (> 64 KB) are heap-allocated; smaller payloads use a
 * stack buffer.
 *
 * in:             input stream positioned after the meta header
 * directory:      queue base directory
 * queue_name:     queue subdirectory name
 * start_index:    desired start index (0 for rebase)
 * region_size:    region size from stream metadata
 * max_file_size:  max file size from stream metadata
 * archivable:     archivable flag from stream metadata
 * total_entries:  number of entries (or batches) to read
 * is_batch_stream: 1 = batched stream, 0 = flat stream
 * wrt:            runtime for writers (NULL = sync)
 * rrt:            runtime for readers (NULL = sync)
 * mapper:         MmapqAppenderIdMapper callback (NULL = identity mapping)
 * mapper_ctx:     caller context passed to the mapper callback
 * on_conflict:    MMAPQ_CONFLICT_AUTO or MMAPQ_CONFLICT_ABORT
 * Returns: number of entries (individual messages) imported, or -1 on error
 */

static int64_t do_import(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index,
    int32_t region_size, int64_t max_file_size,
    uint8_t archivable,
    int64_t total_entries,
    int is_batch_stream,
    MmapqRuntime *wrt, MmapqRuntime *rrt,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (total_entries < 0) return MMAPQ_ECORRUPT;
    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", directory, queue_name);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int ring = mmapq_min_ring_size(region_size);
    if (ring < 4) ring = 4;
    MmapqIndexedQueue *q = NULL;
    /* CAND-50 §11: a created destination takes the stream's archivable
     * flag; an existing destination keeps its on-disk flag (disk wins on
     * every open) and the import proceeds. */
    int rc = mmapq_indexed_queue_create(
        directory, queue_name,
        region_size, ring, 1, max_file_size, start_index,
        10000000000ULL, archivable, wrt, rrt, &q);

    if (rc != 0) return rc;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t imported = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = appender_create_bridge,
        .create_with_id = appender_create_with_id_bridge,
        .close = appender_close_bridge,
        .queue_close = queue_close_bridge,
    };

    int error = 0;
    for (int64_t entry = 0; entry < total_entries; entry++) {
        uint16_t source_id;
        int32_t message_count = 1;
        if (read_all(in, &source_id, sizeof(source_id))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
        if (is_batch_stream) {
            if (read_all(in, &message_count, sizeof(message_count))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
            if (message_count <= 0) { error = MMAPQ_ECORRUPT; goto failed; }
        }
        int32_t mapped = appender_cache_map(mapped_cache, source_id, mapper, mapper_ctx);
        MmapqIndexedAppender *appender = appender_cache_get(&ac, mapped, q, on_conflict, &error);
        if (!appender) goto failed;

        for (int32_t message = 0; message < message_count; message++) {
            int32_t length;
            if (read_all(in, &length, sizeof(length))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
            if (length <= 0 || length > region_size - 4) { error = MMAPQ_ECORRUPT; goto failed; }
            uint8_t buffer[65536];
            uint8_t *payload = buffer;
            if ((size_t)length > sizeof(buffer)) {
                payload = malloc((size_t)length);
                if (!payload) { error = MMAPQ_ENOMEM; goto failed; }
            }
            if (read_all(in, payload, (size_t)length)) {
                error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT;
                if (payload != buffer) free(payload);
                goto failed;
            }
            int64_t appended = mmapq_indexed_appender_append(appender, payload, length);
            if (payload != buffer) free(payload);
            if (appended < 0) { error = (int)appended; goto failed; }
            imported++;
        }

    }
failed:
    appender_cache_cleanup(&ac);
    mmapq_indexed_queue_close(q);
    return error != 0 ? error : imported;
}

/* ── Import (flat stream) ─────────────────────────────────────────────────── */

int64_t mmapq_queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (!in || !directory || !queue_name) return MMAPQ_EINVAL;

    int chk = mmapqi_queue_meta_dest_check(directory, queue_name, QUEUE_TYPE_NORMAL,
                                start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    uint8_t hdr[EXPORT_META_SIZE];
    if (read_all(in, hdr, EXPORT_META_SIZE)) return ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT;

    ExportMeta meta;
    export_meta_decode(hdr, &meta);
    if (meta.total_entries < 0) return MMAPQ_ECORRUPT;
    if (meta.queue_type == QUEUE_TYPE_FIXED8) return MMAPQ_ESCHEMA;
    if (meta.queue_type != QUEUE_TYPE_NORMAL) return MMAPQ_ESCHEMA;
    int64_t effective = (start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE)
                        ? meta.start_index : 0;
    return do_import(in, directory, queue_name, effective,
                     meta.region_size, meta.max_file_size,
                     meta.archivable,
                     meta.total_entries, 0, write_rt, read_rt,
                     mapper, mapper_ctx, on_conflict);
}

int64_t mmapq_queue_transfer_import_batch_stream(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (!in || !directory || !queue_name) return MMAPQ_EINVAL;

    int chk = mmapqi_queue_meta_dest_check(directory, queue_name, QUEUE_TYPE_NORMAL,
                                start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    uint8_t hdr[EXPORT_META_SIZE];
    if (read_all(in, hdr, EXPORT_META_SIZE)) return ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT;

    ExportMeta meta;
    export_meta_decode(hdr, &meta);
    if (meta.total_entries < 0) return MMAPQ_ECORRUPT;
    if (meta.queue_type == QUEUE_TYPE_FIXED8) return MMAPQ_ESCHEMA;
    if (meta.queue_type != QUEUE_TYPE_BATCH)  return MMAPQ_ESCHEMA;
    int64_t effective = (start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE)
                        ? meta.start_index : 0;
    return do_import(in, directory, queue_name, effective,
                     meta.region_size, meta.max_file_size,
                     meta.archivable,
                     meta.total_entries, 1, write_rt, read_rt,
                     mapper, mapper_ctx, on_conflict);
}

/* ── Copy ─────────────────────────────────────────────────────────────────── */

int64_t mmapq_queue_transfer_copy(
    MmapqIndexedReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    MmapqQueueFilterFn include, MmapqQueueFilterFn exclude, void *ctx,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (!reader || !dest_dir || !dest_queue) return MMAPQ_EINVAL;
    if (start > end) return MMAPQ_EINVAL;

    int chk = mmapqi_queue_meta_dest_check(dest_dir, dest_queue, QUEUE_TYPE_NORMAL,
                                start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    QueueMeta *qm = mmapqi_indexed_reader_queue_meta(reader);
    if (!qm) return mmapq_indexed_reader_last_error(reader);

    int64_t qstart = mmapq_indexed_reader_start_index(reader);
    int64_t qend   = mmapq_indexed_reader_last_index(reader);
    int discovery_error = mmapq_indexed_reader_last_error(reader);
    if (discovery_error != 0) return discovery_error;
    if (qend < 0) return 0;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;
    if (start > end) return 0;

    int no_filter = (!include && !exclude);

    int64_t dest_start = (start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE)
                         ? qstart : 0;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", dest_dir, dest_queue);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int ring = mmapq_min_ring_size(queue_meta_region_size(qm));
    if (ring < 4) ring = 4;
    MmapqIndexedQueue *q = NULL;
    /* CAND-50 §11: a created destination takes the source queue's
     * archivable flag; an existing destination keeps its on-disk flag. */
    int create_rc = mmapq_indexed_queue_create(dest_dir, dest_queue,
        queue_meta_region_size(qm), ring, 1, queue_meta_max_file_size(qm),
        dest_start, 10000000000ULL, queue_meta_archivable(qm), write_rt, read_rt, &q);

    if (create_rc != 0) return create_rc;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t copied = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = appender_create_bridge,
        .create_with_id = appender_create_with_id_bridge,
        .close = appender_close_bridge,
        .queue_close = queue_close_bridge,
    };

    MmapqReadingContext rc;
    for (int64_t i = start; i <= end; i++) {
        mmapq_indexed_reader_read(reader, i, &rc);
        if (rc.status == MMAPQ_ENTRY_ERROR) { int error = mmapq_indexed_reader_last_error(reader); appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return error; }
        if (rc.status != MMAPQ_ENTRY_AVAILABLE) continue;
        if (!no_filter) {
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
        }

        uint16_t aid = rc.appender_id;
        int32_t mapped = appender_cache_map(mapped_cache, aid, mapper, mapper_ctx);

        int abort_flag;
        MmapqIndexedAppender *a = (MmapqIndexedAppender *)appender_cache_get(
            &ac, mapped, q, on_conflict, &abort_flag);
        if (!a) { appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return abort_flag; }

        int64_t appended = mmapq_indexed_appender_append(a, rc.data, rc.length);
        if (appended < 0) { appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return appended; }
        copied++;
    }

    appender_cache_cleanup(&ac);
    mmapq_indexed_queue_close(q);
    return copied;
}

/* ── Cross-type copy: batch queue → normal queue (flatten) ──────────────────── */

int64_t mmapq_queue_transfer_copy_batch_to_normal(
    MmapqIndexedBatchReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (!reader || !dest_dir || !dest_queue) return MMAPQ_EINVAL;
    if (start > end) return MMAPQ_EINVAL;

    int chk = mmapqi_queue_meta_dest_check(dest_dir, dest_queue, QUEUE_TYPE_NORMAL,
                                start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    QueueMeta *qm = mmapqi_indexed_batch_reader_queue_meta(reader);
    if (!qm) return mmapq_indexed_batch_reader_last_error(reader);

    int64_t qstart = mmapq_indexed_batch_reader_start_index(reader);
    int64_t qend   = mmapq_indexed_batch_reader_last_index(reader);
    int discovery_error = mmapq_indexed_batch_reader_last_error(reader);
    if (discovery_error != 0) return discovery_error;
    if (qend < 0) return 0;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;
    if (start > end) return 0;

    int no_filter = (!include && !exclude);

    int64_t bstart = mmapq_indexed_batch_reader_start_index(reader);
    int64_t dest_start = (start_index_mode == MMAPQ_QUEUE_TRANSFER_PRESERVE)
                         ? bstart : 0;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", dest_dir, dest_queue);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int ring = mmapq_min_ring_size(queue_meta_region_size(qm));
    if (ring < 4) ring = 4;
    MmapqIndexedQueue *q = NULL;
    /* CAND-50 §11: a created destination takes the source queue's
     * archivable flag; an existing destination keeps its on-disk flag. */
    int rc = mmapq_indexed_queue_create(dest_dir, dest_queue,
        queue_meta_region_size(qm), ring, 1, queue_meta_max_file_size(qm),
        dest_start, 10000000000ULL, queue_meta_archivable(qm), write_rt, read_rt, &q);
    if (rc != 0) return rc;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t copied = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = appender_create_bridge,
        .create_with_id = appender_create_with_id_bridge,
        .close = appender_close_bridge,
        .queue_close = queue_close_bridge,
    };

    MmapqBatchReadingContext bctx;
    for (int64_t b = start; b <= end; b++) {
        mmapq_indexed_batch_reader_read(reader, b, &bctx);
        if (bctx.status == MMAPQ_BATCH_ENTRY_ERROR) { appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return mmapq_indexed_batch_reader_last_error(reader); }
        if (bctx.status != MMAPQ_BATCH_ENTRY_AVAILABLE) continue;

        uint16_t aid = bctx.appender_id;
        int32_t mapped = appender_cache_map(mapped_cache, aid, mapper, mapper_ctx);

        int abort_flag;
        MmapqIndexedAppender *a = (MmapqIndexedAppender *)appender_cache_get(
            &ac, mapped, q, on_conflict, &abort_flag);
        if (!a) { appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return abort_flag; }

        mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                            bctx.message_count, 0);
        while (mmapq_batch_iterator_advance(bctx.iterator)) {
            int m_idx = bctx.iterator->current_msg_index;
            int m_cnt = bctx.iterator->message_count;
            const void *d = batch_iterator_current_data(bctx.iterator);
            int32_t l = batch_iterator_current_length(bctx.iterator);

            if (!no_filter) {
                int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                if (!in || ex) continue;
            }
            int64_t appended = mmapq_indexed_appender_append(a, d, l);
            if (appended < 0) { appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return appended; }
            copied++;
        }
        if (bctx.iterator->last_error != 0) { appender_cache_cleanup(&ac); mmapq_indexed_queue_close(q); return bctx.iterator->last_error; }
    }

    appender_cache_cleanup(&ac);
    mmapq_indexed_queue_close(q);
    return copied;
}
