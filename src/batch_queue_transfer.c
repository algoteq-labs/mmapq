#include "batch_queue_transfer.h"
#include "internal/queue_meta.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_reader.h"
#include "internal/export_meta.h"
#include "internal/appender_cache.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Internal accessors — declared in src/internal/queue_meta.h */
QueueMeta *mmapqi_indexed_reader_queue_meta(struct MmapqIndexedReader *r);
QueueMeta *mmapqi_indexed_batch_reader_queue_meta(struct MmapqIndexedBatchReader *r);
#include <sys/stat.h>
#include <errno.h>

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

static int batch_appender_create_bridge(void *q, void **out)
{
    return mmapq_indexed_batch_queue_create_appender((MmapqIndexedBatchQueue *)q, (MmapqIndexedBatchAppender **)out);
}

static int batch_appender_create_with_id_bridge(void *q, uint8_t id, void **out)
{
    return mmapq_indexed_batch_queue_create_appender_with_id((MmapqIndexedBatchQueue *)q, id, (MmapqIndexedBatchAppender **)out);
}

static void batch_appender_close_bridge(void *a)
{
    mmapq_indexed_batch_appender_close((MmapqIndexedBatchAppender *)a);
}

static void batch_queue_close_bridge(void *q)
{
    mmapq_indexed_batch_queue_close((MmapqIndexedBatchQueue *)q);
}

static int read_all(FILE *f, void *buf, size_t n)
{
    return fread(buf, 1, n, f) == n ? 0 : -1;
}

/**
 * ChunkDesc — describes one chunk file in chunked batch export:
 *   start_idx, end_idx: batch index range in the source queue
 *   count:              number of batches in this chunk
 */
typedef struct {
    int64_t start_idx;
    int64_t end_idx;
    int64_t count;
} ChunkDesc;

#define MAX_CHUNKS 4096

/* ── Export ───────────────────────────────────────────────────────────────── */

int64_t mmapq_batch_queue_transfer_export(
    MmapqIndexedBatchReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx)
{
    if (!reader || !out) return MMAPQ_EINVAL;
    if (start > end) return MMAPQ_EINVAL;

    QueueMeta *qm = mmapqi_indexed_batch_reader_queue_meta(reader);
    if (!qm) return mmapq_indexed_batch_reader_last_error(reader);

    int64_t qstart = mmapq_indexed_batch_reader_start_index(reader);
    int64_t qend   = mmapq_indexed_batch_reader_last_index(reader);
    int discovery_error = mmapq_indexed_batch_reader_last_error(reader);
    if (discovery_error != 0) return discovery_error;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;

    int no_filter = (!include && !exclude);

    /* First pass: count batches with at least one matching message */
    int64_t batch_count = 0;
    if (no_filter) {
        batch_count = start <= end ? end - start + 1 : 0;
    } else {
        MmapqBatchReadingContext bctx;
        for (int64_t b = start; b <= end; b++) {
            mmapq_indexed_batch_reader_read(reader, b, &bctx);
            if (bctx.status == MMAPQ_BATCH_ENTRY_ERROR) { return mmapq_indexed_batch_reader_last_error(reader); }
            if (bctx.status != MMAPQ_BATCH_ENTRY_AVAILABLE) continue;
            int matched = 0;
            mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                                bctx.message_count, 0);
            while (mmapq_batch_iterator_advance(bctx.iterator)) {
                int m_idx = bctx.iterator->current_msg_index;
                int m_cnt = bctx.iterator->message_count;
                const void *d = batch_iterator_current_data(bctx.iterator);
                int32_t l = batch_iterator_current_length(bctx.iterator);
                int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                if (in && !ex) { matched = 1; break; }
            }
            if (bctx.iterator->last_error != 0) { return bctx.iterator->last_error; }
            if (matched) batch_count++;
        }
    }

    /* Write ExportMeta header */
    ExportMeta meta;
    export_meta_init(&meta);
    meta.schema_version = 1;
    meta.region_size    = queue_meta_region_size(qm);
    meta.max_file_size  = queue_meta_max_file_size(qm);
    meta.start_index    = (start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_REBASE) ? 0 : start;
    meta.total_entries  = batch_count;
    meta.archivable     = (uint8_t)queue_meta_archivable(qm);
    meta.queue_type     = QUEUE_TYPE_BATCH;

    uint8_t hdr[EXPORT_META_SIZE];
    export_meta_encode(&meta, hdr);
    if (write_all(out, hdr, EXPORT_META_SIZE)) return MMAPQ_EIO;

    /* Second pass: write matching batches and their matching messages */
    MmapqBatchReadingContext bctx;
    int64_t written = 0;
    for (int64_t b = start; b <= end; b++) {
        mmapq_indexed_batch_reader_read(reader, b, &bctx);
        if (bctx.status == MMAPQ_BATCH_ENTRY_ERROR) { return mmapq_indexed_batch_reader_last_error(reader); }
        if (bctx.status != MMAPQ_BATCH_ENTRY_AVAILABLE) continue;

        /* Count matching messages in this batch */
        int msg_matched = 0;
        if (no_filter) {
            msg_matched = bctx.message_count;
        } else {
            mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                                bctx.message_count, 0);
            while (mmapq_batch_iterator_advance(bctx.iterator)) {
                int m_idx = bctx.iterator->current_msg_index;
                int m_cnt = bctx.iterator->message_count;
                const void *d = batch_iterator_current_data(bctx.iterator);
                int32_t l = batch_iterator_current_length(bctx.iterator);
                int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                if (in && !ex) msg_matched++;
            }
            if (bctx.iterator->last_error != 0) { return bctx.iterator->last_error; }
        }
        if (msg_matched == 0) continue;

        /* Write appender_id (uint16, matching normal queue framing) + msg_count */
        uint16_t batch_aid = bctx.appender_id;
        if (write_all(out, &batch_aid, sizeof(uint16_t))) return MMAPQ_EIO;
        if (write_all(out, &msg_matched, sizeof(int32_t))) return MMAPQ_EIO;

        int32_t messages_written = 0;
        /* Write matching messages */
        mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                            bctx.message_count, 0);
        while (mmapq_batch_iterator_advance(bctx.iterator)) {
            int m_idx = bctx.iterator->current_msg_index;
            int m_cnt = bctx.iterator->message_count;
            const void *d = batch_iterator_current_data(bctx.iterator);
            int32_t l = batch_iterator_current_length(bctx.iterator);
            if (no_filter) {
                if (write_all(out, &l, sizeof(int32_t))) return MMAPQ_EIO;
                if (l > 0 && write_all(out, d, (size_t)l)) return MMAPQ_EIO;
                messages_written++;
            } else {
                int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                if (in && !ex) {
                    if (write_all(out, &l, sizeof(int32_t))) return MMAPQ_EIO;
                    if (l > 0 && write_all(out, d, (size_t)l)) return MMAPQ_EIO;
                    messages_written++;
                }
            }
        }
        if (bctx.iterator->last_error != 0) { return bctx.iterator->last_error; }
        if (messages_written != msg_matched) return MMAPQ_ECORRUPT;
        written++;
    }
    return written == batch_count ? written : MMAPQ_ECORRUPT;
}

/* ── Chunked export ───────────────────────────────────────────────────────── */

int64_t mmapq_batch_queue_transfer_export_chunked(
    MmapqIndexedBatchReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    MmapqBatchQueueFilterFn include, MmapqBatchQueueFilterFn exclude, void *ctx)
{
    if (!reader || !base_path || chunk_size_mb <= 0) return MMAPQ_EINVAL;
    if (start > end) return MMAPQ_EINVAL;

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
    int64_t chunk_max = (int64_t)chunk_size_mb * 1024 * 1024;

    /* First pass: count matching batches and entries, determine chunk boundaries */
    ChunkDesc chunks[MAX_CHUNKS];
    int nchunks = 0;
    int64_t chunk_start = start;
    int64_t chunk_cnt   = 0;
    int64_t chunk_bytes = EXPORT_META_SIZE;

    MmapqBatchReadingContext bctx;
    for (int64_t b = start; b <= end; b++) {
        mmapq_indexed_batch_reader_read(reader, b, &bctx);
        if (bctx.status == MMAPQ_BATCH_ENTRY_ERROR) { return mmapq_indexed_batch_reader_last_error(reader); }
        if (bctx.status != MMAPQ_BATCH_ENTRY_AVAILABLE) continue;

        /* Count/size this batch */
        int msg_matched = 0;
        int64_t batch_size = (int64_t)(sizeof(uint16_t) + sizeof(int32_t)); /* appender_id + msg_count */
        mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                            bctx.message_count, 0);
        while (mmapq_batch_iterator_advance(bctx.iterator)) {
            int m_idx = bctx.iterator->current_msg_index;
            int m_cnt = bctx.iterator->message_count;
            const void *d = batch_iterator_current_data(bctx.iterator);
            int32_t l = batch_iterator_current_length(bctx.iterator);
            if (no_filter) {
                msg_matched++;
                batch_size += (int64_t)(sizeof(int32_t) + l);
            } else {
                int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                if (in && !ex) {
                    msg_matched++;
                    batch_size += (int64_t)(sizeof(int32_t) + l);
                }
            }
        }
        if (bctx.iterator->last_error != 0) { return bctx.iterator->last_error; }
        if (msg_matched == 0) continue;

        if (chunk_cnt > 0 && chunk_bytes + batch_size > chunk_max) {
            if (nchunks >= MAX_CHUNKS) return MMAPQ_EINVAL;
            chunks[nchunks].start_idx = chunk_start;
            chunks[nchunks].end_idx   = b - 1;
            chunks[nchunks].count     = chunk_cnt;
            nchunks++;
            chunk_start = b;
            chunk_cnt   = 0;
            chunk_bytes = EXPORT_META_SIZE;
        }
        chunk_bytes += batch_size;
        chunk_cnt++;
    }
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

        ExportMeta meta;
        export_meta_init(&meta);
        meta.schema_version = 1;
        meta.region_size    = queue_meta_region_size(qm);
        meta.max_file_size  = queue_meta_max_file_size(qm);
        meta.start_index    = (start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_REBASE) ? cum_exported : chunks[c].start_idx;
        meta.total_entries  = chunks[c].count;
        meta.archivable     = (uint8_t)queue_meta_archivable(qm);
        meta.queue_type     = QUEUE_TYPE_BATCH;

        uint8_t hdr[EXPORT_META_SIZE];
        export_meta_encode(&meta, hdr);
        if (write_all(cf, hdr, EXPORT_META_SIZE)) { fclose(cf); return MMAPQ_EIO; }

        int64_t written = 0;
        /* Write batches in this chunk range */
        for (int64_t b = chunks[c].start_idx; b <= chunks[c].end_idx; b++) {
            mmapq_indexed_batch_reader_read(reader, b, &bctx);
            if (bctx.status == MMAPQ_BATCH_ENTRY_ERROR) { fclose(cf); return mmapq_indexed_batch_reader_last_error(reader); }
            if (bctx.status != MMAPQ_BATCH_ENTRY_AVAILABLE) continue;

            int msg_matched = 0;
            mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                                bctx.message_count, 0);
            while (mmapq_batch_iterator_advance(bctx.iterator)) {
                int m_idx = bctx.iterator->current_msg_index;
                int m_cnt = bctx.iterator->message_count;
                const void *d = batch_iterator_current_data(bctx.iterator);
                int32_t l = batch_iterator_current_length(bctx.iterator);
                if (no_filter) { msg_matched++; }
                else {
                    int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                    int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                    if (in && !ex) msg_matched++;
                }
            }
            if (bctx.iterator->last_error != 0) { fclose(cf); return bctx.iterator->last_error; }
            if (msg_matched == 0) continue;

            uint16_t batch_aid = bctx.appender_id;
            if (write_all(cf, &batch_aid, sizeof(uint16_t))) { fclose(cf); return MMAPQ_EIO; }
            if (write_all(cf, &msg_matched, sizeof(int32_t)))  { fclose(cf); return MMAPQ_EIO; }
            int32_t messages_written = 0;
            mmapq_batch_iterator_init(bctx.iterator, bctx.batch_offset,
                                bctx.message_count, 0);
            while (mmapq_batch_iterator_advance(bctx.iterator)) {
                int m_idx = bctx.iterator->current_msg_index;
                int m_cnt = bctx.iterator->message_count;
                const void *d = batch_iterator_current_data(bctx.iterator);
                int32_t l = batch_iterator_current_length(bctx.iterator);
                if (no_filter) {
                    if (write_all(cf, &l, sizeof(int32_t))) { fclose(cf); return MMAPQ_EIO; }
                    if (l > 0 && write_all(cf, d, (size_t)l)) { fclose(cf); return MMAPQ_EIO; }
                    messages_written++;
                } else {
                    int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                    int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                    if (in && !ex) {
                        if (write_all(cf, &l, sizeof(int32_t))) { fclose(cf); return MMAPQ_EIO; }
                        if (l > 0 && write_all(cf, d, (size_t)l)) { fclose(cf); return MMAPQ_EIO; }
                        messages_written++;
                    }
                }
            }
            if (bctx.iterator->last_error != 0) { fclose(cf); return bctx.iterator->last_error; }
            if (messages_written != msg_matched) { fclose(cf); return MMAPQ_ECORRUPT; }
            written++;
        }
        if (written != chunks[c].count) { fclose(cf); return MMAPQ_ECORRUPT; }
        if (fclose(cf)) return MMAPQ_EIO;

        cum_exported  += chunks[c].count;
        total_written += chunks[c].count;
    }
    return total_written;
}

/* ── Import (batch stream into batch queue) ────────────────────────────────── */

int64_t mmapq_batch_queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (!in || !directory || !queue_name) return MMAPQ_EINVAL;

    int chk = mmapqi_queue_meta_dest_check(directory, queue_name, QUEUE_TYPE_BATCH,
                                start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    uint8_t hdr[EXPORT_META_SIZE];
    if (read_all(in, hdr, EXPORT_META_SIZE)) return ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT;

    ExportMeta meta;
    export_meta_decode(hdr, &meta);
    if (meta.total_entries < 0) return MMAPQ_ECORRUPT;
    if (meta.queue_type == QUEUE_TYPE_FIXED8) return MMAPQ_ESCHEMA;
    if (meta.queue_type != QUEUE_TYPE_BATCH)  return MMAPQ_ESCHEMA;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", directory, queue_name);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int64_t effective = (start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE)
                        ? meta.start_index : 0;
    int ring = mmapq_min_ring_size(meta.region_size);
    if (ring < 4) ring = 4;
    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(
        directory, queue_name,
        meta.region_size, ring, 1, meta.max_file_size, effective,
        10000000000ULL, 0, write_rt, read_rt, &q);
    if (rc_q != 0) return rc_q;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t imported = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = batch_appender_create_bridge,
        .create_with_id = batch_appender_create_with_id_bridge,
        .close = batch_appender_close_bridge,
        .queue_close = batch_queue_close_bridge,
    };

    int error = 0;
    for (int64_t entry = 0; entry < meta.total_entries; entry++) {
        uint16_t source_id;
        int32_t message_count = 1;
        if (read_all(in, &source_id, sizeof(source_id))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
         {
            if (read_all(in, &message_count, sizeof(message_count))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
            if (message_count <= 0) { error = MMAPQ_ECORRUPT; goto failed; }
        }
        int32_t mapped = appender_cache_map(mapped_cache, source_id, mapper, mapper_ctx);
        MmapqIndexedBatchAppender *appender = appender_cache_get(&ac, mapped, q, on_conflict, &error);
        if (!appender) goto failed;
        MmapqMutableBatch *batch = NULL;
        error = mmapq_indexed_batch_appender_start_batch_checked(appender, &batch);
        if (error != 0) goto failed;
        for (int32_t message = 0; message < message_count; message++) {
            int32_t length;
            if (read_all(in, &length, sizeof(length))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
            if (length <= 0 || length > meta.region_size - 4) { error = MMAPQ_ECORRUPT; goto failed; }
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
            int64_t appended = mmapq_mutable_batch_append(batch, payload, length);
            if (payload != buffer) free(payload);
            if (appended < 0) { error = (int)appended; goto failed; }

        }
        int64_t offset = mmapq_mutable_batch_complete(batch);
        if (offset < 0) { error = (int)offset; goto failed; }
        int64_t committed = mmapq_indexed_batch_appender_commit_batch(appender, offset);
        if (committed < 0) { error = (int)committed; goto failed; }
        imported++;
    }
failed:
    appender_cache_cleanup(&ac);
    mmapq_indexed_batch_queue_close(q);
    return error != 0 ? error : imported;
}

/* ── Import (flat stream into batch queue, 1-msg batches) ──────────────────── */


int64_t mmapq_batch_queue_transfer_import_flat_stream(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    MmapqRuntime *write_rt, MmapqRuntime *read_rt,
    int allow_append,
    MmapqAppenderIdMapper mapper, void *mapper_ctx,
    MmapqConflictBehavior on_conflict)
{
    if (!in || !directory || !queue_name) return MMAPQ_EINVAL;

    int chk = mmapqi_queue_meta_dest_check(directory, queue_name, QUEUE_TYPE_BATCH,
                                start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    uint8_t hdr[EXPORT_META_SIZE];
    if (read_all(in, hdr, EXPORT_META_SIZE)) return ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT;

    ExportMeta meta;
    export_meta_decode(hdr, &meta);
    if (meta.total_entries < 0) return MMAPQ_ECORRUPT;
    if (meta.queue_type == QUEUE_TYPE_FIXED8) return MMAPQ_ESCHEMA;
    if (meta.queue_type != QUEUE_TYPE_NORMAL) return MMAPQ_ESCHEMA;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", directory, queue_name);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int64_t effective = (start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE)
                        ? meta.start_index : 0;
    int ring = mmapq_min_ring_size(meta.region_size);
    if (ring < 4) ring = 4;
    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(
        directory, queue_name,
        meta.region_size, ring, 1, meta.max_file_size, effective,
        10000000000ULL, 0, write_rt, read_rt, &q);
    if (rc_q != 0) return rc_q;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t imported = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = batch_appender_create_bridge,
        .create_with_id = batch_appender_create_with_id_bridge,
        .close = batch_appender_close_bridge,
        .queue_close = batch_queue_close_bridge,
    };

    int error = 0;
    for (int64_t entry = 0; entry < meta.total_entries; entry++) {
        uint16_t source_id;
        int32_t message_count = 1;
        if (read_all(in, &source_id, sizeof(source_id))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }

        int32_t mapped = appender_cache_map(mapped_cache, source_id, mapper, mapper_ctx);
        MmapqIndexedBatchAppender *appender = appender_cache_get(&ac, mapped, q, on_conflict, &error);
        if (!appender) goto failed;
        MmapqMutableBatch *batch = NULL;
        error = mmapq_indexed_batch_appender_start_batch_checked(appender, &batch);
        if (error != 0) goto failed;
        for (int32_t message = 0; message < message_count; message++) {
            int32_t length;
            if (read_all(in, &length, sizeof(length))) { error = ferror(in) ? MMAPQ_EIO : MMAPQ_ECORRUPT; goto failed; }
            if (length <= 0 || length > meta.region_size - 4) { error = MMAPQ_ECORRUPT; goto failed; }
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
            int64_t appended = mmapq_mutable_batch_append(batch, payload, length);
            if (payload != buffer) free(payload);
            if (appended < 0) { error = (int)appended; goto failed; }

        }
        int64_t offset = mmapq_mutable_batch_complete(batch);
        if (offset < 0) { error = (int)offset; goto failed; }
        int64_t committed = mmapq_indexed_batch_appender_commit_batch(appender, offset);
        if (committed < 0) { error = (int)committed; goto failed; }
        imported++;
    }
failed:
    appender_cache_cleanup(&ac);
    mmapq_indexed_batch_queue_close(q);
    return error != 0 ? error : imported;
}

/* ── Copy ─────────────────────────────────────────────────────────────────── */

int64_t mmapq_batch_queue_transfer_copy(
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

    int chk = mmapqi_queue_meta_dest_check(dest_dir, dest_queue, QUEUE_TYPE_BATCH,
                                start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, allow_append);
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
    int64_t dest_start = (start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE)
                         ? bstart : 0;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", dest_dir, dest_queue);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int ring = mmapq_min_ring_size(queue_meta_region_size(qm));
    if (ring < 4) ring = 4;
    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(
        dest_dir, dest_queue,
        queue_meta_region_size(qm), ring, 1, queue_meta_max_file_size(qm),
        dest_start, 10000000000ULL, 0, write_rt, read_rt, &q);
    if (rc_q != 0) return rc_q;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t copied = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = batch_appender_create_bridge,
        .create_with_id = batch_appender_create_with_id_bridge,
        .close = batch_appender_close_bridge,
        .queue_close = batch_queue_close_bridge,
    };

    MmapqBatchReadingContext bctx;
    for (int64_t b = start; b <= end; b++) {
        mmapq_indexed_batch_reader_read(reader, b, &bctx);
        if (bctx.status == MMAPQ_BATCH_ENTRY_ERROR) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return mmapq_indexed_batch_reader_last_error(reader); }
        if (bctx.status != MMAPQ_BATCH_ENTRY_AVAILABLE) continue;

        uint16_t aid = bctx.appender_id;
        int32_t mapped = appender_cache_map(mapped_cache, aid, mapper, mapper_ctx);
        int abort_flag;
        MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)appender_cache_get(
            &ac, mapped, q, on_conflict, &abort_flag);
        if (!a) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return abort_flag; }

        MmapqMutableBatch *mb = NULL;
        int batch_has_msgs = 0;

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
            if (!mb) {
                int started = mmapq_indexed_batch_appender_start_batch_checked(a, &mb);
                if (started != 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return started; }
            }
            int appended = mmapq_mutable_batch_append(mb, d, l);
            if (appended < 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return appended; }
            batch_has_msgs = 1;
        }
        if (bctx.iterator->last_error != 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return bctx.iterator->last_error; }

        if (batch_has_msgs) {
            int64_t off = mmapq_mutable_batch_complete(mb);
            if (off < 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return off; }
            int64_t committed = mmapq_indexed_batch_appender_commit_batch(a, off);
            if (committed < 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return committed; }
            copied++;
        }
    }

    appender_cache_cleanup(&ac);
    mmapq_indexed_batch_queue_close(q);
    return copied;
}

/* ── Cross-type copy: normal queue → batch queue (1-msg batches) ────────────── */

int64_t mmapq_batch_queue_transfer_copy_normal_to_batch(
    MmapqIndexedReader *reader,
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

    int chk = mmapqi_queue_meta_dest_check(dest_dir, dest_queue, QUEUE_TYPE_BATCH,
                                start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE, allow_append);
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
    int64_t nstart = mmapq_indexed_reader_start_index(reader);
    int64_t dest_start = (start_index_mode == MMAPQ_BATCH_QUEUE_TRANSFER_PRESERVE)
                         ? nstart : 0;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", dest_dir, dest_queue);
    if (create_dir(dpath)) return MMAPQ_EIO;

    int ring = mmapq_min_ring_size(queue_meta_region_size(qm));
    if (ring < 4) ring = 4;
    MmapqIndexedBatchQueue *q = NULL;
    int rc_q = mmapq_indexed_batch_queue_create(
        dest_dir, dest_queue,
        queue_meta_region_size(qm), ring, 1, queue_meta_max_file_size(qm),
        dest_start, 10000000000ULL, 0, write_rt, read_rt, &q);
    if (rc_q != 0) return rc_q;

    void *cache_store[APPENDER_CACHE_SIZE] = {NULL};
    int32_t mapped_cache[APPENDER_CACHE_SIZE];
    int64_t copied = 0;

    for (int _i = 0; _i < APPENDER_CACHE_SIZE; _i++) mapped_cache[_i] = MAPPED_NOT_COMPUTED;

    AppenderCache ac = {
        .cache = cache_store, .auto_appender = NULL,
        .create = batch_appender_create_bridge,
        .create_with_id = batch_appender_create_with_id_bridge,
        .close = batch_appender_close_bridge,
        .queue_close = batch_queue_close_bridge,
    };

    MmapqReadingContext rc;
    for (int64_t i = start; i <= end; i++) {
        mmapq_indexed_reader_read(reader, i, &rc);
        if (rc.status == MMAPQ_ENTRY_ERROR) { int error = mmapq_indexed_reader_last_error(reader); appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return error; }
        if (rc.status != MMAPQ_ENTRY_AVAILABLE) continue;

        if (!no_filter) {
            int in = include ? include(i, 0, 1, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, 0, 1, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
        }

        uint16_t aid = rc.appender_id;
        int32_t mapped = appender_cache_map(mapped_cache, aid, mapper, mapper_ctx);
        int abort_flag;
        MmapqIndexedBatchAppender *a = (MmapqIndexedBatchAppender *)appender_cache_get(
            &ac, mapped, q, on_conflict, &abort_flag);
        if (!a) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return abort_flag; }

        MmapqMutableBatch *mb = NULL;
        int started = mmapq_indexed_batch_appender_start_batch_checked(a, &mb);
        if (started != 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return started; }
        int appended = mmapq_mutable_batch_append(mb, rc.data, rc.length);
        if (appended < 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return appended; }
        int64_t off = mmapq_mutable_batch_complete(mb);
        if (off < 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return off; }
        int64_t committed = mmapq_indexed_batch_appender_commit_batch(a, off);
        if (committed < 0) { appender_cache_cleanup(&ac); mmapq_indexed_batch_queue_close(q); return committed; }
        copied++;
    }

    appender_cache_cleanup(&ac);
    mmapq_indexed_batch_queue_close(q);
    return copied;
}
