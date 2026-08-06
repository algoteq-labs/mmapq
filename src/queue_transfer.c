#include "queue_transfer.h"
#include "queue_meta.h"
#include "indexed_queue.h"
#include "indexed_appender.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_queue.h"
#include "indexed_batch_appender.h"
#include "export_meta.h"
#include "mmap_assert.h"
#include "mmap_platform.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
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

static int read_all(FILE *f, void *buf, size_t n)
{
    return fread(buf, 1, n, f) == n ? 0 : -1;
}

static int write_framed_entry(FILE *out, uint16_t aid, const void *data, int32_t len)
{
    if (write_all(out, &aid, sizeof(uint16_t))) return -1;
    if (write_all(out, &len,  sizeof(int32_t)))  return -1;
    if (len > 0 && write_all(out, data, (size_t)len)) return -1;
    return 0;
}
typedef struct {
    int64_t start_idx;
    int64_t end_idx;
    int64_t count;
} ChunkDesc;

#define MAX_CHUNKS 4096

/* ── Export ───────────────────────────────────────────────────────────────── */

int64_t queue_transfer_export(
    IndexedReader *reader, FILE *out,
    int64_t start, int64_t end, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx)
{
    if (!reader || !out) return -1;

    QueueMeta *qm = indexed_reader_queue_meta(reader);
    if (!qm) return -1;

    int64_t qstart = indexed_reader_start_index(reader);
    int64_t qend   = indexed_reader_last_index(reader);
    if (qend < 0) {
        /* Empty queue: write a valid header with 0 entries */
        ExportMeta meta;
        export_meta_init(&meta);
        meta.schema_version    = 1;
        meta.region_size       = queue_meta_region_size(qm);
        meta.max_file_size     = queue_meta_max_file_size(qm);
        meta.start_index       = (start_index_mode == QUEUE_TRANSFER_REBASE) ? 0 : start;
        meta.total_entries     = 0;
        meta.archivable        = (uint8_t)queue_meta_archivable(qm);
        meta.queue_type = (uint8_t)queue_meta_queue_type(qm);
        uint8_t hdr[EXPORT_META_SIZE];
        export_meta_encode(&meta, hdr);
        write_all(out, hdr, EXPORT_META_SIZE);
        return 0;
    }
    if (start > end) return -1;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;
    if (start > end) return 0;

    int no_filter = (!include && !exclude);

    /* Count matching entries */
    int64_t count = 0;
    if (no_filter) {
        count = end - start + 1;
    } else {
        ReadingContext rc;
        for (int64_t i = start; i <= end; i++) {
            indexed_reader_read(reader, i, &rc);
            if (rc.status != ENTRY_AVAILABLE) continue;
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
    meta.start_index       = (start_index_mode == QUEUE_TRANSFER_REBASE) ? 0 : start;
    meta.total_entries     = count;
    meta.archivable        = (uint8_t)queue_meta_archivable(qm);
    meta.queue_type = (uint8_t)queue_meta_queue_type(qm);

    uint8_t hdr[EXPORT_META_SIZE];
    export_meta_encode(&meta, hdr);
    if (write_all(out, hdr, EXPORT_META_SIZE)) return -1;

    /* Write matching entries */
    ReadingContext rc;
    int64_t written = 0;
    for (int64_t i = start; i <= end && written < count; i++) {
        indexed_reader_read(reader, i, &rc);
        if (rc.status != ENTRY_AVAILABLE) continue;
        if (!no_filter) {
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
        }
        if (write_framed_entry(out, rc.appender_id, rc.data, rc.length)) return -1;
        written++;
    }
    return written;
}

/* ── Chunked export ──────────────────────────────────────────────────────── */

int64_t queue_transfer_export_chunked(
    IndexedReader *reader, const char *base_path,
    int64_t start, int64_t end, int chunk_size_mb, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx)
{
    if (!reader || !base_path || chunk_size_mb <= 0) return -1;
    if (start > end) return -1;

    QueueMeta *qm = indexed_reader_queue_meta(reader);
    if (!qm) return -1;

    int64_t qstart = indexed_reader_start_index(reader);
    int64_t qend   = indexed_reader_last_index(reader);
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

    ReadingContext rc;
    for (int64_t i = start; i <= end; i++) {
        indexed_reader_read(reader, i, &rc);
        if (rc.status != ENTRY_AVAILABLE) continue;
        int in = include ? include(i, rc.data, rc.length, ctx) : 1;
        int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
        if (!in || ex) continue;

        int esize = (int)(sizeof(uint16_t) + sizeof(int32_t) + rc.length);
        if (chunk_cnt > 0 && chunk_bytes + esize > chunk_max) {
            if (nchunks < MAX_CHUNKS) {
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
    if (chunk_cnt > 0 && nchunks < MAX_CHUNKS) {
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
        if (!cf) return -1;

        /* Write header */
        ExportMeta meta;
        export_meta_init(&meta);
        meta.schema_version    = 1;
        meta.region_size       = queue_meta_region_size(qm);
        meta.max_file_size     = queue_meta_max_file_size(qm);
        meta.start_index       = (start_index_mode == QUEUE_TRANSFER_REBASE) ? cum_exported : chunks[c].start_idx;
        meta.total_entries     = chunks[c].count;
        meta.archivable        = (uint8_t)queue_meta_archivable(qm);
        meta.queue_type = (uint8_t)queue_meta_queue_type(qm);

        uint8_t hdr[EXPORT_META_SIZE];
        export_meta_encode(&meta, hdr);
        fwrite(hdr, 1, EXPORT_META_SIZE, cf);

        /* Write entries */
        for (int64_t i = chunks[c].start_idx; i <= chunks[c].end_idx; i++) {
            indexed_reader_read(reader, i, &rc);
            if (rc.status != ENTRY_AVAILABLE) continue;
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
            fwrite(&rc.appender_id, sizeof(uint16_t), 1, cf);
            fwrite(&rc.length,      sizeof(int32_t),  1, cf);
            if (rc.length > 0) fwrite(rc.data, 1, (size_t)rc.length, cf);
        }
        fclose(cf);

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
 * Returns: number of entries (individual messages) imported, or -1 on error
 */
static int64_t do_import(
    FILE *in, const char *directory, const char *queue_name,
    int64_t start_index,
    int32_t region_size, int64_t max_file_size,
    uint8_t archivable,
    int64_t total_entries,
    int is_batch_stream,
    Runtime *wrt, Runtime *rrt)
{
    (void)archivable;
    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", directory, queue_name);
    create_dir(dpath);

    IndexedQueue *q = indexed_queue_create(
        directory, queue_name,
        region_size, 4, 1, max_file_size, start_index,
        10000000000ULL, 0, wrt, rrt);

    if (!q) return -1;

    IndexedAppender *appender = indexed_queue_create_appender(q);
    if (!appender) { indexed_queue_close(q); return -1; }

    int64_t imported = 0;

    if (is_batch_stream) {
        /* Flatten batch stream: read batches, append each message individually */
        for (int64_t b = 0; b < total_entries; b++) {
            int32_t msg_cnt;
            if (read_all(in, &msg_cnt, sizeof(int32_t))) break;
            if (msg_cnt < 0) break;
            for (int32_t m = 0; m < msg_cnt; m++) {
                int32_t len;
                if (read_all(in, &len, sizeof(int32_t))) break;
                if (len < 0) break;
                uint8_t buf[65536];
                uint8_t *payload = buf;
                if ((size_t)len > sizeof(buf)) {
                    payload = malloc((size_t)len);
                    if (!payload) break;
                }
                if (len > 0 && read_all(in, payload, (size_t)len)) {
                    if (payload != buf) { free(payload); break; }
                }
                indexed_appender_append(appender, payload, len);
                if (payload != buf) free(payload);
                imported++;
            }
        }
    } else {
        /* Flat stream: read entries directly */
        for (int64_t i = 0; i < total_entries; i++) {
            uint16_t aid;
            int32_t  len;
            if (read_all(in, &aid, sizeof(uint16_t))) break;
            if (read_all(in, &len,  sizeof(int32_t)))  break;
            if (len < 0) break;
            uint8_t buf[65536];
            uint8_t *payload = buf;
            if ((size_t)len > sizeof(buf)) {
                payload = malloc((size_t)len);
                if (!payload) break;
            }
            if (len > 0 && read_all(in, payload, (size_t)len)) {
                if (payload != buf) { free(payload); break; }
            }
            indexed_appender_append(appender, payload, len);
            if (payload != buf) free(payload);
            imported++;
        }
    }

    indexed_appender_close(appender);
    indexed_queue_close(q);
    return imported;
}

/* ── Import (flat stream) ─────────────────────────────────────────────────── */

int64_t queue_transfer_import(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append)
{
    if (!in || !directory || !queue_name) return -1;

    int chk = queue_meta_dest_check(directory, queue_name, QUEUE_TYPE_NORMAL,
                                start_index_mode == QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    uint8_t hdr[EXPORT_META_SIZE];
    if (read_all(in, hdr, EXPORT_META_SIZE)) return -1;

    ExportMeta meta;
    export_meta_decode(hdr, &meta);
    if (meta.queue_type == QUEUE_TYPE_FIXED8) return MMAPQ_ESCHEMA;
    if (meta.queue_type != QUEUE_TYPE_NORMAL) return MMAPQ_ESCHEMA;
    int64_t effective = (start_index_mode == QUEUE_TRANSFER_PRESERVE)
                        ? meta.start_index : 0;
    return do_import(in, directory, queue_name, effective,
                     meta.region_size, meta.max_file_size,
                     meta.archivable,
                     meta.total_entries, 0, write_rt, read_rt);
}

int64_t queue_transfer_import_batch_stream(
    FILE *in, const char *directory, const char *queue_name,
    int start_index_mode,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append)
{
    if (!in || !directory || !queue_name) return -1;

    int chk = queue_meta_dest_check(directory, queue_name, QUEUE_TYPE_NORMAL,
                                start_index_mode == QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    uint8_t hdr[EXPORT_META_SIZE];
    if (read_all(in, hdr, EXPORT_META_SIZE)) return -1;

    ExportMeta meta;
    export_meta_decode(hdr, &meta);
    if (meta.queue_type == QUEUE_TYPE_FIXED8) return MMAPQ_ESCHEMA;
    if (meta.queue_type != QUEUE_TYPE_BATCH)  return MMAPQ_ESCHEMA;
    int64_t effective = (start_index_mode == QUEUE_TRANSFER_PRESERVE)
                        ? meta.start_index : 0;
    return do_import(in, directory, queue_name, effective,
                     meta.region_size, meta.max_file_size,
                     meta.archivable,
                     meta.total_entries, 1, write_rt, read_rt);
}

/* ── Copy ─────────────────────────────────────────────────────────────────── */

int64_t queue_transfer_copy(
    IndexedReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    QueueFilterFn include, QueueFilterFn exclude, void *ctx,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append)
{
    if (!reader || !dest_dir || !dest_queue) return -1;
    if (start > end) return -1;

    int chk = queue_meta_dest_check(dest_dir, dest_queue, QUEUE_TYPE_NORMAL,
                                start_index_mode == QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    QueueMeta *qm = indexed_reader_queue_meta(reader);
    if (!qm) return -1;

    int64_t qstart = indexed_reader_start_index(reader);
    int64_t qend   = indexed_reader_last_index(reader);
    if (qend < 0) return 0;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;
    if (start > end) return 0;

    int no_filter = (!include && !exclude);

    int64_t dest_start = (start_index_mode == QUEUE_TRANSFER_PRESERVE)
                         ? qstart : 0;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", dest_dir, dest_queue);
    create_dir(dpath);

    IndexedQueue *q = indexed_queue_create(dest_dir, dest_queue,
        queue_meta_region_size(qm), 4, 1, queue_meta_max_file_size(qm),
        dest_start, 10000000000ULL, 0, write_rt, read_rt);

    if (!q) return -1;

    IndexedAppender *appender = indexed_queue_create_appender(q);
    if (!appender) { indexed_queue_close(q); return -1; }

    int64_t copied = 0;
    ReadingContext rc;
    for (int64_t i = start; i <= end; i++) {
        indexed_reader_read(reader, i, &rc);
        if (rc.status != ENTRY_AVAILABLE) continue;
        if (!no_filter) {
            int in = include ? include(i, rc.data, rc.length, ctx) : 1;
            int ex = exclude ? exclude(i, rc.data, rc.length, ctx) : 0;
            if (!in || ex) continue;
        }
        indexed_appender_append(appender, rc.data, rc.length);
        copied++;
    }

    indexed_appender_close(appender);
    indexed_queue_close(q);
    return copied;
}

/* ── Cross-type copy: batch queue → normal queue (flatten) ──────────────────── */

int64_t queue_transfer_copy_batch_to_normal(
    IndexedBatchReader *reader,
    const char *dest_dir, const char *dest_queue,
    int64_t start, int64_t end, int start_index_mode,
    BatchQueueFilterFn include, BatchQueueFilterFn exclude, void *ctx,
    Runtime *write_rt, Runtime *read_rt,
    int allow_append)
{
    if (!reader || !dest_dir || !dest_queue) return -1;
    if (start > end) return -1;

    int chk = queue_meta_dest_check(dest_dir, dest_queue, QUEUE_TYPE_NORMAL,
                                start_index_mode == QUEUE_TRANSFER_PRESERVE, allow_append);
    if (chk != 0) return chk;

    QueueMeta *qm = indexed_batch_reader_queue_meta(reader);
    if (!qm) return -1;

    int64_t qstart = indexed_batch_reader_start_index(reader);
    int64_t qend   = indexed_batch_reader_last_index(reader);
    if (qend < 0) return 0;
    if (start < qstart) start = qstart;
    if (end > qend) end = qend;
    if (start > end) return 0;

    int no_filter = (!include && !exclude);

    int64_t bstart = indexed_batch_reader_start_index(reader);
    int64_t dest_start = (start_index_mode == QUEUE_TRANSFER_PRESERVE)
                         ? bstart : 0;

    char dpath[4096];
    snprintf(dpath, sizeof(dpath), "%s/%s", dest_dir, dest_queue);
    create_dir(dpath);

    IndexedQueue *q = indexed_queue_create(dest_dir, dest_queue,
        queue_meta_region_size(qm), 4, 1, queue_meta_max_file_size(qm),
        dest_start, 10000000000ULL, 0, write_rt, read_rt);
    if (!q) return -1;

    IndexedAppender *appender = indexed_queue_create_appender(q);
    if (!appender) { indexed_queue_close(q); return -1; }

    int64_t copied = 0;
    BatchReadingContext bctx;
    for (int64_t b = start; b <= end; b++) {
        indexed_batch_reader_read(reader, b, &bctx);
        if (bctx.status != BATCH_ENTRY_AVAILABLE) continue;

        batch_iterator_init(&bctx.batch_iterator, bctx.batch_offset,
                            bctx.message_count, 0);
        while (batch_iterator_advance(&bctx.batch_iterator)) {
            int m_idx = bctx.batch_iterator.current_msg_index;
            int m_cnt = bctx.batch_iterator.message_count;
            const void *d = batch_iterator_current_data(&bctx.batch_iterator);
            int32_t l = batch_iterator_current_length(&bctx.batch_iterator);

            if (!no_filter) {
                int in = include ? include(b, m_idx, m_cnt, d, l, ctx) : 1;
                int ex = exclude ? exclude(b, m_idx, m_cnt, d, l, ctx) : 0;
                if (!in || ex) continue;
            }
            indexed_appender_append(appender, d, l);
            copied++;
        }
    }

    indexed_appender_close(appender);
    indexed_queue_close(q);
    return copied;
}
