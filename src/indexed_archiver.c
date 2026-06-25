#include "indexed_archiver.h"
#include "queue_meta.h"
#include "queue_region_accessor.h"
#include "queue_files.h"
#include "appender_meta.h"
#include "header_reader.h"
#include "header_codec.h"
#include "payload_walk.h"
#include "mmap_assert.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define OFFSET_NOT_SET ((int64_t)-1)

/* ── Internal struct ─────────────────────────────────────────────────────── */

struct IndexedArchiver {
    char        *directory;
    char        *queue_name;
    int          ring_size;
    Runtime     *runtime;

    /* Lazy-initialised on first use */
    QueueMeta           *ro_meta;
    QueueMeta           *rw_meta;
    QueueRegionAccessor *ro_acc;
    RegionAccessor      *header_acc;
    Buffer               header_buf;

    /* Per-appender meta — opened lazily, keyed by appender_id (0–255) */
    AppenderMeta *appender_metas[MAX_APPENDERS];
};

/* ── Lazy initialisers ───────────────────────────────────────────────────── */

static QueueMeta *ro_init(IndexedArchiver *a)
{
    if (a->ro_meta) return a->ro_meta;

    a->ro_meta = queue_meta_open_ro(a->directory, a->queue_name);
    if (!a->ro_meta) return NULL;

    /* Sync mapping (regions_to_map_ahead = 0): the archiver does a single
     * sequential pass over headers and never benefits from background premap. */
    a->ro_acc = queue_region_accessor_create_ro(
        a->directory, a->queue_name,
        queue_meta_region_size(a->ro_meta),
        a->ring_size,
        0,   /* regions_to_map_ahead — sync, no background mapper needed */
        queue_meta_max_file_size(a->ro_meta),
        10000000000ULL, a->runtime);
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
static QueueMeta *rw_meta_open(IndexedArchiver *a)
{
    if (a->rw_meta) return a->rw_meta;
    a->rw_meta = queue_meta_open_rw(a->directory, a->queue_name,
        queue_meta_start_index(a->ro_meta),
        queue_meta_region_size(a->ro_meta),
        queue_meta_max_file_size(a->ro_meta),
        queue_meta_archivable(a->ro_meta),
        queue_meta_fixed_size_8bytes(a->ro_meta));
    return a->rw_meta;
}

static AppenderMeta *get_appender_meta(IndexedArchiver *a, uint8_t id)
{
    if (!a->appender_metas[id]) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/appender_meta_%u",
                 a->directory, a->queue_name, (unsigned)id);
        if (!queue_file_exists(path)) return NULL;
        a->appender_metas[id] = appender_meta_open_rw(
            a->directory, a->queue_name, id);
    }
    return a->appender_metas[id];
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

IndexedArchiver *indexed_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    Runtime    *runtime)
{
    MMAP_ASSERT(directory  != NULL, "indexed_archiver_create: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "indexed_archiver_create: queue_name is NULL");
    MMAP_ASSERT(ring_size > 0,      "indexed_archiver_create: ring_size must be > 0");

    IndexedArchiver *a = calloc(1, sizeof(IndexedArchiver));
    MMAP_ASSERT(a != NULL, "indexed_archiver_create: out of memory");

    a->directory  = strdup(directory);
    a->queue_name = strdup(queue_name);
    a->ring_size  = ring_size;
    a->runtime    = runtime;
    return a;
}

/* ── Archiving ───────────────────────────────────────────────────────────── */

int indexed_archiver_archivable(IndexedArchiver *a)
{
    MMAP_ASSERT(a != NULL, "indexed_archiver_archivable: archiver is NULL");
    QueueMeta *m = ro_init(a);
    MMAP_ASSERT(m != NULL, "indexed_archiver_archivable: queue not initialised");
    return queue_meta_archivable(m);
}

void indexed_archiver_archive_up_to_index(IndexedArchiver *a, int64_t index)
{
    MMAP_ASSERT(a     != NULL, "archive_up_to_index: archiver is NULL");
    MMAP_ASSERT(index >= 0,    "archive_up_to_index: index must be >= 0");

    QueueMeta *rom = ro_init(a);
    MMAP_ASSERT(rom != NULL,                "archive_up_to_index: queue not created yet");
    MMAP_ASSERT(queue_meta_archivable(rom), "archive_up_to_index: queue is not archivable");

    int64_t current   = queue_meta_start_index(rom);
    if (index == current) return; /* no-op */

    int64_t max_file_size = queue_meta_max_file_size(rom);
    int     fixed_size    = queue_meta_fixed_size_8bytes(rom);

    if (index > current) {
        /* ── Forward archiving ─────────────────────────────────────────── */

        if (!fixed_size) {
            /* Walk backwards from index-1: putIfAbsent — captures the
             * highest-index (most recent) committed payload position per
             * appender, which is the one whose successor is the next start. */
            int64_t last_positions[MAX_APPENDERS];
            int64_t next_offsets[MAX_APPENDERS];
            for (int i = 0; i < MAX_APPENDERS; i++) {
                last_positions[i] = OFFSET_NOT_SET;
                next_offsets[i]   = OFFSET_NOT_SET;
            }

            for (int64_t i = index - 1; i >= current; i--) {
                uint64_t hdr = header_read(a->header_acc, &a->header_buf,
                                           i, a->queue_name);
                MMAP_ASSERT(hdr != HEADER_NULL,
                    "archive_up_to_index: NULL header during forward walk");
                uint8_t aid = header_appender_id(hdr);
                int64_t pos = header_payload_position(hdr);
                if (last_positions[aid] == OFFSET_NOT_SET)
                    last_positions[aid] = pos;
            }

            /* For each appender: read the length at the last payload position
             * and compute the byte offset immediately following the message. */
            for (int aid = 0; aid < MAX_APPENDERS; aid++) {
                int64_t pos = last_positions[aid];
                if (pos == OFFSET_NOT_SET) continue;

                RegionAccessor *pay_acc =
                    queue_region_accessor_payload(a->ro_acc, (uint8_t)aid);
                Buffer pay_buf = {0};
                WrapResult wr = region_accessor_wrap(pay_acc, pos, &pay_buf);
                MMAP_ASSERT(wr == WRAP_SUCCESS,
                    "archive_up_to_index: failed to wrap payload region");
                (void)wr; /* suppress warning when MMAP_ASSERT is compiled away */

                int32_t len;
                memcpy(&len, pay_buf.data + PAYLOAD_LENGTH_OFFSET,
                       PAYLOAD_LENGTH_SIZE);
                MMAP_ASSERT(len > 0,
                    "archive_up_to_index: committed message has zero or negative length");

                next_offsets[aid] = pos + PAYLOAD_LENGTH_SIZE + len;
            }

            /* All validated — update appender metas */
            for (int aid = 0; aid < MAX_APPENDERS; aid++) {
                if (next_offsets[aid] == OFFSET_NOT_SET) continue;
                AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
                MMAP_ASSERT(m != NULL,
                    "archive_up_to_index: appender meta not found");
                appender_meta_set_start_offset(m, next_offsets[aid]);
            }
        } else {
            /* Fixed-size queue: just validate the boundary entry exists */
            MMAP_ASSERT(
                header_found(a->header_acc, &a->header_buf,
                             index - 1, a->queue_name),
                "archive_up_to_index: could not find entry at last index");
        }

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
            uint64_t hdr = header_read(a->header_acc, &a->header_buf,
                                       i, a->queue_name);
            MMAP_ASSERT(hdr != HEADER_NULL,
                "archive_up_to_index: NULL header during backward walk");

            file_index_set_set(&hdr_files, (int)(i / headers_in_file));

            if (!fixed_size) {
                uint8_t aid = header_appender_id(hdr);
                int64_t pos = header_payload_position(hdr);
                if (first_positions[aid] == OFFSET_NOT_SET)
                    first_positions[aid] = pos;
                file_index_set_set(&pay_files[aid], (int)(pos / max_file_size));
            }
        }

        /* Validate all required header files exist */
        for (int fi = file_index_set_first(&hdr_files); fi >= 0;
             fi = file_index_set_next(&hdr_files, fi)) {
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s/header_%d",
                     a->directory, a->queue_name, fi);
            MMAP_ASSERT(queue_file_exists(path),
                "archive_up_to_index: missing header file — undo not possible");
        }

        /* Validate all required payload files exist */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            MMAP_ASSERT(first_positions[aid] >= INITIAL_PAYLOAD_POSITION,
                "archive_up_to_index: invalid first payload position");
            for (int fi = file_index_set_first(&pay_files[aid]); fi >= 0;
                 fi = file_index_set_next(&pay_files[aid], fi)) {
                char path[4096];
                snprintf(path, sizeof(path), "%s/%s/payload_%d_%d",
                         a->directory, a->queue_name, aid, fi);
                MMAP_ASSERT(queue_file_exists(path),
                    "archive_up_to_index: missing payload file — undo not possible");
            }
        }

        /* All files present — update appender metas */
        for (int aid = 0; aid < MAX_APPENDERS; aid++) {
            if (first_positions[aid] == OFFSET_NOT_SET) continue;
            AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
            MMAP_ASSERT(m != NULL,
                "archive_up_to_index: appender meta not found");
            appender_meta_set_start_offset(m, first_positions[aid]);
        }
    }

    /* Commit new start_index atomically — visible to all readers immediately */
    QueueMeta *rwm = rw_meta_open(a);
    MMAP_ASSERT(rwm != NULL, "archive_up_to_index: failed to open rw queue meta");
    queue_meta_set_start_index(rwm, index);
}

/* ── Query ───────────────────────────────────────────────────────────────── */

int64_t indexed_archiver_start_index(IndexedArchiver *a)
{
    MMAP_ASSERT(a != NULL, "indexed_archiver_start_index: archiver is NULL");
    QueueMeta *m = ro_init(a);
    MMAP_ASSERT(m != NULL, "indexed_archiver_start_index: queue not initialised");
    return queue_meta_start_index(m);
}

int indexed_archiver_start_offset(
    IndexedArchiver *a, uint8_t appender_id, int64_t *out_offset)
{
    MMAP_ASSERT(a          != NULL, "indexed_archiver_start_offset: archiver is NULL");
    MMAP_ASSERT(out_offset != NULL, "indexed_archiver_start_offset: out_offset is NULL");

    if (!ro_init(a)) return 0;

    AppenderMeta *am = get_appender_meta(a, appender_id);
    if (!am) return 0;

    *out_offset = appender_meta_start_offset(am);
    return 1;
}

int indexed_archiver_archivable_files(
    IndexedArchiver  *a,
    char            **paths_out,
    int               max_paths)
{
    MMAP_ASSERT(a         != NULL, "archivable_files: archiver is NULL");
    MMAP_ASSERT(paths_out != NULL, "archivable_files: paths_out is NULL");

    QueueMeta *m = ro_init(a);
    MMAP_ASSERT(m != NULL, "archivable_files: queue not created yet");

    int64_t start = queue_meta_start_index(m);
    if (start <= QUEUE_META_INITIAL_START_INDEX) return 0;

    int64_t max_file_size   = queue_meta_max_file_size(m);
    int64_t headers_in_file = max_file_size / HEADER_SIZE;
    int     count = 0;

    /* Fully covered header files: files whose entire index range < start */
    int full_header_files = (int)((start - 1) / headers_in_file);
    for (int i = 0; i < full_header_files && count < max_paths; i++) {
        char path[4096];
        snprintf(path, sizeof(path), "%s/%s/header_%d",
                 a->directory, a->queue_name, i);
        /* Caller must free each returned string */
        if (queue_file_exists(path))
            paths_out[count++] = strdup(path);
    }

    /* Fully covered payload files: files whose byte range < start_offset */
    if (!queue_meta_fixed_size_8bytes(m)) {
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
    }

    return count;
}

static int64_t archiver_start_index_fn(void *ctx)
{
    return queue_meta_start_index((QueueMeta *)ctx);
}

int indexed_archiver_print_summary(IndexedArchiver *a, char *sb, int n)
{
    MMAP_ASSERT(a  != NULL, "print_summary: archiver is NULL");
    MMAP_ASSERT(sb != NULL, "print_summary: buffer is NULL");
    MMAP_ASSERT(n  > 0,     "print_summary: buffer size must be > 0");

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
    int64_t last_index      = header_search_last_index(
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
        if (!queue_file_exists(path)) continue;
        int64_t s = fi * headers_in_file;
        int64_t e = (fi + 1) * headers_in_file - 1;
        if (e > last_index) e = last_index;
        APPEND("  header_%ld: [%ld, %ld]\n", (long)fi, (long)s, (long)e);
    }

    if (!queue_meta_fixed_size_8bytes(m)) {
        APPEND("\nPayload Files:\n");
        uint8_t ids[MAX_APPENDERS];
        int n_ids = queue_files_get_appender_ids(
            a->directory, a->queue_name, ids, MAX_APPENDERS);
        for (int k = 0; k < n_ids; k++) {
            APPEND("  Appender %u:\n", (unsigned)ids[k]);
            for (int fi = 0; ; fi++) {
                char path[4096];
                snprintf(path, sizeof(path), "%s/%s/payload_%u_%d",
                         a->directory, a->queue_name, (unsigned)ids[k], fi);
                if (!queue_file_exists(path)) break;
                APPEND("    payload_%u_%d\n", (unsigned)ids[k], fi);
            }
        }
    }

#undef APPEND

    return written;
}

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

void indexed_archiver_close(IndexedArchiver *a)
{
    if (!a) return;

    for (int i = 0; i < MAX_APPENDERS; i++) {
        if (a->appender_metas[i])
            appender_meta_close(a->appender_metas[i]);
    }
    if (a->ro_acc)  queue_region_accessor_close(a->ro_acc);
    if (a->ro_meta) queue_meta_close(a->ro_meta);
    if (a->rw_meta) queue_meta_close(a->rw_meta);

    free(a->directory);
    free(a->queue_name);
    free(a);
}
