#include "indexed_archiver.h"
#include "internal/queue_meta.h"
#include "internal/queue_region_accessor.h"
#include "internal/queue_files.h"
#include "internal/appender_meta.h"
#include "internal/header_reader.h"
#include "internal/header_codec.h"
#include "internal/payload_walk.h"
#include "mmap_platform.h"

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

    int meta_init_failed;
};

/* ── Lazy initialisers ───────────────────────────────────────────────────── */

static QueueMeta *ro_init(IndexedArchiver *a)
{
    if (a->ro_meta) return a->ro_meta;
    if (a->meta_init_failed) return NULL;

    int rc = queue_meta_open_ro(a->directory, a->queue_name, QUEUE_TYPE_NORMAL, &a->ro_meta);
    if (rc != 0) {
        if (rc != MMAPQ_ENOENT && rc != MMAPQ_ENOTREADY && rc != MMAPQ_ETIMEDOUT)
            a->meta_init_failed = 1;
        return NULL;
    }

    /* Sync mapping (regions_to_map_ahead = 0): the archiver does a single
     * sequential pass over headers and never benefits from background premap. */
    int acc_rc = queue_region_accessor_create_ro(
        a->directory, a->queue_name,
        queue_meta_region_size(a->ro_meta),
        a->ring_size,
        0,   /* regions_to_map_ahead — sync, no background mapper needed */
        queue_meta_max_file_size(a->ro_meta),
        10000000000ULL, a->runtime,
        &a->ro_acc);
    if (acc_rc != 0) {
        queue_meta_close(a->ro_meta);
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
static QueueMeta *rw_meta_open(IndexedArchiver *a)
{
    if (a->rw_meta) return a->rw_meta;
    if (queue_meta_open_rw(a->directory, a->queue_name,
        queue_meta_start_index(a->ro_meta),
        queue_meta_region_size(a->ro_meta),
        queue_meta_max_file_size(a->ro_meta),
        queue_meta_archivable(a->ro_meta),
        QUEUE_TYPE_NORMAL,
        &a->rw_meta) != 0) { a->rw_meta = NULL; }
    return a->rw_meta;
}

static AppenderMeta *get_appender_meta(IndexedArchiver *a, uint8_t id)
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

/* ── Factory ─────────────────────────────────────────────────────────────── */

int indexed_archiver_create(
    const char *directory,
    const char *queue_name,
    int         ring_size,
    Runtime    *runtime,
    IndexedArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (ring_size <= 0) return MMAPQ_EINVAL;

    IndexedArchiver *a = calloc(1, sizeof(IndexedArchiver));
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

int indexed_archiver_archivable(IndexedArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_archivable(m);
}

int indexed_archiver_archive_up_to_index(IndexedArchiver *a, int64_t index)
{
    if (a == NULL || index < 0) return MMAPQ_EINVAL;

    QueueMeta *rom = ro_init(a);
    if (rom == NULL || !queue_meta_archivable(rom)) return MMAPQ_EINVAL;

    int64_t current   = queue_meta_start_index(rom);
    if (index == current) return 0; /* no-op */

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
                if (hdr == HEADER_NULL)
                    return MMAPQ_EINVAL;
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
                if (wr == WRAP_MAP_TIMED_OUT)
                    return MMAPQ_ETIMEDOUT;
                if (wr != WRAP_SUCCESS)
                    return MMAPQ_ECORRUPT;

                int32_t len;
                memcpy(&len, pay_buf.data + PAYLOAD_LENGTH_OFFSET,
                       PAYLOAD_LENGTH_SIZE);
                if (len <= 0)
                    return MMAPQ_ECORRUPT;

                next_offsets[aid] = pos + PAYLOAD_LENGTH_SIZE + len;
            }

            /* Validate all appender metas are accessible before mutating */
            for (int aid = 0; aid < MAX_APPENDERS; aid++) {
                if (next_offsets[aid] == OFFSET_NOT_SET) continue;
                if (get_appender_meta(a, (uint8_t)aid) == NULL)
                    return MMAPQ_ECORRUPT;
            }

            /* All validated — update appender metas */
            for (int aid = 0; aid < MAX_APPENDERS; aid++) {
                if (next_offsets[aid] == OFFSET_NOT_SET) continue;
                AppenderMeta *m = get_appender_meta(a, (uint8_t)aid);
                appender_meta_set_start_offset(m, next_offsets[aid]);
            }
        } else {
            /* Fixed-size queue: just validate the boundary entry exists */
            if (!header_found(a->header_acc, &a->header_buf,
                              index - 1, a->queue_name))
                return MMAPQ_EINVAL;
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
            if (hdr == HEADER_NULL)
                return MMAPQ_EINVAL;

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

int64_t indexed_archiver_start_index(IndexedArchiver *a)
{
    if (a == NULL) return 0;
    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;
    return queue_meta_start_index(m);
}

int indexed_archiver_start_offset(
    IndexedArchiver *a, uint8_t appender_id, int64_t *out_offset)
{
    if (a == NULL || out_offset == NULL) return 0;

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
    if (a == NULL || paths_out == NULL) return 0;

    QueueMeta *m = ro_init(a);
    if (m == NULL) return 0;

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
        if (queue_file_exists(path)) {
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
                if (queue_file_exists(path)) {
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

static int64_t archiver_start_index_fn(void *ctx)
{
    return queue_meta_start_index((QueueMeta *)ctx);
}

int indexed_archiver_print_summary(IndexedArchiver *a, char *sb, int n)
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
