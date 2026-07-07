#include "queue_meta.h"
#include "mmap_assert.h"
#include "mmap_platform.h"
#include "file_initialiser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ── File path helper ────────────────────────────────────────────────────── */

static void meta_file_path(const char *directory, const char *queue_name,
                            char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/%s/meta", directory, queue_name);
}

/* ── File initialiser — writes initial values when file is new ───────────── */

typedef struct {
    int64_t start_index;
    int32_t region_size;
    int64_t max_file_size;
    int     archivable;
    uint8_t queue_type;
} MetaInitCtx;

static int meta_file_init(int fd, size_t size, void *ctx)
{
    MetaInitCtx *mctx = (MetaInitCtx *)ctx;

    /* Zero the whole region first */
    if (ftruncate(fd, (off_t)size) != 0) return -1;

    /* Write fields at their byte offsets into the mapped file.
     * We write via pwrite — file must be at least QUEUE_META_ENCODED_LENGTH bytes. */
    int32_t schema = QUEUE_META_SCHEMA_VERSION;
    int32_t rs     = mctx->region_size;
    int64_t si     = mctx->start_index;
    int64_t mfs    = mctx->max_file_size;
    uint8_t arch   = (uint8_t)(mctx->archivable ? 1 : 0);
    uint8_t qt     = mctx->queue_type;

    if (pwrite(fd, &schema, sizeof(schema), QUEUE_META_SCHEMA_VERSION_OFFSET) < 0) return -1;
    if (pwrite(fd, &rs,     sizeof(rs),     QUEUE_META_REGION_SIZE_OFFSET)    < 0) return -1;
    if (pwrite(fd, &si,     sizeof(si),     QUEUE_META_START_INDEX_OFFSET)    < 0) return -1;
    if (pwrite(fd, &mfs,    sizeof(mfs),    QUEUE_META_MAX_FILE_SIZE_OFFSET)  < 0) return -1;
    if (pwrite(fd, &arch,   sizeof(arch),   QUEUE_META_ARCHIVABLE_OFFSET)     < 0) return -1;
    if (pwrite(fd, &qt,     sizeof(qt),     QUEUE_META_QUEUE_TYPE_OFFSET)     < 0) return -1;

    return 0;
}

/* ── Populate cached fields from mapped memory ───────────────────────────── */

static void meta_cache_fields(QueueMeta *m)
{
    memcpy(&m->region_size,   m->base + QUEUE_META_REGION_SIZE_OFFSET,   sizeof(int32_t));
    memcpy(&m->max_file_size, m->base + QUEUE_META_MAX_FILE_SIZE_OFFSET, sizeof(int64_t));
    m->archivable = (m->base[QUEUE_META_ARCHIVABLE_OFFSET] == 1) ? 1 : 0;
    m->queue_type = (int)m->base[QUEUE_META_QUEUE_TYPE_OFFSET];
}

/* ── Validation helper (real check, NOT an assert — survives -DNDEBUG) ──── */

static int meta_validate(const QueueMeta *m, uint8_t expected_type,
                          const char *directory, const char *queue_name)
{
    int32_t schema = queue_meta_schema_version(m);
    if (schema != QUEUE_META_SCHEMA_VERSION) {
        fprintf(stderr,
            "mmap-c: %s/%s: unsupported meta schema_version %d (expected %d)"
            " — refusing to open\n",
            directory, queue_name, schema, QUEUE_META_SCHEMA_VERSION);
        return MMAPQ_ESCHEMA;
    }
    if (m->queue_type == QUEUE_TYPE_FIXED8) {
        fprintf(stderr,
            "mmap-c: %s/%s: fixed-8-byte queues are Java-only and not"
            " supported by mmap-c — refusing to open\n",
            directory, queue_name);
        return MMAPQ_ESCHEMA;
    }
    if ((uint8_t)m->queue_type != expected_type) {
        fprintf(stderr,
            "mmap-c: %s/%s: queue_type mismatch — file is type %d, opener"
            " expected %d (0=normal, 2=batch). Opening a queue with the wrong"
            " participant kind misparses payload framing — refusing to open\n",
            directory, queue_name, m->queue_type, (int)expected_type);
        return MMAPQ_ESCHEMA;
    }
    return 0;
}

/* ── queue_meta_open_rw ──────────────────────────────────────────────────── */

QueueMeta *queue_meta_open_rw(
    const char *directory,
    const char *queue_name,
    int64_t     start_index,
    int32_t     region_size,
    int64_t     max_file_size,
    int         archivable,
    uint8_t     queue_type)
{
    if (directory  == NULL) return NULL;
    if (queue_name == NULL) return NULL;
    if (region_size <= 0) return NULL;
    if (max_file_size <= 0) return NULL;

    char path[4096];
    meta_file_path(directory, queue_name, path, sizeof(path));

    MetaInitCtx ctx = {
        .start_index  = start_index,
        .region_size  = region_size,
        .max_file_size = max_file_size,
        .archivable   = archivable,
        .queue_type   = queue_type,
    };

    RegionAccessor *region = fixed_size_region_create(
        path,
        QUEUE_META_ENCODED_LENGTH,
        meta_file_init,
        &ctx);

    if (region == NULL) return NULL;

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    (void)r;
    if (r != WRAP_SUCCESS) { fixed_size_region_destroy(region); return NULL; }

    QueueMeta *m = calloc(1, sizeof(QueueMeta));
    if (m == NULL) { fixed_size_region_destroy(region); return NULL; }

    m->region = region;
    m->base   = buf.data;
    meta_cache_fields(m);

    if (meta_validate(m, queue_type, directory, queue_name) != 0) {
        fixed_size_region_destroy(region);
        free(m);
        return NULL;
    }

    return m;
}

/* ── queue_meta_open_ro ──────────────────────────────────────────────────── */

QueueMeta *queue_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     expected_type)
{
    if (directory  == NULL) return NULL;
    if (queue_name == NULL) return NULL;

    char path[4096];
    meta_file_path(directory, queue_name, path, sizeof(path));

    /* Check file exists before trying to map it */
    struct stat st;
    if (stat(path, &st) != 0) {
        return NULL;  /* queue not yet created */
    }
    if (st.st_size < QUEUE_META_ENCODED_LENGTH) {
        return NULL;  /* file exists but not yet fully written */
    }

    RegionAccessor *region = fixed_size_region_create(
        path,
        QUEUE_META_ENCODED_LENGTH,
        file_init_none,
        NULL);

    if (region == NULL) return NULL;

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    if (r != WRAP_SUCCESS) {
        region_accessor_close(region);
        fixed_size_region_destroy(region);
        return NULL;
    }

    QueueMeta *m = calloc(1, sizeof(QueueMeta));
    if (m == NULL) {
        region_accessor_close(region);
        fixed_size_region_destroy(region);
        return NULL;
    }

    m->region = region;
    m->base   = buf.data;
    meta_cache_fields(m);

    if (meta_validate(m, expected_type, directory, queue_name) != 0) {
        region_accessor_close(region);
        fixed_size_region_destroy(region);
        free(m);
        return NULL;
    }

    return m;
}

/* ── queue_meta_close ────────────────────────────────────────────────────── */

void queue_meta_close(QueueMeta *m)
{
    if (m == NULL) return;
    if (m->region != NULL) {
        fixed_size_region_destroy(m->region);
        m->region = NULL;
    }
    free(m);
}
