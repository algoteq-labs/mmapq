#include "internal/queue_meta.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/file_initialiser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

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
    memcpy(&m->start_index,   m->base + QUEUE_META_START_INDEX_OFFSET,   sizeof(int64_t));
    m->archivable = (m->base[QUEUE_META_ARCHIVABLE_OFFSET] == 1) ? 1 : 0;
    m->queue_type = (int)m->base[QUEUE_META_QUEUE_TYPE_OFFSET];
}

/* ── Validation helper (real check, NOT an assert — survives -DNDEBUG) ──── */

static int meta_validate(const QueueMeta *m, uint8_t expected_type,
                          const char *directory, const char *queue_name)
{
    (void)directory;
    (void)queue_name;
    int32_t schema = queue_meta_schema_version(m);
    if (schema != QUEUE_META_SCHEMA_VERSION) {
        return MMAPQ_ESCHEMA;
    }
    if (m->queue_type == QUEUE_TYPE_FIXED8) {
        return MMAPQ_ESCHEMA;
    }
    if ((uint8_t)m->queue_type != expected_type) {
        return MMAPQ_ESCHEMA;
    }
    return 0;
}

/* ── queue_meta_open_rw ──────────────────────────────────────────────────── */

int queue_meta_open_rw(
    const char *directory,
    const char *queue_name,
    int64_t     start_index,
    int32_t     region_size,
    int64_t     max_file_size,
    int         archivable,
    uint8_t     queue_type,
    QueueMeta **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (region_size <= 0) return MMAPQ_EINVAL;
    if (max_file_size <= 0) return MMAPQ_EINVAL;

    char path[4096];
    meta_file_path(directory, queue_name, path, sizeof(path));

    MetaInitCtx ctx = {
        .start_index  = start_index,
        .region_size  = region_size,
        .max_file_size = max_file_size,
        .archivable   = archivable,
        .queue_type   = queue_type,
    };

    RegionAccessor *region = NULL;
    int fsrerr = fixed_size_region_create(
        path,
        QUEUE_META_ENCODED_LENGTH,
        meta_file_init,
        &ctx,
        &region);

    if (fsrerr != 0) return fsrerr;

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    if (r != WRAP_SUCCESS) { fixed_size_region_destroy(region); return MMAPQ_ETIMEDOUT; }

    QueueMeta *m = calloc(1, sizeof(QueueMeta));
    if (m == NULL) { fixed_size_region_destroy(region); return MMAPQ_ENOMEM; }

    m->region = region;
    m->base   = buf.data;
    meta_cache_fields(m);

    if (meta_validate(m, queue_type, directory, queue_name) != 0) {
        fixed_size_region_destroy(region);
        free(m);
        return MMAPQ_ESCHEMA;
    }

    *out = m;
    return 0;
}

/* ── queue_meta_open_ro ──────────────────────────────────────────────────── */

int queue_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     expected_type,
    QueueMeta **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    char path[4096];
    meta_file_path(directory, queue_name, path, sizeof(path));

    /* Check file exists before trying to map it */
    struct stat st;
    if (stat(path, &st) != 0) {
        return (errno == ENOENT) ? MMAPQ_ENOENT : MMAPQ_ETIMEDOUT;
    }
    if (st.st_size < QUEUE_META_ENCODED_LENGTH) {
        return MMAPQ_ENOTREADY;  /* file exists but not yet fully written — transient, retry */
    }

    RegionAccessor *region = NULL;
    int fsrerr = fixed_size_region_create(
        path,
        QUEUE_META_ENCODED_LENGTH,
        file_init_none,
        NULL,
        &region);

    if (fsrerr != 0) return fsrerr;

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    if (r != WRAP_SUCCESS) {
        region_accessor_close(region);
        fixed_size_region_destroy(region);
        return MMAPQ_ETIMEDOUT;
    }

    QueueMeta *m = calloc(1, sizeof(QueueMeta));
    if (m == NULL) {
        region_accessor_close(region);
        fixed_size_region_destroy(region);
        return MMAPQ_ENOMEM;
    }

    m->region = region;
    m->base   = buf.data;
    meta_cache_fields(m);

    if (meta_validate(m, expected_type, directory, queue_name) != 0) {
        region_accessor_close(region);
        fixed_size_region_destroy(region);
        free(m);
        return MMAPQ_ESCHEMA;
    }

    *out = m;
    return 0;
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

/* ── queue_meta_dest_check ────────────────────────────────────────────────── */

int queue_meta_dest_check(
    const char *directory, const char *queue_name,
    uint8_t expected_type, int preserve, int allow_append)
{
    QueueMeta *dm = NULL;
    int qerr = queue_meta_open_ro(directory, queue_name, expected_type, &dm);
    if (qerr != 0) {
        if (qerr == MMAPQ_ENOENT) return 0;
        return qerr;
    }
    queue_meta_close(dm);

    if (!allow_append) return MMAPQ_EEXIST;
    if (preserve) return MMAPQ_EINVAL;
    return 0;
}
