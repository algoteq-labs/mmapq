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
    uint8_t fs8    = 0;  /* C implementation does not support fixed_size_8bytes */

    if (pwrite(fd, &schema, sizeof(schema), QUEUE_META_SCHEMA_VERSION_OFFSET) < 0) return -1;
    if (pwrite(fd, &rs,     sizeof(rs),     QUEUE_META_REGION_SIZE_OFFSET)    < 0) return -1;
    if (pwrite(fd, &si,     sizeof(si),     QUEUE_META_START_INDEX_OFFSET)    < 0) return -1;
    if (pwrite(fd, &mfs,    sizeof(mfs),    QUEUE_META_MAX_FILE_SIZE_OFFSET)  < 0) return -1;
    if (pwrite(fd, &arch,   sizeof(arch),   QUEUE_META_ARCHIVABLE_OFFSET)     < 0) return -1;
    if (pwrite(fd, &fs8,    sizeof(fs8),    QUEUE_META_FIXED_SIZE_8BYTES_OFFSET) < 0) return -1;

    return 0;
}

/* ── Populate cached fields from mapped memory ───────────────────────────── */

static void meta_cache_fields(QueueMeta *m)
{
    memcpy(&m->region_size,   m->base + QUEUE_META_REGION_SIZE_OFFSET,   sizeof(int32_t));
    memcpy(&m->max_file_size, m->base + QUEUE_META_MAX_FILE_SIZE_OFFSET, sizeof(int64_t));
    m->archivable = (m->base[QUEUE_META_ARCHIVABLE_OFFSET] == 1) ? 1 : 0;
    m->fixed_size_8bytes = (m->base[QUEUE_META_FIXED_SIZE_8BYTES_OFFSET] == 1) ? 1 : 0;
}

/* ── queue_meta_open_rw ──────────────────────────────────────────────────── */

QueueMeta *queue_meta_open_rw(
    const char *directory,
    const char *queue_name,
    int64_t     start_index,
    int32_t     region_size,
    int64_t     max_file_size,
    int         archivable)
{
    MMAP_ASSERT(directory  != NULL, "queue_meta_open_rw: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "queue_meta_open_rw: queue_name is NULL");
    MMAP_ASSERT(region_size > 0,    "queue_meta_open_rw: region_size must be > 0");
    MMAP_ASSERT(max_file_size > 0,  "queue_meta_open_rw: max_file_size must be > 0");

    char path[4096];
    meta_file_path(directory, queue_name, path, sizeof(path));

    MetaInitCtx ctx = {
        .start_index  = start_index,
        .region_size  = region_size,
        .max_file_size = max_file_size,
        .archivable   = archivable,
    };

    RegionAccessor *region = fixed_size_region_create(
        path,
        QUEUE_META_ENCODED_LENGTH,
        meta_file_init,
        &ctx);

    MMAP_ASSERT(region != NULL, "queue_meta_open_rw: failed to open meta region");

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS, "queue_meta_open_rw: failed to wrap meta region");

    QueueMeta *m = calloc(1, sizeof(QueueMeta));
    MMAP_ASSERT(m != NULL, "queue_meta_open_rw: out of memory");

    m->region = region;
    m->base   = buf.data;
    meta_cache_fields(m);

    MMAP_ASSERT(m->fixed_size_8bytes == 0,
        "C implementation does not support fixed_size_8bytes mode. "
        "Queue was created by Java with fixed-size 8-byte mode enabled.");

    return m;
}

/* ── queue_meta_open_ro ──────────────────────────────────────────────────── */

QueueMeta *queue_meta_open_ro(
    const char *directory,
    const char *queue_name)
{
    MMAP_ASSERT(directory  != NULL, "queue_meta_open_ro: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "queue_meta_open_ro: queue_name is NULL");

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
    MMAP_ASSERT(m != NULL, "queue_meta_open_ro: out of memory");

    m->region = region;
    m->base   = buf.data;
    meta_cache_fields(m);

    MMAP_ASSERT(m->fixed_size_8bytes == 0,
        "C implementation does not support fixed_size_8bytes mode. "
        "Queue was created by Java with fixed-size 8-byte mode enabled.");

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
