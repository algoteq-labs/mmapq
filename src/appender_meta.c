#include "appender_meta.h"
#include "mmap_assert.h"
#include "file_initialiser.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── File path helper ────────────────────────────────────────────────────── */

static void appender_meta_file_path(const char *directory, const char *queue_name,
                                     uint8_t appender_id, char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/%s/appender_meta_%u", directory, queue_name, appender_id);
}

/* ── File initialiser ────────────────────────────────────────────────────── */

typedef struct {
    uint8_t appender_id;
} AppenderMetaInitCtx;

static int appender_meta_file_init(int fd, size_t size, void *ctx)
{
    AppenderMetaInitCtx *mctx = (AppenderMetaInitCtx *)ctx;

    /* Zero the whole region */
    if (ftruncate(fd, (off_t)size) != 0) return -1;

    /* Write initial start_offset = INITIAL_PAYLOAD_POSITION */
    int64_t start_offset = INITIAL_PAYLOAD_POSITION;
    int16_t appender_id  = (int16_t)mctx->appender_id;

    if (pwrite(fd, &start_offset, sizeof(start_offset),
               APPENDER_META_START_OFFSET_OFFSET) < 0) return -1;
    if (pwrite(fd, &appender_id,  sizeof(appender_id),
               APPENDER_META_APPENDER_ID_OFFSET)  < 0) return -1;

    return 0;
}

/* ── Internal open helper ────────────────────────────────────────────────── */

static AppenderMeta *appender_meta_open_internal(
    const char      *directory,
    const char      *queue_name,
    uint8_t          appender_id,
    FileInitialiser  initialiser,
    void            *initialiser_ctx)
{
    char path[4096];
    appender_meta_file_path(directory, queue_name, appender_id, path, sizeof(path));

    RegionAccessor *region = fixed_size_region_create(
        path,
        APPENDER_META_ENCODED_LENGTH,
        initialiser,
        initialiser_ctx);

    if (region == NULL) return NULL;

    Buffer buf = {0};
    WrapResult r = region_accessor_wrap(region, 0, &buf);
    if (r != WRAP_SUCCESS) {
        fixed_size_region_destroy(region);
        return NULL;
    }

    AppenderMeta *m = calloc(1, sizeof(AppenderMeta));
    MMAP_ASSERT(m != NULL, "appender_meta_open: out of memory");

    m->region      = region;
    m->base        = buf.data;
    m->appender_id = appender_id;

    return m;
}

/* ── appender_meta_open_rw ───────────────────────────────────────────────── */

AppenderMeta *appender_meta_open_rw(
    const char *directory,
    const char *queue_name,
    uint8_t     appender_id)
{
    MMAP_ASSERT(directory  != NULL, "appender_meta_open_rw: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "appender_meta_open_rw: queue_name is NULL");

    AppenderMetaInitCtx ctx = { .appender_id = appender_id };

    AppenderMeta *m = appender_meta_open_internal(
        directory, queue_name, appender_id,
        appender_meta_file_init, &ctx);

    MMAP_ASSERT(m != NULL, "appender_meta_open_rw: failed to open appender meta");
    return m;
}

/* ── appender_meta_open_ro ───────────────────────────────────────────────── */

AppenderMeta *appender_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     appender_id)
{
    MMAP_ASSERT(directory  != NULL, "appender_meta_open_ro: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "appender_meta_open_ro: queue_name is NULL");

    char path[4096];
    appender_meta_file_path(directory, queue_name, appender_id, path, sizeof(path));

    struct stat st;
    if (stat(path, &st) != 0) return NULL;

    return appender_meta_open_internal(
        directory, queue_name, appender_id,
        file_init_none, NULL);
}

/* ── appender_meta_close ─────────────────────────────────────────────────── */

void appender_meta_close(AppenderMeta *m)
{
    if (m == NULL) return;
    if (m->region != NULL) {
        fixed_size_region_destroy(m->region);
        m->region = NULL;
    }
    free(m);
}
