#include "indexed_queue.h"
#include "indexed_archiver.h"
#include "internal/queue_meta.h"
#include "internal/appender_id_pool.h"
#include "internal/header_codec.h"
#include "internal/region_ring_accessor.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

struct IndexedQueue {
    char    *directory;
    char    *queue_name;
    int32_t  region_size;   /* preferred/initial */
    int      ring_size;
    int      regions_to_map_ahead;
    int64_t  max_file_size; /* preferred/initial */
    int64_t  start_index;   /* initial */
    int      archivable;    /* 0 or 1 — applies at creation, disk wins on reopen */
    uint64_t timeout_ns;
    Runtime *write_runtime;
    Runtime *read_runtime;

    QueueMeta *queue_meta;   /* Lazily opened RO meta */
    atomic_int closed;
};

/* ── Geometry constraint helper ──────────────────────────────────────────── */

int mmapq_min_ring_size(int32_t region_size)
{
    if (region_size <= 0) return INDEXED_QUEUE_DEFAULT_RING_SIZE;
    int64_t block = (int64_t)(1 << HEADER_BLOCK_SIZE_BITS) * HEADER_SIZE;
    int64_t need  = (block + region_size - 1) / region_size
                  + REGION_RING_ACCESSOR_DEFAULT_REGIONS_AHEAD;
    int p2 = 1;
    while ((int64_t)p2 < need) p2 <<= 1;
    return p2;
}

/* ── Directory creation ──────────────────────────────────────────────────── */

static int create_queue_directory(const char *directory, const char *queue_name)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", directory, queue_name);

    if (mkdir(path, 0775) != 0) {
        if (errno != EEXIST) {
            return -1;
        }
        /* EEXIST is fine — directory already exists */
    }
    return 0;
}

/* ── indexed_queue_create ────────────────────────────────────────────────── */

int indexed_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    int         archivable,
    Runtime    *write_runtime,
    Runtime    *read_runtime,
    IndexedQueue **out_queue)
{
    if (out_queue == NULL) return MMAPQ_EINVAL;
    *out_queue = NULL;

    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;
    if (region_size <= 0) return MMAPQ_EINVAL;
    if ((region_size & (region_size - 1)) != 0) return MMAPQ_EINVAL;
    if (ring_size <= 0) return MMAPQ_EINVAL;
    if ((ring_size & (ring_size - 1)) != 0) return MMAPQ_EINVAL;
    if (ring_size < mmapq_min_ring_size(region_size)) return MMAPQ_EINVAL;
    if (max_file_size <= 0) return MMAPQ_EINVAL;
    if (max_file_size % region_size != 0) return MMAPQ_EINVAL;
    if (start_index < 0) return MMAPQ_EINVAL;

    IndexedQueue *q = calloc(1, sizeof(IndexedQueue));
    if (q == NULL) return MMAPQ_ENOMEM;

    q->directory            = strdup(directory);
    if (q->directory == NULL) { free(q); return MMAPQ_ENOMEM; }
    q->queue_name           = strdup(queue_name);
    if (q->queue_name == NULL) { free(q->directory); free(q); return MMAPQ_ENOMEM; }
    q->region_size          = region_size;
    q->ring_size            = ring_size;
    q->regions_to_map_ahead = regions_to_map_ahead;
    q->max_file_size        = max_file_size;
    q->start_index          = start_index;
    q->archivable           = archivable;
    q->timeout_ns           = timeout_ns;
    q->write_runtime        = write_runtime;
    q->read_runtime         = read_runtime;
    atomic_store(&q->closed, 0);

    /*
     * Validate cross-process atomic requirements at startup.
     */
    {
        _Atomic uint64_t test64 = 0;
        _Atomic uint32_t test32 = 0;
        (void)test64;
        (void)test32;
        if (!atomic_is_lock_free(&test64)) {
            free(q->queue_name); free(q->directory); free(q);
            return MMAPQ_ENOMEM;
        }
        if (!atomic_is_lock_free(&test32)) {
            free(q->queue_name); free(q->directory); free(q);
            return MMAPQ_ENOMEM;
        }
    }

    *out_queue = q;
    return 0;
}

/* ── indexed_queue_create_default ────────────────────────────────────────── */

int indexed_queue_create_default(
    const char *directory,
    const char *queue_name,
    Runtime    *write_runtime,
    Runtime    *read_runtime,
    IndexedQueue **out_queue)
{
    return indexed_queue_create(
        directory, queue_name,
        INDEXED_QUEUE_DEFAULT_REGION_SIZE,
        INDEXED_QUEUE_DEFAULT_RING_SIZE,
        INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD,
        INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE,
        0,
        INDEXED_QUEUE_DEFAULT_TIMEOUT_NS,
        0,
        write_runtime,
        read_runtime,
        out_queue);
}

/* ── Participant factories ────────────────────────────────────────────────── */

int indexed_queue_create_appender(IndexedQueue *q, IndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    if (create_queue_directory(q->directory, q->queue_name) != 0) return MMAPQ_ETIMEDOUT;

    return indexed_appender_create(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns,
        q->archivable, q->write_runtime,
        q->start_index, out);
}

int indexed_queue_create_appender_with_id(
    IndexedQueue *q,
    uint8_t       appender_id,
    IndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    if (create_queue_directory(q->directory, q->queue_name) != 0) return MMAPQ_ETIMEDOUT;

    return indexed_appender_create_with_id(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns,
        q->archivable, q->write_runtime, appender_id,
        q->start_index, out);
}

int indexed_queue_create_poller(IndexedQueue *q, IndexedPoller **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    return indexed_poller_create(
        q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime, out);
}

int indexed_queue_create_reader(IndexedQueue *q, IndexedReader **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    return indexed_reader_create(
        q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime, out);
}

int indexed_queue_create_archiver(IndexedQueue *q, IndexedArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    return indexed_archiver_create(
        q->directory, q->queue_name, q->ring_size, q->read_runtime, out);
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

static void ensure_meta_open(IndexedQueue *q)
{
    if (q->queue_meta != NULL) return;
    queue_meta_open_ro(q->directory, q->queue_name, QUEUE_TYPE_NORMAL, &q->queue_meta);
}

int32_t indexed_queue_region_size(const IndexedQueue *q)
{
    if (q == NULL) return 0;
    ensure_meta_open((IndexedQueue *)q);
    return q->queue_meta ? queue_meta_region_size(q->queue_meta) : q->region_size;
}

int64_t indexed_queue_max_file_size(const IndexedQueue *q)
{
    if (q == NULL) return 0;
    ensure_meta_open((IndexedQueue *)q);
    return q->queue_meta ? queue_meta_max_file_size(q->queue_meta) : q->max_file_size;
}

int64_t indexed_queue_start_index(const IndexedQueue *q)
{
    if (q == NULL) return 0;
    ensure_meta_open((IndexedQueue *)q);
    return q->queue_meta ? queue_meta_start_index(q->queue_meta) : q->start_index;
}

/* ── indexed_queue_close ─────────────────────────────────────────────────── */

/* ── indexed_queue_close ─────────────────────────────────────────────────── */

void indexed_queue_close(IndexedQueue *q)
{
    if (q == NULL || atomic_load_explicit(&q->closed, memory_order_relaxed)) return;
    atomic_store_explicit(&q->closed, 1, memory_order_release);

    if (q->queue_meta != NULL) {
        queue_meta_close(q->queue_meta);
        q->queue_meta = NULL;
    }

    free(q->directory);
    free(q->queue_name);
    free(q);
}
