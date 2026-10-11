#include "indexed_queue.h"
#include "indexed_archiver.h"
#include "internal/header_codec.h"
#include "internal/region_ring_accessor.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

struct MmapqIndexedQueue {
    char    *directory;
    char    *queue_name;
    int32_t  region_size;   /* preferred/initial */
    int      ring_size;
    int      regions_to_map_ahead;
    int64_t  max_file_size; /* preferred/initial */
    int64_t  start_index;   /* initial */
    int      archivable;    /* 0 or 1 — applies at creation, disk wins on reopen */
    uint64_t timeout_ns;
    MmapqRuntime *write_runtime;
    MmapqRuntime *read_runtime;

    atomic_int closed;
};

/* ── Geometry constraint helper ──────────────────────────────────────────── */

int mmapq_min_ring_size(int32_t region_size)
{
    if (region_size <= 0) return MMAPQ_INDEXED_QUEUE_DEFAULT_RING_SIZE;
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

/* ── mmapq_indexed_queue_create ────────────────────────────────────────────────── */

int mmapq_indexed_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    int         archivable,
    MmapqRuntime    *write_runtime,
    MmapqRuntime    *read_runtime,
    MmapqIndexedQueue **out_queue)
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
    if (!header_index_in_range(start_index)) return MMAPQ_EINVAL;

    MmapqIndexedQueue *q = calloc(1, sizeof(MmapqIndexedQueue));
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

/* ── mmapq_indexed_queue_create_default ────────────────────────────────────────── */

int mmapq_indexed_queue_create_default(
    const char *directory,
    const char *queue_name,
    MmapqRuntime    *write_runtime,
    MmapqRuntime    *read_runtime,
    MmapqIndexedQueue **out_queue)
{
    return mmapq_indexed_queue_create(
        directory, queue_name,
        MMAPQ_INDEXED_QUEUE_DEFAULT_REGION_SIZE,
        MMAPQ_INDEXED_QUEUE_DEFAULT_RING_SIZE,
        MMAPQ_INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD,
        MMAPQ_INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE,
        0,
        MMAPQ_INDEXED_QUEUE_DEFAULT_TIMEOUT_NS,
        0,
        write_runtime,
        read_runtime,
        out_queue);
}

/* ── Participant factories ────────────────────────────────────────────────── */

int mmapq_indexed_queue_create_appender(MmapqIndexedQueue *q, MmapqIndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    if (create_queue_directory(q->directory, q->queue_name) != 0) return MMAPQ_ETIMEDOUT;

    return mmapq_indexed_appender_create(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns,
        q->archivable, q->write_runtime,
        q->start_index, out);
}

int mmapq_indexed_queue_create_appender_with_id(
    MmapqIndexedQueue *q,
    uint8_t       appender_id,
    MmapqIndexedAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    if (create_queue_directory(q->directory, q->queue_name) != 0) return MMAPQ_ETIMEDOUT;

    return mmapq_indexed_appender_create_with_id(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns,
        q->archivable, q->write_runtime, appender_id,
        q->start_index, out);
}

int mmapq_indexed_queue_create_poller(MmapqIndexedQueue *q, MmapqIndexedPoller **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    return mmapq_indexed_poller_create(
        q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime, out);
}

int mmapq_indexed_queue_create_reader(MmapqIndexedQueue *q, MmapqIndexedReader **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    return mmapq_indexed_reader_create(
        q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime, out);
}

int mmapq_indexed_queue_create_archiver(MmapqIndexedQueue *q, MmapqIndexedArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (atomic_load_explicit(&q->closed, memory_order_relaxed)) return MMAPQ_ECLOSED;

    return mmapq_indexed_archiver_create(
        q->directory, q->queue_name, q->ring_size, q->read_runtime, out);
}

/* ── Lifecycle ────────────────────────────────────────────────────────────── */

void mmapq_indexed_queue_close(MmapqIndexedQueue *q)
{
    if (q == NULL || atomic_load_explicit(&q->closed, memory_order_relaxed)) return;
    atomic_store_explicit(&q->closed, 1, memory_order_release);

    free(q->directory);
    free(q->queue_name);
    free(q);
}
