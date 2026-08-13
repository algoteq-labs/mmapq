#include "indexed_batch_queue.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_archiver.h"
#include "indexed_queue.h"
#include "internal/queue_meta.h"
#include "mmap_platform.h"
#include "internal/mmap_assert.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

struct MmapqIndexedBatchQueue {
    char    *directory;
    char    *queue_name;
    int32_t  region_size;
    int      ring_size;
    int      regions_to_map_ahead;
    int64_t  max_file_size;
    int64_t  start_index;
    int      archivable;
    uint64_t timeout_ns;
    MmapqRuntime *write_runtime;
    MmapqRuntime *read_runtime;

    QueueMeta *queue_meta;
    int        closed;
};

static int create_queue_directory(const char *directory, const char *queue_name)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", directory, queue_name);

    if (mkdir(path, 0775) != 0) {
        if (errno != EEXIST) {
            return -1;
        }
    }
    return 0;
}

int mmapq_indexed_batch_queue_create(
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
    MmapqIndexedBatchQueue **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
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

    MmapqIndexedBatchQueue *q = calloc(1, sizeof(MmapqIndexedBatchQueue));
    if (q == NULL) return MMAPQ_ENOMEM;

    q->directory = strdup(directory);
    if (q->directory == NULL) { free(q); return MMAPQ_ENOMEM; }
    q->queue_name = strdup(queue_name);
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
    q->closed               = 0;

    *out = q;
    return 0;
}

int mmapq_indexed_batch_queue_create_default(
    const char *directory,
    const char *queue_name,
    MmapqRuntime    *write_runtime,
    MmapqRuntime    *read_runtime,
    MmapqIndexedBatchQueue **out)
{
    return mmapq_indexed_batch_queue_create(
        directory, queue_name,
        MMAPQ_INDEXED_QUEUE_DEFAULT_REGION_SIZE,
        MMAPQ_INDEXED_QUEUE_DEFAULT_RING_SIZE,
        MMAPQ_INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD,
        MMAPQ_INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE,
        0,
        MMAPQ_INDEXED_QUEUE_DEFAULT_TIMEOUT_NS,
        0,
        write_runtime, read_runtime, out);
}

int mmapq_indexed_batch_queue_create_appender(
    MmapqIndexedBatchQueue *q, MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (q->closed) return MMAPQ_ECLOSED;
    if (create_queue_directory(q->directory, q->queue_name) != 0) return MMAPQ_ETIMEDOUT;
    return mmapq_indexed_batch_appender_create(q->directory, q->queue_name,
        q->region_size, q->ring_size, q->max_file_size, q->timeout_ns,
        q->archivable, q->write_runtime, q->start_index, out);
}

int mmapq_indexed_batch_queue_create_appender_with_id(
    MmapqIndexedBatchQueue *q, uint8_t appender_id, MmapqIndexedBatchAppender **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (q->closed) return MMAPQ_ECLOSED;
    if (create_queue_directory(q->directory, q->queue_name) != 0) return MMAPQ_ETIMEDOUT;
    return mmapq_indexed_batch_appender_create_with_id(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns,
        q->archivable, q->write_runtime, appender_id,
        q->start_index, out);
}

int mmapq_indexed_batch_queue_create_poller(
    MmapqIndexedBatchQueue *q, MmapqIndexedBatchPoller **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (q->closed) return MMAPQ_ECLOSED;
    return mmapq_indexed_batch_poller_create(q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime, out);
}

int mmapq_indexed_batch_queue_create_reader(
    MmapqIndexedBatchQueue *q, MmapqIndexedBatchReader **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (q->closed) return MMAPQ_ECLOSED;
    return mmapq_indexed_batch_reader_create(q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime, out);
}

int mmapq_indexed_batch_queue_create_archiver(
    MmapqIndexedBatchQueue *q, MmapqIndexedBatchArchiver **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (q == NULL) return MMAPQ_EINVAL;
    if (q->closed) return MMAPQ_ECLOSED;
    return mmapq_indexed_batch_archiver_create(
        q->directory, q->queue_name, q->ring_size, q->read_runtime, out);
}

void mmapq_indexed_batch_queue_close(MmapqIndexedBatchQueue *q)
{
    if (!q || q->closed) return;
    q->closed = 1;
    if (q->queue_meta) mmapqi_queue_meta_close(q->queue_meta);
    free(q->directory);
    free(q->queue_name);
    free(q);
}
