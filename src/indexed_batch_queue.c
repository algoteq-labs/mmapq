#include "indexed_batch_queue.h"
#include "indexed_batch_reader.h"
#include "indexed_batch_archiver.h"
#include "indexed_queue.h"
#include "queue_meta.h"
#include "mmap_assert.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

struct IndexedBatchQueue {
    char    *directory;
    char    *queue_name;
    int32_t  region_size;
    int      ring_size;
    int      regions_to_map_ahead;
    int64_t  max_file_size;
    int64_t  start_index;
    uint64_t timeout_ns;
    Runtime *write_runtime;
    Runtime *read_runtime;

    QueueMeta *queue_meta;
    int        closed;
};

static void create_queue_directory(const char *directory, const char *queue_name)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", directory, queue_name);

    if (mkdir(path, 0775) != 0) {
        if (errno != EEXIST) {
            MMAP_ASSERT(0, "indexed_batch_queue: failed to create queue directory");
        }
    }
}

IndexedBatchQueue *indexed_batch_queue_create(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    int64_t     start_index,
    uint64_t    timeout_ns,
    Runtime    *write_runtime,
    Runtime    *read_runtime)
{
    IndexedBatchQueue *q = calloc(1, sizeof(IndexedBatchQueue));
    MMAP_ASSERT(q != NULL, "out of memory");

    q->directory            = strdup(directory);
    q->queue_name           = strdup(queue_name);
    q->region_size          = region_size;
    q->ring_size            = ring_size;
    q->regions_to_map_ahead = regions_to_map_ahead;
    q->max_file_size        = max_file_size;
    q->start_index          = start_index;
    q->timeout_ns           = timeout_ns;
    q->write_runtime        = write_runtime;
    q->read_runtime         = read_runtime;
    q->closed               = 0;

    return q;
}

IndexedBatchQueue *indexed_batch_queue_create_default(
    const char *directory,
    const char *queue_name,
    Runtime    *write_runtime,
    Runtime    *read_runtime)
{
    return indexed_batch_queue_create(
        directory, queue_name,
        INDEXED_QUEUE_DEFAULT_REGION_SIZE,
        INDEXED_QUEUE_DEFAULT_RING_SIZE,
        INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD,
        INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE,
        0,
        INDEXED_QUEUE_DEFAULT_TIMEOUT_NS,
        write_runtime, read_runtime);
}

IndexedBatchAppender *indexed_batch_queue_create_appender(IndexedBatchQueue *q)
{
    MMAP_ASSERT(!q->closed, "queue closed");
    create_queue_directory(q->directory, q->queue_name);
    return indexed_batch_appender_create(q->directory, q->queue_name, q->region_size, q->ring_size, q->max_file_size, q->timeout_ns, q->write_runtime);
}

IndexedBatchPoller *indexed_batch_queue_create_poller(IndexedBatchQueue *q)
{
    MMAP_ASSERT(!q->closed, "queue closed");
    return indexed_batch_poller_create(q->directory, q->queue_name, q->ring_size, q->timeout_ns, q->read_runtime);
}

IndexedBatchReader *indexed_batch_queue_create_reader(IndexedBatchQueue *q)
{
    MMAP_ASSERT(!q->closed, "queue closed");
    return indexed_batch_reader_create(q->directory, q->queue_name, q->ring_size, q->timeout_ns, q->read_runtime);
}

IndexedBatchArchiver *indexed_batch_queue_create_archiver(IndexedBatchQueue *q)
{
    MMAP_ASSERT(q != NULL,  "indexed_batch_queue_create_archiver: queue is NULL");
    MMAP_ASSERT(!q->closed, "indexed_batch_queue_create_archiver: queue is closed");
    return indexed_batch_archiver_create(
        q->directory, q->queue_name, q->ring_size, q->read_runtime);
}

void indexed_batch_queue_close(IndexedBatchQueue *q)
{
    if (!q || q->closed) return;
    q->closed = 1;
    if (q->queue_meta) queue_meta_close(q->queue_meta);
    free(q->directory);
    free(q->queue_name);
    free(q);
}
