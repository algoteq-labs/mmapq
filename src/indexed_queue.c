#include "indexed_queue.h"
#include "indexed_archiver.h"
#include "queue_meta.h"
#include "appender_id_pool.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
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
    uint64_t timeout_ns;
    Runtime *write_runtime;
    Runtime *read_runtime;

    QueueMeta *queue_meta;   /* Lazily opened RO meta */
    int        closed;
};

/* ── Directory creation ──────────────────────────────────────────────────── */

static void create_queue_directory(const char *directory, const char *queue_name)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", directory, queue_name);

    if (mkdir(path, 0775) != 0) {
        if (errno != EEXIST) {
            MMAP_ASSERT(0, "indexed_queue_create: failed to create queue directory");
        }
        /* EEXIST is fine — directory already exists */
    }
}

/* ── indexed_queue_create ────────────────────────────────────────────────── */

IndexedQueue *indexed_queue_create(
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
    MMAP_ASSERT(directory  != NULL, "indexed_queue_create: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "indexed_queue_create: queue_name is NULL");
    MMAP_ASSERT(region_size > 0,    "indexed_queue_create: region_size must be > 0");
    MMAP_ASSERT((region_size & (region_size - 1)) == 0,
        "indexed_queue_create: region_size must be a power of two");
    MMAP_ASSERT(ring_size > 0,      "indexed_queue_create: ring_size must be > 0");
    MMAP_ASSERT((ring_size & (ring_size - 1)) == 0,
        "indexed_queue_create: ring_size must be a power of two");
    MMAP_ASSERT(max_file_size > 0,  "indexed_queue_create: max_file_size must be > 0");
    MMAP_ASSERT(max_file_size % region_size == 0,
        "indexed_queue_create: max_file_size must be a multiple of region_size");
    MMAP_ASSERT(start_index >= 0,   "indexed_queue_create: start_index must be >= 0");

    IndexedQueue *q = calloc(1, sizeof(IndexedQueue));
    MMAP_ASSERT(q != NULL, "indexed_queue_create: out of memory");

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

    /*
     * Validate cross-process atomic requirements at startup.
     */
    {
        _Atomic uint64_t test64 = 0;
        _Atomic uint32_t test32 = 0;
        (void)test64;
        (void)test32;
        MMAP_ASSERT(atomic_is_lock_free(&test64),
            "indexed_queue_create: uint64_t atomics are not lock-free on this platform.");
        MMAP_ASSERT(atomic_is_lock_free(&test32),
            "indexed_queue_create: uint32_t atomics are not lock-free on this platform.");
    }

    return q;
}

/* ── indexed_queue_create_default ────────────────────────────────────────── */

IndexedQueue *indexed_queue_create_default(
    const char *directory,
    const char *queue_name,
    Runtime    *write_runtime,
    Runtime    *read_runtime)
{
    return indexed_queue_create_default_opt(directory, queue_name, write_runtime, read_runtime);
}

/* ── indexed_queue_create_default_opt ────────────────────────────────────── */

IndexedQueue *indexed_queue_create_default_opt(
    const char *directory,
    const char *queue_name,
    Runtime    *write_runtime,
    Runtime    *read_runtime)
{
    return indexed_queue_create(
        directory, queue_name,
        INDEXED_QUEUE_DEFAULT_REGION_SIZE,
        INDEXED_QUEUE_DEFAULT_RING_SIZE,
        INDEXED_QUEUE_DEFAULT_REGIONS_TO_MAP_AHEAD,
        INDEXED_QUEUE_DEFAULT_MAX_FILE_SIZE,
        /* start_index = */ 0,
        INDEXED_QUEUE_DEFAULT_TIMEOUT_NS,
        write_runtime,
        read_runtime);
}

/* ── Participant factories ────────────────────────────────────────────────── */

IndexedAppender *indexed_queue_create_appender(IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL,  "indexed_queue_create_appender: queue is NULL");
    MMAP_ASSERT(!q->closed, "indexed_queue_create_appender: queue is closed");

    create_queue_directory(q->directory, q->queue_name);

    return indexed_appender_create(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns, q->write_runtime);
}

IndexedAppender *indexed_queue_create_appender_with_id(
    IndexedQueue *q,
    uint8_t       appender_id)
{
    MMAP_ASSERT(q != NULL,  "indexed_queue_create_appender_with_id: queue is NULL");
    MMAP_ASSERT(!q->closed, "indexed_queue_create_appender_with_id: queue is closed");

    create_queue_directory(q->directory, q->queue_name);

    return indexed_appender_create_with_id(
        q->directory, q->queue_name,
        q->region_size, q->ring_size,
        q->max_file_size, q->timeout_ns, q->write_runtime,
        appender_id);
}

IndexedPoller *indexed_queue_create_poller(IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL,  "indexed_queue_create_poller: queue is NULL");
    MMAP_ASSERT(!q->closed, "indexed_queue_create_poller: queue is closed");

    return indexed_poller_create(
        q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime);
}

IndexedReader *indexed_queue_create_reader(IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL,  "indexed_queue_create_reader: queue is NULL");
    MMAP_ASSERT(!q->closed, "indexed_queue_create_reader: queue is closed");

    return indexed_reader_create(
        q->directory, q->queue_name,
        q->ring_size, q->timeout_ns, q->read_runtime);
}

IndexedArchiver *indexed_queue_create_archiver(IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL,  "indexed_queue_create_archiver: queue is NULL");
    MMAP_ASSERT(!q->closed, "indexed_queue_create_archiver: queue is closed");

    return indexed_archiver_create(
        q->directory, q->queue_name, q->ring_size, q->read_runtime);
}

/* ── Metadata ─────────────────────────────────────────────────────────────── */

static void ensure_meta_open(IndexedQueue *q)
{
    if (q->queue_meta != NULL) return;
    q->queue_meta = queue_meta_open_ro(q->directory, q->queue_name);
}

int32_t indexed_queue_region_size(const IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL, "indexed_queue_region_size: queue is NULL");
    ensure_meta_open((IndexedQueue *)q);
    return q->queue_meta ? queue_meta_region_size(q->queue_meta) : q->region_size;
}

int64_t indexed_queue_max_file_size(const IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL, "indexed_queue_max_file_size: queue is NULL");
    ensure_meta_open((IndexedQueue *)q);
    return q->queue_meta ? queue_meta_max_file_size(q->queue_meta) : q->max_file_size;
}

int64_t indexed_queue_start_index(const IndexedQueue *q)
{
    MMAP_ASSERT(q != NULL, "indexed_queue_start_index: queue is NULL");
    ensure_meta_open((IndexedQueue *)q);
    return q->queue_meta ? queue_meta_start_index(q->queue_meta) : q->start_index;
}

/* ── indexed_queue_close ─────────────────────────────────────────────────── */

/* ── indexed_queue_close ─────────────────────────────────────────────────── */

void indexed_queue_close(IndexedQueue *q)
{
    if (q == NULL || q->closed) return;
    q->closed = 1;

    if (q->queue_meta != NULL) {
        queue_meta_close(q->queue_meta);
        q->queue_meta = NULL;
    }

    free(q->directory);
    free(q->queue_name);
    free(q);
}
