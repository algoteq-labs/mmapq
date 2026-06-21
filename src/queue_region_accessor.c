#include "queue_region_accessor.h"
#include "file_mapper.h"
#include "file_initialiser.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── File path helpers ───────────────────────────────────────────────────── */

static void header_file_path(const char *directory, const char *queue_name,
                              char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/%s/header", directory, queue_name);
}

static void payload_file_path(const char *directory, const char *queue_name,
                               uint8_t appender_id, char *out, size_t out_size)
{
    snprintf(out, out_size, "%s/%s/payload_%u", directory, queue_name, appender_id);
}

/* ── Header file initialiser ─────────────────────────────────────────────── */

/*
 * RO header file: no initialisation — file must already exist.
 */

/* ── Create a RegionRingAccessor ─────────────────────────────────────────── */

static RegionAccessor *make_ring_accessor(
    FileMapper *mapper,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    double      premap_ratio,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    RegionRing *ring = region_ring_async_create(
        ring_size, region_size, mapper, timeout_ns, runtime);

    return region_ring_accessor_create(
        ring, region_size, regions_to_map_ahead, premap_ratio);
}

/* ── Allocate and zero a QueueRegionAccessor ─────────────────────────────── */

static QueueRegionAccessor *acc_alloc(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    int         read_only)
{
    QueueRegionAccessor *acc = calloc(1, sizeof(QueueRegionAccessor));
    MMAP_ASSERT(acc != NULL, "queue_region_accessor: out of memory");

    acc->directory           = strdup(directory);
    acc->queue_name          = strdup(queue_name);
    acc->read_only           = read_only;
    acc->region_size         = region_size;
    acc->ring_size           = ring_size;
    acc->regions_to_map_ahead = regions_to_map_ahead;
    acc->max_file_size       = max_file_size;
    acc->timeout_ns          = timeout_ns;
    acc->runtime             = runtime;

    return acc;
}

/* ── queue_region_accessor_create_rw ─────────────────────────────────────── */

QueueRegionAccessor *queue_region_accessor_create_rw(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime,
    uint8_t     appender_id)
{
    MMAP_ASSERT(directory  != NULL, "queue_region_accessor_create_rw: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "queue_region_accessor_create_rw: queue_name is NULL");
    MMAP_ASSERT(runtime    != NULL, "queue_region_accessor_create_rw: runtime is NULL");

    QueueRegionAccessor *acc = acc_alloc(directory, queue_name, region_size,
                                          ring_size, regions_to_map_ahead,
                                          max_file_size, timeout_ns, runtime, 0);

    /* ── RW header accessor ─────────────────────────────────────────────── */
    char hdr_path[4096];
    header_file_path(directory, queue_name, hdr_path, sizeof(hdr_path));

    FileMapper *hdr_mapper = file_mapper_rolled_create(
        hdr_path, max_file_size, region_size,
        file_init_none, NULL,
        MAP_MODE_READ_WRITE,
        ring_size + 2);  /* cache > ring_size */

    /*
     * Writer premap ratio = QUEUE_RW_PREMAP_RATIO (0.0):
     * Request next region immediately on entering a new one.
     * regions_to_map_ahead is forced to 0 for sync rings by
     * region_ring_accessor_create — not relevant here (we use async).
     */
    acc->header = make_ring_accessor(hdr_mapper, region_size, ring_size,
                                     regions_to_map_ahead,
                                     QUEUE_RW_PREMAP_RATIO,
                                     timeout_ns, runtime);

    /* ── RW payload accessor for this appender ──────────────────────────── */
    char pay_path[4096];
    payload_file_path(directory, queue_name, appender_id, pay_path, sizeof(pay_path));

    FileMapper *pay_mapper = file_mapper_rolled_create(
        pay_path, max_file_size, region_size,
        file_init_none, NULL,
        MAP_MODE_READ_WRITE,
        ring_size + 2);

    acc->payload[appender_id].accessor = make_ring_accessor(
        pay_mapper, region_size, ring_size,
        regions_to_map_ahead, QUEUE_RW_PREMAP_RATIO,
        timeout_ns, runtime);
    acc->payload[appender_id].occupied = 1;

    return acc;
}

/* ── queue_region_accessor_create_ro ─────────────────────────────────────── */

QueueRegionAccessor *queue_region_accessor_create_ro(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    Runtime    *runtime)
{
    MMAP_ASSERT(directory  != NULL, "queue_region_accessor_create_ro: directory is NULL");
    MMAP_ASSERT(queue_name != NULL, "queue_region_accessor_create_ro: queue_name is NULL");
    MMAP_ASSERT(runtime    != NULL, "queue_region_accessor_create_ro: runtime is NULL");

    QueueRegionAccessor *acc = acc_alloc(directory, queue_name, region_size,
                                          ring_size, regions_to_map_ahead,
                                          max_file_size, timeout_ns, runtime, 1);

    /* ── RO header accessor ─────────────────────────────────────────────── */
    char hdr_path[4096];
    header_file_path(directory, queue_name, hdr_path, sizeof(hdr_path));

    FileMapper *hdr_mapper = file_mapper_rolled_create(
        hdr_path, max_file_size, region_size,
        file_init_none, NULL,
        MAP_MODE_READ_ONLY,
        ring_size + 2);

    /*
     * Reader premap ratio = QUEUE_RO_PREMAP_RATIO (0.5):
     * Request next region when halfway through current one.
     */
    acc->header = make_ring_accessor(hdr_mapper, region_size, ring_size,
                                     regions_to_map_ahead,
                                     QUEUE_RO_PREMAP_RATIO,
                                     timeout_ns, runtime);

    /* Payload accessors are created lazily in queue_region_accessor_payload() */
    return acc;
}

/* ── queue_region_accessor_payload ───────────────────────────────────────── */

RegionAccessor *queue_region_accessor_payload(QueueRegionAccessor *acc,
                                               uint8_t appender_id)
{
    MMAP_ASSERT(acc != NULL, "queue_region_accessor_payload: acc is NULL");

    if (acc->payload[appender_id].occupied) {
        return acc->payload[appender_id].accessor;
    }

    /* Lazy creation — first time this appender_id is encountered */
    char pay_path[4096];
    payload_file_path(acc->directory, acc->queue_name, appender_id,
                      pay_path, sizeof(pay_path));

    MapMode mode = acc->read_only ? MAP_MODE_READ_ONLY : MAP_MODE_READ_WRITE;

    /*
     * Note: header availability implies payload file exists and can be
     * mapped without file-size checks (mirrors Java CHECK_FILE_SIZE_FALSE).
     */
    FileMapper *pay_mapper = file_mapper_rolled_create(
        pay_path, acc->max_file_size, acc->region_size,
        file_init_none, NULL,
        mode,
        acc->ring_size + 2);

    double premap_ratio = acc->read_only
        ? QUEUE_RO_PREMAP_RATIO
        : QUEUE_RW_PREMAP_RATIO;

    acc->payload[appender_id].accessor = make_ring_accessor(
        pay_mapper, acc->region_size, acc->ring_size,
        acc->regions_to_map_ahead, premap_ratio,
        acc->timeout_ns, acc->runtime);
    acc->payload[appender_id].occupied = 1;

    return acc->payload[appender_id].accessor;
}

/* ── queue_region_accessor_close ─────────────────────────────────────────── */

void queue_region_accessor_close(QueueRegionAccessor *acc)
{
    if (acc == NULL) return;

    if (acc->header != NULL) {
        region_ring_accessor_destroy(acc->header);
        acc->header = NULL;
    }

    for (int i = 0; i < MAX_APPENDERS; i++) {
        if (acc->payload[i].occupied && acc->payload[i].accessor != NULL) {
            region_ring_accessor_destroy(acc->payload[i].accessor);
            acc->payload[i].accessor = NULL;
            acc->payload[i].occupied = 0;
        }
    }

    free(acc->directory);
    free(acc->queue_name);
    free(acc);
}
