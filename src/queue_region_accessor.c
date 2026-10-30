#include "internal/queue_region_accessor.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"
#include "internal/mmap_assert.h"
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

/*
 * make_ring_accessor — wrap mapper in a RegionRing + RegionAccessor.
 *
 * Ownership: on success the ring owns the mapper. On ANY failure the mapper
 * has been destroyed — by the ring create, which consumes the mapper on its
 * own failure paths (region_ring.c ownership rule), or by the ring's
 * destroy after an accessor-creation failure — and the caller must not
 * close, destroy, or otherwise touch it.
 */
static int make_ring_accessor(
    FileMapper *mapper,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    double      premap_ratio,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    RegionAccessor **out)
{
    /* Consumption is total, matching the ring creates: even a caller-error
     * EINVAL destroys the mapper (NULL-safe), keeping "any failure" exact. */
    if (out == NULL) { mmapqi_file_mapper_destroy(mapper); return MMAPQ_EINVAL; }
    *out = NULL;

    RegionRing *ring = NULL;
    int rc;
    if (runtime == NULL) {
        rc = mmapqi_region_ring_sync_create(ring_size, region_size, mapper, timeout_ns, &ring);
    } else {
        rc = mmapqi_region_ring_async_create(ring_size, region_size, mapper, timeout_ns, runtime, &ring);
    }
    if (rc != 0) return rc;   /* create consumed the mapper on failure */

    rc = mmapqi_region_ring_accessor_create(
        ring, region_size, regions_to_map_ahead, premap_ratio, out);
    if (rc != 0) {
        mmapqi_region_ring_destroy(ring);
        return rc;
    }
    return 0;
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
    MmapqRuntime    *runtime,
    int         read_only,
    int         payload_checked)
{
    QueueRegionAccessor *acc = calloc(1, sizeof(QueueRegionAccessor));
    if (acc == NULL) return NULL;

    acc->directory           = strdup(directory);
    if (acc->directory == NULL) { free(acc); return NULL; }
    acc->queue_name          = strdup(queue_name);
    if (acc->queue_name == NULL) { free(acc->directory); free(acc); return NULL; }
    acc->read_only           = read_only;
    acc->payload_checked     = payload_checked;
    acc->region_size         = region_size;
    acc->ring_size           = ring_size;
    acc->regions_to_map_ahead = regions_to_map_ahead;
    acc->max_file_size       = max_file_size;
    acc->timeout_ns          = timeout_ns;
    acc->runtime             = runtime;

    return acc;
}

/* ── mmapqi_queue_region_accessor_create_rw ─────────────────────────────────────── */

int mmapqi_queue_region_accessor_create_rw(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    uint8_t     appender_id,
    QueueRegionAccessor **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    QueueRegionAccessor *acc = acc_alloc(directory, queue_name, region_size,
                                          ring_size, regions_to_map_ahead,
                                          max_file_size, timeout_ns, runtime,
                                          0, 0);
    if (acc == NULL) return MMAPQ_ENOMEM;

    /* ── RW header accessor ─────────────────────────────────────────────── */
    char hdr_path[4096];
    header_file_path(directory, queue_name, hdr_path, sizeof(hdr_path));

    FileMapper *hdr_mapper = NULL;
    {
        int rc = mmapqi_file_mapper_rolled_create(
            hdr_path, max_file_size, region_size,
            mmapqi_file_init_none, NULL,
            MAP_MODE_READ_WRITE,
            ring_size + 2, 1,
            1 /* CAND-59: open existing only — never re-create a reclaimed
               * header file; frontier creation is explicit (header_create) */,
            &hdr_mapper);
        if (rc != 0) { free(acc->queue_name); free(acc->directory); free(acc); return rc; }
    }
    acc->header_mapper = hdr_mapper;   /* non-owning; the ring owns it */

    {
        int rc = make_ring_accessor(hdr_mapper, region_size, ring_size,
                                     regions_to_map_ahead,
                                     QUEUE_RW_PREMAP_RATIO,
                                     timeout_ns, runtime, &acc->header);
        if (rc != 0) {
            /* hdr_mapper destroyed by make_ring_accessor — do not touch */
            free(acc->queue_name); free(acc->directory); free(acc);
            return rc;
        }
    }

    /* ── RW payload accessor for this appender ──────────────────────────── */
    char pay_path[4096];
    payload_file_path(directory, queue_name, appender_id, pay_path, sizeof(pay_path));

    FileMapper *pay_mapper = NULL;
    {
        int rc = mmapqi_file_mapper_rolled_create(
            pay_path, max_file_size, region_size,
            mmapqi_file_init_none, NULL,
            MAP_MODE_READ_WRITE,
            ring_size + 2, 1, 0, &pay_mapper);
        if (rc != 0) {
            mmapqi_queue_region_accessor_close(acc);
            return rc;
        }
    }

    {
        int rc = make_ring_accessor(pay_mapper, region_size, ring_size,
                                     regions_to_map_ahead,
                                     QUEUE_RW_PREMAP_RATIO,
                                     timeout_ns, runtime,
                                     &acc->payload[appender_id].accessor);
        if (rc != 0) {
            /* pay_mapper destroyed by make_ring_accessor; close(acc) cannot
             * see it (occupied still 0) and must not */
            mmapqi_queue_region_accessor_close(acc);
            return rc;
        }
    }
    acc->payload[appender_id].occupied = 1;

    *out = acc;
    return 0;
}

/* ── mmapqi_queue_region_accessor_header_create (CAND-59) ────────────────── */

int mmapqi_queue_region_accessor_header_create(QueueRegionAccessor *acc,
                                               int64_t position)
{
    if (acc == NULL || acc->read_only || acc->header_mapper == NULL)
        return MMAPQ_EINVAL;
    return mmapqi_file_mapper_rolled_create_file(acc->header_mapper, position);
}

/* ── mmapqi_queue_region_accessor_create_ro ─────────────────────────────────────── */

int mmapqi_queue_region_accessor_create_ro(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    QueueRegionAccessor **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    QueueRegionAccessor *acc = acc_alloc(directory, queue_name, region_size,
                                          ring_size, regions_to_map_ahead,
                                          max_file_size, timeout_ns, runtime,
                                          1, 0);
    if (acc == NULL) return MMAPQ_ENOMEM;

    /* ── RO header accessor ─────────────────────────────────────────────── */
    char hdr_path[4096];
    header_file_path(directory, queue_name, hdr_path, sizeof(hdr_path));

    FileMapper *hdr_mapper = NULL;
    {
        int rc = mmapqi_file_mapper_rolled_create(
            hdr_path, max_file_size, region_size,
            mmapqi_file_init_none, NULL,
            MAP_MODE_READ_ONLY,
            ring_size + 2, 1, 0, &hdr_mapper);
        if (rc != 0) { free(acc->queue_name); free(acc->directory); free(acc); return rc; }
    }

    {
        int rc = make_ring_accessor(hdr_mapper, region_size, ring_size,
                                     regions_to_map_ahead,
                                     QUEUE_RO_PREMAP_RATIO,
                                     timeout_ns, runtime, &acc->header);
        if (rc != 0) {
            /* hdr_mapper destroyed by make_ring_accessor — do not touch */
            free(acc->queue_name); free(acc->directory); free(acc);
            return rc;
        }
    }

    /* Payload accessors are created lazily in mmapqi_queue_region_accessor_payload() */
    *out = acc;
    return 0;
}

/* ── mmapqi_queue_region_accessor_create_walk ────────────────────────────────────── */

/*
 * CAND-58 (spec v2.2 §4.1): the walk accessor — the writer walks' and scans'
 * own accessor. Read-only everywhere (never creates, never extends), but
 * unlike the poller's RO accessor its payload side is size-checked
 * (check_file_size = 1): a walk probes past committed data, where the
 * header-driven readers' no-check premise (Anti-Pattern E) fails — the
 * check is what turns a probe of a never-grown region into
 * BEYOND_FILE_SIZE instead of SIGBUS (CAND-51e's corrections, preserved).
 * The header side is the RO checked accessor, as before.
 */
int mmapqi_queue_region_accessor_create_walk(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    QueueRegionAccessor **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (directory  == NULL) return MMAPQ_EINVAL;
    if (queue_name == NULL) return MMAPQ_EINVAL;

    QueueRegionAccessor *acc = acc_alloc(directory, queue_name, region_size,
                                           ring_size, regions_to_map_ahead,
                                           max_file_size, timeout_ns, runtime,
                                           1, 1);
    if (acc == NULL) return MMAPQ_ENOMEM;

    /* ── RO header accessor (checked, as on the poller side) ────────────── */
    char hdr_path[4096];
    header_file_path(directory, queue_name, hdr_path, sizeof(hdr_path));

    FileMapper *hdr_mapper = NULL;
    {
        int rc = mmapqi_file_mapper_rolled_create(
            hdr_path, max_file_size, region_size,
            mmapqi_file_init_none, NULL,
            MAP_MODE_READ_ONLY,
            ring_size + 2, 1, 0, &hdr_mapper);
        if (rc != 0) { free(acc->queue_name); free(acc->directory); free(acc); return rc; }
    }

    {
        int rc = make_ring_accessor(hdr_mapper, region_size, ring_size,
                                     regions_to_map_ahead,
                                     QUEUE_RO_PREMAP_RATIO,
                                     timeout_ns, runtime, &acc->header);
        if (rc != 0) {
            /* hdr_mapper destroyed by make_ring_accessor — do not touch */
            free(acc->queue_name); free(acc->directory); free(acc);
            return rc;
        }
    }

    /* Payload accessors are created lazily in mmapqi_queue_region_accessor_payload() */
    *out = acc;
    return 0;
}

/* ── mmapqi_queue_region_accessor_payload ───────────────────────────────────────── */

RegionAccessor *mmapqi_queue_region_accessor_payload(QueueRegionAccessor *acc,
                                               uint8_t appender_id)
{
    if (acc == NULL) return NULL;

    if (acc->payload[appender_id].occupied) {
        return acc->payload[appender_id].accessor;
    }

    /* Lazy creation — first time this appender_id is encountered */
    char pay_path[4096];
    payload_file_path(acc->directory, acc->queue_name, appender_id,
                      pay_path, sizeof(pay_path));

    MapMode mode = acc->read_only ? MAP_MODE_READ_ONLY : MAP_MODE_READ_WRITE;

    /*
     * No file-size check on the payload read path (mirrors Java
     * CHECK_FILE_SIZE_FALSE). A committed header implies the writer has
     * already extended the file and written the message — commit order is
     * ftruncate → payload write → acq_rel header CAS. Checking the size
     * here is both hot-path cost (fstat per frontier region map) and wrong
     * where attribute visibility lags data: a committed, readable message
     * would fail BEYOND_FILE_SIZE against a stale size. A corrupt header
     * pointing past EOF is accepted risk (SIGBUS): queues are transient —
     * recreate. Header accessors keep the check (literal 1 in create_ro
     * above), where BEYOND_FILE_SIZE bounds re-wrap attempts on
     * out-of-range seeks.
     */
    /* CAND-58: the walk accessor (create_walk) is read-only but
     * size-checked on the payload side — the walks probe past committed
     * data, where the no-check premise does not hold (§3.2/§3.3 of the
     * spec); the RO mapper never creates, so a probe of a reclaimed file
     * misses instead of re-creating it. */
    int check_file_size = acc->read_only ? acc->payload_checked : 1;
    FileMapper *pay_mapper = NULL;
    if (mmapqi_file_mapper_rolled_create(
            pay_path, acc->max_file_size, acc->region_size,
            mmapqi_file_init_none, NULL,
            mode,
            acc->ring_size + 2,
            check_file_size, 0,
            &pay_mapper) != 0) return NULL;

    double premap_ratio = acc->read_only
        ? QUEUE_RO_PREMAP_RATIO
        : QUEUE_RW_PREMAP_RATIO;

    /* on failure make_ring_accessor has destroyed pay_mapper — do not touch */
    if (make_ring_accessor(
            pay_mapper, acc->region_size, acc->ring_size,
            acc->regions_to_map_ahead, premap_ratio,
            acc->timeout_ns, acc->runtime,
            &acc->payload[appender_id].accessor) != 0) return NULL;
    acc->payload[appender_id].occupied = 1;

    return acc->payload[appender_id].accessor;
}

/* ── mmapqi_queue_region_accessor_close ─────────────────────────────────────────── */

void mmapqi_queue_region_accessor_close(QueueRegionAccessor *acc)
{
    if (acc == NULL) return;

    if (acc->header != NULL) {
        mmapqi_region_ring_accessor_destroy(acc->header);
        acc->header = NULL;
    }

    for (int i = 0; i < MAX_APPENDERS; i++) {
        if (acc->payload[i].occupied && acc->payload[i].accessor != NULL) {
            mmapqi_region_ring_accessor_destroy(acc->payload[i].accessor);
            acc->payload[i].accessor = NULL;
            acc->payload[i].occupied = 0;
        }
    }

    free(acc->directory);
    free(acc->queue_name);
    free(acc);
}
