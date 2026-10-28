#ifndef QUEUE_REGION_ACCESSOR_H
#define QUEUE_REGION_ACCESSOR_H

#include <stdint.h>
#include "internal/region_accessor.h"
#include "region_ring.h"
#include "region_ring_accessor.h"
#include "file_mapper.h"
#include "file_initialiser.h"
#include "runtime.h"
#include "header_codec.h"
#include "mmap_assert.h"

/**
 * QueueRegionAccessor — header + payload region accessors for one queue participant.
 *
 * Each appender, poller, or reader owns one QueueRegionAccessor.
 * It holds:
 *   - One RegionAccessor for the shared header file (all appenders/pollers share
 *     the same header file; each gets their own accessor instance)
 *   - One RegionAccessor per appender payload file accessed
 *     (appender: one RW accessor to its own payload file;
 *      poller/reader: one RO accessor per appender ID encountered)
 *
 * Header files: "<directory>/<queue_name>/header_<N>" (rolled: N = 0, 1, …)
 *   - Shared between all appenders and readers
 *   - Read-only for pollers/readers, read-write for appenders
 *
 * Payload files: "<directory>/<queue_name>/payload_<A>_<N>" (A = appender ID, N = roll index)
 *   - One file per appender
 *   - Read-write for the owning appender
 *   - Read-only for pollers/readers
 *
 * FILE NAMING MATCHES Java:
 *   header files  = "header_<N>"      (N = roll index, decimal)
 *   payload files = "payload_<A>_<N>" (A = appender ID, N = roll index)
 *
 * PRE-MAP TRIGGER RATIOS (from Java RegionAccessorSupplier):
 *   RO: 0.5 (premap at halfway through current region)
 *   RW: 0.0 (premap immediately — appender always at the frontier)
 *
 * Payload accessors for pollers/readers are created lazily: the first time
 * a header is decoded pointing to a given appender ID, that appender's
 * payload accessor is created.
 */

#define QUEUE_RO_PREMAP_RATIO  0.5
#define QUEUE_RW_PREMAP_RATIO  0.0

/**
 * PayloadAccessorEntry — one RO payload accessor for a single appender.
 * Used in a simple open-addressing table keyed by appender_id (0–255).
 */
typedef struct {
    RegionAccessor *accessor;
    int             occupied;
} PayloadAccessorEntry;

/**
 * QueueRegionAccessor — owns header + all payload region accessors.
 *
 * For appenders: one RW header accessor + one RW payload accessor (own file).
 * For pollers/readers: one RO header accessor + lazy RO payload accessors.
 */
typedef struct QueueRegionAccessor {
    RegionAccessor     *header;
    PayloadAccessorEntry payload[MAX_APPENDERS];  /* indexed by appender_id */
    char               *directory;
    char               *queue_name;
    int                 read_only;
    int                 payload_checked;   /* CAND-58 walk accessor: RO but
                                            * size-checked payload maps */
    int32_t             region_size;
    int                 ring_size;
    int                 regions_to_map_ahead;
    int64_t             max_file_size;
    uint64_t            timeout_ns;
    MmapqRuntime            *runtime;
} QueueRegionAccessor;

/* ── Factory ─────────────────────────────────────────────────────────────── */

/**
 * mmapqi_queue_region_accessor_create_rw — create read-write accessors (for appenders).
 *
 * Creates a RW header accessor and a RW payload accessor for appender_id.
 * The header file is created if it does not exist. The payload file is
 * created if it does not exist.
 *
 * runtime: the region mapper runtime for async mapping.
 * out:     out-parameter for the created accessor (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure, MMAPQ_ETIMEDOUT on I/O error.
 * Sets *out to NULL on failure.
 */
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
    QueueRegionAccessor **out);

/**
 * mmapqi_queue_region_accessor_create_ro — create read-only accessors (for pollers/readers).
 *
 * Creates a RO header accessor. Payload accessors are created lazily on
 * first access via mmapqi_queue_region_accessor_payload().
 *
 * runtime: the region mapper runtime for async mapping.
 * out:     out-parameter for the created accessor (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure, MMAPQ_ETIMEDOUT on I/O error.
 * Sets *out to NULL on failure.
 */
int mmapqi_queue_region_accessor_create_ro(
    const char *directory,
    const char *queue_name,
    int32_t     region_size,
    int         ring_size,
    int         regions_to_map_ahead,
    int64_t     max_file_size,
    uint64_t    timeout_ns,
    MmapqRuntime    *runtime,
    QueueRegionAccessor **out);

/**
 * mmapqi_queue_region_accessor_create_walk — the CAND-58 walk accessor.
 *
 * Read-only everywhere (never creates, never extends), but the payload side
 * is size-checked (check_file_size = 1): the writer walks/scans probe past
 * committed data, where the RO readers' no-check premise (Anti-Pattern E)
 * does not hold. The appender holds it next to rw_acc/ro_acc; the
 * uncommitted factory's span walk and header scan, the iterator's use
 * phase, and the join's payload walks map through it. The append path keeps
 * the RW accessors — only the append position ever creates or extends a
 * file (CAND-51e's rule, now by construction).
 *
 * runtime: the region mapper runtime for async mapping.
 * out:     out-parameter for the created accessor (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure, MMAPQ_ETIMEDOUT on I/O error.
 * Sets *out to NULL on failure.
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
    QueueRegionAccessor **out);

/* ── Accessor retrieval ──────────────────────────────────────────────────── */

/**
 * queue_region_accessor_header — get the header RegionAccessor.
 */
static inline RegionAccessor *queue_region_accessor_header(QueueRegionAccessor *acc)
{
    MMAP_ASSERT(acc != NULL,         "queue_region_accessor_header: acc is NULL");
    MMAP_ASSERT(acc->header != NULL, "queue_region_accessor_header: header not initialised");
    return acc->header;
}

/**
 * mmapqi_queue_region_accessor_payload — get (or lazily create) a payload RegionAccessor.
 *
 * For RO accessors: creates the payload accessor on first call for this appender_id.
 * For RW accessor: the appender's own payload accessor was created at construction.
 *
 * Returns the RegionAccessor for appender_id's payload file.
 */
RegionAccessor *mmapqi_queue_region_accessor_payload(QueueRegionAccessor *acc, uint8_t appender_id);

/* ── Lifecycle ───────────────────────────────────────────────────────────── */

/**
 * mmapqi_queue_region_accessor_close — close all owned accessors and free memory.
 */
void mmapqi_queue_region_accessor_close(QueueRegionAccessor *acc);

#endif /* QUEUE_REGION_ACCESSOR_H */
