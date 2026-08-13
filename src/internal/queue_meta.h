#ifndef QUEUE_META_H
#define QUEUE_META_H

#include <stdint.h>
#include <stdatomic.h>
#include "mmap_assert.h"
#include "mmap_platform.h"
#include "fixed_size_region.h"
#include "file_initialiser.h"

/**
 * QueueMeta — persistent queue configuration stored in a fixed-size memory-mapped file.
 *
 * File: "<directory>/<queue_name>/meta"
 * Size: QUEUE_META_ENCODED_LENGTH (64 bytes, one cache line)
 *
 * Layout (byte offsets):
 *   0  : schema_version  (int32)
 *   4  : region_size     (int32)
 *   8  : start_index     (int64, volatile — atomic read when archivable)
 *   16 : max_file_size   (int64)
 *   24 : archivable      (uint8)
 *   25 : queue_type      (uint8) — 0 normal, 1 fixed-8 (Java, unsupported), 2 batch
 *   26 : padding to 64 bytes
 *
 * schema_version and queue_type are validated on every open; mismatch
 * refuses with MMAPQ_ESCHEMA.
 *
 * start_index:
 *   Always QUEUE_META_INITIAL_START_INDEX (0) for non-archivable queues.
 *   For archivable queues, read with atomic_load (acquire) to observe
 *   archiver writes from other processes. Written with atomic_store (release).
 *
 * Archiving is fully implemented in this C port — archivable may be 1
 * and start_index advances when entries are archived. The field layout
 * is compatible with the Java implementation.
 */

#define QUEUE_META_SCHEMA_VERSION        ((int32_t)1)
#define QUEUE_META_INITIAL_START_INDEX   ((int64_t)0)
/*
 * The meta file size must be 64 bytes for binary compatibility with Java.
 */
#define QUEUE_META_ENCODED_LENGTH        ((int32_t)64)

/*
 * Queue type discriminator, stored at offset 25.
 * Historically Java's boolean fixed_size_8bytes; values 0 and 1 keep their
 * exact Java semantics, so Java compatibility is preserved. Value 2 is
 * new and C-ecosystem only (batch queues must not be opened from Java).
 */
#define QUEUE_TYPE_NORMAL           ((uint8_t)0)   /* single-message, length-prefixed */
#define QUEUE_TYPE_FIXED8           ((uint8_t)1)   /* Java fixed 8-byte — NOT supported */
#define QUEUE_TYPE_BATCH            ((uint8_t)2)   /* batch framing */

/* Byte offsets within the meta region */
#define QUEUE_META_SCHEMA_VERSION_OFFSET ((int32_t)0)
#define QUEUE_META_REGION_SIZE_OFFSET    ((int32_t)4)
#define QUEUE_META_START_INDEX_OFFSET    ((int32_t)8)   /* must be 8-byte aligned */
#define QUEUE_META_MAX_FILE_SIZE_OFFSET  ((int32_t)16)
#define QUEUE_META_ARCHIVABLE_OFFSET     ((int32_t)24)
#define QUEUE_META_QUEUE_TYPE_OFFSET            ((int32_t)25)
#define QUEUE_META_FIXED_SIZE_8BYTES_OFFSET QUEUE_META_QUEUE_TYPE_OFFSET /* legacy alias */

/**
 * QueueMeta — in-memory view of the queue configuration.
 *
 * Backed by a memory-mapped file via a FixedSizeRegion. The fields are
 * read directly from the mapped buffer; start_index uses atomic access.
 *
 * Ownership: QueueMeta owns the RegionAccessor and manages its lifetime.
 * The caller must call mmapqi_queue_meta_close() when done.
 */
typedef struct {
    RegionAccessor *region;   /* FixedSizeRegion backing the meta file       */
    uint8_t        *base;     /* base pointer into mapped memory              */
    int32_t         region_size;    /* cached — written once at creation      */
    int64_t         max_file_size;  /* cached — written once at creation      */
    int             archivable;     /* cached — written once at creation      */
    int             queue_type;     /* cached — offset 25, see QUEUE_TYPE_*   */
    int64_t         start_index;    /* cached for non-archivable; atomic read for archivable */
} QueueMeta;

/* ── Field accessors ─────────────────────────────────────────────────────── */

static inline int32_t queue_meta_schema_version(const QueueMeta *m)
{
    return *(const int32_t *)(m->base + QUEUE_META_SCHEMA_VERSION_OFFSET);
}

static inline int32_t queue_meta_region_size(const QueueMeta *m)
{
    return m->region_size;
}

static inline int64_t queue_meta_max_file_size(const QueueMeta *m)
{
    return m->max_file_size;
}

static inline int queue_meta_archivable(const QueueMeta *m)
{
    return m->archivable;
}

static inline int queue_meta_queue_type(const QueueMeta *m)
{
    return m->queue_type;
}

/* Legacy alias — true for QUEUE_TYPE_FIXED8 (Java fixed-8-byte mode). */
static inline int queue_meta_fixed_size_8bytes(const QueueMeta *m)
{
    return m->queue_type == QUEUE_TYPE_FIXED8;
}

/**
 * queue_meta_start_index — read the queue's logical start index.
 *
 * For non-archivable queues the value is cached at open time and never
 * changes.  For archivable queues the archiver may advance it atomically.
 */
static inline int64_t queue_meta_start_index(const QueueMeta *m)
{
    if (!m->archivable)
        return m->start_index;
    const _Atomic int64_t *p = (const _Atomic int64_t *)(m->base + QUEUE_META_START_INDEX_OFFSET);
    return atomic_load_explicit(p, memory_order_acquire);
}

/**
 * queue_meta_set_start_index — write start_index with release semantics.
 * Only valid when archivable == 1. Not used in this C port but provided
 * for completeness.
 */
static inline void queue_meta_set_start_index(QueueMeta *m, int64_t index)
{
    MMAP_ASSERT(m->archivable, "queue_meta_set_start_index: queue is not archivable");
    _Atomic int64_t *p = (_Atomic int64_t *)(m->base + QUEUE_META_START_INDEX_OFFSET);
    atomic_store_explicit(p, index, memory_order_release);
}

/* ── Factory functions ───────────────────────────────────────────────────── */

/**
 * mmapqi_queue_meta_open_rw — open or create the meta file for writing.
 *
 * If the file does not exist, it is created and initialised with the given
 * parameters. If it already exists, the existing values are used (the
 * parameters are ignored for existing files).
 *
 * directory:      base directory for the queue
 * queue_name:     queue subdirectory name
 * start_index:    initial index (typically 0)
 * region_size:    region size in bytes (power of two, page-aligned)
 * max_file_size:  maximum rolled file size
 * archivable:     0 or 1
 * queue_type:     QUEUE_TYPE_NORMAL or QUEUE_TYPE_BATCH (FIXED8 is refused)
 * out:            out-parameter for the created QueueMeta (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure, MMAPQ_ETIMEDOUT on I/O error,
 * MMAPQ_ESCHEMA on schema mismatch. Sets *out to NULL on failure.
 */
int mmapqi_queue_meta_open_rw(
    const char *directory,
    const char *queue_name,
    int64_t     start_index,
    int32_t     region_size,
    int64_t     max_file_size,
    int         archivable,
    uint8_t     queue_type,
    QueueMeta **out);

/**
 * mmapqi_queue_meta_open_ro — open the meta file for reading only.
 *
 * directory:       base directory for the queue
 * queue_name:      queue subdirectory name
 * expected_type:   QUEUE_TYPE_NORMAL or QUEUE_TYPE_BATCH (FIXED8 is refused)
 * out:             out-parameter for the created QueueMeta (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_ENOENT if file does not exist,
 * MMAPQ_ETIMEDOUT on I/O error, MMAPQ_ESCHEMA on type/schema mismatch,
 * MMAPQ_EINVAL for NULL args. Sets *out to NULL on failure.
 */
int mmapqi_queue_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     expected_type,
    QueueMeta **out);

/**
 * mmapqi_queue_meta_close — unmap and free a QueueMeta.
 */
void mmapqi_queue_meta_close(QueueMeta *m);

/**
 * mmapqi_queue_meta_dest_check — validate a destination queue for import/copy.
 *
 * Returns 0 if the operation may proceed:
 *   - queue does not exist yet → caller should create it
 *   - exists and allow_append + REBASE → caller may append
 *
 * On failure returns a negative MMAPQ_* code:
 *   MMAPQ_ETIMEDOUT — stat, mapping, or allocation failure
 *   MMAPQ_ESCHEMA   — file exists but expected_type doesn't match
 *   MMAPQ_EEXIST    — exists and allow_append=0
 *   MMAPQ_EINVAL    — exists, allow_append=1, and preserve=1
 *
 * directory:      base queue directory
 * queue_name:     queue subdirectory name
 * expected_type:  QUEUE_TYPE_NORMAL or QUEUE_TYPE_BATCH
 * preserve:       1 if PRESERVE mode, 0 if REBASE
 * allow_append:   1 if appending to existing queue is permitted
 */
int mmapqi_queue_meta_dest_check(
    const char *directory, const char *queue_name,
    uint8_t expected_type, int preserve, int allow_append);

/* ── Reader internal accessors (declared here for cross-TU use) ─────────── */
struct MmapqIndexedReader;
struct MmapqIndexedBatchReader;
QueueMeta *mmapqi_indexed_reader_queue_meta(struct MmapqIndexedReader *r);
QueueMeta *mmapqi_indexed_batch_reader_queue_meta(struct MmapqIndexedBatchReader *r);

#endif /* QUEUE_META_H */
