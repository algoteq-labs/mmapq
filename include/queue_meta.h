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
 *   25 : fixed_size_8bytes (uint8)
 *   26 : padding to 64 bytes
 *
 * start_index:
 *   Always QUEUE_META_INITIAL_START_INDEX (0) for non-archivable queues.
 *   For archivable queues, read with atomic_load (acquire) to observe
 *   archiver writes from other processes. Written with atomic_store (release).
 *
 * Archiving is excluded from this C port — archivable is always 0 and
 * start_index is always 0. The field layout is preserved for binary
 * compatibility with the Java implementation.
 */

#define QUEUE_META_SCHEMA_VERSION        ((int32_t)1)
#define QUEUE_META_INITIAL_START_INDEX   ((int64_t)0)
/*
 * The meta file size must be 64 bytes for binary compatibility with Java.
 */
#define QUEUE_META_ENCODED_LENGTH        ((int32_t)64)

/* Byte offsets within the meta region */
#define QUEUE_META_SCHEMA_VERSION_OFFSET ((int32_t)0)
#define QUEUE_META_REGION_SIZE_OFFSET    ((int32_t)4)
#define QUEUE_META_START_INDEX_OFFSET    ((int32_t)8)   /* must be 8-byte aligned */
#define QUEUE_META_MAX_FILE_SIZE_OFFSET  ((int32_t)16)
#define QUEUE_META_ARCHIVABLE_OFFSET     ((int32_t)24)
#define QUEUE_META_FIXED_SIZE_8BYTES_OFFSET ((int32_t)25)

/**
 * QueueMeta — in-memory view of the queue configuration.
 *
 * Backed by a memory-mapped file via a FixedSizeRegion. The fields are
 * read directly from the mapped buffer; start_index uses atomic access.
 *
 * Ownership: QueueMeta owns the RegionAccessor and manages its lifetime.
 * The caller must call queue_meta_close() when done.
 */
typedef struct {
    RegionAccessor *region;   /* FixedSizeRegion backing the meta file       */
    uint8_t        *base;     /* base pointer into mapped memory              */
    int32_t         region_size;    /* cached — written once at creation      */
    int64_t         max_file_size;  /* cached — written once at creation      */
    int             archivable;     /* cached — written once at creation      */
    int             fixed_size_8bytes; /* cached — written once at creation   */
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

static inline int queue_meta_fixed_size_8bytes(const QueueMeta *m)
{
    return m->fixed_size_8bytes;
}

/**
 * queue_meta_start_index — read start_index with acquire semantics.
 *
 * For non-archivable queues (this C port) always returns 0.
 * The atomic load is retained for binary compatibility and future archiving support.
 */
static inline int64_t queue_meta_start_index(const QueueMeta *m)
{
    if (!m->archivable) {
        return QUEUE_META_INITIAL_START_INDEX;
    }
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
 * queue_meta_open_rw — open or create the meta file for writing.
 *
 * If the file does not exist, it is created and initialised with the given
 * parameters. If it already exists, the existing values are used (the
 * parameters are ignored for existing files).
 *
 * directory:    base directory for the queue (e.g. "/data/queues")
 * queue_name:   queue subdirectory name
 * region_size:  region size in bytes (power of two, page-aligned)
 * max_file_size: maximum rolled file size
 * archivable:   0 or 1 (always 0 in this C port)
 *
 * Returns a heap-allocated QueueMeta on success.
 * Aborts via MMAP_ASSERT on failure.
 */
QueueMeta *queue_meta_open_rw(
    const char *directory,
    const char *queue_name,
    int64_t     start_index,
    int32_t     region_size,
    int64_t     max_file_size,
    int         archivable);

/**
 * queue_meta_open_ro — open the meta file for reading only.
 *
 * Returns NULL if the meta file does not exist yet (queue not created).
 * Returns a heap-allocated QueueMeta on success.
 */
QueueMeta *queue_meta_open_ro(
    const char *directory,
    const char *queue_name);

/**
 * queue_meta_close — unmap and free a QueueMeta.
 */
void queue_meta_close(QueueMeta *m);

#endif /* QUEUE_META_H */
