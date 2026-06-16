#ifndef APPENDER_META_H
#define APPENDER_META_H

#include <stdint.h>
#include <stdatomic.h>
#include "mmap_assert.h"
#include "fixed_size_region.h"
#include "header_codec.h"

/**
 * AppenderMeta — per-appender persistent metadata stored in a fixed-size
 * memory-mapped file.
 *
 * File: "<directory>/<queue_name>/appender_meta_<appenderId>"
 * Size: APPENDER_META_ENCODED_LENGTH (64 bytes, one cache line)
 *
 * Layout (byte offsets):
 *   0  : start_offset  (int64, volatile — read/written with atomic ops)
 *   8  : appender_id   (int16)
 *   10 : padding to 64 bytes
 *
 * start_offset:
 *   The earliest valid payload position for this appender.
 *   Initialised to INITIAL_PAYLOAD_POSITION (64) at creation.
 *   Updated by the archiver when old data is removed.
 *   Read with acquire, written with release — safe across processes.
 *
 * Archiving is excluded from this C port. start_offset remains at its
 * initial value throughout the queue's lifetime here. The field layout
 * is preserved for binary compatibility with the Java implementation.
 */

/*
 * The appender meta struct needs ~10 bytes of data, but fixed_size_region_create
 * requires a multiple of page_size() (4096 bytes on Linux, 16384 on macOS M1).
 * We use page_size() at runtime so this works on both platforms.
 */
#include "mmap_platform.h"
static inline int32_t appender_meta_encoded_length(void)
{
    return (int32_t)page_size();
}
#define APPENDER_META_ENCODED_LENGTH appender_meta_encoded_length()
#define APPENDER_META_START_OFFSET_OFFSET  ((int32_t)0)
#define APPENDER_META_APPENDER_ID_OFFSET   ((int32_t)8)

/**
 * AppenderMeta — in-memory view of a single appender's persistent metadata.
 *
 * Backed by a memory-mapped file via a FixedSizeRegion.
 */
typedef struct {
    RegionAccessor *region;  /* FixedSizeRegion backing the meta file */
    uint8_t        *base;    /* base pointer into mapped memory       */
    uint8_t         appender_id;
} AppenderMeta;

/* ── Field accessors ─────────────────────────────────────────────────────── */

/**
 * appender_meta_start_offset — read start_offset with acquire semantics.
 *
 * This is the earliest valid payload position for this appender. Readers
 * and pollers below this offset are in archived territory.
 */
static inline int64_t appender_meta_start_offset(const AppenderMeta *m)
{
    const _Atomic int64_t *p =
        (const _Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    return atomic_load_explicit(p, memory_order_acquire);
}

/**
 * appender_meta_set_start_offset — write start_offset with release semantics.
 * Used by the archiver. Provided for completeness; not used in this C port.
 */
static inline void appender_meta_set_start_offset(AppenderMeta *m, int64_t offset)
{
    _Atomic int64_t *p =
        (_Atomic int64_t *)(m->base + APPENDER_META_START_OFFSET_OFFSET);
    atomic_store_explicit(p, offset, memory_order_release);
}

static inline uint8_t appender_meta_appender_id(const AppenderMeta *m)
{
    return m->appender_id;
}

/* ── Factory functions ───────────────────────────────────────────────────── */

/**
 * appender_meta_open_rw — open or create the appender meta file.
 *
 * Creates the file if it does not exist, initialised with the given
 * appender_id and start_offset = INITIAL_PAYLOAD_POSITION.
 * Returns a heap-allocated AppenderMeta. Aborts on failure.
 */
AppenderMeta *appender_meta_open_rw(
    const char *directory,
    const char *queue_name,
    uint8_t     appender_id);

/**
 * appender_meta_open_ro — open the appender meta file for reading.
 *
 * Returns NULL if the file does not exist.
 * Returns a heap-allocated AppenderMeta on success.
 */
AppenderMeta *appender_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     appender_id);

/**
 * appender_meta_close — unmap and free an AppenderMeta.
 */
void appender_meta_close(AppenderMeta *m);

#endif /* APPENDER_META_H */
