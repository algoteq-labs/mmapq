#ifndef FILE_MAPPER_H
#define FILE_MAPPER_H

#include <stdint.h>
#include <stddef.h>
#include "file_initialiser.h"

/**
 * FileMapper — maps and unmaps file regions to memory addresses.
 *
 * Abstracts over single-file and multi-file (rolled) mapping strategies.
 * Absolute logical positions are translated to per-file local positions
 * transparently, providing a contiguous logical address space across
 * multiple physical files.
 *
 * All region sizes must be aligned to page_size() (typically 4096 bytes).
 *
 * Special return values from map():
 *   FILE_MAPPER_NULL_ADDRESS    : mapping failed (generic)
 *   FILE_MAPPER_FILE_NOT_FOUND  : backing file does not exist yet
 *   FILE_MAPPER_BEYOND_FILE_SIZE: position exceeds current file size
 *
 * Positive return value: the mapped memory address (cast from uintptr_t).
 */

#define FILE_MAPPER_NULL_ADDRESS     ((uintptr_t)0)
#define FILE_MAPPER_FILE_NOT_FOUND   ((uintptr_t)(uintptr_t)-2)
#define FILE_MAPPER_BEYOND_FILE_SIZE ((uintptr_t)(uintptr_t)-3)

/**
 * Map mode — controls how files are opened and mapped.
 */
typedef enum {
    MAP_MODE_READ_ONLY,   /* open O_RDONLY, mmap PROT_READ */
    MAP_MODE_READ_WRITE,  /* open O_RDWR,   mmap PROT_READ|PROT_WRITE */
} MapMode;

/**
 * FileMapper — vtable-based polymorphic file mapper.
 *
 * The impl pointer holds the concrete implementation state.
 * Callers interact only through the function pointers.
 *
 * CONTRACT:
 *   map()   — maps a region of `length` bytes at `position`.
 *             Returns a positive address on success, or a sentinel on failure.
 *             length must equal the region_size the mapper was created with.
 *
 *   unmap() — unmaps a previously mapped region.
 *             address: value previously returned by map()
 *             position: same position passed to map()
 *             length: same length passed to map()
 *
 *   close() — releases all resources. After close(), map() and unmap()
 *             must not be called. Idempotent — safe to call more than once.
 */
typedef struct FileMapper {
    uintptr_t (*map)  (struct FileMapper *self, int64_t position, int32_t length);
    void      (*unmap)(struct FileMapper *self, uintptr_t address, int64_t position, int32_t length);
    void      (*close)(struct FileMapper *self);
    void      *impl;
} FileMapper;

/* ── Convenience wrappers ────────────────────────────────────────────────── */

static inline uintptr_t file_mapper_map(FileMapper *fm, int64_t position, int32_t length)
{
    return fm->map(fm, position, length);
}

static inline void file_mapper_unmap(FileMapper *fm, uintptr_t address, int64_t position, int32_t length)
{
    fm->unmap(fm, address, position, length);
}

static inline void file_mapper_close(FileMapper *fm)
{
    fm->close(fm);
}

/* ── Factory functions ───────────────────────────────────────────────────── */

/**
 * Single-file read-only mapper.
 *
 * Maps regions within a single named file. File must exist before map() is
 * called — returns FILE_MAPPER_FILE_NOT_FOUND otherwise.
 *
 * check_file_size: if true, verifies position < file_size before mapping.
 *                  Returns FILE_MAPPER_BEYOND_FILE_SIZE if exceeded.
 *                  Set false when file size is known to be pre-allocated.
 * out:            out-parameter for the created FileMapper (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL args,
 * MMAPQ_ENOMEM on allocation failure. Sets *out to NULL on failure.
 */
int mmapqi_file_mapper_single_ro_create(
    const char      *file_path,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    int              check_file_size,
    FileMapper     **out);

/**
 * Single-file read-write mapper.
 *
 * Creates the file if it does not exist. Extends file length as needed
 * up to max_size. Pre-touches newly extended pages to avoid page faults
 * on the hot path.
 *
 * out:            out-parameter for the created FileMapper (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure. Sets *out to NULL on failure.
 */
int mmapqi_file_mapper_single_rw_create(
    const char      *file_path,
    int64_t          max_size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    FileMapper     **out);

/**
 * Rolled (multi-file) mapper.
 *
 * Transparently spans multiple files. When a logical position exceeds
 * max_file_size, the next file is opened automatically.
 *
 * File names are derived from file_prefix: "<prefix>_0", "<prefix>_1", ...
 *
 * Maintains a cache of open file mappers (default max_cache_size = 4).
 * Files are evicted from the cache when:
 *   - ref_count reaches zero (no open regions reference this file), AND
 *   - cache size exceeds max_cache_size
 * Eviction uses insertion order (oldest first).
 *
 * INVARIANT: max_cache_size >= ring_size (enforced at creation with MMAP_ASSERT)
 *
 * mode: MAP_MODE_READ_ONLY or MAP_MODE_READ_WRITE
 * out:  out-parameter for the created FileMapper (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure. Sets *out to NULL on failure.
 */
int mmapqi_file_mapper_rolled_create(
    const char      *file_prefix,
    int64_t          max_file_size,
    int32_t          region_size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    MapMode          mode,
    int              max_cache_size,
    int              check_file_size,
    FileMapper     **out);

/**
 * Destroy a FileMapper and free all associated memory.
 *
 * Calls close() then frees the FileMapper struct and its impl.
 * After destroy(), the pointer must not be used.
 */
void mmapqi_file_mapper_destroy(FileMapper *fm);

#endif /* FILE_MAPPER_H */
