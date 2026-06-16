#ifndef FIXED_SIZE_REGION_H
#define FIXED_SIZE_REGION_H

#include <stdint.h>
#include "region_accessor.h"
#include "file_initialiser.h"

/**
 * FixedSizeRegion — a single permanently-mapped file region.
 *
 * Maps an entire file of a fixed size into memory once at creation.
 * The mapping persists until close() is called.
 * No ring, no rolling, no async mapping — just one mmap for the
 * lifetime of the object.
 *
 * Used for structures that require a small fixed shared memory area:
 *   - AppenderId bitset pool (typically 32 bytes for 256 IDs)
 *   - Other fixed shared state between processes
 *
 * Implements RegionAccessor:
 *   wrap()           — wraps buffer to (data + position, size - position).
 *                      Returns WRAP_BEYOND_FILE_SIZE if position >= size.
 *   wrap_no_premap() — identical to wrap() (no ring, premap irrelevant).
 *   close()          — unmaps the region and closes the file.
 *   size()           — returns the fixed size.
 *
 * INVARIANT: size must be a multiple of page_size() (MMAP_ASSERT enforced).
 * INVARIANT: position passed to wrap() must be < size (returns error, not abort).
 */

/**
 * Create a FixedSizeRegion.
 *
 * file_path:       path to the backing file (created if it does not exist)
 * size:            exact size of the region in bytes (must be page-aligned)
 * initialiser:     called when file is newly created (typically file_init_zeros)
 * initialiser_ctx: context passed to initialiser (may be NULL)
 *
 * Returns a RegionAccessor* on success.
 * Aborts via MMAP_ASSERT if the mmap fails — this is a fatal configuration error.
 */
RegionAccessor *fixed_size_region_create(
    const char      *file_path,
    int32_t          size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx);

/**
 * Destroy a FixedSizeRegion and free all memory.
 * Calls close() then frees the RegionAccessor and its impl.
 */
void fixed_size_region_destroy(RegionAccessor *acc);

#endif /* FIXED_SIZE_REGION_H */
