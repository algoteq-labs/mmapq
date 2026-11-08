#ifndef FIXED_SIZE_REGION_H
#define FIXED_SIZE_REGION_H

#include <stdint.h>
#include "internal/region_accessor.h"
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
 * initialiser:     called when file is newly created (typically mmapqi_file_init_zeros)
 * initialiser_ctx: context passed to initialiser (may be NULL)
 * out:             out-parameter for the created accessor (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_EINVAL for NULL/invalid args,
 * MMAPQ_ENOMEM on allocation failure, MMAPQ_ETIMEDOUT on mmap failure.
 * Sets *out to NULL on failure.
 */
int mmapqi_fixed_size_region_create(
    const char      *file_path,
    int32_t          size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    RegionAccessor **out);

/**
 * Open an existing FixedSizeRegion read-only.
 *
 * Opens O_RDONLY (never creates the file) and maps PROT_READ, so a caller
 * needs only read permission. A file shorter than size is answered
 * without locking; at full size the open takes flock(LOCK_SH), re-checks
 * the size and maps, so it waits out a creator's LOCK_EX initialisation
 * and never maps a partially written file.
 *
 * file_path:  path to the backing file (must exist)
 * size:       exact size of the region in bytes
 * out:        out-parameter for the accessor (NULL on failure)
 *
 * Returns 0 on success, MMAPQ_ENOENT if the file does not exist,
 * MMAPQ_ENOTREADY if it is shorter than size, MMAPQ_EINVAL for NULL/invalid
 * args, MMAPQ_ENOMEM on allocation failure, MMAPQ_ETIMEDOUT on any other
 * open/fstat/flock/mmap failure. Release with mmapqi_fixed_size_region_destroy.
 */
int mmapqi_fixed_size_region_open_ro(
    const char      *file_path,
    int32_t          size,
    RegionAccessor **out);

/**
 * Destroy a FixedSizeRegion and free all memory.
 * Calls close() then frees the RegionAccessor and its impl.
 */
void mmapqi_fixed_size_region_destroy(RegionAccessor *acc);

#endif /* FIXED_SIZE_REGION_H */
