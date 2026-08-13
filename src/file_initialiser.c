#include "internal/file_initialiser.h"
#include "internal/mmap_assert.h"

#include <unistd.h>
#include <errno.h>

/**
 * mmapqi_file_init_zeros — initialise file to the given size, all bytes zero.
 *
 * Uses ftruncate(). The OS guarantees zero-filled pages on first access.
 * This is the correct approach for shared memory files — no need to
 * manually write zeros, the kernel handles it on page fault.
 *
 * Returns 0 on success, -1 on failure (errno set by ftruncate).
 */
int mmapqi_file_init_zeros(int fd, size_t size, void *ctx)
{
    (void)ctx;  /* unused */
    if (fd < 0) return -1;
    if (size <= 0) return -1;

    int result = ftruncate(fd, (off_t)size);
    return result;
}

/**
 * mmapqi_file_init_none — no-op initialiser.
 *
 * Use when the file already exists and no initialisation is required,
 * or when the caller has already initialised the file by other means.
 *
 * Always returns 0.
 */
int mmapqi_file_init_none(int fd, size_t size, void *ctx)
{
    (void)fd;
    (void)size;
    (void)ctx;
    return 0;
}
