#ifndef FILE_INITIALISER_H
#define FILE_INITIALISER_H

#include <stddef.h>

/**
 * FileInitialiser — callback invoked when a new file is created before mapping.
 *
 * The initialiser is responsible for pre-filling the file with initial values
 * (typically zeros) and performing any file-level setup required before the
 * first mmap. It is called from the region mapper thread (async) or the
 * application thread (sync) during file creation.
 *
 * CONTRACT:
 *   - fd      : open file descriptor, positioned at offset 0
 *   - size    : exact size the file must be initialised to (bytes)
 *   - ctx     : caller-supplied context pointer (may be NULL)
 *   - returns : 0 on success, -1 on failure (errno set)
 *
 * The initialiser must not close fd. The caller retains ownership.
 *
 * Example — fill with zeros:
 *   int zero_initialiser(int fd, size_t size, void *ctx) {
 *       return ftruncate(fd, (off_t)size);
 *   }
 */
typedef int (*FileInitialiser)(int fd, size_t size, void *ctx);

/**
 * Built-in initialisers.
 */

/**
 * file_init_zeros — initialise file to the given size, all bytes zero.
 *
 * Uses ftruncate — the simplest and most efficient approach on Linux.
 * The OS guarantees zero-filled pages on first access.
 */
int file_init_zeros(int fd, size_t size, void *ctx);

/**
 * file_init_none — no-op initialiser.
 *
 * Use when the file already exists and no initialisation is required.
 * Returns 0 immediately.
 */
int file_init_none(int fd, size_t size, void *ctx);

#endif /* FILE_INITIALISER_H */
