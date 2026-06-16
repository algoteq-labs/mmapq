#include "file_mapper.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    char            *file_path;
    int64_t          max_size;
    FileInitialiser  initialiser;
    void            *initialiser_ctx;
    int              fd;               /* -1 when not open */
    int64_t          length_cache;     /* cached file length to avoid stat() on hot path */
} SingleFileRwImpl;

/* ── Forward declarations ────────────────────────────────────────────────── */

static uintptr_t rw_map  (FileMapper *self, int64_t position, int32_t length);
static void      rw_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length);
static void      rw_close(FileMapper *self);

/* ── File length helpers ─────────────────────────────────────────────────── */

static int64_t rw_file_length(SingleFileRwImpl *impl)
{
    struct stat st;
    if (fstat(impl->fd, &st) != 0) {
        MMAP_ASSERT(0, "rw_file_length: fstat failed");
    }
    return st.st_size;
}

static int rw_extend_file_length(SingleFileRwImpl *impl, int64_t min_length)
{
    MMAP_ASSERT(min_length <= impl->max_size,
        "rw_extend_file_length: exceeded max_size — configuration error");

    int64_t current = rw_file_length(impl);
    if (current >= min_length) {
        impl->length_cache = current;
        return 1;
    }

    if (ftruncate(impl->fd, (off_t)min_length) != 0) {
        return 0;
    }
    impl->length_cache = min_length;
    return 1;
}

/* ── Pre-touch newly extended pages ─────────────────────────────────────── */

/**
 * Pre-touch newly mapped pages by reading and writing each page boundary.
 * Forces the OS to allocate physical pages now rather than on first access,
 * eliminating page fault latency from the hot path.
 *
 * extended_bytes: number of newly extended bytes
 * addr:           start of the mapped region
 * offset:         offset within the region where new bytes begin
 */
static void rw_pretouch(uintptr_t addr, int32_t region_length,
                         int32_t offset, int32_t extended_bytes)
{
    if (extended_bytes < (int32_t)sizeof(int64_t)) {
        return;
    }

    long ps = page_size();
    volatile int64_t *p = (volatile int64_t *)(addr + offset);
    int32_t remaining = extended_bytes;

    while (remaining >= (int32_t)sizeof(int64_t)) {
        /* Read-modify-write: compare-and-set with itself (no-op) */
        int64_t val = *p;
        (void)val;  /* touch the page */
        p       = (volatile int64_t *)((uint8_t *)p + ps);
        remaining -= (int32_t)ps;
    }
    (void)region_length;
}

/* ── Lazy init — create file on first map() call ─────────────────────────── */

static int rw_init(SingleFileRwImpl *impl)
{
    if (impl->fd >= 0) {
        return 1;  /* already open */
    }

    /* Create file if it doesn't exist */
    int fd = open(impl->file_path,
                  O_RDWR | O_CREAT,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        MMAP_ASSERT(0, "rw_init: failed to open/create file");
        return 0;
    }

    /* Run initialiser (typically file_init_zeros for new files) */
    if (impl->initialiser != NULL) {
        struct stat st;
        fstat(fd, &st);
        if (st.st_size == 0 && impl->initialiser(fd, (size_t)impl->max_size,
                                                   impl->initialiser_ctx) != 0) {
            close(fd);
            MMAP_ASSERT(0, "rw_init: file initialiser failed");
            return 0;
        }
    }

    impl->fd           = fd;
    impl->length_cache = 0;
    return 1;
}

/* ── map ─────────────────────────────────────────────────────────────────── */

static uintptr_t rw_map(FileMapper *self, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL, "rw_map: self is NULL");
    MMAP_ASSERT(length > 0,   "rw_map: length must be > 0");

    SingleFileRwImpl *impl = (SingleFileRwImpl *)self->impl;

    /* Init is expected to be called from the region mapper thread */
    rw_init(impl);

    int64_t min_length = position + length;

    /* Check cache first — avoid stat on hot path */
    int32_t extended_bytes = 0;
    if (impl->length_cache < min_length) {
        int64_t old_length = rw_file_length(impl);
        if (old_length < min_length) {
            if (!rw_extend_file_length(impl, min_length)) {
                return FILE_MAPPER_NULL_ADDRESS;
            }
            extended_bytes = (int32_t)(min_length - old_length);
        } else {
            impl->length_cache = old_length;
        }
    }

    void *addr = mmap(NULL, (size_t)length,
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED, impl->fd, (off_t)position);
    if (addr == MAP_FAILED) {
        return FILE_MAPPER_NULL_ADDRESS;
    }

    /* Pre-touch newly extended pages */
    if (extended_bytes >= (int32_t)sizeof(int64_t)) {
        rw_pretouch((uintptr_t)addr, length,
                    length - extended_bytes, extended_bytes);
    }

    return (uintptr_t)addr;
}

/* ── unmap ───────────────────────────────────────────────────────────────── */

static void rw_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL,  "rw_unmap: self is NULL");
    MMAP_ASSERT(address != 0, "rw_unmap: address is NULL");
    MMAP_ASSERT(length > 0,   "rw_unmap: length must be > 0");
    (void)position;

    if (munmap((void *)address, (size_t)length) != 0) {
        MMAP_ASSERT(0, "rw_unmap: munmap failed");
    }
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void rw_close(FileMapper *self)
{
    MMAP_ASSERT(self != NULL, "rw_close: self is NULL");

    SingleFileRwImpl *impl = (SingleFileRwImpl *)self->impl;
    if (impl->fd >= 0) {
        close(impl->fd);
        impl->fd = -1;
    }
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

FileMapper *file_mapper_single_rw_create(
    const char      *file_path,
    int64_t          max_size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx)
{
    MMAP_ASSERT(file_path != NULL, "file_mapper_single_rw_create: file_path is NULL");
    MMAP_ASSERT(max_size > 0,      "file_mapper_single_rw_create: max_size must be > 0");

    SingleFileRwImpl *impl = calloc(1, sizeof(SingleFileRwImpl));
    MMAP_ASSERT(impl != NULL, "file_mapper_single_rw_create: out of memory");

    impl->file_path       = strdup(file_path);
    impl->max_size        = max_size;
    impl->initialiser     = initialiser;
    impl->initialiser_ctx = initialiser_ctx;
    impl->fd              = -1;
    impl->length_cache    = 0;

    FileMapper *fm = calloc(1, sizeof(FileMapper));
    MMAP_ASSERT(fm != NULL, "file_mapper_single_rw_create: out of memory");

    fm->map   = rw_map;
    fm->unmap = rw_unmap;
    fm->close = rw_close;
    fm->impl  = impl;

    return fm;
}
