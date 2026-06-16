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
    FileInitialiser  initialiser;
    void            *initialiser_ctx;
    int              check_file_size;
    int              fd;              /* -1 when not open */
    int64_t          cached_size;     /* cached file size, -1 = unknown */
} SingleFileRoImpl;

/* ── Forward declarations ────────────────────────────────────────────────── */

static uintptr_t ro_map  (FileMapper *self, int64_t position, int32_t length);
static void      ro_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length);
static void      ro_close(FileMapper *self);

/* ── Lazy init — open file on first map() call ───────────────────────────── */

static int ro_init(SingleFileRoImpl *impl)
{
    if (impl->fd >= 0) {
        return 1;  /* already open */
    }

    /* Check file exists and is non-empty */
    struct stat st;
    if (stat(impl->file_path, &st) != 0) {
        return 0;  /* FILE_NOT_FOUND */
    }
    if (st.st_size == 0) {
        return 0;  /* treat empty file as not found */
    }

    int fd = open(impl->file_path, O_RDONLY);
    if (fd < 0) {
        return 0;
    }

    /* Run initialiser — for RO this is typically file_init_none */
    if (impl->initialiser != NULL) {
        if (impl->initialiser(fd, (size_t)st.st_size, impl->initialiser_ctx) != 0) {
            close(fd);
            return 0;
        }
    }

    impl->fd          = fd;
    impl->cached_size = st.st_size;
    return 1;
}

/* ── map ─────────────────────────────────────────────────────────────────── */

static uintptr_t ro_map(FileMapper *self, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL, "ro_map: self is NULL");
    MMAP_ASSERT(length > 0,   "ro_map: length must be > 0");

    SingleFileRoImpl *impl = (SingleFileRoImpl *)self->impl;

    if (!ro_init(impl)) {
        return FILE_MAPPER_FILE_NOT_FOUND;
    }

    /* Optionally verify position is within file */
    if (impl->check_file_size) {
        if (position >= impl->cached_size) {
            /* Refresh cached size — file may have grown */
            struct stat st;
            if (fstat(impl->fd, &st) != 0) {
                return FILE_MAPPER_FILE_NOT_FOUND;
            }
            impl->cached_size = st.st_size;
            if (position >= impl->cached_size) {
                return FILE_MAPPER_BEYOND_FILE_SIZE;
            }
        }
    }

    void *addr = mmap(NULL, (size_t)length, PROT_READ,
                      MAP_SHARED, impl->fd, (off_t)position);
    if (addr == MAP_FAILED) {
        return FILE_MAPPER_NULL_ADDRESS;
    }

    return (uintptr_t)addr;
}

/* ── unmap ───────────────────────────────────────────────────────────────── */

static void ro_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL,    "ro_unmap: self is NULL");
    MMAP_ASSERT(address != 0,   "ro_unmap: address is NULL");
    MMAP_ASSERT(length > 0,     "ro_unmap: length must be > 0");
    (void)position;

    if (munmap((void *)address, (size_t)length) != 0) {
        /* munmap failure is a programming error — wrong address or length */
        MMAP_ASSERT(0, "ro_unmap: munmap failed");
    }
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void ro_close(FileMapper *self)
{
    MMAP_ASSERT(self != NULL, "ro_close: self is NULL");

    SingleFileRoImpl *impl = (SingleFileRoImpl *)self->impl;
    if (impl->fd >= 0) {
        close(impl->fd);
        impl->fd = -1;
    }
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

FileMapper *file_mapper_single_ro_create(
    const char      *file_path,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    int              check_file_size)
{
    MMAP_ASSERT(file_path != NULL, "file_mapper_single_ro_create: file_path is NULL");

    SingleFileRoImpl *impl = calloc(1, sizeof(SingleFileRoImpl));
    MMAP_ASSERT(impl != NULL, "file_mapper_single_ro_create: out of memory");

    impl->file_path       = strdup(file_path);
    impl->initialiser     = initialiser;
    impl->initialiser_ctx = initialiser_ctx;
    impl->check_file_size = check_file_size;
    impl->fd              = -1;
    impl->cached_size     = -1;

    FileMapper *fm = calloc(1, sizeof(FileMapper));
    MMAP_ASSERT(fm != NULL, "file_mapper_single_ro_create: out of memory");

    fm->map   = ro_map;
    fm->unmap = ro_unmap;
    fm->close = ro_close;
    fm->impl  = impl;

    return fm;
}
