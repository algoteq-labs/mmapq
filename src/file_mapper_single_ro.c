#include "internal/file_mapper.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/platform_internal.h"

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

/* The sentinel for a failed stat/open/fstat/initialiser/mmap on an existing
 * file (mmapqi_write_errno_rc, the write side's rule): a transient errno
 * (EINTR, EAGAIN, EMFILE, ENFILE, EBUSY, ENOLCK) is FILE_MAPPER_NULL_ADDRESS,
 * which the region retries until its timeout; anything else (EACCES,
 * ENOTDIR, ELOOP, ENOMEM, ...) is FILE_MAPPER_IO_ERROR, which the region
 * reports at once. Before 0.2.1 every open-path failure was FILE_NOT_FOUND,
 * so a header file without read permission read as end of stream and a
 * payload file without it as corruption (READ-PERM-EOS-01). */
static uintptr_t ro_fail(int err)
{
    return mmapqi_write_errno_rc(err != 0 ? err : EIO) == MMAPQ_ETIMEDOUT
        ? FILE_MAPPER_NULL_ADDRESS : FILE_MAPPER_IO_ERROR;
}

/* Returns 1 once the file is open; else 0 with *fail set to the
 * FILE_MAPPER_* sentinel for map() to return (NULL_ADDRESS is 0, so the
 * sentinel cannot double as the return value). FILE_NOT_FOUND only for
 * "not there yet / gone": ENOENT (the file is not created yet, or was
 * archived — also between our stat and open) and a zero-length file (created, not yet extended by the
 * writer's first map: the RW mapper opens with O_CREAT and extends with
 * ftruncate later, so a reader can see size 0). */
static int ro_init(SingleFileRoImpl *impl, uintptr_t *fail)
{
    if (impl->fd >= 0) {
        return 1;  /* already open */
    }

    struct stat st;
    if (stat(impl->file_path, &st) != 0) {
        *fail = errno == ENOENT ? FILE_MAPPER_FILE_NOT_FOUND : ro_fail(errno);
        return 0;
    }
    if (st.st_size == 0) {
        *fail = FILE_MAPPER_FILE_NOT_FOUND;  /* created, not yet extended */
        return 0;
    }

    int fd = open(impl->file_path, O_RDONLY);
    if (fd < 0) {
        *fail = errno == ENOENT ? FILE_MAPPER_FILE_NOT_FOUND : ro_fail(errno);
        return 0;
    }

    /* Run initialiser — for RO this is mmapqi_file_init_none */
    if (impl->initialiser != NULL) {
        if (impl->initialiser(fd, (size_t)st.st_size, impl->initialiser_ctx) != 0) {
            int err = errno;
            close(fd);
            *fail = ro_fail(err);
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

    uintptr_t fail;
    if (!ro_init(impl, &fail)) {
        return fail;
    }

    /* Optionally verify position is within file */
    if (impl->check_file_size) {
        if (position >= impl->cached_size) {
            /* Refresh cached size — file may have grown */
            struct stat st;
            if (fstat(impl->fd, &st) != 0) {
                return ro_fail(errno);
            }
            impl->cached_size = st.st_size;
            if (position >= impl->cached_size) {
                return FILE_MAPPER_BEYOND_FILE_SIZE;
            }
        }
    }
    void *addr = mmap(NULL, (size_t)length,
                      PROT_READ,
                      MAP_SHARED | MAP_POPULATE, impl->fd, (off_t)position);
    if (addr == MAP_FAILED) {
        return ro_fail(errno);
    }

    huge_pages_hint(addr, (size_t)length);

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
        return;
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

int mmapqi_file_mapper_single_ro_create(
    const char      *file_path,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    int              check_file_size,
    FileMapper     **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_path == NULL) return MMAPQ_EINVAL;

    SingleFileRoImpl *impl = calloc(1, sizeof(SingleFileRoImpl));
    if (impl == NULL) return MMAPQ_ENOMEM;

    impl->file_path       = strdup(file_path);
    if (impl->file_path == NULL) { free(impl); return MMAPQ_ENOMEM; }
    impl->initialiser     = initialiser;
    impl->initialiser_ctx = initialiser_ctx;
    impl->check_file_size = check_file_size;
    impl->fd              = -1;
    impl->cached_size     = -1;

    FileMapper *fm = calloc(1, sizeof(FileMapper));
    if (fm == NULL) { free(impl); return MMAPQ_ENOMEM; }

    fm->map   = ro_map;
    fm->unmap = ro_unmap;
    fm->close = ro_close;
    fm->impl  = impl;

    *out = fm;
    return 0;
}
