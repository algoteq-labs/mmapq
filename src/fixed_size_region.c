#include "internal/fixed_size_region.h"
#include "internal/file_mapper.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/platform_internal.h"

#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    char       *file_path;
    int32_t     size;
    uintptr_t   address;   /* permanently mapped address */
    int         fd;
} FixedSizeImpl;

/* ── wrap ────────────────────────────────────────────────────────────────── */

static WrapResult fixed_wrap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self   != NULL, "fixed_wrap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "fixed_wrap: buffer is NULL");

    FixedSizeImpl *impl = (FixedSizeImpl *)self->impl;

    if (position >= impl->size) {
        return WRAP_BEYOND_FILE_SIZE;
    }

    buffer->data     = (uint8_t *)impl->address + position;
    buffer->capacity = impl->size - (int32_t)position;
    return WRAP_SUCCESS;
}

/* wrap_no_premap is identical for fixed size — no ring, no premap */
static WrapResult fixed_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    return fixed_wrap(self, position, buffer);
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void fixed_close(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "fixed_close: self is NULL");

    FixedSizeImpl *impl = (FixedSizeImpl *)self->impl;

    if (impl->address != 0) {
        munmap((void *)impl->address, (size_t)impl->size);
        impl->address = 0;
    }
    if (impl->fd >= 0) {
        close(impl->fd);
        impl->fd = -1;
    }
}

static int32_t fixed_size(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "fixed_size: self is NULL");
    return ((FixedSizeImpl *)self->impl)->size;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapqi_fixed_size_region_create(
    const char      *file_path,
    int32_t          size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    RegionAccessor **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_path == NULL) return MMAPQ_EINVAL;
    if (size <= 0)         return MMAPQ_EINVAL;

    /* Open or create the backing file */
    int fd = open(file_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (fd < 0) return MMAPQ_ETIMEDOUT;

    /*
     * Coordination for multi-process initialisation:
     * flock(LOCK_EX) ensures exactly one process runs the initialiser
     * on a new file. The second process will acquire the lock after the
     * first has run the initialiser and released it, at which point
     * st.st_size > 0 and the initialiser is skipped.
     */
    if (initialiser != NULL) {
        if (flock(fd, LOCK_EX) == 0) {
            struct stat st;
            if (fstat(fd, &st) == 0 && st.st_size == 0) {
                if (initialiser(fd, (size_t)size, initialiser_ctx) != 0) {
                    close(fd);
                    return MMAPQ_ETIMEDOUT;
                }
            }
            flock(fd, LOCK_UN);
        }
    }

    /* 
     * Map the file. mmap requires length to be a multiple of page size,
     * but we only want to expose 'size' bytes to the caller.
     */
    long ps = page_size();
    size_t map_len = (size_t)(((size + ps - 1) / ps) * ps);

    void *addr = mmap(NULL, map_len,
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_POPULATE, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        return MMAPQ_ETIMEDOUT;
    }

    huge_pages_hint(addr, map_len);

    FixedSizeImpl *impl = calloc(1, sizeof(FixedSizeImpl));
    if (impl == NULL) {
        munmap(addr, map_len);
        close(fd);
        return MMAPQ_ENOMEM;
    }

    impl->file_path = strdup(file_path);  /* keep for toString/debugging */
    if (impl->file_path == NULL) {
        free(impl); munmap(addr, map_len); close(fd); return MMAPQ_ENOMEM;
    }
    impl->size      = size;
    impl->address   = (uintptr_t)addr;
    impl->fd        = fd;

    RegionAccessor *acc = calloc(1, sizeof(RegionAccessor));
    if (acc == NULL) {
        free(impl->file_path);
        free(impl);
        munmap(addr, map_len);
        close(fd);
        return MMAPQ_ENOMEM;
    }

    acc->wrap           = fixed_wrap;
    acc->wrap_no_premap = fixed_wrap_no_premap;
    acc->close          = fixed_close;
    acc->size           = fixed_size;
    acc->impl           = impl;

    /* Initialize fast-path cache */
    acc->active_address = (uint8_t *)addr;
    acc->active_base    = 0;
    acc->active_end     = size;

    *out = acc;
    return 0;
}

/* ── Destroy ─────────────────────────────────────────────────────────────── */

void mmapqi_fixed_size_region_destroy(RegionAccessor *acc)
{
    if (acc == NULL) return;

    region_accessor_close(acc);

    FixedSizeImpl *impl = (FixedSizeImpl *)acc->impl;
    if (impl != NULL) {
        long ps = page_size();
        size_t map_len = (size_t)(((impl->size + ps - 1) / ps) * ps);
        munmap((void *)impl->address, map_len);
        impl->address = 0;
        free(impl->file_path);
        free(impl);
    }
    free(acc);
}
