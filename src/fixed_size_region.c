#include "internal/fixed_size_region.h"
#include "internal/file_mapper.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"
#include "internal/platform_internal.h"
#include "internal/test_hooks.h"

#include <errno.h>
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

/* ── Accessor construction ───────────────────────────────────────────────── */

/* Wrap an open fd and its mapping in a FixedSizeRegion accessor. Takes
 * ownership of fd and the mapping: on failure both are released. */
static int fixed_region_build(const char *file_path, int32_t size, int fd,
                              void *addr, size_t map_len, RegionAccessor **out)
{
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
    acc->region_size           = size;
    acc->impl           = impl;

    /* Initialize fast-path cache */
    acc->active_address = (uint8_t *)addr;
    acc->active_base    = 0;
    acc->fast_path_end  = size;
    acc->mapped_capacity = size;

    *out = acc;
    return 0;
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
    if (fd < 0) return mmapqi_fs_errno_rc(errno);

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
                    int err = errno;
                    close(fd);
                    return mmapqi_fs_errno_rc(err);
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
        int err = errno;
        close(fd);
        return mmapqi_fs_errno_rc(err);
    }

    huge_pages_hint(addr, map_len);
    return fixed_region_build(file_path, size, fd, addr, map_len, out);
}

/* ── Read-only open ──────────────────────────────────────────────────────── */

#ifdef MMAPQ_TEST_HOOKS
static void (*meta_ro_pre_open_hook)(const char *path);
void mmapq_test_hook_set_meta_ro_pre_open(void (*fn)(const char *path)) { meta_ro_pre_open_hook = fn; }
#endif

int mmapqi_fixed_size_region_open_ro(
    const char      *file_path,
    int32_t          size,
    RegionAccessor **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_path == NULL) return MMAPQ_EINVAL;
    if (size <= 0)         return MMAPQ_EINVAL;

#ifdef MMAPQ_TEST_HOOKS
    if (meta_ro_pre_open_hook != NULL) meta_ro_pre_open_hook(file_path);
#endif

    /* O_RDONLY, never O_CREAT: a read-only participant needs only read
     * permission, and a file that vanished is reported, not re-created. */
    int fd = open(file_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return mmapqi_fs_errno_rc(errno);   /* ENOENT stays MMAPQ_ENOENT */

    /* Short file: answer without the lock. A creator ftruncates to the
     * full size only while holding its LOCK_EX, so a short file is not
     * being initialised yet (or is outside damage) and needs no wait —
     * and polling readers must not queue LOCK_SH against a creator
     * waiting for LOCK_EX (flock gives no fairness). */
    struct stat st;
    if (fstat(fd, &st) != 0) { int err = errno; close(fd); return mmapqi_fs_errno_rc(err); }
    if (st.st_size < size)   { close(fd); return MMAPQ_ENOTREADY; }

    /* Full size means initialised, or being initialised under the
     * creator's LOCK_EX: LOCK_SH waits that out, so this open never maps a
     * partially written file. A failed lock is an error, not a reason to
     * map unlocked. */
    while (flock(fd, LOCK_SH) != 0) {
        if (errno != EINTR) { int err = errno; close(fd); return mmapqi_fs_errno_rc(err); }
    }
    if (fstat(fd, &st) != 0) { int err = errno; flock(fd, LOCK_UN); close(fd); return mmapqi_fs_errno_rc(err); }
    if (st.st_size < size)   { flock(fd, LOCK_UN); close(fd); return MMAPQ_ENOTREADY; }

    long ps = page_size();
    size_t map_len = (size_t)(((size + ps - 1) / ps) * ps);
    void *addr = mmap(NULL, map_len, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    /* Initialised content is fixed after the creator's unlock, apart from
     * fields readers load atomically, so the lock is not held for the
     * mapping's lifetime. */
    int map_err = errno;
    flock(fd, LOCK_UN);
    if (addr == MAP_FAILED) { close(fd); return mmapqi_fs_errno_rc(map_err); }

    huge_pages_hint(addr, map_len);
    return fixed_region_build(file_path, size, fd, addr, map_len, out);
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
