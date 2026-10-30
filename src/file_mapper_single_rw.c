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
#include <stdatomic.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    char            *file_path;
    int64_t          max_size;
    FileInitialiser  initialiser;
    void            *initialiser_ctx;
    int              fd;               /* -1 when not open */
    int              no_create;        /* CAND-59: open existing only (never O_CREAT) */
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
        return 0;
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
    int32_t remaining = extended_bytes;
    uintptr_t current_addr = addr + offset;

    while (remaining >= (int32_t)sizeof(int64_t)) {
        _Atomic int64_t *p = (_Atomic int64_t *)current_addr;
        int64_t expected = 0;
        /* 
         * Atomic CAS (0 -> 0) forces a Read-For-Ownership (RFO) and marks 
         * the page as dirty in the hardware and kernel. This ensures the
         * background thread takes the page fault and dirty-bit penalty.
         */
        atomic_compare_exchange_strong(p, &expected, 0);

        current_addr += ps;
        remaining -= (int32_t)ps;
    }
    (void)region_length;
}

/* ── Lazy init — open (or create) the file on first map() call ──────────── */

/* Returns 1 when the file is open, 0 when it does not exist and this mapper
 * is open-existing-only (no_create: ENOENT), -1 on any other error.
 *
 * CAND-59: a no_create mapper never brings a file into existence — the
 * commit path's header mapper is no_create, so a header file that was
 * reclaimed is never silently re-created as zeros; frontier creation is
 * the explicit mmapqi_file_mapper_rolled_create_file (O_CREAT|O_EXCL).
 * A missing file is retried on the next map() (fd stays -1). */
static int rw_init(SingleFileRwImpl *impl)
{
    if (impl->fd >= 0) {
        return 1;  /* already open */
    }

    int fd = open(impl->file_path,
                  impl->no_create ? O_RDWR : (O_RDWR | O_CREAT),
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (fd < 0) {
        if (impl->no_create && errno == ENOENT) return 0;   /* missing */
        return -1;
    }

    /* Run initialiser (typically mmapqi_file_init_zeros for new files) */
    if (impl->initialiser != NULL) {
        struct stat st;
        fstat(fd, &st);
        if (st.st_size == 0 && impl->initialiser(fd, (size_t)impl->max_size,
                                                   impl->initialiser_ctx) != 0) {
            close(fd);
            return -1;
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

    /* Init is expected to be called from the region mapper thread.
     * CAND-59: the result is checked (it was ignored since 91d37a9, which
     * made a missing file indistinguishable from an I/O error). */
    int irc = rw_init(impl);
    if (irc == 0) return FILE_MAPPER_FILE_NOT_FOUND;
    if (irc < 0)  return FILE_MAPPER_NULL_ADDRESS;

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
                      MAP_SHARED | MAP_POPULATE, impl->fd, (off_t)position);
    if (addr == MAP_FAILED) {
        return FILE_MAPPER_NULL_ADDRESS;
    }

    huge_pages_hint(addr, (size_t)length);

    /* Pre-touch newly extended pages — cap to mapped region
     * to avoid negative offset when file is extended by more than
     * region_size (e.g. first map at a high start_index). */
    if (extended_bytes >= (int32_t)sizeof(int64_t)) {
        int32_t pb = extended_bytes > length ? length : extended_bytes;
        rw_pretouch((uintptr_t)addr, length, length - pb, pb);
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
        return;
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

int mmapqi_file_mapper_single_rw_create(
    const char      *file_path,
    int64_t          max_size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    int              no_create,
    FileMapper     **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_path == NULL) return MMAPQ_EINVAL;
    if (max_size <= 0) return MMAPQ_EINVAL;

    SingleFileRwImpl *impl = calloc(1, sizeof(SingleFileRwImpl));
    if (impl == NULL) return MMAPQ_ENOMEM;

    impl->file_path       = strdup(file_path);
    if (impl->file_path == NULL) { free(impl); return MMAPQ_ENOMEM; }
    impl->max_size        = max_size;
    impl->initialiser     = initialiser;
    impl->initialiser_ctx = initialiser_ctx;
    impl->fd              = -1;
    impl->no_create       = no_create ? 1 : 0;
    impl->length_cache    = 0;

    FileMapper *fm = calloc(1, sizeof(FileMapper));
    if (fm == NULL) { free(impl); return MMAPQ_ENOMEM; }

    fm->map   = rw_map;
    fm->unmap = rw_unmap;
    fm->close = rw_close;
    fm->impl  = impl;

    *out = fm;
    return 0;
}
