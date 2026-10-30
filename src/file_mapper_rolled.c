#include "internal/file_mapper.h"
#include "internal/mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

/* Rolled file names are built with "%ld" and (long) casts on the int64_t
 * file_index — correct only where long is 64 bits (the supported targets
 * are Linux x86-64/ARM64, LP64). */
_Static_assert(sizeof(long) == 8, "rolled file naming assumes LP64 (64-bit long)");

/* ── File cache entry ────────────────────────────────────────────────────── */

typedef struct {
    int64_t      file_index;   /* which rolled file this entry covers    */
    FileMapper  *mapper;       /* the underlying single-file mapper       */
    int          ref_count;    /* number of currently open regions        */
    uint64_t     insert_order; /* monotonically increasing insertion tick */
} FileCacheEntry;

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    char            *file_prefix;
    int64_t          max_file_size;
    int32_t          region_size;
    FileInitialiser  initialiser;
    void            *initialiser_ctx;
    MapMode          mode;
    int              max_cache_size;
    int              check_file_size;
    int              no_create;    /* CAND-59: RW per-file mappers open existing only */

    FileCacheEntry  *cache;        /* array of max_cache_size entries          */
    int              cache_count;  /* number of entries currently in cache     */
    uint64_t         insert_tick;  /* monotonic counter for insertion ordering */
} RolledImpl;

/* ── Forward declarations ────────────────────────────────────────────────── */

static uintptr_t rolled_map  (FileMapper *self, int64_t position, int32_t length);
static void      rolled_unmap(FileMapper *self, uintptr_t address, int64_t position, int32_t length);
static void      rolled_close(FileMapper *self);

/* ── File name construction ──────────────────────────────────────────────── */

static void build_file_name(const char *prefix, int64_t file_index, char *out, size_t out_size)
{
    snprintf(out, out_size, "%s_%ld", prefix, (long)file_index);
}

/* ── Cache helpers ───────────────────────────────────────────────────────── */

static FileCacheEntry *cache_find(RolledImpl *impl, int64_t file_index)
{
    for (int i = 0; i < impl->cache_count; i++) {
        if (impl->cache[i].file_index == file_index) {
            return &impl->cache[i];
        }
    }
    return NULL;
}

/**
 * Evict the oldest entry with ref_count == 0.
 * Fails fast if no such entry exists — that is a configuration error.
 *
 * INVARIANT: max_cache_size >= ring_size ensures at least one entry
 *            is always evictable when the cache is full under normal operation.
 */
static void cache_evict_oldest(RolledImpl *impl)
{
    int      oldest_idx    = -1;
    uint64_t oldest_order  = UINT64_MAX;

    for (int i = 0; i < impl->cache_count; i++) {
        if (impl->cache[i].ref_count == 0 &&
            impl->cache[i].insert_order < oldest_order) {
            oldest_order = impl->cache[i].insert_order;
            oldest_idx   = i;
        }
    }

    MMAP_ASSERT(oldest_idx >= 0,
        "rolled file cache full with all ref_counts > 0 — "
        "max_cache_size must be >= ring_size (configuration error)");

    FileCacheEntry *entry = &impl->cache[oldest_idx];
    file_mapper_close(entry->mapper);
    mmapqi_file_mapper_destroy(entry->mapper);

    /* Compact the cache array by shifting remaining entries down */
    int last = impl->cache_count - 1;
    if (oldest_idx < last) {
        impl->cache[oldest_idx] = impl->cache[last];
    }
    impl->cache_count--;
}

/**
 * Get or open a FileMapper for the given file_index.
 * If the cache is full, evicts the oldest zero-ref entry first.
 */
static FileCacheEntry *cache_get_or_open(RolledImpl *impl, int64_t file_index)
{
    FileCacheEntry *entry = cache_find(impl, file_index);
    if (entry != NULL) {
        return entry;
    }

    /* Need to open a new entry */
    if (impl->cache_count >= impl->max_cache_size) {
        cache_evict_oldest(impl);
    }

    /* Build file name */
    char file_name[4096];
    build_file_name(impl->file_prefix, file_index, file_name, sizeof(file_name));

    /* Create the appropriate single-file mapper */
    FileMapper *mapper = NULL;
    int fmerr;
    if (impl->mode == MAP_MODE_READ_ONLY) {
        fmerr = mmapqi_file_mapper_single_ro_create(
            file_name,
            impl->initialiser,
            impl->initialiser_ctx,
            impl->check_file_size,
            &mapper);
    } else {
        fmerr = mmapqi_file_mapper_single_rw_create(
            file_name,
            impl->max_file_size,
            impl->initialiser,
            impl->initialiser_ctx,
            impl->no_create,
            &mapper);
    }

    if (fmerr != 0) return NULL;

    /* Add to cache */
    FileCacheEntry *new_entry = &impl->cache[impl->cache_count++];
    new_entry->file_index   = file_index;
    new_entry->mapper       = mapper;
    new_entry->ref_count    = 0;
    new_entry->insert_order = impl->insert_tick++;

    return new_entry;
}

/* ── map ─────────────────────────────────────────────────────────────────── */

static uintptr_t rolled_map(FileMapper *self, int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL, "rolled_map: self is NULL");
    MMAP_ASSERT(length > 0,   "rolled_map: length must be > 0");

    RolledImpl *impl = (RolledImpl *)self->impl;

    MMAP_ASSERT(length == impl->region_size,
        "rolled_map: length must equal region_size");

    int64_t file_index        = position / impl->max_file_size;
    int64_t position_in_file  = position % impl->max_file_size;

    FileCacheEntry *entry = cache_get_or_open(impl, file_index);

    uintptr_t result = file_mapper_map(entry->mapper, position_in_file, length);
    if (result != FILE_MAPPER_NULL_ADDRESS &&
        result != FILE_MAPPER_FILE_NOT_FOUND &&
        result != FILE_MAPPER_BEYOND_FILE_SIZE) {
        entry->ref_count++;
    }
    return result;
}

/* ── unmap ───────────────────────────────────────────────────────────────── */

static void rolled_unmap(FileMapper *self, uintptr_t address,
                          int64_t position, int32_t length)
{
    MMAP_ASSERT(self != NULL,  "rolled_unmap: self is NULL");
    MMAP_ASSERT(address != 0, "rolled_unmap: address is NULL");
    MMAP_ASSERT(length > 0,   "rolled_unmap: length must be > 0");

    RolledImpl *impl = (RolledImpl *)self->impl;

    int64_t file_index       = position / impl->max_file_size;
    int64_t position_in_file = position % impl->max_file_size;

    FileCacheEntry *entry = cache_find(impl, file_index);
    MMAP_ASSERT(entry != NULL, "rolled_unmap: no cache entry for file_index — unmap without map?");
    if (entry == NULL) return;   /* NDEBUG: degrade rather than deref NULL */

    file_mapper_unmap(entry->mapper, address, position_in_file, length);

    entry->ref_count--;
    MMAP_ASSERT(entry->ref_count >= 0,
        "rolled_unmap: ref_count went negative — unmap called more times than map");

    /*
     * Evict if ref_count == 0 and cache is full.
     * For RW regions: appending is always forward so a zero-ref RW file
     * will never be needed again — evict eagerly.
     * For RO regions: keep in cache until pressure forces eviction.
     * cache_evict_oldest() handles both cases by evicting the oldest
     * zero-ref entry when the cache is full.
     */
    if (entry->ref_count == 0 && impl->mode == MAP_MODE_READ_WRITE) {
        /* Eagerly evict completed RW file — appending never goes backwards */
        file_mapper_close(entry->mapper);
        mmapqi_file_mapper_destroy(entry->mapper);

        /* Compact */
        int idx  = (int)(entry - impl->cache);
        int last = impl->cache_count - 1;
        if (idx < last) {
            impl->cache[idx] = impl->cache[last];
        }
        impl->cache_count--;
    }
}

/* ── close ───────────────────────────────────────────────────────────────── */

static void rolled_close(FileMapper *self)
{
    MMAP_ASSERT(self != NULL, "rolled_close: self is NULL");

    RolledImpl *impl = (RolledImpl *)self->impl;
    if (impl == NULL) return;

    for (int i = 0; i < impl->cache_count; i++) {
        if (impl->cache[i].mapper != NULL) {
            file_mapper_close(impl->cache[i].mapper);
            mmapqi_file_mapper_destroy(impl->cache[i].mapper);
            impl->cache[i].mapper = NULL;
        }
    }
    impl->cache_count = 0;
}

/* Test-only reach (see internal/test_reach.h): expose the prefix + mode so
 * a hook can distinguish the RW header accessor's maps from RO/payload
 * probes. */
const char *mmapqi_rolled_prefix_and_mode(FileMapper *self, int *is_rw)
{
    RolledImpl *impl = (RolledImpl *)self->impl;
    if (is_rw != NULL) *is_rw = (impl->mode == MAP_MODE_READ_WRITE);
    return impl->file_prefix;
}

/* ── Explicit frontier create (CAND-59) ──────────────────────────────────── */

int mmapqi_file_mapper_rolled_create_file(FileMapper *self, int64_t position)
{
    MMAP_ASSERT(self != NULL, "rolled_create_file: self is NULL");
    RolledImpl *impl = (RolledImpl *)self->impl;
    MMAP_ASSERT(impl->mode == MAP_MODE_READ_WRITE,
        "rolled_create_file: READ_WRITE rolled mappers only");
    if (position < 0) return MMAPQ_EINVAL;

    int64_t file_index = position / impl->max_file_size;   /* as rolled_map */
    char file_name[4096];
    build_file_name(impl->file_prefix, file_index, file_name, sizeof(file_name));

    /* No-overwrite create: never replaces or re-opens an existing file
     * here — EEXIST means another lane created it first, which is success
     * (the caller's retried map opens it). */
    int fd = open(file_name, O_RDWR | O_CREAT | O_EXCL,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (fd < 0) {
        if (errno == EEXIST) return 0;
        return MMAPQ_EIO;
    }
    if (impl->initialiser != NULL) {
        struct stat st;
        if (fstat(fd, &st) != 0 ||
            (st.st_size == 0 &&
             impl->initialiser(fd, (size_t)impl->max_file_size,
                               impl->initialiser_ctx) != 0)) {
            close(fd);
            return MMAPQ_EIO;
        }
    }
    close(fd);
    return 0;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

int mmapqi_file_mapper_rolled_create(
    const char      *file_prefix,
    int64_t          max_file_size,
    int32_t          region_size,
    FileInitialiser  initialiser,
    void            *initialiser_ctx,
    MapMode          mode,
    int              max_cache_size,
    int              check_file_size,
    int              no_create,
    FileMapper     **out)
{
    if (out == NULL) return MMAPQ_EINVAL;
    *out = NULL;
    if (file_prefix == NULL) return MMAPQ_EINVAL;
    if (max_file_size <= 0) return MMAPQ_EINVAL;
    if (region_size <= 0) return MMAPQ_EINVAL;
    if (max_file_size % region_size != 0) return MMAPQ_EINVAL;
    if (max_cache_size <= 0) return MMAPQ_EINVAL;

    RolledImpl *impl = calloc(1, sizeof(RolledImpl));
    if (impl == NULL) return MMAPQ_ENOMEM;

    impl->file_prefix      = strdup(file_prefix);
    if (impl->file_prefix == NULL) { free(impl); return MMAPQ_ENOMEM; }
    impl->max_file_size    = max_file_size;
    impl->region_size      = region_size;
    impl->initialiser      = initialiser;
    impl->initialiser_ctx  = initialiser_ctx;
    impl->mode             = mode;
    impl->max_cache_size   = max_cache_size;
    impl->check_file_size  = check_file_size;
    impl->no_create        = no_create ? 1 : 0;
    impl->cache_count      = 0;
    impl->insert_tick      = 0;

    impl->cache = calloc((size_t)max_cache_size, sizeof(FileCacheEntry));
    if (impl->cache == NULL) { free(impl->file_prefix); free(impl); return MMAPQ_ENOMEM; }

    FileMapper *fm = calloc(1, sizeof(FileMapper));
    if (fm == NULL) { free(impl->file_prefix); free(impl->cache); free(impl); return MMAPQ_ENOMEM; }

    fm->map   = rolled_map;
    fm->unmap = rolled_unmap;
    fm->close = rolled_close;
    fm->impl  = impl;

    *out = fm;
    return 0;
}

/* ── Destroy ─────────────────────────────────────────────────────────────── */

void mmapqi_file_mapper_destroy(FileMapper *fm)
{
    if (fm == NULL) return;

    /*
     * Close first — unmaps all regions and closes file descriptors.
     * file_mapper_close is idempotent; calling it here is safe even if
     * the caller already closed it (e.g. region_ring_close -> file_mapper_close).
     * For rolled mappers, close also calls mmapqi_file_mapper_destroy on each
     * cached sub-mapper, so those are fully cleaned up here.
     */
    file_mapper_close(fm);

    if (fm->impl != NULL) {
        if (fm->close == rolled_close) {
            RolledImpl *impl = (RolledImpl *)fm->impl;
            free(impl->file_prefix);
            free(impl->cache);
            free(impl);
        } else {
            /* Single-file mappers (RO and RW) store file_path as first field */
            char **file_path_ptr = (char **)fm->impl;
            free(*file_path_ptr);
            free(fm->impl);
        }
        fm->impl = NULL;
    }
    free(fm);
}
