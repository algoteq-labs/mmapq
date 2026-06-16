#include "region.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <stdint.h>

/* ── Internal state ──────────────────────────────────────────────────────── */

typedef struct {
    FileMapper *file_mapper;
    int32_t     region_size;
    uint64_t    timeout_ns;
    int         max_file_not_found_errors;
    int         max_beyond_file_size_errors;

    int64_t     current_position;
    uintptr_t   current_address;

    int         error_code;
    int         file_not_found_counter;
    int         beyond_file_size_counter;
} SyncRegionImpl;

#define NULL_POSITION (-1LL)

/* ── Forward declarations ────────────────────────────────────────────────── */

static WrapResult sync_wrap          (RegionAccessor *self, int64_t position, Buffer *buffer);
static WrapResult sync_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buffer);
static void       sync_close         (RegionAccessor *self);
static int32_t    sync_size          (RegionAccessor *self);
static int        sync_map           (Region *self, int64_t position);
static int        sync_unmap         (Region *self);

/* ── Accessor from Region ─────────────────────────────────────────────────── */
/* impl is stored in base.impl */
#define IMPL(acc_ptr) ((SyncRegionImpl *)(acc_ptr)->impl)
#define REGION_IMPL(r) IMPL(&(r)->base)

/* ── map ─────────────────────────────────────────────────────────────────── */

static int sync_map(Region *self, int64_t position)
{
    MMAP_ASSERT(self != NULL,  "sync_map: self is NULL");
    MMAP_ASSERT(position >= 0, "sync_map: position must be >= 0");

    SyncRegionImpl *impl = REGION_IMPL(self);

    if (impl->current_position == position) return 1;

    if (impl->current_address != 0 && impl->current_position != NULL_POSITION) {
        file_mapper_unmap(impl->file_mapper, impl->current_address,
                          impl->current_position, impl->region_size);
        impl->current_address  = 0;
        impl->current_position = NULL_POSITION;
    }

    uintptr_t addr = file_mapper_map(impl->file_mapper, position, impl->region_size);

    if (addr != FILE_MAPPER_NULL_ADDRESS &&
        addr != FILE_MAPPER_FILE_NOT_FOUND &&
        addr != FILE_MAPPER_BEYOND_FILE_SIZE) {
        impl->current_address          = addr;
        impl->current_position         = position;
        impl->error_code               = 0;
        impl->file_not_found_counter   = 0;
        impl->beyond_file_size_counter = 0;
        return 1;
    }

    impl->error_code = (addr == FILE_MAPPER_FILE_NOT_FOUND)   ? (int)FILE_MAPPER_FILE_NOT_FOUND
                     : (addr == FILE_MAPPER_BEYOND_FILE_SIZE) ? (int)FILE_MAPPER_BEYOND_FILE_SIZE
                     : 0;
    return 0;
}

/* ── unmap ───────────────────────────────────────────────────────────────── */

static int sync_unmap(Region *self)
{
    MMAP_ASSERT(self != NULL, "sync_unmap: self is NULL");
    SyncRegionImpl *impl = REGION_IMPL(self);

    if (impl->current_address != 0 && impl->current_position != NULL_POSITION) {
        file_mapper_unmap(impl->file_mapper, impl->current_address,
                          impl->current_position, impl->region_size);
        impl->current_address  = 0;
        impl->current_position = NULL_POSITION;
    }
    return 1;
}

/* ── wrap shared logic ───────────────────────────────────────────────────── */

static WrapResult sync_do_wrap(SyncRegionImpl *impl, int64_t position, Buffer *buffer)
{
    int32_t region_offset         = (int32_t)(position & (impl->region_size - 1));
    int64_t region_start_position = position - region_offset;

    uint64_t start_ns = monotonic_nanos();

    while (1) {
        if (impl->current_position == region_start_position && impl->current_address != 0) {
            buffer->data     = (uint8_t *)impl->current_address + region_offset;
            buffer->capacity = impl->region_size - region_offset;
            return WRAP_SUCCESS;
        }

        if (impl->current_position != region_start_position) {
            if (impl->current_address != 0) {
                file_mapper_unmap(impl->file_mapper, impl->current_address,
                                  impl->current_position, impl->region_size);
                impl->current_address  = 0;
                impl->current_position = NULL_POSITION;
            }

            uintptr_t addr = file_mapper_map(impl->file_mapper,
                                              region_start_position, impl->region_size);

            if (addr != FILE_MAPPER_NULL_ADDRESS &&
                addr != FILE_MAPPER_FILE_NOT_FOUND &&
                addr != FILE_MAPPER_BEYOND_FILE_SIZE) {
                impl->current_address          = addr;
                impl->current_position         = region_start_position;
                impl->error_code               = 0;
                impl->file_not_found_counter   = 0;
                impl->beyond_file_size_counter = 0;
                buffer->data     = (uint8_t *)addr + region_offset;
                buffer->capacity = impl->region_size - region_offset;
                return WRAP_SUCCESS;
            }

            if (addr == FILE_MAPPER_FILE_NOT_FOUND) {
                if (++impl->file_not_found_counter >= impl->max_file_not_found_errors) {
                    impl->file_not_found_counter = 0;
                    return WRAP_FILE_NOT_FOUND;
                }
            } else if (addr == FILE_MAPPER_BEYOND_FILE_SIZE) {
                if (++impl->beyond_file_size_counter >= impl->max_beyond_file_size_errors) {
                    impl->beyond_file_size_counter = 0;
                    return WRAP_BEYOND_FILE_SIZE;
                }
            }
        }

        if (monotonic_nanos() - start_ns >= impl->timeout_ns) {
            return WRAP_MAP_TIMED_OUT;
        }
        spin_hint();
    }
}

/* ── RegionAccessor interface ────────────────────────────────────────────── */

static WrapResult sync_wrap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self != NULL,   "sync_wrap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "sync_wrap: buffer is NULL");
    return sync_do_wrap(IMPL(self), position, buffer);
}

static WrapResult sync_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self != NULL,   "sync_wrap_no_premap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "sync_wrap_no_premap: buffer is NULL");
    return sync_do_wrap(IMPL(self), position, buffer);
}

static void sync_close(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "sync_close: self is NULL");
    /* self IS &region->base, so recover the Region pointer */
    Region *region = (Region *)((char *)self - __builtin_offsetof(Region, base));
    sync_unmap(region);
}

static int32_t sync_size(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "sync_size: self is NULL");
    return IMPL(self)->region_size;
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

Region *region_sync_create(
    FileMapper *file_mapper,
    int32_t     region_size,
    uint64_t    timeout_ns,
    int         max_file_not_found_errors,
    int         max_beyond_file_size_errors)
{
    MMAP_ASSERT(file_mapper != NULL, "region_sync_create: file_mapper is NULL");
    MMAP_ASSERT(region_size > 0,     "region_sync_create: region_size must be > 0");
    MMAP_ASSERT((region_size & (region_size - 1)) == 0,
        "region_sync_create: region_size must be a power of two");

    SyncRegionImpl *impl = calloc(1, sizeof(SyncRegionImpl));
    MMAP_ASSERT(impl != NULL, "region_sync_create: out of memory");

    impl->file_mapper                = file_mapper;
    impl->region_size                = region_size;
    impl->timeout_ns                 = timeout_ns;
    impl->max_file_not_found_errors  = max_file_not_found_errors;
    impl->max_beyond_file_size_errors = max_beyond_file_size_errors;
    impl->current_position           = NULL_POSITION;
    impl->current_address            = 0;

    Region *r = calloc(1, sizeof(Region));
    MMAP_ASSERT(r != NULL, "region_sync_create: out of memory");

    /* RegionAccessor vtable via embedded base */
    r->base.wrap           = sync_wrap;
    r->base.wrap_no_premap = sync_wrap_no_premap;
    r->base.close          = sync_close;
    r->base.size           = sync_size;
    r->base.impl           = impl;

    /* Region vtable */
    r->map   = sync_map;
    r->unmap = sync_unmap;

    return r;
}

/* ── Destroy ─────────────────────────────────────────────────────────────── */

void region_destroy(Region *r)
{
    if (r == NULL) return;
    region_close(r);
    free(r->base.impl);
    free(r);
}
