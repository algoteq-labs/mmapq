#include "region.h"
#include "mmap_assert.h"
#include "mmap_platform.h"

#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>

/* ── State machine ───────────────────────────────────────────────────────── */

typedef struct AsyncRegionImpl AsyncRegionImpl;
typedef struct RegionStateOps  RegionStateOps;

struct RegionStateOps {
    const RegionStateOps *(*request_map)    (AsyncRegionImpl *impl, int64_t position);
    const RegionStateOps *(*request_unmap)  (AsyncRegionImpl *impl);
    const RegionStateOps *(*process_request)(AsyncRegionImpl *impl);
    const char            *name;
};

/* ── Forward declarations ────────────────────────────────────────────────── */

static const RegionStateOps *unmapped_request_map   (AsyncRegionImpl *impl, int64_t position);
static const RegionStateOps *unmapped_request_unmap (AsyncRegionImpl *impl);
static const RegionStateOps *unmapped_process       (AsyncRegionImpl *impl);
static const RegionStateOps *map_req_request_map    (AsyncRegionImpl *impl, int64_t position);
static const RegionStateOps *map_req_request_unmap  (AsyncRegionImpl *impl);
static const RegionStateOps *map_req_process        (AsyncRegionImpl *impl);
static const RegionStateOps *mapped_request_map     (AsyncRegionImpl *impl, int64_t position);
static const RegionStateOps *mapped_request_unmap   (AsyncRegionImpl *impl);
static const RegionStateOps *mapped_process         (AsyncRegionImpl *impl);
static const RegionStateOps *unmap_req_request_map  (AsyncRegionImpl *impl, int64_t position);
static const RegionStateOps *unmap_req_request_unmap(AsyncRegionImpl *impl);
static const RegionStateOps *unmap_req_process      (AsyncRegionImpl *impl);
static const RegionStateOps *error_request_map      (AsyncRegionImpl *impl, int64_t position);
static const RegionStateOps *error_request_unmap    (AsyncRegionImpl *impl);
static const RegionStateOps *error_process          (AsyncRegionImpl *impl);
static const RegionStateOps *unmanaged_request_map  (AsyncRegionImpl *impl, int64_t position);
static const RegionStateOps *unmanaged_request_unmap(AsyncRegionImpl *impl);
static const RegionStateOps *unmanaged_process      (AsyncRegionImpl *impl);

/* ── Static state tables ─────────────────────────────────────────────────── */

static const RegionStateOps UNMAPPED_OPS       = { unmapped_request_map,   unmapped_request_unmap,    unmapped_process,    "UNMAPPED"        };
static const RegionStateOps MAP_REQUESTED_OPS  = { map_req_request_map,    map_req_request_unmap,     map_req_process,     "MAP_REQUESTED"   };
static const RegionStateOps MAPPED_OPS         = { mapped_request_map,     mapped_request_unmap,      mapped_process,      "MAPPED"          };
static const RegionStateOps UNMAP_REQUESTED_OPS= { unmap_req_request_map,  unmap_req_request_unmap,   unmap_req_process,   "UNMAP_REQUESTED" };
static const RegionStateOps ERROR_OPS          = { error_request_map,      error_request_unmap,       error_process,       "ERROR"           };
static const RegionStateOps UNMANAGED_OPS      = { unmanaged_request_map,  unmanaged_request_unmap,   unmanaged_process,   "UNMANAGED"       };

/* ── Internal state ──────────────────────────────────────────────────────── */

#define NULL_POSITION (-1LL)

struct AsyncRegionImpl {
    FileMapper  *file_mapper;
    int32_t      region_size;
    uint64_t     timeout_ns;
    int          max_file_not_found_errors;
    int          max_beyond_file_size_errors;

    _Atomic(const RegionStateOps *) state;

    int64_t   position;
    uintptr_t address;
    int64_t   requested_position;

    int       error_code;
    int       file_not_found_counter;
    int       beyond_file_size_counter;
};

static inline const RegionStateOps *get_state(AsyncRegionImpl *impl)
{
    return atomic_load_explicit(&impl->state, memory_order_acquire);
}

static inline void set_state(AsyncRegionImpl *impl, const RegionStateOps *ops)
{
    atomic_store_explicit(&impl->state, ops, memory_order_release);
}

/* ── UNMAPPED ────────────────────────────────────────────────────────────── */

static const RegionStateOps *unmapped_request_map(AsyncRegionImpl *impl, int64_t position)
{
    impl->error_code         = 0;
    impl->requested_position = position;
    return &MAP_REQUESTED_OPS;
}

static const RegionStateOps *unmapped_request_unmap(AsyncRegionImpl *impl)
{
    (void)impl; return &UNMAPPED_OPS;
}

static const RegionStateOps *unmapped_process(AsyncRegionImpl *impl)
{
    (void)impl; return &UNMAPPED_OPS;
}

/* ── MAP_REQUESTED ───────────────────────────────────────────────────────── */

static const RegionStateOps *map_req_request_map(AsyncRegionImpl *impl, int64_t position)
{
    (void)impl; (void)position; return &MAP_REQUESTED_OPS;
}

static const RegionStateOps *map_req_request_unmap(AsyncRegionImpl *impl)
{
    (void)impl; return &MAP_REQUESTED_OPS;
}

static const RegionStateOps *map_req_process(AsyncRegionImpl *impl)
{
    if (impl->address != 0 && impl->position != NULL_POSITION) {
        file_mapper_unmap(impl->file_mapper, impl->address,
                          impl->position, impl->region_size);
        impl->address  = 0;
        impl->position = NULL_POSITION;
    }

    uintptr_t addr = file_mapper_map(impl->file_mapper,
                                      impl->requested_position, impl->region_size);

    if (addr != FILE_MAPPER_NULL_ADDRESS &&
        addr != FILE_MAPPER_FILE_NOT_FOUND &&
        addr != FILE_MAPPER_BEYOND_FILE_SIZE) {
        impl->file_not_found_counter   = 0;
        impl->beyond_file_size_counter = 0;
        impl->address                  = addr;
        impl->position                 = impl->requested_position;
        impl->requested_position       = NULL_POSITION;
        impl->error_code               = 0;
        return &MAPPED_OPS;
    }

    impl->error_code = (addr == FILE_MAPPER_FILE_NOT_FOUND)   ? (int)FILE_MAPPER_FILE_NOT_FOUND
                     : (addr == FILE_MAPPER_BEYOND_FILE_SIZE) ? (int)FILE_MAPPER_BEYOND_FILE_SIZE
                     : 0;

    if (addr == FILE_MAPPER_FILE_NOT_FOUND)   impl->file_not_found_counter++;
    if (addr == FILE_MAPPER_BEYOND_FILE_SIZE) impl->beyond_file_size_counter++;

    impl->requested_position = NULL_POSITION;
    return &ERROR_OPS;
}

/* ── MAPPED ──────────────────────────────────────────────────────────────── */

static const RegionStateOps *mapped_request_map(AsyncRegionImpl *impl, int64_t position)
{
    if (impl->position == position) return &MAPPED_OPS;
    impl->error_code         = 0;
    impl->requested_position = position;
    return &MAP_REQUESTED_OPS;
}

static const RegionStateOps *mapped_request_unmap(AsyncRegionImpl *impl)
{
    (void)impl; return &UNMAP_REQUESTED_OPS;
}

static const RegionStateOps *mapped_process(AsyncRegionImpl *impl)
{
    (void)impl; return &MAPPED_OPS;
}

/* ── UNMAP_REQUESTED ─────────────────────────────────────────────────────── */

static const RegionStateOps *unmap_req_request_map(AsyncRegionImpl *impl, int64_t position)
{
    (void)impl; (void)position; return &UNMAP_REQUESTED_OPS;
}

static const RegionStateOps *unmap_req_request_unmap(AsyncRegionImpl *impl)
{
    (void)impl; return &UNMAP_REQUESTED_OPS;
}

static const RegionStateOps *unmap_req_process(AsyncRegionImpl *impl)
{
    if (impl->address != 0 && impl->position != NULL_POSITION) {
        file_mapper_unmap(impl->file_mapper, impl->address,
                          impl->position, impl->region_size);
        impl->address            = 0;
        impl->position           = NULL_POSITION;
        impl->requested_position = NULL_POSITION;
    }
    return &UNMAPPED_OPS;
}

/* ── ERROR ───────────────────────────────────────────────────────────────── */

static const RegionStateOps *error_request_map(AsyncRegionImpl *impl, int64_t position)
{
    (void)impl; (void)position; return &ERROR_OPS;
}

static const RegionStateOps *error_request_unmap(AsyncRegionImpl *impl)
{
    (void)impl; return &ERROR_OPS;
}

static const RegionStateOps *error_process(AsyncRegionImpl *impl)
{
    (void)impl; return &ERROR_OPS;
}

/* ── UNMANAGED ───────────────────────────────────────────────────────────── */

static const RegionStateOps *unmanaged_request_map(AsyncRegionImpl *impl, int64_t position)
{
    (void)impl; (void)position;
    MMAP_ASSERT(0, "request_map() on UNMANAGED region — runtime has stopped");
    return &UNMANAGED_OPS;
}

static const RegionStateOps *unmanaged_request_unmap(AsyncRegionImpl *impl)
{
    if (impl->address != 0 && impl->position != NULL_POSITION) {
        file_mapper_unmap(impl->file_mapper, impl->address,
                          impl->position, impl->region_size);
        impl->address  = 0;
        impl->position = NULL_POSITION;
    }
    return &UNMANAGED_OPS;
}

static const RegionStateOps *unmanaged_process(AsyncRegionImpl *impl)
{
    (void)impl; return &UNMANAGED_OPS;
}

/* ── wrap (RegionAccessor interface) ─────────────────────────────────────── */

static WrapResult await_mapped(AsyncRegionImpl *impl, int64_t region_start_position)
{
    const RegionStateOps *s = get_state(impl);
    if (__builtin_expect(s == &MAPPED_OPS && impl->position == region_start_position, 1)) {
        return WRAP_SUCCESS;
    }

    uint64_t start_ns = monotonic_nanos();

    while (1) {
        s = get_state(impl);
        const RegionStateOps *next = s->request_map(impl, region_start_position);
        if (next != s) set_state(impl, next);

        s = get_state(impl);
        if (s == &MAPPED_OPS && impl->position == region_start_position) {
            return WRAP_SUCCESS;
        }

        if (s == &ERROR_OPS) {
            WrapResult result = WRAP_MAP_TIMED_OUT;
            if (impl->error_code == (int)FILE_MAPPER_FILE_NOT_FOUND &&
                impl->file_not_found_counter >= impl->max_file_not_found_errors) {
                impl->file_not_found_counter = 0;
                result = WRAP_FILE_NOT_FOUND;
            } else if (impl->error_code == (int)FILE_MAPPER_BEYOND_FILE_SIZE &&
                       impl->beyond_file_size_counter >= impl->max_beyond_file_size_errors) {
                impl->beyond_file_size_counter = 0;
                result = WRAP_BEYOND_FILE_SIZE;
            }
            if (result != WRAP_MAP_TIMED_OUT) {
                impl->error_code = 0;
                set_state(impl, &UNMAPPED_OPS);
                return result;
            }
            /*
             * Counter not yet at threshold — reset ERROR→UNMAPPED so the
             * mapper thread will process another MAP_REQUESTED on the next
             * iteration. Without this reset the region stays stuck in ERROR
             * and the counter never accumulates.
             */
            impl->error_code = 0;
            set_state(impl, &UNMAPPED_OPS);
        }

        if (monotonic_nanos() - start_ns >= impl->timeout_ns) {
            return WRAP_MAP_TIMED_OUT;
        }
        spin_hint();
    }
}

static WrapResult async_wrap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    MMAP_ASSERT(self != NULL,   "async_wrap: self is NULL");
    MMAP_ASSERT(buffer != NULL, "async_wrap: buffer is NULL");

    AsyncRegionImpl *impl    = (AsyncRegionImpl *)self->impl;
    int32_t  region_offset   = (int32_t)(position & (impl->region_size - 1));
    int64_t  region_start    = position - region_offset;

    WrapResult r = await_mapped(impl, region_start);
    if (r == WRAP_SUCCESS) {
        buffer->data     = (uint8_t *)impl->address + region_offset;
        buffer->capacity = impl->region_size - region_offset;
    }
    return r;
}

static WrapResult async_wrap_no_premap(RegionAccessor *self, int64_t position, Buffer *buffer)
{
    return async_wrap(self, position, buffer);
}

static void async_close(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "async_close: self is NULL");

    AsyncRegionImpl *impl  = (AsyncRegionImpl *)self->impl;
    uint64_t start_ns = monotonic_nanos();

    while (1) {
        const RegionStateOps *s    = get_state(impl);
        const RegionStateOps *next = s->request_unmap(impl);
        if (next != s) set_state(impl, next);

        s = get_state(impl);
        if (s == &UNMAPPED_OPS || s == &UNMANAGED_OPS) break;
        if (s == &ERROR_OPS) {
            impl->error_code = 0;
            set_state(impl, &UNMAPPED_OPS);
            break;
        }
        if (monotonic_nanos() - start_ns >= impl->timeout_ns) break;

        /*
         * Self-drive: process any pending MAP_REQUESTED or UNMAP_REQUESTED
         * here. In normal operation the mapper thread does this, but during
         * close() — particularly in tests where the region is driven manually
         * without a mapper thread — we must drive it ourselves to avoid
         * spinning for the full timeout_ns waiting for a state transition
         * that nobody else will trigger.
         *
         * Calling process_request() from a non-mapper thread during close()
         * is safe: close() is called after the ring is deregistered from the
         * runtime (region_ring_close waits for deregistration), so the mapper
         * thread is guaranteed not to call process_request() concurrently.
         */
        region_async_process_request((Region *)self);

        spin_hint();
    }
}

static int32_t async_size(RegionAccessor *self)
{
    MMAP_ASSERT(self != NULL, "async_size: self is NULL");
    return ((AsyncRegionImpl *)self->impl)->region_size;
}

/* ── Region.map / Region.unmap ───────────────────────────────────────────── */

static int async_map(Region *self, int64_t position)
{
    MMAP_ASSERT(self != NULL,  "async_map: self is NULL");
    MMAP_ASSERT(position >= 0, "async_map: position must be >= 0");

    AsyncRegionImpl *impl = (AsyncRegionImpl *)self->base.impl;
    const RegionStateOps *s    = get_state(impl);
    const RegionStateOps *next = s->request_map(impl, position);
    if (next != s) set_state(impl, next);

    return (get_state(impl) == &MAPPED_OPS && impl->position == position) ? 1 : 0;
}

static int async_unmap(Region *self)
{
    MMAP_ASSERT(self != NULL, "async_unmap: self is NULL");

    AsyncRegionImpl *impl = (AsyncRegionImpl *)self->base.impl;
    const RegionStateOps *s    = get_state(impl);
    const RegionStateOps *next = s->request_unmap(impl);
    if (next != s) set_state(impl, next);

    s = get_state(impl);
    return (s == &UNMAPPED_OPS || s == &UNMANAGED_OPS) ? 1 : 0;
}

/* ── RegionMapper thread interface ───────────────────────────────────────── */

int region_async_process_request(Region *r)
{
    MMAP_ASSERT(r != NULL, "region_async_process_request: r is NULL");
    AsyncRegionImpl *impl = (AsyncRegionImpl *)r->base.impl;
    const RegionStateOps *s    = get_state(impl);
    const RegionStateOps *next = s->process_request(impl);
    if (next != s) { set_state(impl, next); return 1; }
    return 0;
}

void region_async_unmanage(Region *r)
{
    MMAP_ASSERT(r != NULL, "region_async_unmanage: r is NULL");
    set_state((AsyncRegionImpl *)r->base.impl, &UNMANAGED_OPS);
}

/* ── Factory ─────────────────────────────────────────────────────────────── */

Region *region_async_create(
    FileMapper *file_mapper,
    int32_t     region_size,
    uint64_t    timeout_ns,
    int         max_file_not_found_errors,
    int         max_beyond_file_size_errors)
{
    MMAP_ASSERT(file_mapper != NULL, "region_async_create: file_mapper is NULL");
    MMAP_ASSERT(region_size > 0,     "region_async_create: region_size must be > 0");
    MMAP_ASSERT((region_size & (region_size - 1)) == 0,
        "region_async_create: region_size must be a power of two");

    AsyncRegionImpl *impl = calloc(1, sizeof(AsyncRegionImpl));
    MMAP_ASSERT(impl != NULL, "region_async_create: out of memory");

    impl->file_mapper                 = file_mapper;
    impl->region_size                 = region_size;
    impl->timeout_ns                  = timeout_ns;
    impl->max_file_not_found_errors   = max_file_not_found_errors;
    impl->max_beyond_file_size_errors = max_beyond_file_size_errors;
    impl->position                    = NULL_POSITION;
    impl->address                     = 0;
    impl->requested_position          = NULL_POSITION;
    impl->error_code                  = 0;
    impl->file_not_found_counter      = 0;
    impl->beyond_file_size_counter    = 0;
    atomic_store_explicit(&impl->state, &UNMAPPED_OPS, memory_order_relaxed);

    Region *r = calloc(1, sizeof(Region));
    MMAP_ASSERT(r != NULL, "region_async_create: out of memory");

    r->base.wrap           = async_wrap;
    r->base.wrap_no_premap = async_wrap_no_premap;
    r->base.close          = async_close;
    r->base.size           = async_size;
    r->base.impl           = impl;

    r->map   = async_map;
    r->unmap = async_unmap;

    return r;
}
