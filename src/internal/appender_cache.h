#ifndef APPENDER_CACHE_H
#define APPENDER_CACHE_H

#include <stdint.h>
#include "queue_transfer.h"

/**
 * AppenderCache — generic lazy appender pool shared by import/copy functions.
 *
 * Eliminates the boilerplate of maintaining a per-ID appender cache across
 * both single-message (IndexedAppender) and batch (IndexedBatchAppender)
 * transfer functions.  Callers initialise an AppenderCache with the relevant
 * create/close function pointers, then use appender_cache_get() for per-entry
 * lookup-or-create and appender_cache_cleanup() to close everything at the end.
 */

/* Cache array size — indexed by mapped appender ID (0–255). */
#define APPENDER_CACHE_SIZE 256

/* Sentinel for mapped_cache[] — the mapper has not been called for this aid. */
#define MAPPED_NOT_COMPUTED (-2)

/* Type-erased appender factory — callers pass the typed function pointer. */
typedef int (*AppenderCreateFn)(void *q, void **out);
typedef int (*AppenderCreateWithIdFn)(void *q, uint8_t id, void **out);
typedef void  (*AppenderCloseFn)(void *a);
typedef void  (*QueueCloseFn)(void *q);

/**
 * Lazy appender cache state.
 *
 * cache:        array of APPENDER_CACHE_SIZE pointers, indexed by mapped ID
 * auto_appender: shared appender for APPENDER_ID_AUTO mappings (created once)
 * mapped_cache:  unused by this helper (reserved for future use)
 * create:        indexed_queue_create_appender or _batch_ equivalent
 * create_with_id: indexed_queue_create_appender_with_id or _batch_ equivalent
 * close:         indexed_appender_close or _batch_appender_ equivalent
 * queue_close:   indexed_queue_close or _batch_queue_ equivalent
 */
typedef struct {
    void   **cache;
    void    *auto_appender;
    AppenderCreateFn        create;
    AppenderCreateWithIdFn  create_with_id;
    AppenderCloseFn         close;
    QueueCloseFn            queue_close;
} AppenderCache;

/**
 * Look up or create an appender for the given mapped ID.
 *
 * mapped:      the mapped appender ID (APPENDER_ID_AUTO for auto-assign,
 *              0–254 for a specific ID)
 * q:           the queue (passed to the create function pointers)
 * on_conflict: CONFLICT_AUTO (fall back to auto-assign; the fallback
 *              appender is cached at the original @p mapped slot —
 *              its actual ID may differ from @p mapped) or
 *              CONFLICT_ABORT (return error)
 * out_abort:   set to 1 when CONFLICT_ABORT triggered and all cached
 *              appenders have been closed — caller must close the queue
 *              (via indexed_queue_close or equivalent) and return
 *              MMAPQ_EIDCONFLICT immediately
 * Returns: the appender pointer, or NULL on error / abort
 */
static inline void *appender_cache_get(
    AppenderCache *c, int32_t mapped, void *q,
    ConflictBehavior on_conflict, int *out_abort)
{
    void *a;
    *out_abort = 0;

    if (mapped == APPENDER_ID_AUTO) {
        if (!c->auto_appender) {
            a = NULL;
            if (c->create(q, &a) != 0) return NULL;
            c->auto_appender = a;
        }
        return c->auto_appender;
    }

    if (mapped < 0 || mapped >= APPENDER_CACHE_SIZE) {
        if (!c->auto_appender) {
            a = NULL;
            if (c->create(q, &a) != 0) return NULL;
            c->auto_appender = a;
        }
        return c->auto_appender;
    }

    a = c->cache[mapped];
    if (a) return a;

    a = NULL;
    if (c->create_with_id(q, (uint8_t)mapped, &a) == 0) {
        c->cache[mapped] = a;
        return a;
    }

    if (on_conflict == CONFLICT_ABORT) {
        for (int i = 0; i < APPENDER_CACHE_SIZE; i++)
            if (c->cache[i]) c->close(c->cache[i]);
        if (c->auto_appender) c->close(c->auto_appender);
        *out_abort = 1;
        return NULL;
    }

    a = NULL;
    if (c->create(q, &a) != 0) return NULL;
    c->cache[mapped] = a;
    return a;
}

/**
 * Map a source appender ID through the AppenderIdMapper, caching the result
 * so the mapper is called at most once per unique source aid.
 *
 * mapped_cache: array of APPENDER_CACHE_SIZE, each entry initialised to
 *               MAPPED_NOT_COMPUTED before the first call
 * aid:          source appender ID from the stream or reader
 * mapper:       the AppenderIdMapper callback (NULL = identity)
 * mapper_ctx:   context passed to the mapper callback
 * Returns: the mapped appender ID, or APPENDER_ID_AUTO for auto-assign
 */
static inline int32_t appender_cache_map(
    int32_t *mapped_cache, uint16_t aid,
    AppenderIdMapper mapper, void *mapper_ctx)
{
    if (!mapper) {
        /* Source IDs >= 256 are outside the valid 0-255 appender range.
           Return auto-assign without invoking or caching the mapper. */
        if (aid >= APPENDER_CACHE_SIZE)
            return APPENDER_ID_AUTO;
        return (int32_t)aid;
    }

    /* Same guard for the mapper path — bypass mapper for out-of-range IDs. */
    if (aid >= APPENDER_CACHE_SIZE)
        return APPENDER_ID_AUTO;

    int32_t mapped = mapped_cache[aid];
    if (mapped == MAPPED_NOT_COMPUTED) {
        mapped = mapper(aid, mapper_ctx);
        mapped_cache[aid] = mapped;
    }
    return mapped;
}

/**
 * Close all appenders in the cache.
 *
 * Safe to call even when no appenders were created (cache array all NULL).
 */
static inline void appender_cache_cleanup(AppenderCache *c)
{
    for (int i = 0; i < APPENDER_CACHE_SIZE; i++)
        if (c->cache[i]) c->close(c->cache[i]);
    if (c->auto_appender) c->close(c->auto_appender);
}

#endif /* APPENDER_CACHE_H */
