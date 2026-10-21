#ifndef WRAP_FAULT_ONESHOT_H
#define WRAP_FAULT_ONESHOT_H

/*
 * wrap_fault_oneshot.h — one-shot wrap-failure injection on the batch
 * appender's payload accessor (the D8 stub idiom, adapted: the shared
 * accessor is reached through the in-progress batch's internal iterator;
 * the vtable is patched and the fast path invalidated).
 *
 * TEST-ONLY (header-only, static inline — the batch_lane_validator.h
 * discipline). No MMAPQ_TEST_HOOKS dependency; runs in both build configs.
 * Moved verbatim from test_indexed_batch_queue.c for CAND-51e (the crash
 * -composition suite arms it for the padding-marker kill point, P4).
 */

#include <stdint.h>

#include "unity.h"
#include "indexed_batch_appender.h"
#include "internal/batch_payload_iterator_internal.h"
#include "internal/region_accessor.h"

static WrapResult (*bq_oneshot_real_wrap)(RegionAccessor *, int64_t, Buffer *);
static int64_t bq_oneshot_pos;
static int     bq_oneshot_hits;
static WrapResult bq_oneshot_wrap(RegionAccessor *self, int64_t position, Buffer *buf)
{
    if (position == bq_oneshot_pos) {
        bq_oneshot_hits++;
        self->wrap = bq_oneshot_real_wrap;   /* one-shot: restore in place */
        return WRAP_MAP_TIMED_OUT;
    }
    return bq_oneshot_real_wrap(self, position, buf);
}
static void bq_arm_oneshot(MmapqMutableBatch *b, int64_t fail_pos)
{
    MmapqBatchPayloadIterator *bit = mmapq_mutable_batch_iterator(b, 0);
    TEST_ASSERT_NOT_NULL(bit);
    RegionAccessor *acc = bit->payload_acc;
    bq_oneshot_real_wrap = acc->wrap;
    bq_oneshot_pos = fail_pos;
    bq_oneshot_hits = 0;
    acc->wrap = bq_oneshot_wrap;
    region_accessor_invalidate_fast_path(acc);
}

#endif /* WRAP_FAULT_ONESHOT_H */
