#ifndef TEST_REACH_H
#define TEST_REACH_H

#include "internal/region_accessor.h"
#include "indexed_appender.h"
#include "indexed_batch_appender.h"

/**
 * Test-only reach into the commit path's header accessor for fault
 * injection. CAND-58 removed the iterator alias
 * (it->header_accessor == a->header_acc) these tests relied on.
 * Not for library use.
 */
RegionAccessor *mmapqi_appender_header_acc(MmapqIndexedAppender *a);
RegionAccessor *mmapqi_batch_appender_header_acc(MmapqIndexedBatchAppender *a);
/* The payload-side twin: the uncommitted walker's payload_acc alias
 * (== a->payload_acc) is likewise gone (the walker maps through the walk
 * accessor); append-path payload fault injection uses these. */
RegionAccessor *mmapqi_appender_payload_acc(MmapqIndexedAppender *a);
RegionAccessor *mmapqi_batch_appender_payload_acc(MmapqIndexedBatchAppender *a);

#endif /* TEST_REACH_H */
