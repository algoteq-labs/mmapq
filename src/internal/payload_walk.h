#ifndef PAYLOAD_WALK_H
#define PAYLOAD_WALK_H

#include <stdint.h>
#include "internal/region_accessor.h"
#include "header_codec.h"
#include "mmap_assert.h"

/**
 * PayloadWalk — advance through a payload stream handling region boundary padding.
 *
 * Mirrors Java PayloadWalk.advancePayload exactly.
 *
 * Normal queue polling does NOT use this — pollers use the header to jump
 * directly to the payload position. PayloadWalk is only needed during:
 *   - Appender initialisation (find end of stream after restart)
 *   - MmapqUncommittedPayloadIterator (walk gaps between committed spans)
 *   - MmapqPayloadIterator (sequential scan of a payload stream)
 *
 * PADDING RULES:
 *   Short padding: capacity <= 4 bytes at region end.
 *     Action: skip to next region, position += capacity.
 *     No length field is written.
 *     CAND-51e: this is now the ONE tail rule for every item — the batch
 *     count slot follows it too (a count could fit in exactly 4 bytes but
 *     deliberately does not use them: four zero bytes at a region end must
 *     mean only padding, because an open batch's count slot is also zero
 *     and recovery could not tell them apart).
 *
 *   Long padding: length field < 0.
 *     Action: skip forward by (-length + 4) bytes.
 *     Written when payload would straddle a region boundary.
 *
 *   End of stream: length field == 0.
 *     Action: return 0, do not advance.
 *
 *   Valid message: length field > 0.
 *     message_offset_out: set to position of the length prefix.
 *     probing_offset_out: set to position AFTER message (start of next probe).
 *     Returns message length (> 0).
 *
 * CONTRACT:
 *   The caller owns probing_offset (in/out). On entry it is the position to
 *   probe. On exit (if return > 0) it is the next position to probe.
 *   message_offset_out is set to the position of the current message's
 *   4-byte length prefix.
 *
 * Returns:
 *   > 0  : message payload length (bytes, excluding 4-byte length prefix)
 *   == 0 : end of stream at probing_offset
 *   < 0  : MMAPQ_ETIMEDOUT on a transient mapping timeout (retryable);
 *          MMAPQ_ECORRUPT on a corrupt on-disk length/padding or any other
 *          mapping failure — including a missing or too-short file at a
 *          wrap site (F1). Timeout and corruption are never collapsed into
 *          one value: their remedies (retry vs recreate) are opposite.
 *          Same convention as the batch payload iterator's last_error
 *          channel (batch_payload_iterator.c).
 */
int32_t mmapqi_payload_walk_advance(
    const char     *queue_name,
    int64_t        *probing_offset,     /* in/out: current probe position  */
    int64_t        *message_offset_out, /* out:    position of length prefix */
    RegionAccessor *payload_accessor,
    Buffer         *payload_buf);

#endif /* PAYLOAD_WALK_H */
