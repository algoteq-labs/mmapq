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
 *   - UncommittedPayloadIterator (walk gaps between committed spans)
 *   - PayloadIterator (sequential scan of a payload stream)
 *
 * PADDING RULES:
 *   Short padding: capacity <= 4 bytes at region end.
 *     Action: skip to next region, position += capacity.
 *     No length field is written.
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
 */
int32_t payload_walk_advance(
    const char     *queue_name,
    int64_t        *probing_offset,     /* in/out: current probe position  */
    int64_t        *message_offset_out, /* out:    position of length prefix */
    RegionAccessor *payload_accessor,
    Buffer         *payload_buf);

#endif /* PAYLOAD_WALK_H */
