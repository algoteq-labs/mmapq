#include "internal/payload_walk.h"
#include "internal/mmap_assert.h"

#include <string.h>

/*
 * payload_walk_advance — the core payload traversal primitive.
 *
 * This is the only place in the codebase that handles padding.
 * Gets it right here so everything above it inherits correctness.
 *
 * Mirrors Java PayloadWalk.advancePayload exactly.
 *
 * Entry state:  *probing_offset = position to probe
 * Exit state:
 *   return > 0: *message_offset_out = position of length prefix
 *               *probing_offset     = position after message (next probe)
 *   return == 0: end of stream, neither output modified meaningfully
 */
int32_t payload_walk_advance(
    const char     *queue_name,
    int64_t        *probing_offset,
    int64_t        *message_offset_out,
    RegionAccessor *payload_accessor,
    Buffer         *payload_buf)
{
    int64_t position = *probing_offset;
    (void)queue_name;  /* carried for diagnostics; used in MMAP_ASSERT messages */

    /* ── Map current position ────────────────────────────────────────────── */
    WrapResult r = region_accessor_wrap(payload_accessor, position, payload_buf);
    if (r != WRAP_SUCCESS) {
        return -1;
    }

    int32_t capacity = payload_buf->capacity;

    /* ── Short padding: region end has < 4 bytes ─────────────────────────── */
    if (payload_is_short_padding(capacity)) {
        position += capacity;
        *message_offset_out = position;
        *probing_offset     = position;

        r = region_accessor_wrap(payload_accessor, position, payload_buf);
        if (r != WRAP_SUCCESS) {
            return -1;
        }
    }

    /* ── Read length field ───────────────────────────────────────────────── */
    int32_t length;
    memcpy(&length, payload_buf->data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));

    /* ── Long padding: negative length field ─────────────────────────────── */
    if (payload_is_long_padding(length)) {
        int32_t skip = payload_long_padding_skip(length);

        MMAP_ASSERT(payload_buf->capacity == skip,
            "payload_walk_advance: long padding size mismatch — "
            "payload stream is corrupt");

        position += skip;
        *message_offset_out = position;
        *probing_offset     = position;

        r = region_accessor_wrap(payload_accessor, position, payload_buf);
        if (r != WRAP_SUCCESS) {
            return -1;
        }

        /* Re-read length at new position */
        memcpy(&length, payload_buf->data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));
    }

    /* ── End of stream: zero length ──────────────────────────────────────── */
    if (length == 0) {
        return 0;
    }

    /* ── Valid message ───────────────────────────────────────────────────── */
    if (length <= 0) return -1;

    *message_offset_out = position;
    *probing_offset     = position + PAYLOAD_LENGTH_SIZE + length;

    return length;
}
