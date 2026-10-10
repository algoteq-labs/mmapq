#include "internal/payload_walk.h"
#include "internal/mmap_assert.h"

#include <string.h>

/*
 * mmapqi_payload_walk_advance — the core payload traversal primitive.
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
int32_t mmapqi_payload_walk_advance(
    const char     *queue_name,
    int64_t        *probing_offset,
    int64_t        *message_offset_out,
    RegionAccessor *payload_accessor,
    Buffer         *payload_buf)
{
    int64_t position = *probing_offset;
    (void)queue_name;  /* carried for diagnostics; currently unused */

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
        /* Skip = -length + PAYLOAD_LENGTH_SIZE, computed in int64_t so the
         * negation of INT32_MIN is defined. The appender writes padding whose
         * skip equals the remaining region capacity (write_padding_if_needed),
         * so a mismatch means the on-disk length is corrupt — a runtime check,
         * not an assert, because the value is on-disk data (see AGENTS.md). */
        int64_t skip = -(int64_t)length + PAYLOAD_LENGTH_SIZE;
        if ((int64_t)payload_buf->capacity != skip) return -1;

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
    /* Bound the on-disk length against the mapped region capacity: the message
     * occupies PAYLOAD_LENGTH_SIZE + length bytes, so length may not exceed
     * capacity - PAYLOAD_LENGTH_SIZE. Runtime check — length is on-disk data. */
    if (length <= 0 ||
        (int64_t)length > (int64_t)payload_buf->capacity - PAYLOAD_LENGTH_SIZE)
        return -1;

    *message_offset_out = position;
    *probing_offset     = position + PAYLOAD_LENGTH_SIZE + length;

    return length;
}
