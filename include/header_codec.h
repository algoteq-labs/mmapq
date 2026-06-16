#ifndef HEADER_CODEC_H
#define HEADER_CODEC_H

#include <stdint.h>
#include <stddef.h>
#include "mmap_assert.h"

/**
 * HeaderCodec — encoding and layout for indexed queue headers.
 *
 * Each queue entry has an 8-byte header stored in the header file:
 *
 *   bits[63:56] = appenderId  (top 8 bits, unsigned)
 *   bits[55:0]  = payloadPosition (56 bits => ~72 petabytes per appender lane)
 *   NULL_HEADER = 0  (zero bytes = no entry)
 *
 * The header is written atomically via CAS (compare-and-swap) from NULL_HEADER
 * to the encoded value. This is the single commit point for each queue entry.
 *
 * PAYLOAD FRAMING (per appender file):
 *   [int32 length][payload bytes...]
 *
 *   length > 0  : valid message, payload_bytes = length
 *   length < 0  : long padding  — region boundary, abs(length) - 4 bytes to skip
 *   length == 0 : end of stream (no message written here yet)
 *
 *   Short padding: when remaining capacity at region end < 4 bytes,
 *                  skip to next region (no length field written).
 *
 * HEADER LAYOUT — false-sharing-aware:
 *
 *   Each header entry occupies exactly one cache line slot so that
 *   adjacent appenders (consecutive indices) never share a cache line.
 *
 *   HEADER_SIZE     = 8 bytes
 *   CACHE_LINE_SIZE = 64 bytes
 *   SLOTS_PER_LINE  = 1  (one header per cache line)
 *   SLOTS_PER_BLOCK = CACHE_LINE_SIZE / HEADER_SIZE = 8
 *   BYTES_PER_BLOCK = CACHE_LINE_SIZE * SLOTS_PER_BLOCK = 512
 *
 *   position(index):
 *     block = index / SLOTS_PER_BLOCK
 *     slot  = index % SLOTS_PER_BLOCK
 *     return block * BYTES_PER_BLOCK + slot * CACHE_LINE_SIZE
 *
 *   Examples:
 *     index 0 -> 0       index 1 -> 64      index 7 -> 448
 *     index 8 -> 512     index 9 -> 576     index 15 -> 960
 *
 *   Appenders 0..7 each write to a different cache line within block 0.
 *   No two adjacent index writes share a cache line => zero false sharing.
 */

/* ── Constants ───────────────────────────────────────────────────────────── */

#define HEADER_SIZE              ((int32_t)8)
#define HEADER_NULL              ((uint64_t)0)
#define HEADER_OFFSET            ((int32_t)0)
#define CACHE_LINE_SIZE          ((int32_t)64)
#define SLOTS_PER_BLOCK          ((int32_t)(CACHE_LINE_SIZE / HEADER_SIZE))   /* 8 */
#define BYTES_PER_BLOCK          ((int32_t)(CACHE_LINE_SIZE * SLOTS_PER_BLOCK)) /* 512 */

#define PAYLOAD_LENGTH_OFFSET    ((int32_t)0)
#define PAYLOAD_LENGTH_SIZE      ((int32_t)4)
#define PAYLOAD_DATA_OFFSET      ((int32_t)(PAYLOAD_LENGTH_OFFSET + PAYLOAD_LENGTH_SIZE))

/* First valid payload position — reserved prefix for alignment/metadata */
#define INITIAL_PAYLOAD_POSITION ((int64_t)64)

/* Maximum appenders — 8-bit appender id field in header */
#define MAX_APPENDER_ID          ((int32_t)255)
#define MAX_APPENDERS            ((int32_t)256)

/* Masks for header field extraction */
#define HEADER_PAYLOAD_MASK      ((uint64_t)0x00FFFFFFFFFFFFFFULL) /* bits[55:0] */
#define HEADER_APPENDER_BITS     ((int32_t)56)                     /* shift to reach bits[63:56] */

/**
 * ASSERT_HEADER_ALIGNED — verify that a mapped header pointer is naturally
 * aligned for atomic access. Call this whenever a new buffer is wrapped
 * for header CAS operations.
 *
 * Cross-process atomics require natural alignment. mmap returns page-aligned
 * memory and header_position() always produces 8-byte aligned offsets
 * (cache-line multiples of 64 bytes), so this should never fire in practice.
 * It is a safety net for misconfiguration or platform surprises.
 */
#define ASSERT_HEADER_ALIGNED(ptr) \
    MMAP_ASSERT(((uintptr_t)(ptr) & 7) == 0, \
        "header pointer is not 8-byte aligned — cross-process CAS is unsafe")

/**
 * Encode an (appenderId, payloadPosition) pair into a uint64 header.
 *
 * INVARIANT: payloadPosition must fit in 56 bits.
 *            appenderId must be in [0, 255].
 */
static inline uint64_t header_encode(uint8_t appender_id, int64_t payload_position)
{
    MMAP_ASSERT(payload_position > 0,
        "header_encode: payload_position must be > 0 (NULL_HEADER = 0)");
    MMAP_ASSERT((payload_position & ~(int64_t)HEADER_PAYLOAD_MASK) == 0,
        "header_encode: payload_position exceeds 56-bit field");
    return ((uint64_t)appender_id << HEADER_APPENDER_BITS)
         | ((uint64_t)payload_position & HEADER_PAYLOAD_MASK);
}

/**
 * Decode the appenderId from a header.
 */
static inline uint8_t header_appender_id(uint64_t header)
{
    return (uint8_t)(header >> HEADER_APPENDER_BITS);
}

/**
 * Decode the payloadPosition from a header.
 */
static inline int64_t header_payload_position(uint64_t header)
{
    return (int64_t)(header & HEADER_PAYLOAD_MASK);
}

/* ── Header layout — position of header entry in header file ─────────────── */

/**
 * header_position — compute the file byte offset for index in the header file.
 *
 * False-sharing-aware: adjacent indices land on different cache lines.
 *
 * INVARIANT: index >= 0
 */
static inline int64_t header_position(int64_t index)
{
    MMAP_ASSERT(index >= 0, "header_position: index must be >= 0");
    int64_t block = index / SLOTS_PER_BLOCK;
    int64_t slot  = index % SLOTS_PER_BLOCK;
    return block * BYTES_PER_BLOCK + slot * CACHE_LINE_SIZE;
}

/* ── Payload length field helpers ────────────────────────────────────────── */

/**
 * payload_is_short_padding — true when region end has fewer than 4 bytes remaining.
 * The caller must skip to the next region without reading a length field.
 */
static inline int payload_is_short_padding(int32_t capacity)
{
    return capacity <= PAYLOAD_LENGTH_SIZE;
}

/**
 * payload_is_long_padding — true when the 4-byte length field is negative.
 * The padding size = abs(length) bytes (including the 4-byte length field itself).
 * The caller must skip forward by abs(length) bytes.
 *
 * NOTE: The Java implementation writes (PAYLOAD_LENGTH_SIZE - capacity) as the negative
 * length, so abs(length) + PAYLOAD_LENGTH_SIZE = original capacity.
 * In C: long_padding_skip = -length + PAYLOAD_LENGTH_SIZE
 */
static inline int payload_is_long_padding(int32_t length)
{
    return length < 0;
}

/**
 * payload_long_padding_skip — number of bytes to skip past long padding.
 */
static inline int32_t payload_long_padding_skip(int32_t length)
{
    MMAP_ASSERT(length < 0, "payload_long_padding_skip: length must be negative");
    return -length + PAYLOAD_LENGTH_SIZE;
}

#endif /* HEADER_CODEC_H */
