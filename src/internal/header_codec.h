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
 *   length < 0  : long padding  — region boundary, abs(length) + 4 bytes to skip (= remaining region capacity)
 *   length == 0 : end of stream (no message written here yet)
 *
 *   Short padding: when remaining capacity at region end <= 4 bytes,
 *                  skip to next region (no length field written).
 *
 *   Alignment contract (see doc/payload_alignment_design.md):
 *     Regions are 8-byte and page aligned. Individual messages within a
 *     region are packed densely and have no alignment guarantee. Consumers
 *     must use offset-based accessors (like memcpy or flyweights) to read
 *     fields safely.
 *
 * HEADER LAYOUT — false-sharing-proof non-linear mapping:
 *
 *   The layout uses a non-linear mapping strategy that spreads sequential indices
 *   across different sectors (cache lines). This prevents "false sharing" and
 *   reduces CPU contention when multiple threads are reading or writing adjacent
 *   indices.
 *
 *   Indices are organised into blocks of 4096 headers. Within each block,
 *   sequential indices are mapped to different sectors (striding by 512 bytes)
 *   until all 64 sectors have been touched. At that point, the mapping rotates
 *   back to the next available 8-byte word within the first sector.
 *
 *   This implementation matches the tools4j/mmap and Algoteq Java implementations.
 *
 *   Constants (for 8-byte headers and 64-byte cache lines):
 *     HEADER_LENGTH      = 8 bytes
 *     SECTOR_SIZE        = 64 sectors (providing 64 cache lines of isolation)
 *     BLOCK_SIZE         = 4096 headers (64 * 64)
 *
 *   Forward Mapping (index -> position):
 *     block          = index / 4096
 *     index_in_block = index % 4096
 *     sector_index   = index_in_block % 64 (fast stride across cache lines)
 *     word_index     = index_in_block / 64 (slow shift within a line)
 *     position       = ((block * 4096) + (sector_index * 64) + word_index) * 8
 */

/* ── Constants ───────────────────────────────────────────────────────────── */

#define HEADER_SIZE              ((int32_t)8)
#define HEADER_NULL              ((uint64_t)0)

#define HEADER_LENGTH_BITS       3   /* 8 bytes */
#define HEADER_SECTOR_SIZE_BITS  6   /* 64 sectors/lines */
#define HEADER_BLOCK_SIZE_BITS   12  /* 64 * 64 = 4096 headers per block */
#define HEADER_SECTOR_MASK       63
#define HEADER_BLOCK_MASK        4095

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

/* ── Maximum valid index ─────────────────────────────────────────────────── */

/*
 * MMAPQI_MAX_INDEX — largest valid queue index: 2^56 - 1.
 *
 * Derivation: header_position(index) is 8*P(index) with P a bijection that
 * permutes within each 4096-entry block, so position < (index + 4096)*8.
 * At MAX the position is < (2^56 + 4096)*8 < 2^59 + 2^15, leaving headroom
 * for every downstream computation to stay defined for any legal geometry:
 *   - position + capacity (region_async.c fast_path_end,
 *     region_ring_accessor.c fast_path_end): capacity <= region_size <= 2^30,
 *     sum < 2^59 + 2^30;
 *   - (absolute_index + regions_to_map_ahead) << shift (region_ring_accessor.c
 *     premap): < 2^59 + 2^15 + 2^31*2^30 = 2^59 + 2^61 < 2^63.
 * region_size <= 2^30 is enforced (int32_t, positive, power of two:
 * indexed_queue.c:83-84, indexed_batch_queue.c:60-61, queue_meta.c:91-96);
 * regions_to_map_ahead is unchecked but is an int, so < 2^31
 * (indexed_queue.c:101, queue_region_accessor.c:98).
 *
 * The value matches the 56-bit payload-position field so the docs carry one
 * number. Beyond MAX the position wraps mod 2^64 and can land on a valid,
 * already-written slot (index 2^61 -> position 0): every public entry point
 * taking an index or a start_index must reject via header_index_in_range,
 * and meta_validate refuses an out-of-range on-disk start_index.
 *
 * Scope of the guarantee: within [0, MAX], header_position itself is
 * alias-free — but the rolled mapper's `int file_index` truncation
 * (file_mapper_rolled.c) can still alias legal indices at small geometries
 * until CAND-28 lands. The bound below is the header-position bound, not a
 * whole-path aliasing guarantee.
 */
#define MMAPQI_MAX_INDEX (((int64_t)1 << 56) - 1)

static inline int header_index_in_range(int64_t index)
{
    return index >= 0 && index <= MMAPQI_MAX_INDEX;
}

/* ── Header layout — position of header entry in header file ─────────────── */

/**
 * header_position — compute the file byte offset for index in the header file.
 *
 * Optimized non-linear mapping matching Java Layout.java.
 *
 * Transposing from horizontal walk to vertical walk to eliminate false sharing:
 * using modulo for sector_index (fast stride) and division for word_index (slow shift).
 *
 * INVARIANT: 0 <= index <= MMAPQI_MAX_INDEX. The computation is done in
 * uint64_t so a violating index yields a defined (wrapped) position rather
 * than signed-shift UB — but a wrapped position can alias a valid,
 * already-written slot, so the INVARIANT is enforced at every public entry
 * point, not here.
 */
static inline int64_t header_position(int64_t index)
{
    MMAP_ASSERT(index >= 0, "header_position: index must be >= 0");
    const uint64_t block          = (uint64_t)index >> HEADER_BLOCK_SIZE_BITS;
    const uint64_t index_in_block = (uint64_t)index & HEADER_BLOCK_MASK;

    /* Logical transposition: sector_index changes every step, providing
     * a 512-byte stride across different cache lines. */
    const uint64_t sector_index = index_in_block & HEADER_SECTOR_MASK; /* index % 64 */
    const uint64_t word_index   = index_in_block >> HEADER_SECTOR_SIZE_BITS; /* index / 64 */

    return (int64_t)(((block << HEADER_BLOCK_SIZE_BITS) +
            (sector_index << HEADER_SECTOR_SIZE_BITS) +
            word_index) << HEADER_LENGTH_BITS);
}

/**
 * header_index — compute the index for a given file byte offset.
 *
 * Inverse of header_position. Used for testing and recovery.
 *
 * INVARIANT: position >= 0
 */
static inline int64_t header_index(int64_t position)
{
    MMAP_ASSERT(position >= 0, "header_index: position must be >= 0");
    const int64_t val   = position >> HEADER_LENGTH_BITS;
    const int64_t block = val >> HEADER_BLOCK_SIZE_BITS;
    const int64_t rem   = val & HEADER_BLOCK_MASK;

    /* Inverse transposition */
    const int64_t sector_index = rem >> HEADER_SECTOR_SIZE_BITS;
    const int64_t word_index   = rem & HEADER_SECTOR_MASK;

    return (block << HEADER_BLOCK_SIZE_BITS) + 
           (word_index << HEADER_SECTOR_SIZE_BITS) + 
           sector_index;
}

/* ── Payload length field helpers ────────────────────────────────────────── */

/**
 * payload_is_short_padding — true when region end has 4 or fewer bytes remaining (capacity <= 4).
 * The caller must skip to the next region without reading a length field.
 */
static inline int payload_is_short_padding(int32_t capacity)
{
    return capacity <= PAYLOAD_LENGTH_SIZE;
}

/**
 * payload_is_long_padding — true when the 4-byte length field is negative.
 * abs(length) counts the bytes after the 4-byte length field itself;
 * the caller skips forward by abs(length) + PAYLOAD_LENGTH_SIZE (= remaining region capacity).
 *
 * NOTE: The Java implementation writes (PAYLOAD_LENGTH_SIZE - capacity) as the negative
 * length, so abs(length) + PAYLOAD_LENGTH_SIZE = original capacity.
 * In C: the walk skips by -length + PAYLOAD_LENGTH_SIZE (see payload_walk.c).
 */
static inline int payload_is_long_padding(int32_t length)
{
    return length < 0;
}

#endif /* HEADER_CODEC_H */
