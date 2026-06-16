#include "unity.h"
#include "header_codec.h"

void setUp(void)    {}
void tearDown(void) {}

/* ── header_encode / decode ──────────────────────────────────────────────── */

void test_encode_decode_round_trip(void)
{
    uint8_t  id  = 42;
    int64_t  pos = 1234567890LL;
    uint64_t hdr = header_encode(id, pos);

    TEST_ASSERT_NOT_EQUAL(HEADER_NULL, hdr);
    TEST_ASSERT_EQUAL_UINT8(id,  header_appender_id(hdr));
    TEST_ASSERT_EQUAL_INT64(pos, header_payload_position(hdr));
}

void test_encode_appender_id_zero(void)
{
    uint64_t hdr = header_encode(0, INITIAL_PAYLOAD_POSITION);
    TEST_ASSERT_EQUAL_UINT8(0, header_appender_id(hdr));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, header_payload_position(hdr));
}

void test_encode_appender_id_max(void)
{
    uint64_t hdr = header_encode(255, INITIAL_PAYLOAD_POSITION);
    TEST_ASSERT_EQUAL_UINT8(255, header_appender_id(hdr));
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, header_payload_position(hdr));
}

void test_encode_large_payload_position(void)
{
    /* 56-bit max: 0x00FFFFFFFFFFFFFF */
    int64_t max_pos = (int64_t)0x00FFFFFFFFFFFFFFLL;
    uint64_t hdr = header_encode(1, max_pos);
    TEST_ASSERT_EQUAL_UINT8(1, header_appender_id(hdr));
    TEST_ASSERT_EQUAL_INT64(max_pos, header_payload_position(hdr));
}

void test_null_header_is_zero(void)
{
    TEST_ASSERT_EQUAL_UINT64(0ULL, HEADER_NULL);
}

void test_encode_never_produces_null_header(void)
{
    /* Any valid encode (pos > 0) must not produce HEADER_NULL */
    uint64_t hdr = header_encode(0, INITIAL_PAYLOAD_POSITION);
    TEST_ASSERT_NOT_EQUAL(HEADER_NULL, hdr);
}

/* ── header_position layout math ────────────────────────────────────────── */

void test_header_position_index_0(void)
{
    TEST_ASSERT_EQUAL_INT64(0, header_position(0));
}

void test_header_position_index_1(void)
{
    /* index 1 -> block 0, slot 1 -> 0 * 512 + 1 * 64 = 64 */
    TEST_ASSERT_EQUAL_INT64(64, header_position(1));
}

void test_header_position_index_7(void)
{
    /* index 7 -> block 0, slot 7 -> 0 + 7 * 64 = 448 */
    TEST_ASSERT_EQUAL_INT64(448, header_position(7));
}

void test_header_position_index_8(void)
{
    /* index 8 -> block 1, slot 0 -> 1 * 512 + 0 = 512 */
    TEST_ASSERT_EQUAL_INT64(512, header_position(8));
}

void test_header_position_index_9(void)
{
    /* index 9 -> block 1, slot 1 -> 512 + 64 = 576 */
    TEST_ASSERT_EQUAL_INT64(576, header_position(9));
}

void test_header_position_index_15(void)
{
    /* index 15 -> block 1, slot 7 -> 512 + 7*64 = 960 */
    TEST_ASSERT_EQUAL_INT64(960, header_position(15));
}

void test_header_position_index_16(void)
{
    /* index 16 -> block 2, slot 0 -> 2 * 512 = 1024 */
    TEST_ASSERT_EQUAL_INT64(1024, header_position(16));
}

void test_adjacent_indices_different_cache_lines(void)
{
    /*
     * The core false-sharing invariant: any two adjacent indices must land
     * on different cache lines (distance >= CACHE_LINE_SIZE = 64).
     */
    for (int64_t i = 0; i < 32; i++) {
        int64_t pos_i   = header_position(i);
        int64_t pos_i1  = header_position(i + 1);
        int64_t distance = pos_i1 > pos_i ? pos_i1 - pos_i : pos_i - pos_i1;
        TEST_ASSERT_GREATER_OR_EQUAL_INT64(CACHE_LINE_SIZE, distance);
    }
}

/* ── payload padding predicates ──────────────────────────────────────────── */

void test_short_padding_capacity_0(void)
{
    TEST_ASSERT_TRUE(payload_is_short_padding(0));
}

void test_short_padding_capacity_4(void)
{
    TEST_ASSERT_TRUE(payload_is_short_padding(4));
}

void test_short_padding_capacity_5_is_false(void)
{
    TEST_ASSERT_FALSE(payload_is_short_padding(5));
}

void test_long_padding_negative_length(void)
{
    TEST_ASSERT_TRUE(payload_is_long_padding(-1));
    TEST_ASSERT_TRUE(payload_is_long_padding(-100));
}

void test_long_padding_zero_not_long(void)
{
    TEST_ASSERT_FALSE(payload_is_long_padding(0));
}

void test_long_padding_positive_not_long(void)
{
    TEST_ASSERT_FALSE(payload_is_long_padding(1));
}

void test_long_padding_skip_value(void)
{
    /* skip = -length + PAYLOAD_LENGTH_SIZE */
    TEST_ASSERT_EQUAL_INT32(PAYLOAD_LENGTH_SIZE + 10, payload_long_padding_skip(-10));
    TEST_ASSERT_EQUAL_INT32(PAYLOAD_LENGTH_SIZE + 60, payload_long_padding_skip(-60));
}

/* ── constants ───────────────────────────────────────────────────────────── */

void test_slots_per_block(void)
{
    TEST_ASSERT_EQUAL_INT32(8, SLOTS_PER_BLOCK);
}

void test_bytes_per_block(void)
{
    TEST_ASSERT_EQUAL_INT32(512, BYTES_PER_BLOCK);
}

void test_initial_payload_position(void)
{
    TEST_ASSERT_EQUAL_INT64(64, INITIAL_PAYLOAD_POSITION);
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    UNITY_BEGIN();

    RUN_TEST(test_encode_decode_round_trip);
    RUN_TEST(test_encode_appender_id_zero);
    RUN_TEST(test_encode_appender_id_max);
    RUN_TEST(test_encode_large_payload_position);
    RUN_TEST(test_null_header_is_zero);
    RUN_TEST(test_encode_never_produces_null_header);

    RUN_TEST(test_header_position_index_0);
    RUN_TEST(test_header_position_index_1);
    RUN_TEST(test_header_position_index_7);
    RUN_TEST(test_header_position_index_8);
    RUN_TEST(test_header_position_index_9);
    RUN_TEST(test_header_position_index_15);
    RUN_TEST(test_header_position_index_16);
    RUN_TEST(test_adjacent_indices_different_cache_lines);

    RUN_TEST(test_short_padding_capacity_0);
    RUN_TEST(test_short_padding_capacity_4);
    RUN_TEST(test_short_padding_capacity_5_is_false);
    RUN_TEST(test_long_padding_negative_length);
    RUN_TEST(test_long_padding_zero_not_long);
    RUN_TEST(test_long_padding_positive_not_long);
    RUN_TEST(test_long_padding_skip_value);

    RUN_TEST(test_slots_per_block);
    RUN_TEST(test_bytes_per_block);
    RUN_TEST(test_initial_payload_position);

    return UNITY_END();
}
