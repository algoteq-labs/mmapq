#include "unity.h"
#include "internal/header_codec.h"
#include "internal/header_reader.h"
#include "mmap_platform.h"

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
    /* index 1 -> block 0, line 1, sector 0 -> (1 << 6) << 3 = 512 */
    TEST_ASSERT_EQUAL_INT64(512, header_position(1));
}

void test_header_position_index_7(void)
{
    /* index 7 -> block 0, line 7, sector 0 -> (7 << 6) << 3 = 3584 */
    TEST_ASSERT_EQUAL_INT64(3584, header_position(7));
}

void test_header_position_index_8(void)
{
    /* index 8 -> block 0, line 8, sector 0 -> (8 << 6) << 3 = 4096 */
    TEST_ASSERT_EQUAL_INT64(4096, header_position(8));
}

void test_header_position_index_64(void)
{
    /* index 64 -> block 0, line 0, sector 1 -> (0 + 1) << 3 = 8 */
    TEST_ASSERT_EQUAL_INT64(8, header_position(64));
}

void test_header_position_index_4096(void)
{
    /* index 4096 -> block 1, line 0, sector 0 -> (1 << 12) << 3 = 32768 */
    TEST_ASSERT_EQUAL_INT64(32768, header_position(4096));
}

void test_header_round_trip(void)
{
    for (int64_t i = 0; i < 100000; i++) {
        int64_t pos = header_position(i);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(i, header_index(pos), "Round-trip failed");
    }
}

void test_adjacent_indices_different_cache_lines(void)
{
    /*
     * The core false-sharing invariant: any two adjacent indices must land
     * on different cache lines (distance >= 64).
     * With the new striding layout, distance is actually 512 bytes.
     */
    for (int64_t i = 0; i < 63; i++) {
        int64_t pos_i   = header_position(i);
        int64_t pos_i1  = header_position(i + 1);
        int64_t distance = pos_i1 > pos_i ? pos_i1 - pos_i : pos_i - pos_i1;
        TEST_ASSERT_EQUAL_INT64_MESSAGE(512, distance, "Adjacent indices should be 512 bytes apart");
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

/* ── constants ───────────────────────────────────────────────────────────── */

void test_initial_payload_position(void)
{
    TEST_ASSERT_EQUAL_INT64(64, INITIAL_PAYLOAD_POSITION);
}

/* ── main ────────────────────────────────────────────────────────────────── */

static WrapResult failing_header_wrap(RegionAccessor *accessor, int64_t position, Buffer *buffer)
{
    (void)accessor;
    static const uint64_t committed = 64;
    if (position != 0) return WRAP_MAP_TIMED_OUT;
    buffer->data = (uint8_t *)&committed;
    buffer->capacity = sizeof(committed);
    return WRAP_SUCCESS;
}

static int64_t zero_start(void *context)
{
    (void)context;
    return 0;
}

static void test_checked_header_search_preserves_mapping_failure(void)
{
    RegionAccessor accessor = {.wrap_no_premap = failing_header_wrap};
    Buffer buffer = {0};
    TEST_ASSERT_EQUAL_INT64(0, mmapqi_header_search_last_index(0, zero_start, NULL, &accessor, &buffer, "fixture"));
    int error = 0;
    TEST_ASSERT_EQUAL_INT64(HEADER_NULL_INDEX, mmapqi_header_search_last_index_checked(0, zero_start, NULL, &accessor, &buffer, "fixture", &error));
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, error);
    uint64_t header = 99;
    TEST_ASSERT_EQUAL_INT(MMAPQ_ETIMEDOUT, mmapqi_header_read_checked(&accessor, &buffer, 1, &header));
    TEST_ASSERT_EQUAL_UINT64(HEADER_NULL, header);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_checked_header_search_preserves_mapping_failure);

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
    RUN_TEST(test_header_position_index_64);
    RUN_TEST(test_header_position_index_4096);
    RUN_TEST(test_header_round_trip);
    RUN_TEST(test_adjacent_indices_different_cache_lines);

    RUN_TEST(test_short_padding_capacity_0);
    RUN_TEST(test_short_padding_capacity_4);
    RUN_TEST(test_short_padding_capacity_5_is_false);
    RUN_TEST(test_long_padding_negative_length);
    RUN_TEST(test_long_padding_zero_not_long);
    RUN_TEST(test_long_padding_positive_not_long);

    RUN_TEST(test_initial_payload_position);

    return UNITY_END();
}
