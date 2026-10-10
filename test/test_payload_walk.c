#include "unity.h"
#include "internal/payload_walk.h"
#include "internal/file_mapper.h"
#include "internal/file_initialiser.h"
#include "internal/region_ring.h"
#include "internal/region_ring_accessor.h"
#include "runtime.h"
#include "internal/header_codec.h"

#include <unistd.h>
#include <string.h>
#include <stdio.h>

static const char *FILE_PREFIX  = "/tmp/mmap_test_pwalk";
static const int32_t REGION_SIZE = 4096;
static const int64_t MAX_FILE   = 4096 * 16;

static MmapqRuntime         *rt  = NULL;
static RegionAccessor  *acc = NULL;

/* Helper — write an int32 length + payload bytes into mapped memory */
static void write_msg(uint8_t *base, int32_t offset, const char *msg)
{
    int32_t len = (int32_t)strlen(msg);
    memcpy(base + offset,                        &len, sizeof(int32_t));
    memcpy(base + offset + PAYLOAD_LENGTH_SIZE,  msg,  (size_t)len);
}

/* Helper — write a zero length (end-of-stream marker) */
static void write_eos(uint8_t *base, int32_t offset)
{
    int32_t zero = 0;
    memcpy(base + offset, &zero, sizeof(int32_t));
}

/* Helper — write long padding */
static void write_long_padding(uint8_t *base, int32_t offset, int32_t capacity)
{
    int32_t pad = PAYLOAD_LENGTH_SIZE - capacity;  /* negative */
    memcpy(base + offset, &pad, sizeof(int32_t));
}

static void cleanup(void)
{
    char path[256];
    for (int i = 0; i < 4; i++) {
        snprintf(path, sizeof(path), "%s_%d", FILE_PREFIX, i);
        unlink(path);
    }
}

void setUp(void)
{
    cleanup();
    int rc_rt = mmapq_runtime_region_mapper(&rt);
    TEST_ASSERT_EQUAL_INT(0, rc_rt);
    TEST_ASSERT_NOT_NULL(rt);

    FileMapper *fm = NULL;
    int fmerr = mmapqi_file_mapper_rolled_create(
        FILE_PREFIX, MAX_FILE, REGION_SIZE,
        mmapqi_file_init_zeros, NULL, MAP_MODE_READ_WRITE, 6, 1, &fm);
    TEST_ASSERT_EQUAL_INT(0, fmerr);
    TEST_ASSERT_NOT_NULL(fm);

    RegionRing *ring = NULL;
    int rerr = mmapqi_region_ring_async_create(4, REGION_SIZE, fm, 2000000000ULL, rt, &ring);
    TEST_ASSERT_EQUAL_INT(0, rerr);
    TEST_ASSERT_NOT_NULL(ring);
    int rerr2 = mmapqi_region_ring_accessor_create(ring, REGION_SIZE, 1, 0.5, &acc);
    TEST_ASSERT_EQUAL_INT(0, rerr2);
    TEST_ASSERT_NOT_NULL(acc);
}

void tearDown(void)
{
    if (acc) { mmapqi_region_ring_accessor_destroy(acc); acc = NULL; }
    cleanup();
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

void test_advance_valid_message(void)
{
    /* Write "hello" at position 64 (INITIAL_PAYLOAD_POSITION) */
    Buffer buf = {0};
    region_accessor_wrap(acc, INITIAL_PAYLOAD_POSITION, &buf);
    write_msg(buf.data, 0, "hello");

    int64_t probing   = INITIAL_PAYLOAD_POSITION;
    int64_t msg_off   = -1;
    int32_t length = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);

    TEST_ASSERT_EQUAL_INT32(5, length);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION, msg_off);
    TEST_ASSERT_EQUAL_INT64(INITIAL_PAYLOAD_POSITION + PAYLOAD_LENGTH_SIZE + 5, probing);
}

void test_advance_end_of_stream_returns_zero(void)
{
    /* Position with zero length = end of stream */
    Buffer buf = {0};
    region_accessor_wrap(acc, INITIAL_PAYLOAD_POSITION, &buf);
    write_eos(buf.data, 0);

    int64_t probing = INITIAL_PAYLOAD_POSITION;
    int64_t msg_off = -1;
    int32_t length = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);

    TEST_ASSERT_EQUAL_INT32(0, length);
}

void test_advance_multiple_messages(void)
{
    Buffer buf = {0};
    region_accessor_wrap(acc, INITIAL_PAYLOAD_POSITION, &buf);

    /* Write 3 messages sequentially */
    int32_t offset = 0;
    write_msg(buf.data, offset, "msg1");  offset += PAYLOAD_LENGTH_SIZE + 4;
    write_msg(buf.data, offset, "msg22"); offset += PAYLOAD_LENGTH_SIZE + 5;
    write_msg(buf.data, offset, "msg333");offset += PAYLOAD_LENGTH_SIZE + 6;
    write_eos(buf.data, offset);

    int64_t probing = INITIAL_PAYLOAD_POSITION;
    int64_t msg_off = -1;
    int32_t len;

    len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(4, len);

    len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(5, len);

    len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(6, len);

    len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(0, len);  /* end of stream */
}

void test_advance_long_padding(void)
{
    /*
     * Appender-style long padding (indexed_appender.c write_padding_if_needed):
     * when the remaining region capacity is > PAYLOAD_LENGTH_SIZE but too small
     * for the next message, the appender writes a negative length field of
     * (PAYLOAD_LENGTH_SIZE - capacity) and continues at the next region boundary.
     *
     * Place the padding at region offset REGION_SIZE-100 (= 3996), so the
     * remaining capacity there is exactly 100 and the skip lands on the next
     * region boundary (absolute offset REGION_SIZE = 4096), where the real
     * message begins. This is the advance-through-padding path; the helpers
     * are exercised only indirectly, through the walk's observable result.
     */
    const int64_t pad_pos = REGION_SIZE - 100;   /* 3996 */
    Buffer buf = {0};

    /* Real message at the start of region 1 (absolute offset REGION_SIZE). */
    TEST_ASSERT_EQUAL_INT(WRAP_SUCCESS, region_accessor_wrap(acc, REGION_SIZE, &buf));
    write_msg(buf.data, 0, "after_padding");   /* length 13 at 4096, bytes at 4100 */

    /* Long padding record at pad_pos (remaining capacity 100). */
    TEST_ASSERT_EQUAL_INT(WRAP_SUCCESS, region_accessor_wrap(acc, pad_pos, &buf));
    write_long_padding(buf.data, 0, 100);       /* writes PAYLOAD_LENGTH_SIZE - 100 = -96 */

    int64_t probing = pad_pos;
    int64_t msg_off = -1;
    int32_t len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);

    /* The walk skips the 100-byte padding and lands exactly on the message. */
    TEST_ASSERT_EQUAL_INT32((int32_t)strlen("after_padding"), len);
    TEST_ASSERT_EQUAL_INT64(REGION_SIZE, msg_off);
    TEST_ASSERT_EQUAL_INT64(REGION_SIZE + PAYLOAD_LENGTH_SIZE + (int32_t)strlen("after_padding"), probing);

    /* Next advance reads the zero length just past the message = end of stream. */
    len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(0, len);
}

void test_payload_walk_rejects_length_beyond_capacity(void)
{
    /*
     * C-04: a corrupt on-disk length larger than the mapped region capacity
     * must be rejected (-1), not trusted. Pre-fix the walk had no bound and
     * returned the bogus length, publishing a message_buf that overruns the
     * region mapping (audit repro C-04: OOB read once a consumer reads the
     * declared capacity). A bare walk call returns the bogus length pre-fix —
     * the fail-first here is the equality failure, not a crash.
     */
    Buffer buf = {0};
    TEST_ASSERT_EQUAL_INT(WRAP_SUCCESS, region_accessor_wrap(acc, INITIAL_PAYLOAD_POSITION, &buf));
    int32_t bogus = REGION_SIZE * 8;   /* far beyond capacity (REGION_SIZE - 64) */
    memcpy(buf.data + PAYLOAD_LENGTH_OFFSET, &bogus, sizeof(int32_t));

    int64_t probing = INITIAL_PAYLOAD_POSITION;
    int64_t msg_off = -1;
    int32_t len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(-1, len);
}

void test_payload_walk_rejects_int32_min_length(void)
{
    /*
     * C-04: a corrupt length of INT32_MIN takes the long-padding branch; the
     * skip computation must not negate INT32_MIN in int32 (signed-overflow UB).
     * The runtime capacity check rejects it (-1). Pre-fix: UB in
     * payload_long_padding_skip, then MMAP_ASSERT(capacity == skip) aborts
     * (SIGABRT) in debug builds.
     */
    Buffer buf = {0};
    TEST_ASSERT_EQUAL_INT(WRAP_SUCCESS, region_accessor_wrap(acc, INITIAL_PAYLOAD_POSITION, &buf));
    int32_t bogus = INT32_MIN;
    memcpy(buf.data + PAYLOAD_LENGTH_OFFSET, &bogus, sizeof(int32_t));

    int64_t probing = INITIAL_PAYLOAD_POSITION;
    int64_t msg_off = -1;
    int32_t len = mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);
    TEST_ASSERT_EQUAL_INT32(-1, len);
}

void test_probing_offset_advances_correctly(void)
{
    Buffer buf = {0};
    region_accessor_wrap(acc, INITIAL_PAYLOAD_POSITION, &buf);
    write_msg(buf.data, 0, "test");

    int64_t probing    = INITIAL_PAYLOAD_POSITION;
    int64_t msg_off    = -1;
    int64_t expected   = INITIAL_PAYLOAD_POSITION + PAYLOAD_LENGTH_SIZE + 4;

    mmapqi_payload_walk_advance("test", &probing, &msg_off, acc, &buf);

    TEST_ASSERT_EQUAL_INT64(expected, probing);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_advance_valid_message);
    RUN_TEST(test_advance_end_of_stream_returns_zero);
    RUN_TEST(test_advance_multiple_messages);
    RUN_TEST(test_advance_long_padding);
    RUN_TEST(test_payload_walk_rejects_length_beyond_capacity);
    RUN_TEST(test_payload_walk_rejects_int32_min_length);
    RUN_TEST(test_probing_offset_advances_correctly);
    return UNITY_END();
}
