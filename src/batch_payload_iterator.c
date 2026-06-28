#include "batch_payload_iterator.h"
#include "header_codec.h"
#include "mmap_assert.h"
#include <string.h>

static void wrap_or_fail(BatchPayloadIterator *it, int64_t position, const char *context)
{
    (void)context;
    WrapResult r = region_accessor_wrap(it->payload_acc, position, &it->payload_buf);
    (void)r;
    MMAP_ASSERT(r == WRAP_SUCCESS, "batch_iterator: failed to wrap region");
}

int32_t batch_iterator_read_message_count(BatchPayloadIterator *it, int64_t batch_offset)
{
    wrap_or_fail(it, batch_offset, "read_message_count");
    int32_t count;
    memcpy(&count, it->payload_buf.data + BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));
    MMAP_ASSERT(count >= 0, "batch_iterator: negative message count");
    return count;
}

int64_t batch_iterator_probe_batch(BatchPayloadIterator *it, int64_t offset)
{
    WrapResult r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
    if (r != WRAP_SUCCESS) return BATCH_ITERATOR_INVALID_POSITION;

    int32_t cap = it->payload_buf.capacity;
    if (cap < BATCH_MSG_COUNT_SIZE) {
        offset += cap;
        r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
        if (r != WRAP_SUCCESS || it->payload_buf.capacity < BATCH_MSG_COUNT_SIZE) {
            return BATCH_ITERATOR_INVALID_POSITION;
        }
    }

    int32_t count;
    memcpy(&count, it->payload_buf.data + BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));
    return count > 0 ? offset : BATCH_ITERATOR_INVALID_POSITION;
}

BatchPayloadIterator *batch_iterator_init(
    BatchPayloadIterator *it,
    int64_t               batch_offset,
    int32_t               message_count,
    int32_t               start_msg_index)
{
    if (message_count < 0) {
        message_count = batch_iterator_read_message_count(it, batch_offset);
    }

    it->batch_offset          = batch_offset;
    it->message_count        = message_count;
    it->current_msg_index    = -1;
    it->next_message_position = batch_offset + BATCH_MSG_COUNT_SIZE;
    it->exhausted            = 0;

    while (it->current_msg_index < start_msg_index - 1) {
        if (!batch_iterator_advance(it)) break;
    }

    return it;
}

static int32_t advance_buffer(BatchPayloadIterator *it)
{
    wrap_or_fail(it, it->next_message_position, "advance_buffer");
    int32_t cap = it->payload_buf.capacity;

    /* Handle short padding */
    if (cap <= PAYLOAD_LENGTH_SIZE) {
        it->next_message_position += cap;
        wrap_or_fail(it, it->next_message_position, "advance_buffer_after_short_padding");
    }

    int32_t msg_len;
    memcpy(&msg_len, it->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));

    /* Handle long padding */
    if (msg_len < 0) {
        int32_t padded = -msg_len + PAYLOAD_LENGTH_SIZE;
        MMAP_ASSERT(it->payload_buf.capacity == padded, "batch_iterator: padding mismatch");
        it->next_message_position += padded;
        wrap_or_fail(it, it->next_message_position, "advance_buffer_after_long_padding");
        memcpy(&msg_len, it->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));
    }

    MMAP_ASSERT(msg_len > 0, "batch_iterator: zero-length message in committed batch");
    it->next_message_position += PAYLOAD_LENGTH_SIZE + msg_len;
    return msg_len;
}

int batch_iterator_advance(BatchPayloadIterator *it)
{
    if (it->current_msg_index + 1 < it->message_count) {
        it->current_msg_index++;
        it->current_msg_offset = it->next_message_position;
        it->current_msg_length = advance_buffer(it);
        return 1;
    }
    it->exhausted = 1;
    return 0;
}

void batch_iterator_exhaust(BatchPayloadIterator *it)
{
    while (batch_iterator_advance(it));
}

/* Non-inline wrappers for static inline accessors — used by Rust FFI. */
const void *batch_iterator_current_data_ffi(const BatchPayloadIterator *it)
{
    return batch_iterator_current_data(it);
}

int32_t batch_iterator_current_length_ffi(const BatchPayloadIterator *it)
{
    return batch_iterator_current_length(it);
}
