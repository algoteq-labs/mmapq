#include "batch_payload_iterator.h"
#include "internal/header_codec.h"
#include "internal/mmap_assert.h"
#include "internal/batch_payload_iterator_internal.h"
#include <string.h>

static void wrap_or_fail(MmapqBatchPayloadIterator *it, int64_t position, const char *context)
{
    (void)context;
    WrapResult r = region_accessor_wrap(it->payload_acc, position, &it->payload_buf);
    (void)r;
    if (r != WRAP_SUCCESS) return;
}

int32_t mmapq_batch_iterator_read_message_count(MmapqBatchPayloadIterator *it, int64_t batch_offset)
{
    wrap_or_fail(it, batch_offset, "read_message_count");
    int32_t count;
    memcpy(&count, it->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));
    if (count < 0) return -1;
    return count;
}

int64_t mmapq_batch_iterator_probe_batch(MmapqBatchPayloadIterator *it, int64_t offset)
{
    WrapResult r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
    if (r != WRAP_SUCCESS) return MMAPQ_BATCH_ITERATOR_INVALID_POSITION;

    int32_t cap = it->payload_buf.capacity;
    if (cap < MMAPQ_BATCH_MSG_COUNT_SIZE) {
        offset += cap;
        r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
        if (r != WRAP_SUCCESS || it->payload_buf.capacity < MMAPQ_BATCH_MSG_COUNT_SIZE) {
            return MMAPQ_BATCH_ITERATOR_INVALID_POSITION;
        }
    }

    int32_t count;
    memcpy(&count, it->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));
    return count > 0 ? offset : MMAPQ_BATCH_ITERATOR_INVALID_POSITION;
}

MmapqBatchPayloadIterator *mmapq_batch_iterator_init(
    MmapqBatchPayloadIterator *it,
    int64_t               batch_offset,
    int32_t               message_count,
    int32_t               start_msg_index)
{
    if (message_count < 0) {
        message_count = mmapq_batch_iterator_read_message_count(it, batch_offset);
    }

    it->batch_offset          = batch_offset;
    it->message_count        = message_count;
    it->current_msg_index    = -1;
    it->next_message_position = batch_offset + MMAPQ_BATCH_MSG_COUNT_SIZE;
    it->exhausted            = 0;

    while (it->current_msg_index < start_msg_index - 1) {
        if (!mmapq_batch_iterator_advance(it)) break;
    }

    return it;
}

static int32_t advance_buffer(MmapqBatchPayloadIterator *it)
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
        if (it->payload_buf.capacity != padded) {
            it->exhausted = 1;
            return 0;
        }
        it->next_message_position += padded;
        wrap_or_fail(it, it->next_message_position, "advance_buffer_after_long_padding");
        memcpy(&msg_len, it->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));
    }

    if (msg_len <= 0) return 0;
    it->next_message_position += PAYLOAD_LENGTH_SIZE + msg_len;
    return msg_len;
}

int mmapq_batch_iterator_advance(MmapqBatchPayloadIterator *it)
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

void mmapq_batch_iterator_exhaust(MmapqBatchPayloadIterator *it)
{
    while (mmapq_batch_iterator_advance(it));
}

/* Non-inline wrappers for accessors — used by Rust FFI and Python cffi. */
int mmapq_batch_iterator_has_current_ffi(const MmapqBatchPayloadIterator *it)
{
    return batch_iterator_has_current(it);
}

const void *mmapq_batch_iterator_current_data_ffi(const MmapqBatchPayloadIterator *it)
{
    return batch_iterator_current_data(it);
}

int32_t mmapq_batch_iterator_current_length_ffi(const MmapqBatchPayloadIterator *it)
{
    return batch_iterator_current_length(it);
}

int64_t mmapq_batch_iterator_next_payload_position_ffi(const MmapqBatchPayloadIterator *it)
{
    return batch_iterator_next_payload_position(it);
}
