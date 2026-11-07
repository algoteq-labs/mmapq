#include "batch_payload_iterator.h"
#include "mmap_platform.h"
#include "internal/header_codec.h"
#include "internal/walk_classify.h"
#include "internal/mmap_assert.h"
#include "internal/batch_payload_iterator_internal.h"
#include <string.h>

static int wrap_or_fail(MmapqBatchPayloadIterator *it, int64_t position)
{
    WrapResult r = region_accessor_wrap(it->payload_acc, position, &it->payload_buf);
    if (r == WRAP_SUCCESS) return 1;
    /* CAND-58: in walk_mode a file miss surfaces as MMAPQI_WALK_MAP_MISS so
     * the walk's caller classifies it (EOS vs restart vs corruption) */
    if (it->walk_mode &&
        (r == WRAP_FILE_NOT_FOUND || r == WRAP_BEYOND_FILE_SIZE))
        it->last_error = MMAPQI_WALK_MAP_MISS;
    else
        it->last_error = r == WRAP_MAP_TIMED_OUT ? MMAPQ_ETIMEDOUT : MMAPQ_ECORRUPT;
    it->exhausted = 1;
    return 0;
}

int32_t mmapq_batch_iterator_read_message_count(MmapqBatchPayloadIterator *it, int64_t batch_offset)
{
    WrapResult result = region_accessor_wrap(it->payload_acc, batch_offset, &it->payload_buf);
    if (result == WRAP_MAP_TIMED_OUT) return MMAPQ_ETIMEDOUT;
    if (result != WRAP_SUCCESS) {
        /* CAND-58: in walk_mode a file miss surfaces for classification —
         * checked only on the failure path; the success path is the public
         * reader path and is unchanged from baseline */
        if (it->walk_mode &&
            (result == WRAP_FILE_NOT_FOUND || result == WRAP_BEYOND_FILE_SIZE))
            return MMAPQI_WALK_MAP_MISS;
        return MMAPQ_ECORRUPT;
    }
    /* capacity <= 4: a batch count never occupies a region tail (the
     * CAND-51e unified tail rule) — a batch offset there is corrupt. */
    if (it->payload_buf.capacity <= MMAPQ_BATCH_MSG_COUNT_SIZE)
        return MMAPQ_ECORRUPT;
    int32_t count;
    memcpy(&count, it->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));
    /* No upper bound by design: a valid batch may span multiple payload
     * files, so no capacity-derived bound exists. An over-stated count
     * is caught downstream — the walk fails ECORRUPT when the framing
     * stops parsing. Nothing allocates proportional to the count. */
    if (count < 0) return MMAPQ_ECORRUPT;
    return count;
}

int64_t mmapq_batch_iterator_probe_batch(MmapqBatchPayloadIterator *it, int64_t offset)
{
    WrapResult r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
    if (r != WRAP_SUCCESS) return MMAPQ_BATCH_ITERATOR_INVALID_POSITION;

    int32_t cap = it->payload_buf.capacity;
    /* CAND-51e unified tail rule: <= 4 bytes left means padding — skip to
     * the next region; a batch count slot never occupies a region tail. */
    if (cap <= MMAPQ_BATCH_MSG_COUNT_SIZE) {
        offset += cap;
        r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
        if (r != WRAP_SUCCESS || it->payload_buf.capacity <= MMAPQ_BATCH_MSG_COUNT_SIZE) {
            return MMAPQ_BATCH_ITERATOR_INVALID_POSITION;
        }
    }

    int32_t count;
    memcpy(&count, it->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));

    /* D-F (CAND-49 §3.8): a negative count at a batch boundary is a
     * long-padding marker — a legal region-skip iff its extent (-(value)+4)
     * is exactly the remaining region capacity; re-probe at the next
     * region. Any other negative value is invalid. */
    while (count < 0) {
        int64_t skip = -(int64_t)count + MMAPQ_BATCH_MSG_COUNT_SIZE;
        if (skip != it->payload_buf.capacity)
            return MMAPQ_BATCH_ITERATOR_INVALID_POSITION;
        offset += it->payload_buf.capacity;
        r = region_accessor_wrap(it->payload_acc, offset, &it->payload_buf);
        if (r != WRAP_SUCCESS || it->payload_buf.capacity <= MMAPQ_BATCH_MSG_COUNT_SIZE)
            return MMAPQ_BATCH_ITERATOR_INVALID_POSITION;
        memcpy(&count, it->payload_buf.data + MMAPQ_BATCH_MSG_COUNT_OFFSET, sizeof(int32_t));
    }
    return count > 0 ? offset : MMAPQ_BATCH_ITERATOR_INVALID_POSITION;
}

MmapqBatchPayloadIterator *mmapq_batch_iterator_init(
    MmapqBatchPayloadIterator *it,
    int64_t               batch_offset,
    int32_t               message_count,
    int32_t               start_msg_index)
{
    if (it == NULL) return NULL;
    it->last_error = 0;
    if (message_count < 0) {
        message_count = mmapq_batch_iterator_read_message_count(it, batch_offset);
        if (message_count < 0) it->last_error = message_count;
    }

    it->batch_offset          = batch_offset;
    it->message_count        = message_count;
    it->current_msg_index    = -1;
    it->next_message_position = batch_offset + MMAPQ_BATCH_MSG_COUNT_SIZE;
    it->exhausted            = it->last_error != 0;

    /* CAND-50 item 3b: a start outside [0, message_count] is refused —
     * the clamp-to-0 was a positioning-loop artifact, never deliberate
     * (the D3.0 rule-13 record; Java moveTo has always thrown), and an
     * over-run start was indistinguishable from the legal empty
     * iteration. The upper bound is checked only when the count is
     * known-good — a failed count read keeps its own error.
     * start_msg_index == message_count stays a legal empty iteration
     * (the loop below walks to the end; the first advance then reports
     * exhaustion with last_error 0). */
    if (start_msg_index < 0 ||
        (it->last_error == 0 && start_msg_index > it->message_count)) {
        it->last_error = MMAPQ_EINVAL;
        it->exhausted  = 1;
        return it;
    }

    while (it->current_msg_index < start_msg_index - 1) {
        if (!mmapq_batch_iterator_advance(it)) break;
    }

    return it;
}

static int32_t advance_buffer(MmapqBatchPayloadIterator *it)
{
    if (!wrap_or_fail(it, it->next_message_position)) return 0;
    int32_t cap = it->payload_buf.capacity;

    /* Handle short padding */
    if (cap <= PAYLOAD_LENGTH_SIZE) {
        it->next_message_position += cap;
        if (!wrap_or_fail(it, it->next_message_position)) return 0;
    }

    if (it->payload_buf.capacity <= PAYLOAD_LENGTH_SIZE) goto corrupt;

    int32_t msg_len;
    memcpy(&msg_len, it->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));

    /* Handle long padding */
    if (msg_len < 0) {
        int64_t padded = -(int64_t)msg_len + PAYLOAD_LENGTH_SIZE;
        if (it->payload_buf.capacity != padded) {
            goto corrupt;
        }
        it->next_message_position += padded;
        if (!wrap_or_fail(it, it->next_message_position)) return 0;
        if (it->payload_buf.capacity <= PAYLOAD_LENGTH_SIZE) goto corrupt;
        memcpy(&msg_len, it->payload_buf.data + PAYLOAD_LENGTH_OFFSET, sizeof(int32_t));
    }

    if (msg_len <= 0 || msg_len > it->payload_buf.capacity - PAYLOAD_LENGTH_SIZE)
        goto corrupt;
    it->next_message_position += PAYLOAD_LENGTH_SIZE + msg_len;
    return msg_len;

corrupt:
    it->last_error = MMAPQ_ECORRUPT;
    it->exhausted = 1;
    return 0;
}

/* One step body shared by the boolean advance() and the checked next().
 * next() must not call the exported advance(): the bindings build this file
 * position-independent, where an exported callee is interposable and is not
 * inlined, so next() paid a second call layer per message (RS-BATCH-LAT-01). */
static inline int iterator_step(MmapqBatchPayloadIterator *it)
{
    if (it->exhausted) return 0;
    if (it->current_msg_index + 1 < it->message_count) {
        it->current_msg_index++;
        it->current_msg_offset = it->next_message_position;
        it->current_msg_length = advance_buffer(it);
        return it->current_msg_length > 0;
    }
    it->exhausted = 1;
    return 0;
}

int mmapq_batch_iterator_advance(MmapqBatchPayloadIterator *it)
{
    return iterator_step(it);
}

int mmapq_batch_iterator_next(MmapqBatchPayloadIterator *iterator,
                             const void **data, int32_t *length)
{
    /* Outputs are cleared on every non-success return (header contract);
     * the success path overwrites both, so it skips the clearing stores. */
    if (iterator == NULL || data == NULL || length == NULL) {
        if (data != NULL) *data = NULL;
        if (length != NULL) *length = 0;
        return MMAPQ_EINVAL;
    }
    if (!iterator_step(iterator)) {
        *data = NULL;
        *length = 0;
        return iterator->last_error;
    }
    *data = batch_iterator_current_data(iterator);
    *length = batch_iterator_current_length(iterator);
    return 1;
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
