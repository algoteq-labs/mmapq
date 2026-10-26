#include "unity.h"
#include "transfer_faults.h"
#include "queue_transfer.h"
#include "batch_queue_transfer.h"
#include "indexed_reader.h"
#include "indexed_batch_reader.h"
#include "mmap_platform.h"
#include "batch_payload_iterator.h"
#include "internal/export_meta.h"
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>

enum Fault { NO_FAULT, APPEND, START, COMPLETE, FACTORY, SOURCE, SOURCE_GONE };  /* CAND-49: COMMIT removed — complete is the one-call publish (§3.5) */
static enum Fault fault;
static int calls;
static int target;
static int consumed;
static int poison_padding;
static int inject(enum Fault operation);

MmapqReadingContext *fault_read(MmapqIndexedReader *reader, int64_t index, MmapqReadingContext *context)
{
    MmapqReadingContext *result = mmapq_indexed_reader_read(reader, index, context);
    if (inject(SOURCE)) { context->status = MMAPQ_ENTRY_ERROR; context->data = NULL; }
    if (inject(SOURCE_GONE)) context->status = MMAPQ_ENTRY_ARCHIVED;
    if (poison_padding && result != NULL)
        ((unsigned char *)result)[offsetof(MmapqReadingContext, appender_id) + 1] = 0x7f;
    return result;
}

int fault_reader_error(const MmapqIndexedReader *reader)
{
    return fault == SOURCE && consumed ? MMAPQ_ETIMEDOUT : mmapq_indexed_reader_last_error(reader);
}

MmapqBatchReadingContext *fault_batch_read(MmapqIndexedBatchReader *reader, int64_t index, MmapqBatchReadingContext *context)
{
    MmapqBatchReadingContext *result = mmapq_indexed_batch_reader_read(reader, index, context);
    if (inject(SOURCE)) { context->status = MMAPQ_BATCH_ENTRY_ERROR; context->iterator = NULL; }
    if (inject(SOURCE_GONE)) context->status = MMAPQ_BATCH_ENTRY_ARCHIVED;
    return result;
}

int fault_batch_reader_error(const MmapqIndexedBatchReader *reader)
{
    return fault == SOURCE && consumed ? MMAPQ_ETIMEDOUT : mmapq_indexed_batch_reader_last_error(reader);
}

static int inject(enum Fault operation)
{
    if (fault != operation) return 0;
    calls++;
    if (calls != target) return 0;
    consumed++;
    return 1;
}

int64_t fault_append(MmapqIndexedAppender *appender, const void *data, int32_t length)
{
    if (inject(APPEND)) return MMAPQ_ENOMEM;
    return mmapq_indexed_appender_append(appender, data, length);
}
int fault_batch_append(MmapqMutableBatch *batch, const void *data, int32_t length)
{
    if (inject(APPEND)) return MMAPQ_ENOMEM;
    return mmapq_mutable_batch_append(batch, data, length);
}
int fault_batch_start(MmapqIndexedBatchAppender *appender, MmapqMutableBatch **out)
{
    if (inject(START)) { *out = NULL; return MMAPQ_ENOMEM; }
    return mmapq_indexed_batch_appender_start_batch_checked(appender, out);
}
int64_t fault_batch_complete(MmapqMutableBatch *batch)
{
    if (inject(COMPLETE)) return MMAPQ_ETIMEDOUT;
    return mmapq_mutable_batch_complete(batch);
}
int fault_create(MmapqIndexedQueue *queue, uint8_t appender_id, MmapqIndexedAppender **out)
{
    if (inject(FACTORY)) { *out = NULL; return MMAPQ_ENOMEM; }
    return mmapq_indexed_queue_create_appender_with_id(queue, appender_id, out);
}
int fault_batch_create(MmapqIndexedBatchQueue *queue, uint8_t appender_id, MmapqIndexedBatchAppender **out)
{
    if (inject(FACTORY)) { *out = NULL; return MMAPQ_ENOMEM; }
    return mmapq_indexed_batch_queue_create_appender_with_id(queue, appender_id, out);
}

void setUp(void) { fault = NO_FAULT; calls = consumed = poison_padding = 0; target = 2; }
void tearDown(void) { int result = system("rm -rf /tmp/mmapq-transfer-fault"); (void)result; }

static void exercise(int batch_source, int batch_destination, enum Fault selected, int fail_at, int import_stream)
{
    tearDown();
    TEST_ASSERT_EQUAL_INT(0, mkdir("/tmp/mmapq-transfer-fault", 0775));
    MmapqIndexedQueue *normal = NULL;
    MmapqIndexedBatchQueue *batch = NULL;
    MmapqIndexedReader *normal_reader = NULL;
    MmapqIndexedBatchReader *batch_reader = NULL;
    const char *directory = "/tmp/mmapq-transfer-fault";
    if (batch_source) {
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &batch));
        for (int index = 0; index < 3; index++) {
            MmapqIndexedBatchAppender *appender = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(batch, (uint8_t)index, &appender));
            TEST_ASSERT_EQUAL_INT64(index, mmapq_indexed_batch_appender_append_and_commit(appender, "data", 4));
            mmapq_indexed_batch_appender_close(appender);
        }
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(batch, &batch_reader));
    } else {
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &normal));
        for (int index = 0; index < 3; index++) {
            MmapqIndexedAppender *appender = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(normal, (uint8_t)index, &appender));
            TEST_ASSERT_EQUAL_INT64(index, mmapq_indexed_appender_append(appender, "data", 4));
            mmapq_indexed_appender_close(appender);
        }
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(normal, &normal_reader));
    }
    FILE *stream = NULL;
    if (import_stream) {
        stream = tmpfile();
        TEST_ASSERT_NOT_NULL(stream);
        int64_t exported = batch_source
            ? mmapq_batch_queue_transfer_export(batch_reader, stream, 0, 2, 0, NULL, NULL, NULL)
            : mmapq_queue_transfer_export(normal_reader, stream, 0, 2, 0, NULL, NULL, NULL);
        TEST_ASSERT_EQUAL_INT64(3, exported);
        rewind(stream);
    }
    fault = selected; calls = consumed = 0; target = fail_at;
    int64_t result;
    if (import_stream && batch_destination && batch_source)
        result = mmapq_batch_queue_transfer_import(stream, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else if (import_stream && batch_destination)
        result = mmapq_batch_queue_transfer_import_flat_stream(stream, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else if (import_stream && batch_source)
        result = mmapq_queue_transfer_import_batch_stream(stream, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else if (import_stream)
        result = mmapq_queue_transfer_import(stream, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else if (batch_destination && batch_source)
        result = mmapq_batch_queue_transfer_copy(batch_reader, directory, "destination", 0, 2, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else if (batch_destination)
        result = mmapq_batch_queue_transfer_copy_normal_to_batch(normal_reader, directory, "destination", 0, 2, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else if (batch_source)
        result = mmapq_queue_transfer_copy_batch_to_normal(batch_reader, directory, "destination", 0, 2, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    else
        result = mmapq_queue_transfer_copy(normal_reader, directory, "destination", 0, 2, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    if (stream) TEST_ASSERT_EQUAL_INT(0, fclose(stream));
    fault = NO_FAULT;
    int expected = selected == COMPLETE || selected == SOURCE ? MMAPQ_ETIMEDOUT : MMAPQ_ENOMEM;
    TEST_ASSERT_EQUAL_INT64(expected, result);
    TEST_ASSERT_EQUAL_INT(1, consumed);
    TEST_ASSERT_EQUAL_INT(fail_at, calls);
    if (batch_destination) {
        MmapqIndexedBatchQueue *destination = NULL;
        MmapqIndexedBatchReader *reader = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "destination", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &destination));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(destination, &reader));
        TEST_ASSERT_EQUAL_INT64(fail_at - 2, mmapq_indexed_batch_reader_last_index(reader));
        MmapqIndexedBatchAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(destination, 0, &appender));
        TEST_ASSERT_EQUAL_INT64(fail_at - 1, mmapq_indexed_batch_appender_append_and_commit(appender, "next", 4));
        mmapq_indexed_batch_appender_close(appender);
        mmapq_indexed_batch_reader_close(reader);
        mmapq_indexed_batch_queue_close(destination);
    } else {
        MmapqIndexedQueue *destination = NULL;
        MmapqIndexedReader *reader = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "destination", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &destination));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(destination, &reader));
        TEST_ASSERT_EQUAL_INT64(fail_at - 2, mmapq_indexed_reader_last_index(reader));
        MmapqIndexedAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(destination, 0, &appender));
        TEST_ASSERT_EQUAL_INT64(fail_at - 1, mmapq_indexed_appender_append(appender, "next", 4));
        mmapq_indexed_appender_close(appender);
        mmapq_indexed_reader_close(reader);
        mmapq_indexed_queue_close(destination);
    }
    mmapq_indexed_reader_close(normal_reader);
    mmapq_indexed_batch_reader_close(batch_reader);
    mmapq_indexed_queue_close(normal);
    mmapq_indexed_batch_queue_close(batch);
}

static void test_copy_failure_matrix(void)
{
    for (int source = 0; source < 2; source++)
        for (int destination = 0; destination < 2; destination++)
            for (int operation = APPEND; operation <= SOURCE; operation++) {
                if (!destination && operation != APPEND && operation != FACTORY && operation != SOURCE) continue;
                for (int fail_at = 1; fail_at <= 2; fail_at++)
                    exercise(source, destination, (enum Fault)operation, fail_at, 0);
            }
}
static void test_import_failure_matrix(void)
{
    for (int source = 0; source < 2; source++)
        for (int destination = 0; destination < 2; destination++)
            for (int operation = APPEND; operation <= FACTORY; operation++) {
                if (!destination && operation != APPEND && operation != FACTORY) continue;
                for (int fail_at = 1; fail_at <= 2; fail_at++)
                    exercise(source, destination, (enum Fault)operation, fail_at, 1);
            }
}
/* CAND-51a F2 pins: a destination-side failure AFTER start_batch leaves a
 * discarded record (the CAND-49 contract), never an abandoned open batch —
 * no join completion on reopen, the offset is not committable before the
 * uncommitted walk refills it, and the lane stays usable. Fail-first
 * discriminator: join_completed_open_batch (pre-fix rc=1, the abandoned
 * open batch was join-completed to staged).
 * Scenario 0: second append fails (partial batch, discarded count=1).
 * Scenario 1: complete fails (both messages appended, discarded count=2). */

/* Shared destination-lane assertions: join rc 0, commit EINVAL pre-walk,
 * exactly one discarded record at offset 64 with `expect_count` messages,
 * and the lane accepts a new batch afterwards. */
static void assert_discarded_record_lane(const char *directory, int32_t expect_count)
{
    MmapqIndexedBatchQueue *destination = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "destination", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &destination));
    MmapqIndexedBatchAppender *appender = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(destination, 0, &appender));
    int64_t joined = -1;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_join_completed_open_batch(appender, &joined));
    /* The discarded offset is not in the outstanding set until a walk refills it. */
    TEST_ASSERT_EQUAL_INT64(MMAPQ_EINVAL, mmapq_indexed_batch_appender_commit_batch(appender, 64));
    MmapqUncommittedBatchIterator *uit = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_uncommitted_batches_checked(appender, &uit));
    int walked = 0;
    int64_t walked_off = -1;
    int32_t walked_count = -1;
    while (1) {
        int64_t boff; int32_t count; MmapqBatchPayloadIterator *msgs;
        int nrc = mmapq_uncommitted_batch_next(uit, &boff, &count, &msgs);
        if (nrc == 0) break;
        TEST_ASSERT_GREATER_OR_EQUAL_INT(1, nrc);
        walked++;
        walked_off = boff;
        walked_count = count;
    }
    TEST_ASSERT_EQUAL_INT(1, walked);
    TEST_ASSERT_EQUAL_INT64(64, walked_off);
    TEST_ASSERT_EQUAL_INT32(expect_count, walked_count);
    /* The lane stays usable: the next batch publishes at index 0. */
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_batch_appender_append_and_commit(appender, "next", 4));
    mmapq_indexed_batch_appender_close(appender);
    mmapq_indexed_batch_queue_close(destination);
}

static void make_two_message_source_batch(const char *directory)
{
    MmapqIndexedBatchQueue *batch = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &batch));
    MmapqIndexedBatchAppender *appender = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(batch, 0, &appender));
    MmapqMutableBatch *mb = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(appender, &mb));
    TEST_ASSERT_NOT_NULL(mb);
    TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(mb, "m-one", 6));
    TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(mb, "m-two", 6));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_mutable_batch_complete(mb));
    mmapq_indexed_batch_appender_close(appender);
    mmapq_indexed_batch_queue_close(batch);
}

static void test_copy_dest_failure_after_start_leaves_discarded_record(void)
{
    for (int scenario = 0; scenario < 2; scenario++) {
        tearDown();
        const char *directory = "/tmp/mmapq-transfer-fault";
        TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
        make_two_message_source_batch(directory);
        MmapqIndexedBatchQueue *batch = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &batch));
        MmapqIndexedBatchReader *batch_reader = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(batch, &batch_reader));

        fault = scenario == 0 ? APPEND : COMPLETE;
        calls = consumed = 0;
        target = scenario == 0 ? 2 : 1;
        int64_t result = mmapq_batch_queue_transfer_copy(batch_reader, directory, "destination", 0, 0, 0,
                                                         NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
        fault = NO_FAULT;
        TEST_ASSERT_EQUAL_INT64(scenario == 0 ? MMAPQ_ENOMEM : MMAPQ_ETIMEDOUT, result);
        mmapq_indexed_batch_reader_close(batch_reader);
        mmapq_indexed_batch_queue_close(batch);

        assert_discarded_record_lane(directory, scenario == 0 ? 1 : 2);
    }
}

static void test_import_dest_failure_after_start_leaves_discarded_record(void)
{
    for (int scenario = 0; scenario < 2; scenario++) {
        tearDown();
        const char *directory = "/tmp/mmapq-transfer-fault";
        TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
        make_two_message_source_batch(directory);
        MmapqIndexedBatchQueue *batch = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &batch));
        MmapqIndexedBatchReader *batch_reader = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(batch, &batch_reader));
        FILE *stream = tmpfile();
        TEST_ASSERT_NOT_NULL(stream);
        TEST_ASSERT_EQUAL_INT64(1, mmapq_batch_queue_transfer_export(batch_reader, stream, 0, 0, 0, NULL, NULL, NULL));
        rewind(stream);
        mmapq_indexed_batch_reader_close(batch_reader);
        mmapq_indexed_batch_queue_close(batch);

        fault = scenario == 0 ? APPEND : COMPLETE;
        calls = consumed = 0;
        target = scenario == 0 ? 2 : 1;
        int64_t result = mmapq_batch_queue_transfer_import(stream, directory, "destination", 0,
                                                           NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
        TEST_ASSERT_EQUAL_INT(0, fclose(stream));
        fault = NO_FAULT;
        TEST_ASSERT_EQUAL_INT64(scenario == 0 ? MMAPQ_ENOMEM : MMAPQ_ETIMEDOUT, result);

        assert_discarded_record_lane(directory, scenario == 0 ? 1 : 2);
    }
}

/* Same discarded-record pin for the two 1-message-per-batch paths: with a
 * COMPLETE fault the single append has already landed, so the discard
 * stores count=1 — pre-fix the abandoned open batch was join-completed on
 * reopen (join rc=1). (An APPEND fault on these paths fails before any
 * store — the empty batch is discarded by position reset and leaves no
 * trace either way, so APPEND cannot fail-first there.) */
static void make_one_message_normal_source(const char *directory)
{
    MmapqIndexedQueue *queue = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &queue));
    MmapqIndexedAppender *appender = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(queue, 0, &appender));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(appender, "m-one", 6));
    mmapq_indexed_appender_close(appender);
    mmapq_indexed_queue_close(queue);
}

static void test_copy_normal_to_batch_complete_failure_leaves_discarded_record(void)
{
    tearDown();
    const char *directory = "/tmp/mmapq-transfer-fault";
    TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
    make_one_message_normal_source(directory);
    MmapqIndexedQueue *queue = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &queue));
    MmapqIndexedReader *normal_reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(queue, &normal_reader));

    fault = COMPLETE;
    calls = consumed = 0;
    target = 1;
    int64_t result = mmapq_batch_queue_transfer_copy_normal_to_batch(normal_reader, directory, "destination", 0, 0, 0,
                                                                     NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    fault = NO_FAULT;
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT, result);
    mmapq_indexed_reader_close(normal_reader);
    mmapq_indexed_queue_close(queue);

    assert_discarded_record_lane(directory, 1);
}

static void test_import_flat_stream_complete_failure_leaves_discarded_record(void)
{
    tearDown();
    const char *directory = "/tmp/mmapq-transfer-fault";
    TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
    make_one_message_normal_source(directory);
    MmapqIndexedQueue *queue = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &queue));
    MmapqIndexedReader *normal_reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(queue, &normal_reader));
    FILE *stream = tmpfile();
    TEST_ASSERT_NOT_NULL(stream);
    TEST_ASSERT_EQUAL_INT64(1, mmapq_queue_transfer_export(normal_reader, stream, 0, 0, 0, NULL, NULL, NULL));
    rewind(stream);
    mmapq_indexed_reader_close(normal_reader);
    mmapq_indexed_queue_close(queue);

    fault = COMPLETE;
    calls = consumed = 0;
    target = 1;
    int64_t result = mmapq_batch_queue_transfer_import_flat_stream(stream, directory, "destination", 0,
                                                                   NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT(0, fclose(stream));
    fault = NO_FAULT;
    TEST_ASSERT_EQUAL_INT64(MMAPQ_ETIMEDOUT, result);

    assert_discarded_record_lane(directory, 1);
}

static void test_chunk_export_does_not_serialize_context_padding(void)
{
    const char *directory = "/tmp/mmapq-transfer-fault";
    TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
    MmapqIndexedQueue *queue = NULL;
    MmapqIndexedAppender *appender = NULL;
    MmapqIndexedReader *reader = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &queue));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(queue, 7, &appender));
    TEST_ASSERT_EQUAL_INT64(0, mmapq_indexed_appender_append(appender, "data", 4));
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(queue, &reader));
    poison_padding = 1;
    TEST_ASSERT_EQUAL_INT64(1, mmapq_queue_transfer_export_chunked(reader, "/tmp/mmapq-transfer-fault/chunk", 0, 0, 1, 0, NULL, NULL, NULL));
    FILE *stream = fopen("/tmp/mmapq-transfer-fault/chunk_0-0", "rb");
    TEST_ASSERT_NOT_NULL(stream);
    TEST_ASSERT_EQUAL_INT(0, fseek(stream, 64, SEEK_SET));
    uint16_t appender_id = 0;
    TEST_ASSERT_EQUAL_UINT64(sizeof(appender_id), fread(&appender_id, 1, sizeof(appender_id), stream));
    TEST_ASSERT_EQUAL_UINT16(7, appender_id);
    TEST_ASSERT_EQUAL_INT(0, fclose(stream));
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_appender_close(appender);
    mmapq_indexed_queue_close(queue);
}

typedef struct {
    int visits[3];
    int mode;
    int chunked;
} ExportFilterState;

static int changing_export_filter(int64_t index, const void *data, int32_t length, void *ctx)
{
    (void)data;
    (void)length;
    ExportFilterState *state = ctx;
    if (state->mode == 5) return 1;
    int visit = ++state->visits[index];
    return state->mode == 1 ? visit == 1 : index == 0 || visit > 1;
}

static int changing_batch_export_filter(int64_t index, int32_t message_index,
    int32_t message_count, const void *data, int32_t length, void *ctx)
{
    (void)message_count;
    ExportFilterState *state = ctx;
    if (state->mode == 5) return 1;
    if (state->mode <= 2)
        return changing_export_filter(index, data, length, ctx);
    int visit = ++state->visits[index];
    int counting_visits = state->chunked ? 4 : 3;
    if (state->mode == 3) return visit <= counting_visits;
    return message_index == 0 || visit > counting_visits;
}

static void exercise_export_counts(int batch_source, int chunked, int mode)
{
    tearDown();
    const char *directory = "/tmp/mmapq-transfer-fault";
    TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
    MmapqIndexedQueue *normal = NULL;
    MmapqIndexedBatchQueue *batch = NULL;
    MmapqIndexedReader *normal_reader = NULL;
    MmapqIndexedBatchReader *batch_reader = NULL;
    if (batch_source) {
        MmapqIndexedBatchAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 1, NULL, NULL, &batch));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(batch, 0, &appender));
        for (int index = 0; index < 3; index++) {
            MmapqMutableBatch *pending = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_appender_start_batch_checked(appender, &pending));
            TEST_ASSERT_EQUAL_INT(0, mmapq_mutable_batch_append(pending, "data", 4));
            if (mode >= 3) TEST_ASSERT_EQUAL_INT(1, mmapq_mutable_batch_append(pending, "more", 4));
            TEST_ASSERT_EQUAL_INT64(index, mmapq_mutable_batch_complete(pending));
        }
        mmapq_indexed_batch_appender_close(appender);
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(batch, &batch_reader));
    } else {
        MmapqIndexedAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, 0, 1000000000, 1, NULL, NULL, &normal));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(normal, 0, &appender));
        for (int index = 0; index < 3; index++)
            TEST_ASSERT_EQUAL_INT64(index, mmapq_indexed_appender_append(appender, "data", 4));
        mmapq_indexed_appender_close(appender);
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(normal, &normal_reader));
    }
    ExportFilterState state = { .mode = mode, .chunked = chunked };
    FILE *stream = tmpfile();
    TEST_ASSERT_NOT_NULL(stream);
    if (mode == 0) {
        fault = SOURCE_GONE;
        calls = consumed = 0;
        target = chunked ? 5 : 2;
    }
    int64_t result;
    if (batch_source && chunked)
        result = mmapq_batch_queue_transfer_export_chunked(batch_reader, "/tmp/mmapq-transfer-fault/chunk", 0, 2, 1, 0, mode ? changing_batch_export_filter : NULL, NULL, &state);
    else if (batch_source)
        result = mmapq_batch_queue_transfer_export(batch_reader, stream, 0, 2, 0, mode ? changing_batch_export_filter : NULL, NULL, &state);
    else if (chunked)
        result = mmapq_queue_transfer_export_chunked(normal_reader, "/tmp/mmapq-transfer-fault/chunk", 0, 2, 1, 0, mode ? changing_export_filter : NULL, NULL, &state);
    else
        result = mmapq_queue_transfer_export(normal_reader, stream, 0, 2, 0, mode ? changing_export_filter : NULL, NULL, &state);
    if (mode == 5) {
        FILE *input = stream;
        if (chunked) {
            input = fopen("/tmp/mmapq-transfer-fault/chunk_0-2", "rb");
            TEST_ASSERT_NOT_NULL(input);
        } else {
            rewind(input);
        }
        int64_t imported = batch_source
            ? mmapq_batch_queue_transfer_import(input, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO)
            : mmapq_queue_transfer_import(input, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
        TEST_ASSERT_EQUAL_INT64(3, imported);
        if (chunked) TEST_ASSERT_EQUAL_INT(0, fclose(input));
        if (batch_source) {
            MmapqIndexedBatchQueue *destination = NULL;
            MmapqIndexedBatchReader *reader = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "destination", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &destination));
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(destination, &reader));
            for (int index = 0; index < 3; index++) {
                MmapqBatchReadingContext context;
                mmapq_indexed_batch_reader_read(reader, index, &context);
                TEST_ASSERT_EQUAL_INT(MMAPQ_BATCH_ENTRY_AVAILABLE, context.status);
                TEST_ASSERT_EQUAL_INT(2, context.message_count);
                const void *data;
                int32_t length;
                TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(context.iterator, &data, &length));
                TEST_ASSERT_EQUAL_INT(4, length);
                TEST_ASSERT_EQUAL_MEMORY("data", data, 4);
                TEST_ASSERT_EQUAL_INT(1, mmapq_batch_iterator_next(context.iterator, &data, &length));
                TEST_ASSERT_EQUAL_INT(4, length);
                TEST_ASSERT_EQUAL_MEMORY("more", data, 4);
                TEST_ASSERT_EQUAL_INT(0, mmapq_batch_iterator_next(context.iterator, &data, &length));
            }
            mmapq_indexed_batch_reader_close(reader);
            mmapq_indexed_batch_queue_close(destination);
        } else {
            MmapqIndexedQueue *destination = NULL;
            MmapqIndexedReader *reader = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "destination", 32768, 16, 1, 32768 * 8, 0, 1000000000, 0, NULL, NULL, &destination));
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(destination, &reader));
            for (int index = 0; index < 3; index++) {
                MmapqReadingContext context;
                mmapq_indexed_reader_read(reader, index, &context);
                TEST_ASSERT_EQUAL_INT(MMAPQ_ENTRY_AVAILABLE, context.status);
                TEST_ASSERT_EQUAL_INT(4, context.length);
                TEST_ASSERT_EQUAL_MEMORY("data", context.data, 4);
            }
            mmapq_indexed_reader_close(reader);
            mmapq_indexed_queue_close(destination);
        }
    }
    TEST_ASSERT_EQUAL_INT(0, fclose(stream));
    mmapq_indexed_reader_close(normal_reader);
    mmapq_indexed_batch_reader_close(batch_reader);
    mmapq_indexed_queue_close(normal);
    mmapq_indexed_batch_queue_close(batch);
    fault = NO_FAULT;
    if (mode == 0) TEST_ASSERT_EQUAL_INT(1, consumed);
    TEST_ASSERT_EQUAL_INT64(mode == 5 ? 3 : MMAPQ_ECORRUPT, result);
}

static void test_export_accepts_stable_filters(void)
{
    for (int batch = 0; batch < 2; batch++)
        for (int chunked = 0; chunked < 2; chunked++)
            exercise_export_counts(batch, chunked, 5);
}

static void test_export_rejects_missing_entries(void)
{
    for (int batch = 0; batch < 2; batch++)
        for (int chunked = 0; chunked < 2; chunked++)
            exercise_export_counts(batch, chunked, 0);
}

static void test_export_rejects_shrinking_filters(void)
{
    for (int batch = 0; batch < 2; batch++)
        for (int chunked = 0; chunked < 2; chunked++)
            exercise_export_counts(batch, chunked, 1);
}

static void test_export_rejects_growing_filters(void)
{
    for (int batch = 0; batch < 2; batch++)
        for (int chunked = 0; chunked < 2; chunked++)
            exercise_export_counts(batch, chunked, 2);
}

static void test_batch_export_rejects_message_count_shrink(void)
{
    for (int chunked = 0; chunked < 2; chunked++)
        exercise_export_counts(1, chunked, 3);
}

static void test_batch_export_rejects_message_count_growth(void)
{
    for (int chunked = 0; chunked < 2; chunked++)
        exercise_export_counts(1, chunked, 4);
}

static void exercise_empty_export(int batch_source, int populated, int after, int preserve)
{
    tearDown();
    const char *directory = "/tmp/mmapq-transfer-fault";
    TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
    int64_t initial_index = populated ? 10 : 0;
    MmapqIndexedQueue *normal = NULL;
    MmapqIndexedBatchQueue *batch = NULL;
    MmapqIndexedReader *normal_reader = NULL;
    MmapqIndexedBatchReader *batch_reader = NULL;
    if (batch_source) {
        MmapqIndexedBatchAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, initial_index, 1000000000, 1, NULL, NULL, &batch));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(batch, 0, &appender));
        for (int index = 0; index < (populated ? 3 : 0); index++)
            TEST_ASSERT_EQUAL_INT64(initial_index + index, mmapq_indexed_batch_appender_append_and_commit(appender, "data", 4));
        mmapq_indexed_batch_appender_close(appender);
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(batch, &batch_reader));
    } else {
        MmapqIndexedAppender *appender = NULL;
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 32768, 16, 1, 32768 * 8, initial_index, 1000000000, 1, NULL, NULL, &normal));
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(normal, 0, &appender));
        for (int index = 0; index < (populated ? 3 : 0); index++)
            TEST_ASSERT_EQUAL_INT64(initial_index + index, mmapq_indexed_appender_append(appender, "data", 4));
        mmapq_indexed_appender_close(appender);
        TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(normal, &normal_reader));
    }
    FILE *stream = tmpfile();
    TEST_ASSERT_NOT_NULL(stream);
    int64_t start = after ? 20 : 0;
    int64_t end = after ? 30 : 5;
    int64_t exported = batch_source
        ? mmapq_batch_queue_transfer_export(batch_reader, stream, start, end, preserve, NULL, NULL, NULL)
        : mmapq_queue_transfer_export(normal_reader, stream, start, end, preserve, NULL, NULL, NULL);
    long bytes = ftell(stream);
    rewind(stream);
    uint8_t encoded[EXPORT_META_SIZE] = {0};
    size_t header_bytes = fread(encoded, 1, sizeof(encoded), stream);
    ExportMeta meta;
    export_meta_decode(encoded, &meta);
    rewind(stream);
    int64_t imported = batch_source
        ? mmapq_batch_queue_transfer_import(stream, directory, "destination", preserve, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO)
        : mmapq_queue_transfer_import(stream, directory, "destination", preserve, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
    TEST_ASSERT_EQUAL_INT(0, fclose(stream));
    mmapq_indexed_reader_close(normal_reader);
    mmapq_indexed_batch_reader_close(batch_reader);
    mmapq_indexed_queue_close(normal);
    mmapq_indexed_batch_queue_close(batch);
    TEST_ASSERT_EQUAL_INT64(0, exported);
    TEST_ASSERT_EQUAL_INT64(0, imported);
    TEST_ASSERT_EQUAL_INT64(EXPORT_META_SIZE, bytes);
    TEST_ASSERT_EQUAL_UINT64(EXPORT_META_SIZE, header_bytes);
    TEST_ASSERT_EQUAL_INT64(0, meta.total_entries);
    TEST_ASSERT_EQUAL_INT(EXPORT_STREAM_VERSION, meta.schema_version);  /* CAND-57: version 2 */
    TEST_ASSERT_EQUAL_INT(32768, meta.region_size);
    TEST_ASSERT_EQUAL_INT64(32768 * 8, meta.max_file_size);
    TEST_ASSERT_EQUAL_INT(1, meta.archivable);
    TEST_ASSERT_EQUAL_INT(batch_source ? 2 : 0, meta.queue_type);
    TEST_ASSERT_EQUAL_INT64(preserve ? (start > initial_index ? start : initial_index) : 0, meta.start_index);
}

static void test_normal_empty_range_roundtrip(void)
{
    for (int after = 0; after < 2; after++)
        for (int preserve = 0; preserve < 2; preserve++)
            exercise_empty_export(0, 1, after, preserve);
}

static void test_batch_empty_range_roundtrip(void)
{
    for (int after = 0; after < 2; after++)
        for (int preserve = 0; preserve < 2; preserve++)
            exercise_empty_export(1, 1, after, preserve);
}

static void test_initialized_empty_queue_roundtrip(void)
{
    for (int batch = 0; batch < 2; batch++)
        for (int preserve = 0; preserve < 2; preserve++)
            exercise_empty_export(batch, 0, 0, preserve);
}

static void test_export_mismatch_in_second_chunk(void)
{
    static const uint8_t payload[600 * 1024] = {42};
    const char *directory = "/tmp/mmapq-transfer-fault";
    for (int batch_source = 0; batch_source < 2; batch_source++) {
        tearDown();
        TEST_ASSERT_EQUAL_INT(0, mkdir(directory, 0775));
        MmapqIndexedQueue *normal = NULL;
        MmapqIndexedBatchQueue *batch = NULL;
        MmapqIndexedReader *normal_reader = NULL;
        MmapqIndexedBatchReader *batch_reader = NULL;
        if (batch_source) {
            MmapqIndexedBatchAppender *appender = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create(directory, "source", 1024 * 1024, 16, 1, 64 * 1024 * 1024, 0, 1000000000, 1, NULL, NULL, &batch));
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_appender_with_id(batch, 0, &appender));
            for (int index = 0; index < 3; index++)
                TEST_ASSERT_EQUAL_INT64(index, mmapq_indexed_batch_appender_append_and_commit(appender, payload, sizeof(payload)));
            mmapq_indexed_batch_appender_close(appender);
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_batch_queue_create_reader(batch, &batch_reader));
        } else {
            MmapqIndexedAppender *appender = NULL;
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "source", 1024 * 1024, 16, 1, 64 * 1024 * 1024, 0, 1000000000, 1, NULL, NULL, &normal));
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(normal, 0, &appender));
            for (int index = 0; index < 3; index++)
                TEST_ASSERT_EQUAL_INT64(index, mmapq_indexed_appender_append(appender, payload, sizeof(payload)));
            mmapq_indexed_appender_close(appender);
            TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_reader(normal, &normal_reader));
        }
        fault = SOURCE_GONE;
        calls = consumed = 0;
        target = 5;
        int64_t exported = batch_source
            ? mmapq_batch_queue_transfer_export_chunked(batch_reader, "/tmp/mmapq-transfer-fault/chunk", 0, 2, 1, 0, NULL, NULL, NULL)
            : mmapq_queue_transfer_export_chunked(normal_reader, "/tmp/mmapq-transfer-fault/chunk", 0, 2, 1, 0, NULL, NULL, NULL);
        fault = NO_FAULT;
        mmapq_indexed_reader_close(normal_reader);
        mmapq_indexed_batch_reader_close(batch_reader);
        mmapq_indexed_queue_close(normal);
        mmapq_indexed_batch_queue_close(batch);
        TEST_ASSERT_EQUAL_INT64(MMAPQ_ECORRUPT, exported);
        TEST_ASSERT_EQUAL_INT(1, consumed);
        TEST_ASSERT_EQUAL_INT(5, calls);
        FILE *first = fopen("/tmp/mmapq-transfer-fault/chunk_0-0", "rb");
        TEST_ASSERT_NOT_NULL(first);
        int64_t imported = batch_source
            ? mmapq_batch_queue_transfer_import(first, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO)
            : mmapq_queue_transfer_import(first, directory, "destination", 0, NULL, NULL, 0, NULL, NULL, MMAPQ_CONFLICT_AUTO);
        TEST_ASSERT_EQUAL_INT(0, fclose(first));
        TEST_ASSERT_EQUAL_INT64(1, imported);
        struct stat second;
        TEST_ASSERT_EQUAL_INT(0, stat("/tmp/mmapq-transfer-fault/chunk_1-1", &second));
        TEST_ASSERT_EQUAL_INT64(EXPORT_META_SIZE, second.st_size);
        TEST_ASSERT_EQUAL_INT(-1, access("/tmp/mmapq-transfer-fault/chunk_2-2", F_OK));
    }
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_normal_empty_range_roundtrip);
    RUN_TEST(test_batch_empty_range_roundtrip);
    RUN_TEST(test_initialized_empty_queue_roundtrip);
    RUN_TEST(test_export_mismatch_in_second_chunk);
    RUN_TEST(test_export_accepts_stable_filters);
    RUN_TEST(test_export_rejects_missing_entries);
    RUN_TEST(test_export_rejects_shrinking_filters);
    RUN_TEST(test_export_rejects_growing_filters);
    RUN_TEST(test_batch_export_rejects_message_count_shrink);
    RUN_TEST(test_batch_export_rejects_message_count_growth);
    RUN_TEST(test_copy_failure_matrix);
    RUN_TEST(test_import_failure_matrix);
    RUN_TEST(test_copy_dest_failure_after_start_leaves_discarded_record);
    RUN_TEST(test_import_dest_failure_after_start_leaves_discarded_record);
    RUN_TEST(test_copy_normal_to_batch_complete_failure_leaves_discarded_record);
    RUN_TEST(test_import_flat_stream_complete_failure_leaves_discarded_record);
    RUN_TEST(test_chunk_export_does_not_serialize_context_padding);
    return UNITY_END();
}
