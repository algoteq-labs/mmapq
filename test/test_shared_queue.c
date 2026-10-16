#include "unity.h"
#include "indexed_queue.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char directory[] = "/tmp/mmapq-shared-XXXXXX";

void setUp(void)
{
    memcpy(directory, "/tmp/mmapq-shared-XXXXXX", sizeof(directory));
    TEST_ASSERT_NOT_NULL(mkdtemp(directory));
}

void tearDown(void)
{
    char command[128];
    snprintf(command, sizeof(command), "rm -rf %s", directory);
    int result = system(command);
    (void)result;
}

typedef struct {
    MmapqIndexedQueue *queue;
    pthread_barrier_t *barrier;
    uint8_t id;
    int ok;
    int64_t index;
} Worker;

static void *create_and_use_participants(void *context)
{
    Worker *worker = context;
    MmapqIndexedAppender *appender = NULL;
    MmapqIndexedReader *reader = NULL;
    MmapqIndexedPoller *poller = NULL;
    MmapqIndexedArchiver *archiver = NULL;
    pthread_barrier_wait(worker->barrier);
    if (mmapq_indexed_queue_create_reader(worker->queue, &reader) != 0) goto done;
    if (mmapq_indexed_queue_create_poller(worker->queue, &poller) != 0) goto done;
    if (mmapq_indexed_queue_create_archiver(worker->queue, &archiver) != 0) goto done;
    if (mmapq_indexed_queue_create_appender_with_id(worker->queue, worker->id, &appender) != 0) goto done;
    worker->index = mmapq_indexed_appender_append(appender, &worker->id, 1);
    if (worker->index < 0) goto done;
    MmapqReadingContext reading = {0};
    mmapq_indexed_reader_read(reader, worker->index, &reading);
    worker->ok = reading.status == MMAPQ_ENTRY_AVAILABLE && reading.length == 1
        && *(const uint8_t *)reading.data == worker->id
        && mmapq_indexed_reader_start_index(reader) == 17
        && mmapq_indexed_poller_exists(poller, worker->index);
done:
    mmapq_indexed_appender_close(appender);
    mmapq_indexed_reader_close(reader);
    mmapq_indexed_poller_close(poller);
    mmapq_indexed_archiver_close(archiver);
    return NULL;
}

static void test_shared_factory_creates_independent_participants(void)
{
    MmapqIndexedQueue *queue = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create(directory, "queue",
        32768, 16, 1, 32768 * 8, 17, 5000000000ULL, 1, NULL, NULL, &queue));
    MmapqIndexedAppender *seed = NULL;
    TEST_ASSERT_EQUAL_INT(0, mmapq_indexed_queue_create_appender_with_id(queue, 0, &seed));
    TEST_ASSERT_EQUAL_INT64(17, mmapq_indexed_appender_append(seed, "seed", 4));
    mmapq_indexed_appender_close(seed);
    pthread_barrier_t barrier;
    TEST_ASSERT_EQUAL_INT(0, pthread_barrier_init(&barrier, NULL, 4));
    pthread_t threads[4];
    Worker workers[4] = {0};
    for (int i = 0; i < 4; i++) {
        workers[i].queue = queue;
        workers[i].barrier = &barrier;
        workers[i].id = (uint8_t)(i + 1);
        TEST_ASSERT_EQUAL_INT(0, pthread_create(&threads[i], NULL, create_and_use_participants, &workers[i]));
    }
    unsigned seen = 0;
    for (int i = 0; i < 4; i++) {
        TEST_ASSERT_EQUAL_INT(0, pthread_join(threads[i], NULL));
        TEST_ASSERT_EQUAL_INT(1, workers[i].ok);
        TEST_ASSERT_TRUE(workers[i].index >= 18 && workers[i].index <= 21);
        seen |= 1U << (workers[i].index - 18);
    }
    TEST_ASSERT_EQUAL_UINT(15, seen);
    TEST_ASSERT_EQUAL_INT(0, pthread_barrier_destroy(&barrier));
    mmapq_indexed_queue_close(queue);
}

int main(void)
{
    UNITY_BEGIN();
    RUN_TEST(test_shared_factory_creates_independent_participants);
    return UNITY_END();
}
