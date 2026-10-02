#ifndef MMAPQ_TEST_TRANSFER_FAULTS_H
#define MMAPQ_TEST_TRANSFER_FAULTS_H
#include "indexed_appender.h"
#include "indexed_batch_appender.h"
#include "indexed_queue.h"
#include "indexed_batch_queue.h"
#include "indexed_reader.h"
MmapqReadingContext *fault_read(MmapqIndexedReader *, int64_t, MmapqReadingContext *);
int fault_reader_error(const MmapqIndexedReader *);
MmapqBatchReadingContext *fault_batch_read(MmapqIndexedBatchReader *, int64_t, MmapqBatchReadingContext *);
int fault_batch_reader_error(const MmapqIndexedBatchReader *);
int64_t fault_append(MmapqIndexedAppender *, const void *, int32_t);
int fault_batch_append(MmapqMutableBatch *, const void *, int32_t);
int fault_batch_start(MmapqIndexedBatchAppender *, MmapqMutableBatch **);
int64_t fault_batch_complete(MmapqMutableBatch *);
int64_t fault_batch_commit(MmapqIndexedBatchAppender *, int64_t);
int fault_create(MmapqIndexedQueue *, uint8_t, MmapqIndexedAppender **);
int fault_batch_create(MmapqIndexedBatchQueue *, uint8_t, MmapqIndexedBatchAppender **);
#endif
