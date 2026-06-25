#ifndef BATCH_EXPORT_META_H
#define BATCH_EXPORT_META_H

#include <stdint.h>
#include <string.h>

/**
 * BatchExportMeta — 64-byte metadata header for a batched (batch queue) export stream.
 *
 * A batch queue export stream consists of:
 *   [BatchExportMeta header: 64 bytes]
 *   [batch 0:
 *       int32  msg_count
 *       [int32 payload_length][uint8 payload...] ... (× msg_count)]
 *   [batch 1: ...]
 *   ... (× total_batches)
 *
 * All multi-byte fields are in native byte order.  Compatible with Java
 * on x86-64 and ARM64 without byte swapping.
 *
 * FIELD LAYOUT:
 *   Offset  Size  Field
 *   0       4     schema_version
 *   4       4     region_size
 *   8       8     start_index        (8-byte aligned for ARM volatile access)
 *   16      8     max_file_size
 *   24      8     total_batches
 *   32      1     archivable
 *   33      31    reserved/padding
 *   Total:  64 bytes
 */

#define BATCH_EXPORT_META_SIZE               64

#define BATCH_EXPORT_META_SCHEMA_VERSION_OFF 0
#define BATCH_EXPORT_META_SCHEMA_VERSION_SIZE 4
#define BATCH_EXPORT_META_REGION_SIZE_OFF     4
#define BATCH_EXPORT_META_REGION_SIZE_SIZE    4
#define BATCH_EXPORT_META_START_INDEX_OFF     8
#define BATCH_EXPORT_META_START_INDEX_SIZE    8
#define BATCH_EXPORT_META_MAX_FILE_SIZE_OFF   16
#define BATCH_EXPORT_META_MAX_FILE_SIZE_SIZE  8
#define BATCH_EXPORT_META_TOTAL_BATCHES_OFF   24
#define BATCH_EXPORT_META_TOTAL_BATCHES_SIZE  8
#define BATCH_EXPORT_META_ARCHIVABLE_OFF      32
#define BATCH_EXPORT_META_PAYLOAD_START       33
#define BATCH_EXPORT_META_PAYLOAD_SIZE        (BATCH_EXPORT_META_SIZE - BATCH_EXPORT_META_PAYLOAD_START)

typedef struct {
    int32_t schema_version;    
    int32_t region_size;       
    int64_t start_index;       
    int64_t max_file_size;     
    int64_t total_batches;     
    uint8_t archivable;        
    uint8_t reserved[BATCH_EXPORT_META_PAYLOAD_SIZE];
} BatchExportMeta;

/** Initialise a BatchExportMeta to all zeros. */
static inline void batch_export_meta_init(BatchExportMeta *m)
{
    memset(m, 0, sizeof(BatchExportMeta));
}

/**
 * Encode @p m into the 64-byte buffer @p out.
 * @param m    source metadata struct
 * @param out  caller-owned buffer of at least BATCH_EXPORT_META_SIZE bytes
 */
static inline void batch_export_meta_encode(const BatchExportMeta *m, uint8_t *out)
{
    memcpy(out + BATCH_EXPORT_META_SCHEMA_VERSION_OFF, &m->schema_version, BATCH_EXPORT_META_SCHEMA_VERSION_SIZE);
    memcpy(out + BATCH_EXPORT_META_REGION_SIZE_OFF,    &m->region_size,    BATCH_EXPORT_META_REGION_SIZE_SIZE);
    memcpy(out + BATCH_EXPORT_META_START_INDEX_OFF,    &m->start_index,    BATCH_EXPORT_META_START_INDEX_SIZE);
    memcpy(out + BATCH_EXPORT_META_MAX_FILE_SIZE_OFF,  &m->max_file_size,  BATCH_EXPORT_META_MAX_FILE_SIZE_SIZE);
    memcpy(out + BATCH_EXPORT_META_TOTAL_BATCHES_OFF,  &m->total_batches,  BATCH_EXPORT_META_TOTAL_BATCHES_SIZE);
    out[BATCH_EXPORT_META_ARCHIVABLE_OFF] = m->archivable;
}

/**
 * Decode the 64-byte buffer @p in into @p m.
 * @param in  caller-owned buffer of at least BATCH_EXPORT_META_SIZE bytes
 * @param m   destination metadata struct
 */
static inline void batch_export_meta_decode(const uint8_t *in, BatchExportMeta *m)
{
    memcpy(&m->schema_version, in + BATCH_EXPORT_META_SCHEMA_VERSION_OFF, BATCH_EXPORT_META_SCHEMA_VERSION_SIZE);
    memcpy(&m->region_size,    in + BATCH_EXPORT_META_REGION_SIZE_OFF,    BATCH_EXPORT_META_REGION_SIZE_SIZE);
    memcpy(&m->start_index,    in + BATCH_EXPORT_META_START_INDEX_OFF,    BATCH_EXPORT_META_START_INDEX_SIZE);
    memcpy(&m->max_file_size,  in + BATCH_EXPORT_META_MAX_FILE_SIZE_OFF,  BATCH_EXPORT_META_MAX_FILE_SIZE_SIZE);
    memcpy(&m->total_batches,  in + BATCH_EXPORT_META_TOTAL_BATCHES_OFF,  BATCH_EXPORT_META_TOTAL_BATCHES_SIZE);
    m->archivable = in[BATCH_EXPORT_META_ARCHIVABLE_OFF];
    memset(m->reserved, 0, BATCH_EXPORT_META_PAYLOAD_SIZE);
}

#endif /* BATCH_EXPORT_META_H */
