#ifndef EXPORT_META_H
#define EXPORT_META_H

#include <stdint.h>
#include <string.h>

/**
 * ExportMeta — 64-byte metadata header for a flat (single-message) queue export stream.
 *
 * A flat queue export stream consists of:
 *   [ExportMeta header: 64 bytes]
 *   [entry: uint16 appender_id][int32 payload_length][uint8 payload...] ... (× total_entries)
 *
 * All multi-byte fields are little-endian — asserted at build by the
 * _Static_assert on __BYTE_ORDER__ in internal/header_codec.h.  The
 * normative specification is doc/reference/wire-format.md, "The export
 * stream".
 *
 * Stream version history (the schema_version field of this header):
 *   1  C 0.1.0/0.2.0-pre-CAND-57 (little-endian); Java up to the CAND-57 fix
 *      (BIG-endian framing — a v1 stream whose frames fail to parse may be a
 *      legacy Java export; re-export with a fixed Java).
 *   2  CAND-57 canonical little-endian, written by C 0.2.0 and fixed Java.
 * Readers accept 1 and 2 and refuse anything else with MMAPQ_ESCHEMA.
 *
 * FIELD LAYOUT (mirrors Java QueueExportMeta.ENCODED_LENGTH):
 *   Offset  Size  Field
 *   0       4     schema_version
 *   4       4     region_size
 *   8       8     start_index        (8-byte aligned for ARM volatile access)
 *   16      8     max_file_size
 *   24      8     total_entries
 *   32      1     archivable
 *   33      1     queue_type         (QUEUE_TYPE_NORMAL=0, QUEUE_TYPE_FIXED8=1, QUEUE_TYPE_BATCH=2)
 *   34      30    reserved/padding
 *   Total:  64 bytes (one cache line)
 */

#define EXPORT_META_SIZE                 64

/* The canonical stream version written today (CAND-57).  Readers accept
 * versions 1 and 2; both are little-endian on the C side. */
#define EXPORT_STREAM_VERSION            2
#define EXPORT_STREAM_VERSION_MIN_READ   1
#define EXPORT_STREAM_VERSION_MAX_READ   2

#define EXPORT_META_SCHEMA_VERSION_OFF   0
#define EXPORT_META_SCHEMA_VERSION_SIZE  4
#define EXPORT_META_REGION_SIZE_OFF      4
#define EXPORT_META_REGION_SIZE_SIZE     4
#define EXPORT_META_START_INDEX_OFF      8
#define EXPORT_META_START_INDEX_SIZE     8
#define EXPORT_META_MAX_FILE_SIZE_OFF    16
#define EXPORT_META_MAX_FILE_SIZE_SIZE   8
#define EXPORT_META_TOTAL_ENTRIES_OFF    24
#define EXPORT_META_TOTAL_ENTRIES_SIZE   8
#define EXPORT_META_ARCHIVABLE_OFF       32
#define EXPORT_META_QUEUE_TYPE_OFF       33
#define EXPORT_META_PAYLOAD_START        34  /* first byte after fixed fields */
#define EXPORT_META_PAYLOAD_SIZE         (EXPORT_META_SIZE - EXPORT_META_PAYLOAD_START)

typedef struct {
    int32_t schema_version;     
    int32_t region_size;        
    int64_t start_index;        
    int64_t max_file_size;      
    int64_t total_entries;      
    uint8_t archivable;         
    uint8_t queue_type;         
    uint8_t reserved[EXPORT_META_PAYLOAD_SIZE];
} ExportMeta;

/** Initialise an ExportMeta to all zeros. */
static inline void export_meta_init(ExportMeta *m)
{
    memset(m, 0, sizeof(ExportMeta));
}

/**
 * Encode @p m into the 64-byte buffer @p out.
 * m:   source metadata struct
 * out: caller-owned buffer of at least EXPORT_META_SIZE bytes
 */
static inline void export_meta_encode(const ExportMeta *m, uint8_t *out)
{
    memcpy(out + EXPORT_META_SCHEMA_VERSION_OFF, &m->schema_version, EXPORT_META_SCHEMA_VERSION_SIZE);
    memcpy(out + EXPORT_META_REGION_SIZE_OFF,    &m->region_size,    EXPORT_META_REGION_SIZE_SIZE);
    memcpy(out + EXPORT_META_START_INDEX_OFF,    &m->start_index,    EXPORT_META_START_INDEX_SIZE);
    memcpy(out + EXPORT_META_MAX_FILE_SIZE_OFF,  &m->max_file_size,  EXPORT_META_MAX_FILE_SIZE_SIZE);
    memcpy(out + EXPORT_META_TOTAL_ENTRIES_OFF,  &m->total_entries,  EXPORT_META_TOTAL_ENTRIES_SIZE);
    out[EXPORT_META_ARCHIVABLE_OFF] = m->archivable;
    out[EXPORT_META_QUEUE_TYPE_OFF] = m->queue_type;
    /* Reserved bytes are zeroed explicitly (not copied from m->reserved) so the
       output does not depend on callers having run export_meta_init; decoders
       ignore reserved, so streams written before this fix remain importable. */
    memset(out + EXPORT_META_PAYLOAD_START, 0, EXPORT_META_PAYLOAD_SIZE);
}

/**
 * Decode the 64-byte buffer @p in into @p m.
 * in: caller-owned buffer of at least EXPORT_META_SIZE bytes
 * m:  destination metadata struct
 */
static inline void export_meta_decode(const uint8_t *in, ExportMeta *m)
{
    memcpy(&m->schema_version, in + EXPORT_META_SCHEMA_VERSION_OFF, EXPORT_META_SCHEMA_VERSION_SIZE);
    memcpy(&m->region_size,    in + EXPORT_META_REGION_SIZE_OFF,    EXPORT_META_REGION_SIZE_SIZE);
    memcpy(&m->start_index,    in + EXPORT_META_START_INDEX_OFF,    EXPORT_META_START_INDEX_SIZE);
    memcpy(&m->max_file_size,  in + EXPORT_META_MAX_FILE_SIZE_OFF,  EXPORT_META_MAX_FILE_SIZE_SIZE);
    memcpy(&m->total_entries,  in + EXPORT_META_TOTAL_ENTRIES_OFF,  EXPORT_META_TOTAL_ENTRIES_SIZE);
    m->archivable = in[EXPORT_META_ARCHIVABLE_OFF];
    m->queue_type = in[EXPORT_META_QUEUE_TYPE_OFF];
    memset(m->reserved, 0, EXPORT_META_PAYLOAD_SIZE);
}

#endif /* EXPORT_META_H */
