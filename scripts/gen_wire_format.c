/*
 * gen_wire_format.c — emit the wire-format constants as resolved NAME=value
 * lines, for embedding in doc/reference/wire-format.md.
 *
 * This probe is not part of the library. It exists so that the constants in
 * the installed specification are DERIVED from the authoritative headers
 * (src/internal/header_codec.h, src/internal/queue_meta.h), not hand-copied.
 * A consumer in another language needs the resolved value (e.g. 4), not the
 * source expression (e.g. (int32_t)(PAYLOAD_LENGTH_OFFSET + PAYLOAD_LENGTH_SIZE)).
 *
 * Build: cc -std=gnu11 -D_GNU_SOURCE -I <include> -I <src/internal> \
 *            gen_wire_format.c -o gen_wire_format
 *
 * No library linkage is required — the probe only reads #define constants.
 */
#include <stdio.h>
#include <stdint.h>

#include "header_codec.h"
#include "queue_meta.h"

int main(void)
{
    printf("HEADER_SIZE=%d\n", (int)HEADER_SIZE);
    printf("HEADER_NULL=%llu\n", (unsigned long long)HEADER_NULL);
    printf("HEADER_LENGTH_BITS=%d\n", (int)HEADER_LENGTH_BITS);
    printf("HEADER_SECTOR_SIZE_BITS=%d\n", (int)HEADER_SECTOR_SIZE_BITS);
    printf("HEADER_BLOCK_SIZE_BITS=%d\n", (int)HEADER_BLOCK_SIZE_BITS);
    printf("HEADER_SECTOR_MASK=%d\n", (int)HEADER_SECTOR_MASK);
    printf("HEADER_BLOCK_MASK=%d\n", (int)HEADER_BLOCK_MASK);
    printf("HEADER_PAYLOAD_MASK=%llu\n", (unsigned long long)HEADER_PAYLOAD_MASK);
    printf("HEADER_APPENDER_BITS=%d\n", (int)HEADER_APPENDER_BITS);

    printf("PAYLOAD_LENGTH_OFFSET=%d\n", (int)PAYLOAD_LENGTH_OFFSET);
    printf("PAYLOAD_LENGTH_SIZE=%d\n", (int)PAYLOAD_LENGTH_SIZE);
    printf("PAYLOAD_DATA_OFFSET=%d\n", (int)PAYLOAD_DATA_OFFSET);
    printf("INITIAL_PAYLOAD_POSITION=%lld\n", (long long)INITIAL_PAYLOAD_POSITION);
    printf("MAX_APPENDER_ID=%d\n", (int)MAX_APPENDER_ID);
    printf("MAX_APPENDERS=%d\n", (int)MAX_APPENDERS);

    printf("QUEUE_META_SCHEMA_VERSION=%d\n", (int)QUEUE_META_SCHEMA_VERSION);
    printf("QUEUE_META_INITIAL_START_INDEX=%lld\n", (long long)QUEUE_META_INITIAL_START_INDEX);
    printf("QUEUE_META_ENCODED_LENGTH=%d\n", (int)QUEUE_META_ENCODED_LENGTH);

    printf("QUEUE_TYPE_NORMAL=%d\n", (int)QUEUE_TYPE_NORMAL);
    printf("QUEUE_TYPE_FIXED8=%d\n", (int)QUEUE_TYPE_FIXED8);
    printf("QUEUE_TYPE_BATCH=%d\n", (int)QUEUE_TYPE_BATCH);

    printf("QUEUE_META_SCHEMA_VERSION_OFFSET=%d\n", (int)QUEUE_META_SCHEMA_VERSION_OFFSET);
    printf("QUEUE_META_REGION_SIZE_OFFSET=%d\n", (int)QUEUE_META_REGION_SIZE_OFFSET);
    printf("QUEUE_META_START_INDEX_OFFSET=%d\n", (int)QUEUE_META_START_INDEX_OFFSET);
    printf("QUEUE_META_MAX_FILE_SIZE_OFFSET=%d\n", (int)QUEUE_META_MAX_FILE_SIZE_OFFSET);
    printf("QUEUE_META_ARCHIVABLE_OFFSET=%d\n", (int)QUEUE_META_ARCHIVABLE_OFFSET);
    printf("QUEUE_META_QUEUE_TYPE_OFFSET=%d\n", (int)QUEUE_META_QUEUE_TYPE_OFFSET);

    return 0;
}
