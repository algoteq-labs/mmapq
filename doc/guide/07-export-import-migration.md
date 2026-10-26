# 7. Export, import and migration

Export flattens queue data into a portable stream format for backup,
migration, or chunked replay. Each stream begins with a 64-byte metadata
header (schema version, region size, max file size, start index, entry
count, archivable flag) followed by framed entries. The archivable flag is
preserved: a created destination queue takes the source's flag; an existing
destination keeps its on-disk flag and the import proceeds.

**Chunked export** splits a large export into multiple files of
approximately `N` MB each. Each chunk is a complete, self-contained stream
with its own header and embedded index range in the file name
(`base_path_start-end`). Chunks can be uploaded independently to object
storage and downloaded on demand during replay.

**Import** reads a stream and reconstructs the queue. All four cross-over
combinations are supported:

| Import function | Stream format | Destination | Behaviour |
|:---|---|---|---|
| `mmapq_queue_transfer_import` | flat (single-message) | normal queue | Direct append |
| `mmapq_queue_transfer_import_batch_stream` | batched | normal queue | Flatten batches |
| `mmapq_batch_queue_transfer_import` | batched | batch queue | Direct batch commit |
| `mmapq_batch_queue_transfer_import_flat_stream` | flat | batch queue | 1-msg batches |

**Start index mode** controls how the stream's `start_index` metadata is
interpreted:

- **REBASE** (default): the start index resets to 0.
- **PRESERVE**: the start index matches the source range — for exact
  replica restoration and replay systems that track position by index.

**Filters**: optional include/exclude callbacks allow selective export,
import, or copy by content or position.

Typical production usage:

1. Every hour, export the queue with chunked export (`chunk_size_mb=64`).
2. Upload each chunk file to object storage.
3. On restart: download the relevant chunks, import with PRESERVE mode,
   and replay to the point of interest.

## Migration: 0.1.0 → 0.2.0

0.2.0 writes `schema_version=2` and refuses 0.1.0 files
(`schema_version=1`) at open with `MMAPQ_ESCHEMA` — a clean refusal, never
a silent misread. To move 0.1.0 data forward: export with a 0.1.0 library
and import into a fresh 0.2.0 queue; the export stream format is unchanged.

The single source for all 0.1.0 → 0.2.0 changes is the
[release notes](../RELEASE-NOTES-0.2.0.md). This chapter links to them
rather than restating them.

## Durability note

A successful export and output close mean buffered writes completed without
a reported error — not that storage survived a power failure. The export
format is where long-term data lives; queues are transport plus recent
history.
