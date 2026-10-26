# API overview (C)

Naming: public symbols are `mmapq_*`, types `Mmapq*`, constants `MMAPQ_*`.
The `i` in `mmapqi_*` means internal — those functions belong to
`src/internal/`, not the installed public API. Every fallible call returns
0/non-negative on success or a negative [`MMAPQ_*` code](error-codes.md).

## Start here — core functions

| Function | What it does |
| :--- | :--- |
| `mmapq_runtime_region_mapper(&rt)` | Get the singleton background mapping thread (call once at startup) |
| `mmapq_indexed_queue_create_default(dir, name, wrt, rrt, &q)` | Open or create a single-message queue |
| `mmapq_indexed_queue_create_appender(q, &a)` | Create a writer |
| `mmapq_indexed_queue_create_poller(q, &p)` | Create a sequential reader |
| `mmapq_indexed_appender_append(a, data, len)` | Write and commit a message |
| `mmapq_indexed_poller_poll(p, handler, ctx)` | Process one message via callback |
| `mmapq_indexed_batch_queue_create_default(dir, name, wrt, rrt, &q)` | Open or create a batch queue |
| `mmapq_indexed_batch_appender_start_batch_checked()` | Begin a new batch (0 or a negative `MMAPQ_*` code) |
| `mmapq_mutable_batch_append()` | Add a message to an in-progress batch |
| `mmapq_indexed_batch_appender_commit_batch()` | Commit the batch atomically |

`mmapq_indexed_queue_create` and `mmapq_indexed_batch_queue_create` accept an
`archivable` parameter (0 = non-archivable, performant; 1 = archivable,
atomic start_index reads). Applies at creation only; existing queues keep
their on-disk value.

## Single-message queue

| Function | Description |
|---|---|
| `mmapq_indexed_queue_create()` | Open or create with explicit parameters |
| `mmapq_indexed_queue_create_reader()` | Create a random-access reader |
| `mmapq_indexed_queue_create_archiver()` | Create an archiver |
| `mmapq_indexed_queue_close()` | Close the queue |
| `mmapq_indexed_appender_append_uncommitted()` | Write without committing |
| `mmapq_indexed_appender_commit()` | Commit a previously written payload |
| `mmapq_indexed_poller_move_to_end()` | Jump past the last entry |
| `mmapq_indexed_poller_move_to_index()` | Jump to a specific index |
| `mmapq_indexed_reader_read()` | Read entry at a specific index |
| `mmapq_indexed_reader_read_last()` | Read the last committed entry |
| `mmapq_indexed_archiver_archive_up_to_index()` | Archive entries up to an index |
| `mmapq_indexed_archiver_archivable_files()` | List files no live entry can need |
| `mmapq_indexed_archiver_print_summary()` | Print queue file layout summary |

The poll callback returns `MMAPQ_NEXT_ADVANCE`, `MMAPQ_NEXT_STOP`, or
`MMAPQ_NEXT_RETREAT`.

## Batch queue

| Function | Description |
|---|---|
| `mmapq_indexed_batch_queue_create()` | Open or create a batch queue with explicit parameters |
| `mmapq_indexed_batch_queue_create_poller()` | Create a batch sequential consumer |
| `mmapq_indexed_batch_queue_create_reader()` | Create a batch random-access reader |
| `mmapq_indexed_batch_queue_create_archiver()` | Create a batch archiver |
| `mmapq_mutable_batch_complete()` | Finalise the batch |
| `mmapq_mutable_batch_append_reserved()` | Reserve space in a batch and get raw write address |
| `mmapq_mutable_batch_get_checked()` | Read a message from an in-progress batch by index (0 or a negative `MMAPQ_*` code) |
| `mmapq_mutable_batch_iterator()` | Iterate over messages in an in-progress batch |
| `mmapq_indexed_batch_poller_poll()` | Process one message via callback |
| `mmapq_indexed_batch_reader_read()` | Read a batch at a specific index |
| `mmapq_indexed_batch_reader_read_last()` | Read the last committed batch |
| `mmapq_indexed_batch_archiver_archive_up_to_index()` | Archive batches up to an index |
| `mmapq_indexed_batch_archiver_archivable_files()` | List files no live entry can need |

The batch poll callback returns the same `MmapqNextMove` enum as the
single-message poller.

## Export / import (QueueTransfer)

| Function | Description |
|---|---|
| `mmapq_queue_transfer_export()` | Export queue entries to a flat stream |
| `mmapq_queue_transfer_export_chunked()` | Export split into ~N MB chunk files |
| `mmapq_queue_transfer_import()` | Import flat stream into a normal queue |
| `mmapq_queue_transfer_import_batch_stream()` | Import batch stream into a normal queue (flatten) |
| `mmapq_queue_transfer_copy()` | Copy entries directly between queues |
| `mmapq_batch_queue_transfer_export()` | Export batch queue entries to a batched stream |
| `mmapq_batch_queue_transfer_export_chunked()` | Batch export split into ~N MB chunk files |
| `mmapq_batch_queue_transfer_import()` | Import batched stream into a batch queue |
| `mmapq_batch_queue_transfer_import_flat_stream()` | Import flat stream into a batch queue (1-msg batches) |
| `mmapq_batch_queue_transfer_copy()` | Copy batches directly between queues |

All export/copy functions support optional include/exclude filter callbacks
and `REBASE` / `PRESERVE` start index mode.

## MmapqRuntime

| Function | Description |
|---|---|
| `mmapq_runtime_create()` | Create a named runtime with a custom idle strategy |
| `mmapq_runtime_close()` | Stop the runtime thread |
| `mmapq_runtime_set_affinity()` | Pin the runtime thread to a CPU core |

The authoritative per-function documentation is the installed headers under
`include/`.
