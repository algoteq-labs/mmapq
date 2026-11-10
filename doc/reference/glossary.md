# Glossary

One definition per term; every other page links here.

| Term | Definition |
|---|---|
| **lane** | The per-appender payload stream (`payload_<id>_<N>` files). A lane has exactly one writer at a time. |
| **header / header lane** | The shared sequence of 8-byte entries (`header_<N>` files), one per committed index, encoding `(appenderId, payloadPosition)`. |
| **payload** | The message bytes in a lane, framed `[int32 length][bytes]` (batch entries add an `[int32 message_count]` prefix). |
| **commit** | Publishing an entry: the single atomic CAS that stores the header after the payload is fully written. A message is visible to consumers the instant the CAS succeeds. |
| **archive line** | The per-appender `archive_offset`: how far the uncommitted iterator's snapshot reaches. Raised only by a forward archive, never lowered. |
| **start index** | The queue-global `start_index` in the meta file: the lowest index a consumer can read. Advanced by archiving. |
| **start offset** | The per-appender `start_offset`: the readability floor — the lowest payload position any live header may still need. The deletion boundary. |
| **intent** | The per-appender commit-intent slot in the appender meta: a slow-path commit records its target before its pre-check, so a stalled or killed commit is visible to the archiver's listing. |
| **live floor** | The lowest payload position of any live header on a lane, found by the archiver's live-header pass. |
| **join** | Opening an appender on a lane with history: the recovery walk to end-of-stream that rebuilds the lane's committed state and surfaces its uncommitted tail. |
| **appender id** | The 8-bit writer identity (0–255) allocated from the `appender_id_pool` CAS bitset; packed with a 56-bit payload position into the header word. |
| **region** | One mapped file segment of a lane or header file; size is fixed at queue creation (`region_size`). |
| **sector** | The header-file stride unit (`sectorSize = 64`): consecutive indices land 512 bytes apart across 64 sectors before wrapping — the vertical walk that keeps concurrent commits off each other's cache lines. |
| **resequenced commit** | Writing payloads with `append_uncommitted` and committing them with `commit` in a different order than written (UDP resequencing, pipelined writes). |
| **uncommitted iterator / restart walk** | The per-lane iterator over appended-but-uncommitted payloads, used at rejoin to commit or skip each one. |
| **snapshot** | Application-level: a saved derived state plus the log index it covers. Recovery from a snapshot means loading it and replaying only the tail. Not a library object. |
| **reclamation** | Deleting files that no live entry can need: `archivable_files()` lists them, `reclaim_files()` deletes them safely. |
| **runtime** | The `MmapqRuntime` background thread that maps/unmaps regions ahead of the hot loop (async mapping); queues may instead map synchronously on the calling thread (sync), or mix the two per direction. |
| **torn tail** | After a crash, the record that was being written — the lane's highest-offset uncommitted record at that crash — may be partially written; every record written before it is byte-intact. A record that is not committed is never removed, so one torn at an earlier crash reappears in later walks at an offset that is no longer the highest. See the recovery recipe in `../guide/05-crash-and-restart.md`. |
| **schema 2** | The `schema_version` written by 0.2.0. Version-1 files are refused at open with `MMAPQ_ESCHEMA`; see `wire-format.md` for the protocol contract. |
| **wire format** | The on-disk byte layout shared by the C, Rust and Python implementations, specified in `wire-format.md`. |
