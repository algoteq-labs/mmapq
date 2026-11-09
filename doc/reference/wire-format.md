# mmapq wire format specification

This is the **normative** specification for the on-disk format written and
read by mmapq. It is installed with the library; a consumer in any language
can build a reader from this document alone.

The authoritative implementation lives in the source distribution at
`src/internal/header_codec.h` (header + payload framing) and
`src/internal/queue_meta.h` (meta file). The constants below are **generated**
from those headers by `scripts/gen_wire_format.sh` — they cannot drift from
the implementation.

**Versioned compatibility.** The wire format is frozen within a schema
version. 0.2.0 writes `schema_version=2` and refuses 0.1.0 files
(`schema_version=1`) at open with `MMAPQ_ESCHEMA`: 0.2.0 changed what
0.1.0 files mean and accept (the archive-line field, the geometry rule,
the unified tail rule). To move 0.1.0 data forward, export with a 0.1.0
library and import into a fresh 0.2.0 queue — the export stream format is
unchanged. Each release states the schema versions it reads; a file of any
other version is refused at open with `MMAPQ_ESCHEMA`. A version change is
always a clean refusal, never a silent misread. Do not change any offset,
size, framing rule, or alignment contract in this document; a change here
is a binary-incompatible break.

## Storage layout

A queue lives in its own directory, `<directory>/<queue_name>/`, containing:

- `meta` — a fixed 64-byte cache-line metadata block.
- `header_0`, `header_1`, … — header files holding one 8-byte entry per
  queue index, rolled when a file reaches `max_file_size` (like payloads).
- `payload_<A>_<N>` — appender payload files, one per appender, rolled when
  a file reaches `max_file_size`.

## Meta file (`meta`)

64 bytes, one cache line, little-endian:

| Offset | Size | Field |
|-------:|-----:|-------|
| 0      | 4    | `schema_version` (int32) |
| 4      | 4    | `region_size` (int32) |
| 8      | 8    | `start_index` (int64) |
| 16     | 8    | `max_file_size` (int64) |
| 24     | 1    | `archivable` (uint8) |
| 25     | 1    | `queue_type` (uint8) — 0 normal, 1 fixed-8 (Java only), 2 batch |
| 26     | 38   | padding to 64 bytes |

`schema_version` and `queue_type` are validated on every open; a mismatch
refuses to open with `MMAPQ_ESCHEMA`.

`start_index` is 8-byte aligned and atomic: written with release, read with
acquire, so an archiver's progress is observable across processes.

## Header files (`header_0`, `header_1`, …)

Each queue entry has an **8-byte header** (`HEADER_SIZE`):

- `bits[63:56]` — appenderId (unsigned, 8 bits).
- `bits[55:0]` — payloadPosition (56 bits, ~72 PB per appender lane).
- `NULL_HEADER = 0` — a zero header means "no entry" and is the CAS initial
  value. The header is written atomically (compare-and-swap) from `NULL_HEADER`
  to the encoded value; this is the single commit point per entry.

The 56-bit position field is why append/poll is safe: a reader sees either a
fully-written header (the commit) or the pre-entry zero, never a partial one.

## Non-linear header mapping (the vertical walk)

Sequential indices are **not** laid out linearly. To eliminate false sharing
under contention, consecutive indices stride across different cache lines
(`HEADER_SECTOR_SIZE_BITS = 6` → 64 sectors), then rotate back.

Indices are organised into blocks of `HEADER_BLOCK_SIZE_BITS = 12` → 4096
headers. The forward mapping `index -> byte position` is:

```
block          = index >> 12                 (index / 4096)
index_in_block = index & 4095                (index % 4096)
sector_index   = index_in_block & 63         (index % 64, fast stride)
word_index     = index_in_block >> 6         (index / 64, slow shift)
position       = ((block * 4096) + (sector_index * 64) + word_index) * 8
```

Because a block has 64 sectors and each header is 8 bytes, the vertical walk
strides **512 bytes** per consecutive index. The header lane's working set
over one full sweep is `64 × 512 = 32,768` bytes. The ring must therefore span
at least twice that (`ring_size × region_size ≥ 2 × 64 KiB`) to avoid thrashing;
creation rejects smaller geometries with `MMAPQ_EINVAL`.

## Payload framing

Payload files are framed per appender as:

```
[int32 length][payload bytes...]
```

- `length > 0` — a valid message; `length` payload bytes follow.
- `length < 0` — long padding; a region boundary marker. Skip forward by
  `abs(length) + 4` bytes (= the remaining region capacity); `abs(length)`
  counts the bytes after the 4-byte length field.
- `length == 0` — end of stream; nothing written at this position yet.

Two padding cases at a region end:

- **Short padding** — when 4 or fewer bytes remain in a region
  (`capacity ≤ 4`), skip to the next region without writing a length field.
- **Long padding** — write a negative length; skip `abs(length) + 4` bytes
  (= the remaining region capacity) across the boundary.

**One tail rule for every item:** with ≤ 4 bytes left in a
region, skip to the next region. A message keeps its length and payload in
one region; a batch count slot could fit in exactly 4 bytes but
deliberately does not use them — four zero bytes at a region end must mean
only padding, because an open batch's count slot is also zero and recovery
could not tell them apart. (0.2.0 change — the batch format WAS released
in 0.1.0; pre-0.2.0 queues are refused at open via `schema_version`.)

### Alignment contract

Regions are 8-byte and page aligned. Individual messages within a region are
packed densely and have **no alignment guarantee**. Consumers must use
offset-based accessors (`memcpy` or flyweights) to read fields — never cast a
message pointer to a wider type.

### Zero-length is the end-of-stream sentinel

A zero length prefix is never written for a real message; all append paths
reject `length <= 0`. Zero bytes at a payload position means the stream has
not reached that position yet.

## Reserved positions

- `INITIAL_PAYLOAD_POSITION = 64` — the first valid payload position; the
  prefix is reserved for alignment and metadata.
- `MAX_APPENDER_ID = 255`, `MAX_APPENDERS = 256` — the 8-bit appender id field
  admits 256 appenders, ids 0–255.

## Constants

The following are the resolved values of the format constants. They are
generated from the implementation headers; do not edit by hand.

<!-- GENERATED by scripts/gen_wire_format.sh; do not edit manually -->
HEADER_SIZE=8
HEADER_NULL=0
HEADER_LENGTH_BITS=3
HEADER_SECTOR_SIZE_BITS=6
HEADER_BLOCK_SIZE_BITS=12
HEADER_SECTOR_MASK=63
HEADER_BLOCK_MASK=4095
HEADER_PAYLOAD_MASK=72057594037927935
HEADER_APPENDER_BITS=56
PAYLOAD_LENGTH_OFFSET=0
PAYLOAD_LENGTH_SIZE=4
PAYLOAD_DATA_OFFSET=4
INITIAL_PAYLOAD_POSITION=64
MAX_APPENDER_ID=255
MAX_APPENDERS=256
QUEUE_META_SCHEMA_VERSION=2
QUEUE_META_INITIAL_START_INDEX=0
QUEUE_META_ENCODED_LENGTH=64
QUEUE_TYPE_NORMAL=0
QUEUE_TYPE_FIXED8=1
QUEUE_TYPE_BATCH=2
QUEUE_META_SCHEMA_VERSION_OFFSET=0
QUEUE_META_REGION_SIZE_OFFSET=4
QUEUE_META_START_INDEX_OFFSET=8
QUEUE_META_MAX_FILE_SIZE_OFFSET=16
QUEUE_META_ARCHIVABLE_OFFSET=24
QUEUE_META_QUEUE_TYPE_OFFSET=25
<!-- /GENERATED -->

## Schema version 2 — the protocol contract

**0.2.0 reads and writes schema 2 only.** Schema 2 is a protocol contract,
not just a layout: the byte-level format in this document (C0) plus the
protocol rules C1–C7. An implementation may write `schema_version=2` only
if it implements the whole contract. Any later change to the contract is a
schema bump.

- **C0 — the wire format in this document:** the 8-bit appender-id +
  56-bit position header encoding; payload framing (`[int32 length]`
  `[payload]`, zero length = end-of-stream sentinel, negative = long
  padding); the padding markers; the meta layouts (queue meta 64 B here;
  appender meta 64 B in `src/internal/appender_meta.h`); the export
  stream format (the "Export stream" section below — stream version 2 is
  the canonical little-endian form).
- **C1 — geometry:** `region_size` a power of two, a multiple of
  32 KB (one transposed header block) and a multiple of the host page
  size; `max_file_size` a multiple of `region_size`; pre-rule geometry is
  refused with `MMAPQ_ESCHEMA`.
- **C2 — the unified ≤ 4 tail rule:** with ≤ 4 bytes left in a
  region, skip to the next region; four zero bytes at a region end mean
  only padding.
- **C3 — appender meta `archive_offset` (byte 16) and `commit_intent`
  (byte 24), with their write rules:** the archiver stores the line
  (seq_cst) BEFORE the raise; a slow-path commit stores its intent
  (seq_cst) before the pre-check and clears it (release) LAST — also on
  refusal and on publish failure; a join clears a stale intent.
- **C4 — the commit slow path:** pre-check against
  `max(start_offset, archive_offset)` → header CAS (acq_rel) → seq_cst
  fence → re-check → atomic-min lowering. Non-archivable queues:
  membership gate + pre-check + CAS only.
- **C5 — archiver and lister:** publish by CAS from the snapshot; the
  per-lane listing bound is `min(start_offset, live_floor, intent)` from
  the live-header scan; the listing loads `start_offset`, then the
  intents, then scans; reclaim re-lists and unlinks in one `archive.lock`
  shared hold.
- **C6 — join:** never write over existing bytes; never read headers
  below `start_index`; a no-live-entry join anchors at
  `max(start_offset, archive_offset)` and clears a stale intent.
- **C7 — appender-ID lease:** a SIGKILLed writer's lane stays occupied
  (`MMAPQ_EIDCONFLICT`); release only via `mmapq-ids --unset`, valid only
  when the holding process is dead.

## Export stream

The export/import stream (QueueTransfer) is a byte-level contract like the
queue format itself. Reference implementation: `src/queue_transfer.c` and
`src/batch_queue_transfer.c`; the header layout is
`src/internal/export_meta.h`. **All multi-byte fields are little-endian.**

**Header: 64 bytes.**

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | stream version (int32) |
| 4 | 4 | region_size (int32) |
| 8 | 8 | start_index (int64) |
| 16 | 8 | max_file_size (int64) |
| 24 | 8 | total_entries (int64) |
| 32 | 1 | archivable (uint8) |
| 33 | 1 | queue_type (uint8: 0 normal, 2 batch) |
| 34 | 30 | reserved (zero on write; ignored on read) |

**Entry framing, flat stream** (queue_type 0): per entry,
`[uint16 appender_id][int32 payload_length][payload bytes]`,
× total_entries. `payload_length > 0` and ≤ region_size − 4.

**Entry framing, batch stream** (queue_type 2): per entry,
`[uint16 appender_id][int32 message_count]`, then per message
`[int32 length][payload bytes]`.

**Stream versions.** Version 2 is the canonical little-endian stream,
written by C 0.2.0 (and by Java implementations fixed for the 0.2.0
contract). Version 1 was written by C 0.1.0 and early 0.2.0 (little-endian)
and by older Java (big-endian framing). Readers accept versions 1 and 2 and
refuse anything else with `MMAPQ_ESCHEMA`. A version-1 stream whose frames
fail to parse (`MMAPQ_ECORRUPT`) may be a legacy big-endian Java export —
re-export it with a fixed Java (version 2).

The framing is pinned byte-for-byte by golden-block tests
(`test_c57_export_golden_bytes_flat` / `_batch` in
`test/test_queue_transfer.c`); the version rules by
`test_c57_import_rejects_stream_version_3` and
`test_c57_import_accepts_v1_stream`.
