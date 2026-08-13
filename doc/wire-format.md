# mmapq wire format specification

This is the **normative** specification for the on-disk format written and
read by mmapq. It is installed with the library; a consumer in any language
can build a reader from this document alone.

The authoritative implementation lives in the source distribution at
`src/internal/header_codec.h` (header + payload framing) and
`src/internal/queue_meta.h` (meta file). The constants below are **generated**
from those headers by `scripts/gen_wire_format.sh` — they cannot drift from
the implementation.

The format is **frozen**. Files written today remain readable by every future
version. Do not change any offset, size, framing rule, or alignment contract
in this document; a change here is a binary-incompatible break.

## Storage layout

A queue lives in its own directory, `<directory>/<queue_name>/`, containing:

- `meta` — a fixed 64-byte cache-line metadata block.
- `header` — a single header file holding one 8-byte entry per queue index.
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

## Header file (`header`)

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
  `abs(length)` bytes (which includes the 4-byte length field itself).
- `length == 0` — end of stream; nothing written at this position yet.

Two padding cases at a region end:

- **Short padding** — when fewer than 4 bytes remain in a region, skip to the
  next region without writing a length field at all.
- **Long padding** — write a negative length whose absolute value encodes the
  bytes to skip across the boundary.

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
QUEUE_META_SCHEMA_VERSION=1
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
