# mmapq payload alignment design

**Status:** Approved design (revised for 0.2.1 to match the code; batch lanes included)
**Scope:** C / Rust / Python interoperability over the operational queue file format, for
both queue types (single-message and batch)
**Java:** not covered — Java-written queues are refused at open in 0.2.0 (schema_version 1); Java support is planned at schema 2.
**Audience:** Implementers across all supported language bindings, and application authors
writing payload codecs

The byte layout itself is specified in [the wire format](../reference/wire-format.md); this
document explains why that layout carries no per-message alignment and why that is safe.

---

## 0. Why the format is unaligned

The payload stream — a 4-byte length prefix followed by the message bytes, packed back to
back with no padding between messages — is part of the file format mmapq derives from
Algoteq's Java implementation (see the README). mmapq keeps the payload stream unchanged so
that Java support at schema 2 needs no change to it.

Aligning messages (for example, padding every record to 8 bytes) was considered and
rejected:

- **Cost:** up to 7 bytes per message, and a payload-stream change.
- **Benefit:** none that the queue's guarantees depend on. Committed reads are correct
  because of the header commit (§2.2); crash recovery is correct because of the appender's
  store order (§2.3). Alignment would let applications overlay naturally aligned types on
  payload bytes, but §3 makes offset-based access the rule anyway.

The rest of this document states what the queue does and does not guarantee about layout,
and why unaligned payloads are safe under those guarantees.

---

## 1. Layout guarantees

The queue makes the following layout guarantees, and no others:

1. **Region start offsets are page aligned (and therefore 8-byte aligned).** `region_size`
   must be a power of two and a multiple of the host page size. Both are checked at create
   and again at open (`src/queue_meta.c`). Consequence: a queue created on a 4 KB-page host is
   refused at open on a host whose page size does not divide its `region_size` (for example
   16 KB or 64 KB pages).
2. **The queue's own infrastructure — the header files, the appender ID pool bitset, the
   runtime's internal state — is aligned as its atomic operations require.** These structures
   are sized and positioned by the queue, independent of message content, and their alignment
   is asserted where they are accessed (for example `ASSERT_HEADER_ALIGNED` on the commit path
   in `src/indexed_appender.c`).
3. **A message never straddles a region boundary.** A record — the 4-byte length prefix and
   its payload — always lies entirely within one region:
   - `max_message_length` is `region_size − 4`; a longer append is rejected with
     `MMAPQ_EINVAL`, as is a length of zero or less (zero is the end-of-stream value, §2.3).
   - If the space left in the current region cannot hold the next record, the appender moves
     to the next region and leaves a padding marker:
     - **long padding** (more than 4 bytes left): a negative length `−(remaining − 4)` is
       written, so `|length| + 4` equals the remaining space;
     - **short padding** (4 bytes or fewer left): nothing is written; readers recognise the
       case from the remaining capacity alone.

   Every record is therefore a contiguous byte range inside one mapping, so a plain unaligned
   read of any of its fields never crosses into a separately mapped (or unmapped) region.

   **Batch lanes** use the same records. A batch is a 4-byte message count (the count slot)
   followed by its messages, each an ordinary record that obeys the rule above; the count slot
   also lies within one region and never uses the last 4 bytes of a region (the
   [wire format](../reference/wire-format.md)'s one tail rule). A batch as a whole may span
   regions: padding between two of its messages moves the rest to the next region.

**The queue does not guarantee that a record starts at any particular byte boundary.** A
record's position depends on the lengths of every earlier record in the region, so it is not
aligned to 8 bytes, 4 bytes or anything else. Fields inside a payload, including the length
prefix, may straddle cache lines and pages within a region.

### 1.1 Byte order

Every integer field of the queue format — the payload length prefix, the batch count slot,
the header entries, the metadata fields — is **little-endian**. The C library writes them in
the host's byte order and refuses to build on a big-endian host
(`_Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)` in `src/internal/header_codec.h`),
so the on-disk order is always little-endian. The export stream is little-endian by
specification ([wire format](../reference/wire-format.md)).

Payload contents are opaque to the queue (§3): their byte order is the application codec's
choice, and a codec shared across languages must state it explicitly. The shipped examples
encode their 6-byte latency header (`uint16` instance id, `uint32` sequence) little-endian in
all three languages: a packed struct in C, `to_le_bytes` / `from_le_bytes` in Rust, `struct`
format `"<HI"` in Python.

---

## 2. Why message-level alignment is unnecessary

### 2.1 No payload field needs hardware atomicity

Atomic operations (C11 `_Atomic` and CAS; the Rust and Python bindings run the same C code)
exist only in queue infrastructure: the header file's commit CAS, the appender ID pool
bitset, the runtime's lock-free executable list. None of these lives in a payload region.
Payload content, the length prefix and the batch count slot are never the target of an atomic
operation.

The hardware alignment requirement that motivates struct-padding schemes elsewhere applies to
atomic operations, not to plain loads and stores. A misaligned `int64_t` read through `memcpy`
or an offset-based accessor is well-defined in C and Rust, and plain unaligned loads and stores
to normal memory (including file-backed `mmap` memory) are handled in hardware on x86-64 and
ARM64. The cost is performance, not correctness: an access that splits a cache line costs a few
extra cycles, and one that splits a page costs more (two TLB lookups, and possibly a page fault
on the second page).

Plain unaligned access is correct only when the code doing it makes no alignment assumption.
Code that assumes natural alignment — aligned SIMD loads (`_mm_load_*`, `vld1q` with an
alignment hint), a reference to a naturally aligned type in Rust, `std::atomic_ref` in C++ — is
incorrect on payload bytes. §3 states the rule that avoids this.

### 2.2 Committed reads: the header commit is the only cross-process synchronization

Each appender owns its payload files (`payload_<id>_<n>`) exclusively. Exclusivity comes from
the appender ID pool: an appender holds its ID for its lifetime, and no other appender is given
that ID while it is held. With no concurrent writer inside a payload file, there is nothing in
the payload for an atomic operation to coordinate. (Releasing a held ID with an operator tool
while its holder is still alive — stopped rather than dead — breaks this premise and is
unsupported.)

For every committed message or batch, the one point of cross-process synchronization is the
**header commit**. The appender writes the whole record (or batch) first and only then
publishes it with a single CAS on the 8-byte header entry, from `HEADER_NULL` to the encoded
`(appenderId, payloadPosition)` value, with `memory_order_acq_rel`. Readers load the header
with `memory_order_acquire`. When a reader observes the committed value, the release/acquire
pair makes every store the appender made before the CAS — length prefixes, count slot,
payload bytes — visible to it, whatever those bytes' alignment. The payload needs no
synchronization of its own.

Because the length is on-disk data, readers also bounds-check it against the space left in the
region (`length <= 0 || length > capacity − 4` is `MMAPQ_ECORRUPT`, in `src/indexed_poller.c`
and `src/indexed_reader.c`); the batch iterator checks each message the same way. One case is
not checked: a corrupt *header* whose payload position lies past the end of the payload file
makes the read-only mapping fault (SIGBUS). This is an accepted risk — see "Anti-Pattern E:
File-Size Checks on the Payload Read Path" in the
[maintainer guide](../contributing/maintainer_guide.md).

### 2.3 The length prefix: plain data for readers, a framing marker for walkers

The length prefix is read and written with plain (non-atomic) `memcpy`, everywhere. It is
written once per record and never rewritten in place. It has two roles, and the alignment
argument differs between them.

**For readers of committed messages**, the length is plain data. It becomes visible together
with the payload, at the header commit (§2.2), and needs no alignment.

**For walkers**, the length is also the record boundary. Some paths read payload files with no
committed header to synchronize on: the appender's rejoin walk on restart (`src/payload_walk.c`;
for batch lanes, the join's open-batch completion in `src/indexed_batch_appender.c`) and the
uncommitted iterators. They read the 4-byte values as follows:

| Value at a record position | Meaning |
|---|---|
| `> 0` | a record of that many payload bytes follows |
| `0` | end of stream: unwritten space (payload files are created with `ftruncate`, so unwritten space reads as zero). A payload file that does not exist yet is also the end of stream. |
| `< 0` | long padding: skip `−value + 4` bytes, to the next region |
| (4 bytes or fewer left in the region) | short padding: skip to the next region |
| batch count slot `= 0` | an open batch (or nothing yet): at join, its messages are walked by their lengths and the count is set to the number found |
| batch count slot `> 0` | a completed batch of that many messages |

After a kill -9 every lane is in one of three states: an end-of-stream zero where the next
record would start, a record whose length is complete (its payload may be partly written; the
walker steps over its declared extent), or a committed record. A kill never leaves payload
bytes past the end of stream without the length that frames them. This framing argument rests
on two properties of the appender, not on alignment:

1. **Store order.** The previous record's payload is stored before this record's length, and
   this record's length before its payload. Plain `memcpy` gives no ordering guarantee, so the
   appender places compiler barriers (`atomic_signal_fence(memory_order_release)`) around the
   length store, in `write_payload` and both batch append paths. No hardware fence is needed: a
   killed process's executed stores are not lost, and any later observer either opens the files
   after the process has died or synchronizes through the header CAS. (The batch count store at
   completion has no barrier before it. A compiler may move it above the last message's payload
   bytes; framing stays intact — every length was already stored — so a kill there leaves a
   completed batch whose last payload may be partly written. The queue makes no promise about
   payload bodies after a crash; applications validate them.)
2. **The length is written by one store instruction.** A kill lands between instructions, so a
   single store is either done or not done, aligned or not. (Other cores may see an unaligned
   store in two parts; readers never look at a length before the header commit, so that does not
   matter.) GCC and Clang emit the 4-byte constant-size `memcpy` as one store at every
   optimisation level, including `-O0`. Only a build with `-fno-builtin` or `-ffreestanding`
   turns it into a library call, which a C library could implement byte by byte; no mmapq build
   (Makefile, CMake, the Rust `build.rs`, the Python `setup.py`) uses either flag.

**Out of scope: power loss and kernel crash.** The queue does not `msync` in any defined order
([persistence contract](../reference/persistence-contract.md)), so after a power loss the kernel
may have written back any subset of dirty pages — including a header page without the payload
page it refers to. The queue makes no consistency guarantee in that case, with or without
alignment.

---

## 3. How fields are accessed: codecs and flyweights

The payload is an opaque byte sequence to the transport layer. Interpreting it — extracting
typed fields, decoding a message schema — is the application's job, through a **codec**: a set
of offset-based accessors that read individual fields from a byte buffer.

**The rule: never form a pointer or reference to a type whose alignment is greater than 1 at a
payload address, and never use an access that assumes alignment.** Read and write fields
through alignment-agnostic operations on byte offsets.

- **C** — `memcpy` of individual fields, or an offset-based accessor layer (the flyweight
  pattern). A cast such as `*(int64_t *)p`, or `*(MyStruct *)p` for a naturally aligned
  `MyStruct`, is undefined behaviour on an unaligned `p`, whether or not the platform tolerates
  it. A struct declared `__attribute__((packed))` has alignment 1 under GCC and Clang, so
  overlaying it on payload bytes is well-defined in GNU C — the C examples do this. It is not
  portable ISO C, and taking the address of a packed member gives an unaligned pointer that
  must not be dereferenced as the member's type.

- **Rust** — the bindings hand payloads out as `&[u8]`. Decode fields with
  `i64::from_le_bytes(buf[o..o + 8].try_into().unwrap())` and the like, or with
  `ptr::read_unaligned` / `ptr::write_unaligned`. Creating a `&T` (or dereferencing a
  `*const T`) for a `T` with alignment greater than 1 at an unaligned address is undefined
  behaviour — for references, even if the reference is never read. With `zerocopy`, use types
  of alignment 1 (`zerocopy::byteorder::{U16, U32, I64, …}`, or types deriving `Unaligned`) or
  the copying `read_from_*` functions; `ref_from_*` on a type with alignment greater than 1
  checks alignment at run time and will fail on payload bytes.

- **Python** — `struct.unpack_from` / `struct.pack_into` on the payload bytes or memoryview,
  with an explicit byte-order prefix (`"<"` for little-endian). The default `"@"` uses native
  byte order and also inserts native alignment padding between fields, which a C or Rust codec
  over the same bytes will not expect.

This is not a workaround for the queue's lack of message alignment; it is the idiomatic
approach in each language, and the queue's design does not need to disturb it.

---

## 4. Interoperability across C, Rust, and Python

There is one implementation of the queue format: the C library. Both bindings build it from a
vendored copy of the C sources — the `c/` git submodule in `mmapq-rs` (compiled by `build.rs`)
and in `mmapq-py` (compiled into the `cffi` extension), each pinned to an mmapq commit. The
region mapper, the padding rules and the header encoding are therefore the same code in every
binding.

So the same queue files can be written and read live, concurrently, by processes in any mix of
the three languages, with no translation layer and no format negotiation:

- A **C** sequencer process can append messages that a **Rust** execution engine or risk
  service polls in real time from the same queue directory.
- A **Python** analytics or monitoring consumer can attach as a reader to the same live files
  without any special accommodation.
- Any process in any of these languages can be the appender for its own lane; readers in any
  other language consume from that lane.

The queue guarantees that every binding sees the same record boundaries and the same bytes. It
does not guarantee that they *interpret* those bytes the same way: that is the shared codec's
job. A codec used across languages must fix the byte order and offset of every field explicitly
(§1.1, §3) rather than relying on native defaults.

**The interop risk to manage is a stale pin**: a change under `mmapq/src` or `mmapq/include`
that a wrapper's `c/` submodule does not yet point at makes that binding run older format code.
The guards are `scripts/check_mirrors.sh` (all mirrored directories of both wrappers, run by
`scripts/check_sync.sh` and the cross-repository Coherence CI) and the wrappers' release
workflows, which refuse a release whose `c/` gitlink is not the mmapq release tag. The mirror
discipline is in `AGENTS.md` at the repository root.

---

## 5. Summary

- Why unaligned: the payload stream is kept as the format mmapq derives from; alignment buys
  nothing the guarantees depend on.
- Region alignment: guaranteed — page (and therefore 8-byte) aligned, checked at create and
  open against the host page size.
- Region containment: guaranteed — no record straddles a region; padding markers fill region
  ends. A batch may span regions; each of its messages stays inside one.
- Record-level alignment: not guaranteed, not needed.
- Byte order: little-endian, enforced at build; payload byte order is the codec's choice and
  must be explicit.
- Atomic operations: confined to queue infrastructure; never applied to payload content, the
  length prefix or the batch count slot.
- Committed reads: the header CAS (`acq_rel`) and header load (`acquire`) are the only
  synchronization; payload visibility is inherited from them.
- Crash recovery: the length prefix (and the batch count slot) also frame the stream; kill -9
  safety rests on store order and a single-store length write, not on alignment. Payload bodies
  after a crash are the application's to validate. Power loss is out of scope.
- Field access: offset-based codecs; no pointer or reference to a type with alignment greater
  than 1 at a payload address.
- Interoperability: one C implementation, vendored into both bindings; a stale `c/` pin is the
  risk to manage.
