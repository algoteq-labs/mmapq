# mmapq payload alignment design

**Status:** Approved design
**Scope:** Java / C / Rust / Python interoperability over the operational queue file format
**Audience:** Implementers across all supported language bindings

---

## 1. Alignment guarantees

The queue makes exactly two alignment guarantees, and no others:

1. **Region start offsets are 8-byte (and page) aligned.** This is a
   property of the region/file-mapping layer and holds unconditionally.
2. **The queue's own infrastructure structures — the header file, the
   appender ID pool bitset, the runtime's internal state — are aligned as
   required for the atomic operations they use.** These structures are
   sized and positioned by the queue itself, independent of message
   content, and this alignment is enforced at the point they're created.

**The queue does not guarantee, and does not need to guarantee, that
individual messages within a payload stream start at any particular byte
boundary.** A message's payload position depends on the cumulative length
of every prior message in that region, and is not aligned to 8 bytes, 4
bytes, or any other boundary beyond whatever the message's own length
happens to produce.

This is a deliberate design choice, not an oversight, for the reasons
below.

---

## 2. Why message-level alignment is unnecessary

### 2.1 No payload field requires hardware atomicity

Volatile and atomic operations (`putLongVolatile`, C `_Atomic` + CAS,
Rust `AtomicU64`) exist in this system **only inside queue infrastructure**
— the header file's commit CAS, the appender ID pool bitset, the runtime's
lock-free executable list. None of these structures live inside a message
payload. Application payload content — including the message length
itself — is never the target of a volatile or atomic operation anywhere
in the system. It is written once, sequentially, by a single appender,
and read as plain bytes thereafter.

This matters because the hardware alignment requirement that motivates
struct-padding schemes in other systems applies specifically to atomic
memory operations, not to plain reads. A misaligned `int64_t` read via
`memcpy` or an offset-based accessor is well-defined and correctly
handled by every relevant CPU architecture (x86-64, ARM64) — it costs at
most a small, bounded number of extra cycles when a read happens to
straddle a cache line, and nothing more. There is no correctness
consequence, in any of the three languages, for plain unaligned access to
payload bytes.

### 2.2 Single-writer-per-lane removes the need for in-payload synchronization

Each appender owns its payload file exclusively. No other writer — no
other thread, no other process — ever writes into that file. Because
there is no concurrent writer, there is structurally nothing for a
volatile or atomic operation on payload bytes to coordinate against. The
appender writes its bytes sequentially with no contention; a second
appender writes to a *different* file entirely (`payload_N` for appender
N), so no two appenders ever share a payload region to synchronize over.

The one and only point of cross-process synchronization in the entire
system is the **header commit**: the appender writes its full payload
(length, then bytes) first, then commits visibility with a single atomic
CAS on the 8-byte header — from `HEADER_NULL` to the encoded
`(appenderId, payloadPosition)` value. A reader's acquire-load of that
header establishes a happens-before relationship, via the memory model,
with everything the writer did beforehand — every payload byte, the
length field, all of it — regardless of those bytes' individual
alignment. The header CAS is the synchronization primitive for the entire
message; the payload bytes that follow it inherit its visibility
guarantee for free and need none of their own.

This is the same reasoning that already governs how response flags are
handled in the queue's own benchmarking code: visibility of payload
content is established once, by the header's acquire/release pair, and
every byte that was written before the commit is guaranteed visible after
it — with no additional synchronization required on the payload itself.

### 2.3 The message length field is plain data, not a synchronization primitive

The 4-byte length prefix that precedes every message is written once by
the appender, as part of the same uninterrupted write sequence as the
payload itself, and becomes visible to readers at exactly the same moment
the payload does — at header commit. It is read by every consumer as a
plain `int32` value to determine how many payload bytes follow and where
the next message begins. It is never the target of a volatile or atomic
operation, never mutated after being written, and carries no
synchronization responsibility of its own. There is consequently no
alignment requirement on the length field beyond what plain-read access
already tolerates.

---

## 3. How fields are actually accessed: codecs and flyweights, not struct casts

The queue's payload is, and remains, an opaque byte sequence as far as
the transport layer is concerned. Interpreting those bytes — extracting
typed fields, decoding a message schema — is entirely the responsibility
of the application layer, via a **codec**: a set of offset-based accessor
functions that read individual fields from a byte buffer, rather than
overlaying a struct directly onto the buffer and dereferencing it as a
whole.

This pattern is already how each target language naturally handles
exactly this situation, and the queue's design leans on that rather than
working against it:

- **Java** — `Agrona`-style `MutableDirectBuffer`/`UnsafeBuffer` accessors
  (`getLong(offset)`, `getInt(offset)`) read individual fields at byte
  offsets, with no assumption that the buffer as a whole is aligned for
  any particular struct layout. This has always been the pattern in the
  Java implementation and tolerates arbitrary offsets transparently.

- **C** — field access via `memcpy` of individual fields, or an
  offset-based accessor layer analogous to the Java flyweight pattern,
  rather than `*(MyStruct*)ptr`. A direct struct cast on a pointer with no
  alignment guarantee is undefined behaviour in C and must be avoided
  regardless of whether the consequence happens to be observable on any
  particular platform.

- **Rust** — the same principle applies, and Rust's own ecosystem has
  converged on it independently. Note that Rust's `Serialize`/
  `Deserialize` traits (`serde`) do not perform raw pointer casting under
  the hood — they walk the data structure field by field, calling
  serializer/deserializer methods per field, which is exactly the
  flyweight/codec pattern described above, implemented generically.
  Where raw byte-level access to a shared buffer is needed directly (as
  with this queue), the correct primitives are `ptr::read_unaligned` /
  `ptr::write_unaligned` from `core::ptr`, or a crate such as `zerocopy`
  built on the same principle — never a direct `&*(ptr as *const T)`
  dereference of a pointer with no alignment guarantee, which Rust
  treats as unsafe and unsound for exactly the same reason C does.

- **Python** — consumers operate on payload bytes via `struct.unpack`
  (or `ctypes`/`cffi` field-by-field access when bridging to the C
  library), which is itself an offset-based decode operation, not a
  pointer cast. The same alignment-agnostic pattern holds without any
  additional consideration.

**The consistent rule across all four languages: field access happens
through generated or hand-written accessor functions operating on byte
offsets within a buffer, never through reinterpreting a raw pointer as a
typed struct and dereferencing it directly.** This pattern is not a
workaround adopted to tolerate the queue's lack of message alignment —
it is the idiomatic, already-correct approach in every one of these
languages, and the queue's design simply does not need to disturb it.

---

## 4. Interoperability across Java, C, Rust, and Python

Because the wire format places no alignment requirement on individual
messages — only on regions, which is a property every language's region
mapper independently enforces identically — the same set of queue files
can be written and read live, concurrently, by processes in any
combination of these four languages, with no translation layer and no
format negotiation required:

- A **Java** sequencer process can append messages that a **C** execution
  engine or **Rust** risk service polls in real time from the same queue
  directory.
- A **Python** analytics or monitoring consumer can attach as a reader to
  the same live files without any special accommodation, using `cffi`/
  `ctypes` bindings to the C library and `struct.unpack` for field
  decoding.
- Any process in any of these languages can be the appender for its own
  lane; readers in any other language consume from that lane using their
  own language's codec, decoding the same bytes the same way, because the
  byte-level layout (length-prefixed, region-aligned, header-CAS-
  committed) is identical regardless of which language wrote it.

This interoperability is a direct consequence of keeping the transport
layer minimal: the queue guarantees atomic visibility and ordering, and
nothing about field-level interpretation. Field interpretation is owned
by a schema/codec layer that can be shared, version-controlled, and
generated consistently across all four languages — independent of, and
unconstrained by, anything in the transport format itself.

---

## 5. Summary

- Region alignment: guaranteed, 8-byte/page aligned, unconditional.
- Message-level alignment: not guaranteed, not needed.
- Volatile/atomic operations: confined to queue infrastructure, never
  applied to payload content, including the length field.
- Synchronization: a single header CAS per message is the sole
  synchronization point; payload visibility is inherited from it via the
  memory model, requiring no alignment of payload bytes themselves.
- Field access: via offset-based codecs/flyweights in every supported
  language — the natural, already-idiomatic pattern in Java, C, Rust, and
  Python alike — never via direct struct-cast dereferencing of a
  raw pointer.
- Interoperability: any combination of Java/C/Rust/Python processes can
  share live operational queue files directly, with field-level decoding
  handled uniformly by a codec layer that sits above, and is decoupled
  from, the transport format described here.

