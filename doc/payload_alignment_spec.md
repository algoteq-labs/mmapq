# Payload framing alignment fix — implementation spec

**Status:** Approved, ready for implementation
**Scope:** Breaking wire-format change. Must be applied identically and simultaneously to Java, C, and Rust.
**Owner:** Anton
**Affects:** Operational queue file format only. Export/import (long-term storage) is unaffected — it already exists as the translation boundary and will absorb this change like any other format detail.

---

## 1. The problem

The queue guarantees **region-level** alignment (every region starts at an 8-byte, page-aligned file offset) but never guaranteed **message-level** alignment. Each message's payload starts at:

```
payload_start = region_start + 4 (length prefix) + sum(prior message footprints in this region)
```

This offset drifts arbitrarily after the first message in a region — there is no guarantee it lands on any particular byte boundary.

This was invisible in Java because the language's `Unsafe`/`Agrona`-style buffer accessors (`getLong(offset)`, `getInt(offset)`) tolerate unaligned access transparently on x86/ARM64 hardware. It surfaced because **some fields inside message payloads use `putLongVolatile`/atomic operations** (e.g. a commit marker or visibility flag inside a message, separate from the queue's own outer header CAS). Atomic 8-byte operations — unlike plain reads — **require natural 8-byte alignment** on the underlying hardware. A misaligned atomic op can silently lose atomicity under contention on x86, or hard-fault on ARM64. This is a correctness bug, not a performance nit, and it exists today in Java as well as C — Java has simply never hit it in a way that surfaced visibly.

C and Rust make this constraint explicit and enforced: directly dereferencing an unaligned pointer as a wider type is undefined behaviour, and Rust's own tooling (Miri, UBSan) will flag it. This is what brought the issue to light during the C port review — it was always present in Java, just silent.

---

## 2. The fix

Widen the per-message length prefix from 4 bytes to 8 bytes by adding a 4-byte reserved field, and pad every message's total footprint to a multiple of 8 bytes.

### New framing

```
[int32 length][int32 reserved][payload bytes...][pad to next 8-byte boundary]
```

| Field | Size | Notes |
|---|---|---|
| `length` | 4 bytes | unchanged — existing semantics (positive = valid message, negative = long padding marker, zero = unwritten) |
| `reserved` | 4 bytes | **queue-owned, zero-initialized, not exposed via the append/poll API.** Never accessed via volatile/atomic ops by the queue. Purely structural — exists to bring the framed header to 8 bytes so payload start is 8-aligned. |
| `payload` | `length` bytes | unchanged content and semantics. Now guaranteed to start at an 8-byte-aligned file offset. |
| trailing pad | 0–7 bytes | brings the message's total on-disk footprint (`8 + length`) up to the next multiple of 8, so the **next** message's framed header also starts 8-aligned. |

### Why pad every message, not just the first

Any message's payload may contain a volatile/atomic field at its start — not only the first message in a region. The padding must therefore be applied uniformly to every message so that every message's `payload[0]` offset is 8-aligned, recursively, region after region.

### Worked example — confirming `length` is untouched

A common point of confusion: padding the *footprint* does **not** mean padding the *reported length*. `length` remains the exact, unpadded byte count of the real payload — identical to today's semantics. Only the *gap before the next message* grows to absorb alignment padding; the payload itself is never inflated.

Example: two consecutive messages, lengths 13 and 5 bytes, written starting at file offset 0 (an 8-aligned region start):

```
Message 1: length=13

  offset  0 ┌──────────────┐
            │ length = 13  │  4 bytes   (exact — unchanged)
  offset  4 ├──────────────┤
            │ reserved = 0 │  4 bytes   (new — structural only)
  offset  8 ├──────────────┤
            │              │
            │  13 payload  │  13 bytes  (exact — unchanged, no inflation)
            │    bytes     │
            │              │
  offset 21 ├──────────────┤
            │   pad (3)    │  3 bytes   (new — skip-only, not part of length)
  offset 24 └──────────────┘
            footprint = align(8+13, 8) = align(21, 8) = 24

Message 2: length=5, starts at offset 24 (8-aligned ✓)

  offset 24 ┌──────────────┐
            │ length = 5   │  4 bytes
  offset 28 ├──────────────┤
            │ reserved = 0 │  4 bytes
  offset 32 ├──────────────┤
            │  5 payload   │  5 bytes
            │    bytes     │
  offset 37 ├──────────────┤
            │   pad (3)    │  3 bytes
  offset 40 └──────────────┘
            footprint = align(8+5, 8) = align(13, 8) = 16... 
            (next message starts at offset 24+16 = 40, 8-aligned ✓)
```

**Reader logic — one extra step, no semantic change:**

```c
int32_t length = read_int32(pos);              // exact payload size, unchanged
int32_t footprint = PAYLOAD_LENGTH_SIZE         // 4
                   + PAYLOAD_RESERVED_SIZE      // 4
                   + length;                    // exact payload bytes
int32_t aligned_footprint = (footprint + 7) & ~7;  // round up to next 8

const void *payload_ptr = pos + 8;              // skip length+reserved
int32_t    payload_len  = length;               // exactly as written — no inflation

int64_t next_message_pos = pos + aligned_footprint;  // skip trailing pad
```

`length` is read once, used as-is for the payload size handed to the application (no rounding, no truncation), and *separately* used to compute how far to advance to the next message. These are two uses of the same exact value — not two different values.

### `INITIAL_PAYLOAD_POSITION`

Changes from `64` to **`8`**.

Rationale: the only hard constraint on this constant is (a) it must be nonzero — a payload position of 0 combined with `appenderId=0` would encode to a header value of `0`, colliding with `HEADER_NULL` — and (b) under the new framing it must be 8-byte aligned. `8` is the minimum value satisfying both. There is no metadata or reserved use for the previous 64-byte gap; queue-level metadata already lives in `queue_meta`/`appender_meta`, not in an implicit gap at the front of the payload stream.

---

## 3. What does NOT change

- **`sectorSize` for the header layout stays at the current value.** This is a separate, already-decided question, unrelated to payload alignment. Do not conflate the two changes.
- **The header file format and `header_position()` calculation are untouched.** This fix is scoped entirely to the *payload* file framing, not the header file.
- **The `append`/`poll` API signatures are unchanged.** No new required parameter. The reserved field is invisible to normal callers.
- **Export/import format is unaffected.** It is the existing translation boundary for long-term storage and already exists specifically to decouple archival format from operational format.

---

## 4. What the reserved field is — and is not

It is a **structural alignment pad**, full stop. It is:

- Zero-initialized by the appender on every write
- Never read or written by the queue's own internal logic
- Never exposed through `indexed_appender_append()` or the message handler callback
- **Not** a message-type tag, **not** a timestamp, **not** any application-semantic field

If a genuine application need for those 4 bytes emerges later (a message-type discriminant was discussed and rejected as premature), it should be added as an **explicit, separate, opt-in accessor function** at that time, driven by an actual caller — not speculatively reserved for a purpose now. This mirrors how the queue already treats other internal details (e.g. `sectorSize`): sensible default, documented layout, opt-in escape hatch for advanced use, no API complexity tax on the common path.

**The one rule that must never be violated:** nobody — queue internals, application code, in any of the three languages — may perform a volatile/atomic/CAS operation against the `reserved` field. It exists purely to satisfy alignment for *application-level* fields that follow it. It is not itself meant to carry data accessed atomically.

---

## 5. Alignment contract (full statement, for documentation)

> Regions are guaranteed to start at an 8-byte (and page) aligned file offset.
> As of this change, every message's framed header (`length` + `reserved`) is
> also 8 bytes, and every message's total footprint is padded to a multiple
> of 8 bytes. Therefore every message's `payload[0]` is guaranteed to be at
> an 8-byte-aligned file offset.
>
> This guarantee covers the **start of the payload only**. Fields within the
> payload beyond the first 8 bytes are aligned only if the application's own
> message schema preserves 8-byte field alignment internally (i.e. by using
> fixed-width fields with no preceding variable-length data).
>
> Plain (non-atomic) reads of payload bytes — `memcpy`, `getLong`,
> `read_unaligned`, field-by-field flyweight accessors — remain
> alignment-agnostic and require no special handling, in any language.
>
> Any field accessed via a volatile/atomic operation (`putLongVolatile`,
> C `_Atomic` + CAS, Rust `AtomicU64`) **must** be placed at `payload[0]`
> (or at another offset within the payload that the application has
> independently ensured is 8-aligned). This is the specific case this
> change makes safe.

---

## 6. Implementation checklist (per language)

For **each** of Java, C, Rust:

- [ ] Update payload write path: emit `[length:int32][reserved:int32 = 0][payload]`, then pad total footprint to next multiple of 8
- [ ] Update payload read path: skip the 4-byte reserved field after reading `length`; skip trailing pad bytes when advancing to the next message (mirrors existing short/long padding skip logic at region boundaries — this is the same kind of skip logic, just per-message instead of per-region)
- [ ] Update `INITIAL_PAYLOAD_POSITION` (or equivalent constant) from `64` → `8`
- [ ] Update any constant/doc referencing the old 4-byte length-only prefix
- [ ] Update unit tests asserting specific byte offsets for payload positions (these will all shift)
- [ ] Update the alignment contract doc/comment block (see §5 above) at the canonical definition site in each language
- [ ] **Do not** add any new required parameter to append/poll APIs for the reserved field
- [ ] **Do not** touch header file layout, `header_position()`, or sectorSize — out of scope for this change

## 7. Cross-language consistency check

Before merging in any one language, confirm the binary layout is byte-identical across all three:

```
offset 0: length    (int32, little-endian)
offset 4: reserved  (int32, always 0 on write, ignored on read)
offset 8: payload[0]   ← guaranteed 8-byte aligned from region start
offset 8+length .. next_8_boundary: pad bytes (value irrelevant, skipped on read)
```

A C writer / Rust reader / Java reader (and every other permutation) must agree on this layout exactly, since live operational file sharing across all three languages is the explicit purpose of this format — this is not mediated by export/import.

---

## 8. Migration

This is a breaking format change. Existing queue files written under the old 4-byte-prefix format are **not** compatible with readers expecting the new 8-byte-prefix format, and vice versa. There is no in-place migration path for live data — queues should be recreated, or migrated explicitly via export-then-reimport through the existing export/import tooling if historical data needs to be preserved under the new operational format.

