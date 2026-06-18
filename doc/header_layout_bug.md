# Header layout bug discovery

mmap-c / tools4j-mmap · found during C port review · June 2026

---

## What was intended

The header layout uses a non-linear mapping to spread sequential indices across different cache lines, preventing false sharing between concurrent appenders. The design intent was:

> **One sector = one cache line = 64 bytes = 8 headers.**
> Adjacent indices land on different cache lines. Each of the 8 sectors in a block maps to exactly one cache line. One block = 64 headers = 512 bytes — fits cleanly in one eighth of a 4KB page.

The `Layout` constructor takes `(length, sectorSize)` where `sectorSize` is the **number of words per sector**. For one sector = one cache line:

```java
int HEADER_SIZE      = 8;   // bytes per header (one word)
int CACHE_LINE_SIZE  = 64;  // bytes per cache line
int WORDS_PER_SECTOR = CACHE_LINE_SIZE / HEADER_SIZE;  // = 8

// intended call:
Layout HEADER_LAYOUT = new Layout(HEADER_SIZE, WORDS_PER_SECTOR);
// = new Layout(8, 8)
```

---

## What actually happened

`CACHE_LINE_SIZE` (64, measured in *bytes*) was passed directly as `sectorSize` (which expects a *word count*). This is a unit mismatch — bytes where words were expected:

```java
// actual call in both Java and C:
Layout HEADER_LAYOUT = new Layout(HEADER_SIZE, CACHE_LINE_SIZE);
// = new Layout(8, 64)   ← 64 is in bytes, but sectorSize expects words
```

Inside the constructor, `blockSize = sectorSize × sectorSize = 64 × 64 = 4096`. Because `sectorSize` is squared, the unit error is amplified:

```
// with sectorSize=64 (wrong — bytes not words):
blockSize      = 64 × 64 = 4096 headers per block
one sector     = 64 headers × 8 bytes = 512 bytes = 8 cache lines  (intended: 1)
one block      = 4096 headers × 8 bytes = 32KB = 8 pages            (intended: 512 bytes)
adjacent index stride = 512 bytes = 8 cache lines apart             (intended: 64 bytes)
```

---

## Impact: intended vs actual

| Property | Intended `sectorSize=8` | Actual `sectorSize=64` |
|---|---|---|
| Words per sector | 8 (= CACHE_LINE_SIZE / HEADER_SIZE) | 64 (= CACHE_LINE_SIZE in bytes) |
| Bytes per sector | 64 bytes = 1 cache line ✓ | 512 bytes = 8 cache lines |
| Headers per block | 64 (8 × 8) | 4096 (64 × 64) — 64× larger |
| Bytes per block | 512 bytes — fits in ⅛ of a 4KB page | 32KB — spans 8 pages |
| Adjacent index stride | 64 bytes = 1 cache line apart | 512 bytes = 8 cache lines apart |
| Indices sharing a cache line | N and N+8, N+16, … (8 apart) | N and N+64, N+128, … (64 apart) |
| False sharing prevented? | Yes ✓ | Yes ✓ (over-engineered) |
| Page alignment property | 8 blocks per 4KB page ✓ | 8 pages per block — lost |

---

## Visual comparison: one block

### Intended — `sectorSize=8` · 64 headers · 512 bytes
8 rows × 8 columns · each row = exactly 1 cache line (64 bytes)

```
         col 0   col 1   col 2   col 3   col 4   col 5   col 6   col 7
row 0     idx 0   idx 8  idx 16  idx 24  idx 32  idx 40  idx 48  idx 56
(CL 0)
row 1     idx 1   idx 9  idx 17  idx 25  idx 33  idx 41  idx 49  idx 57
(CL 1)
row 2     idx 2  idx 10  idx 18  idx 26  idx 34  idx 42  idx 50  idx 58
(CL 2)
  …
row 7     idx 7  idx 15  idx 23  idx 31  idx 39  idx 47  idx 55  idx 63
(CL 7)
```

Adjacent indices are **64 bytes = 1 cache line apart**. Indices 0 and 8 share cache line 0.

### Actual — `sectorSize=64` · 4096 headers · 32KB
64 rows × 64 columns (showing first 4 rows × 4 columns)

```
          col 0    col 1    col 2    col 3   ···
row 0      idx 0   idx 64  idx 128  idx 192  ···
(CL 0)
row 1      idx 1   idx 65  idx 129  idx 193  ···
(CL 8)
row 2      idx 2   idx 66  idx 130  idx 194  ···
(CL 16)
row 3      idx 3   idx 67  idx 131  idx 195  ···
(CL 24)
  …
```

Adjacent indices are **512 bytes = 8 cache lines apart**. Indices 0 and 64 share cache line 0.

---

## Is the queue correct despite the bug?

**Yes — the queue is functionally correct.** False sharing is prevented in both cases. Adjacent indices are separated by more space than required (512 bytes vs 64 bytes), not less. No data is corrupted. The Java and C implementations are interoperable because both carry the same bug.

**The waste is real but not catastrophic.** The header file's block structure is 64× larger than designed. The total header file space for a fully-populated queue is unchanged (each entry still occupies exactly 8 bytes) — the layout affects the organisation of entries within the file, not the total size. The TLB and cache efficiency implications depend on access patterns and whether transparent huge pages are active.

---

## The fix

```java
// Java — fix the construction:
public static final Layout HEADER_LAYOUT =
    new Layout(HEADER_SIZE, CACHE_LINE_SIZE / HEADER_SIZE);
//  = new Layout(8, 8)
```

```c
// C header_codec.h — update four constants:
#define HEADER_SECTOR_SIZE_BITS  3   /* was 6  — 8 words per sector = 1 cache line */
#define HEADER_BLOCK_SIZE_BITS   6   /* was 12 — 8×8 = 64 headers per block        */
#define HEADER_SECTOR_MASK       7   /* was 63                                      */
#define HEADER_BLOCK_MASK        63  /* was 4095                                    */
```

> **Breaking change.** Fixing this changes the header file format. Any queue files written with the current layout are incompatible with the corrected layout. Java and C must be fixed together. Existing data must be migrated or queues recreated. Fix before the open-source release — better to ship the correct format than to carry a known bug into a public API.

---

*Discovered during mmap-c C port review · June 2026 · affects tools4j/mmap Java implementation and mmap-c C port equally*
