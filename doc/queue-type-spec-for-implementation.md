# Implementation Spec: queue_type discriminator in QueueMeta

> *Implementation specification for the queue_type change, June 2026.
> Retained as a design record — the code and tests are authoritative
> where they have since evolved.*

**Audience:** implementing model/engineer with access to the `mmap-c`,
`mmap-c-rs`, and `mmap-c-py` repositories, but WITHOUT access to prior design
discussion. Everything needed is in this document. Follow it exactly; all
design decisions are already made. Where this spec and your own judgment
disagree, the spec wins — flag the disagreement in your summary instead of
deviating.

---

## 1. Goal

The byte at offset 25 of the queue meta file (currently `fixed_size_8bytes`,
values 0/1) becomes a three-value **queue type discriminator**:

| value | meaning                       | supported by mmap-c |
|-------|-------------------------------|---------------------|
| 0     | normal single-message queue   | yes                 |
| 1     | fixed 8-byte payloads (Java)  | NO — refuse to open |
| 2     | batch queue                   | yes                 |

Values 0 and 1 keep their exact Java (`tools4j/mmap`) semantics, so Java
binary compatibility is preserved by construction. Value 2 is new and
C-ecosystem-only.

Additionally, `schema_version` (int32 at offset 0) — currently written but
**never validated** — gains strict validation: exact match against 1, refuse
otherwise.

Both validations must be **real runtime checks that survive `-DNDEBUG`**, not
`MMAP_ASSERT`. This matters: the existing `fixed_size_8bytes == 0` guard in
`queue_meta.c` is an `MMAP_ASSERT`, which compiles to nothing in release
builds — a Java fixed-8-byte queue currently opens silently in release and
misparses. Your change must close that hole.

**Why this exists:** opening a batch queue with single-message participants
(or vice versa) currently misparses the payload framing silently. After this
change, every participant refuses at creation with a clear diagnostic.

---

## 2. Explicit non-goals — do NOT do any of these

- Do NOT change any byte offset, the 64-byte meta size, or the meaning of
  values 0/1 at offset 25.
- Do NOT add migration or auto-stamping logic for existing files. Policy is
  flag-day: pre-change batch queues (which carry 0 at offset 25) will be
  refused when opened as batch after this change — that is intended.
  Operators recreate queues. Add one release-note line, nothing more.
- Do NOT implement fixed-8-byte payload support. Type 1 is refuse-only.
- Do NOT change public function signatures other than the two listed in §4.2.
- Do NOT touch payload framing, header codec, appender/poller logic, or any
  file not listed in §8.
- Do NOT reformat, re-comment, or "clean up" surrounding code.
- Do NOT use `MMAP_ASSERT` for any check added by this spec.

---

## 3. Current state (verified facts about the codebase)

- `include/queue_meta.h`: layout doc block; defines
  `QUEUE_META_SCHEMA_VERSION` (=1) and
  `QUEUE_META_FIXED_SIZE_8BYTES_OFFSET` (=25); struct `QueueMeta` has cached
  field `int fixed_size_8bytes`; inline accessor
  `queue_meta_fixed_size_8bytes()`.
- `src/queue_meta.c`:
  - `meta_file_init()` writes `uint8_t fs8 = 0;` at offset 25 on creation.
  - `meta_cache_fields()` caches offset-25 byte into `fixed_size_8bytes`.
  - `queue_meta_open_rw()` (~line 112) and `queue_meta_open_ro()` (~line 163)
    each contain `MMAP_ASSERT(m->fixed_size_8bytes == 0, ...)`.
  - `schema_version` is written at creation, has an accessor, and is read
    nowhere.
- Error code convention in `include/mmap_platform.h`:
  `MMAPQ_ETIMEDOUT (-1)`, `MMAPQ_EINVAL (-2)`, `MMAPQ_ECLOSED (-3)`,
  `MMAPQ_ECORRUPT (-4)`.
- All meta-open call sites (each participant opens its own meta):

  | file                          | call                    | queue type |
  |-------------------------------|-------------------------|-----------|
  | src/indexed_appender.c:340    | queue_meta_open_rw      | normal (0) |
  | src/indexed_poller.c:53       | queue_meta_open_ro      | normal (0) |
  | src/indexed_reader.c:52       | queue_meta_open_ro      | normal (0) |
  | src/indexed_queue.c:209       | queue_meta_open_ro      | normal (0) |
  | src/indexed_batch_appender.c:158 | queue_meta_open_rw   | batch (2)  |
  | src/indexed_batch_poller.c:36 | queue_meta_open_ro      | batch (2)  |
  | src/indexed_batch_reader.c:47 | queue_meta_open_ro      | batch (2)  |

  (Line numbers approximate; locate by grepping `queue_meta_open`.)
  There may be additional call sites in archiver/transfer code —
  `grep -rn "queue_meta_open" src/` and classify every hit as normal or
  batch by which module it lives in (`*batch*` files → 2, others → 0).
  Every call site must pass the correct type. Missing one is a spec
  violation.

- Rust error mapping: `pub(crate) fn map_rc(rc: i32) -> MmapError` in
  `mmap-c-rs/src/error.rs`.
- Python error mapping: `_RC_ERRORS` dict in
  `mmap-c-py/src/mmap_c_py/_errors.py`.

---

## 4. C changes (`mmap-c`)

### 4.1 `include/mmap_platform.h`

Add after `MMAPQ_ECORRUPT`:

```c
#define MMAPQ_ESCHEMA    (-5)   /* schema version or queue type mismatch */
```

### 4.2 `include/queue_meta.h`

1. Add the type constants near `QUEUE_META_SCHEMA_VERSION`:

```c
/*
 * Queue type discriminator, stored at offset 25.
 * Historically Java's boolean fixedSize8byte; values 0 and 1 keep their
 * exact Java semantics, so Java compatibility is preserved. Value 2 is
 * new and C-ecosystem only (batch queues must not be opened from Java).
 */
#define QUEUE_TYPE_NORMAL   ((uint8_t)0)   /* single-message, length-prefixed */
#define QUEUE_TYPE_FIXED8   ((uint8_t)1)   /* Java fixed 8-byte — NOT supported */
#define QUEUE_TYPE_BATCH    ((uint8_t)2)   /* batch framing */
```

2. Rename the offset macro (keep value 25) and keep the old name as an alias
   so any code this spec missed still compiles:

```c
#define QUEUE_META_QUEUE_TYPE_OFFSET        ((int32_t)25)
#define QUEUE_META_FIXED_SIZE_8BYTES_OFFSET QUEUE_META_QUEUE_TYPE_OFFSET /* legacy alias */
```

3. In the `QueueMeta` struct, rename `int fixed_size_8bytes;` to
   `int queue_type;` (comment: `/* cached — offset 25, see QUEUE_TYPE_* */`).

4. Replace the accessor:

```c
static inline int queue_meta_queue_type(const QueueMeta *m)
{
    return m->queue_type;
}
```

   Delete `queue_meta_fixed_size_8bytes` and fix any compile errors that
   deletion causes by switching callers to the new accessor
   (`grep -rn "fixed_size_8bytes" src/ include/ test/`).

5. Change the two open-function signatures:

```c
QueueMeta *queue_meta_open_rw(
    const char *directory,
    const char *queue_name,
    int64_t     start_index,
    int32_t     region_size,
    int64_t     max_file_size,
    int         archivable,
    uint8_t     queue_type);      /* NEW: QUEUE_TYPE_NORMAL or QUEUE_TYPE_BATCH */

QueueMeta *queue_meta_open_ro(
    const char *directory,
    const char *queue_name,
    uint8_t     expected_type);   /* NEW */
```

6. Update the layout doc block: offset 25 line becomes
   `25 : queue_type (uint8) — 0 normal, 1 fixed-8 (Java, unsupported), 2 batch`,
   and add one sentence: "schema_version and queue_type are validated on
   every open; mismatch refuses with MMAPQ_ESCHEMA."

### 4.3 `src/queue_meta.c`

1. `meta_file_init`: the ctx struct gains `uint8_t queue_type;`; write it at
   offset 25 instead of the hardcoded 0 (`uint8_t qt = mctx->queue_type;`).

2. `meta_cache_fields`: cache the raw byte without collapsing it:

```c
m->queue_type = (int)m->base[QUEUE_META_QUEUE_TYPE_OFFSET];
```

3. Add one static validation helper used by BOTH open paths (this is a real
   check, NOT an assert — it must survive `-DNDEBUG`):

```c
/* Returns 0 if the on-disk meta matches expectations; MMAPQ_ESCHEMA otherwise.
 * Prints a one-line diagnostic to stderr on mismatch so the refusal is
 * actionable (open functions return NULL, which otherwise says nothing). */
static int meta_validate(const QueueMeta *m, uint8_t expected_type,
                         const char *directory, const char *queue_name)
{
    int32_t schema = queue_meta_schema_version(m);
    if (schema != QUEUE_META_SCHEMA_VERSION) {
        fprintf(stderr,
            "mmap-c: %s/%s: unsupported meta schema_version %d (expected %d) "
            "— refusing to open\n",
            directory, queue_name, schema, QUEUE_META_SCHEMA_VERSION);
        return MMAPQ_ESCHEMA;
    }
    if (m->queue_type == QUEUE_TYPE_FIXED8) {
        fprintf(stderr,
            "mmap-c: %s/%s: fixed-8-byte queues are Java-only and not "
            "supported by mmap-c — refusing to open\n",
            directory, queue_name);
        return MMAPQ_ESCHEMA;
    }
    if ((uint8_t)m->queue_type != expected_type) {
        fprintf(stderr,
            "mmap-c: %s/%s: queue_type mismatch — file is type %d, opener "
            "expected %d (0=normal, 2=batch). Opening a queue with the wrong "
            "participant kind misparses payload framing — refusing to open\n",
            directory, queue_name, m->queue_type, (int)expected_type);
        return MMAPQ_ESCHEMA;
    }
    return 0;
}
```

   (`#include <stdio.h>` if not already present.)

4. In `queue_meta_open_rw`: pass `queue_type` through the init ctx (used only
   when the file is being created); after `meta_cache_fields`, DELETE the
   `MMAP_ASSERT(m->fixed_size_8bytes == 0, ...)` block and replace with:

```c
    if (meta_validate(m, queue_type, directory, queue_name) != 0) {
        queue_meta_close(m);   /* or the function's existing cleanup path */
        return NULL;
    }
```

   Use whatever cleanup the function already performs on its existing failure
   paths — mirror it exactly (region destroy + free); do not invent new
   cleanup.

5. In `queue_meta_open_ro`: same replacement, validating against
   `expected_type`.

### 4.4 Call sites

Update every call site found in §3 (plus any found by grep):

- Normal-queue modules pass `QUEUE_TYPE_NORMAL` (rw call in
  `indexed_appender.c` adds it as the last argument; ro calls in
  `indexed_poller.c`, `indexed_reader.c`, `indexed_queue.c`).
- Batch modules pass `QUEUE_TYPE_BATCH` (`indexed_batch_appender.c` rw;
  `indexed_batch_poller.c`, `indexed_batch_reader.c` ro).
- Archiver/transfer call sites: classify by module name (`*batch*` → BATCH,
  else NORMAL).

The existing behavior where open failure propagates as a NULL participant is
unchanged — participants already handle `queue_meta_open_* == NULL` by
failing creation. Verify that each caller's NULL path frees what it allocated
(it already should; do not modify it).

### 4.5 C tests — add to the existing suites (Unity framework, follow the
style of neighboring tests)

In `test/test_indexed_queue.c`:

1. `test_open_normal_as_batch_refused` — create a normal queue (create
   appender, append one message, close all), then attempt
   `indexed_batch_queue` participant creation on the same directory/name;
   assert creation returns NULL.
2. `test_meta_schema_mismatch_refused` — create a normal queue, close all,
   open `<dir>/<name>/meta` with plain `open(2)`/`pwrite` and patch the
   int32 at offset 0 to 99, then assert appender creation returns NULL.
3. `test_meta_fixed8_refused` — same patching approach: write byte 1 at
   offset 25; assert appender creation returns NULL.

In `test/test_indexed_batch_queue.c`:

4. `test_open_batch_as_normal_refused` — create a batch queue, append+commit
   one batch, close; attempt normal `indexed_queue` appender creation on the
   same path; assert NULL.
5. `test_batch_roundtrip_type_stamped` — create a batch queue, close, reopen
   with batch participants successfully; additionally `pread` byte 25 of the
   meta file directly and assert it equals 2.

These five tests must pass under BOTH the normal test build and the
release-flags loop in `scripts/test.sh`. Add `test_indexed_queue` and
`test_indexed_batch_queue` to the release-flags loop's test list if they are
not already in it (they are, as of the current script — verify, don't
assume).

---

## 5. Rust changes (`mmap-c-rs`)

1. Sync the C changes into the vendored/submodule C tree the crate builds
   from (`c/`), the same way previous C changes were synced.
2. `src/error.rs`: add a variant and mapping:

```rust
    /// Schema version or queue type mismatch on open.
    SchemaMismatch,
```

   Display: `"schema version or queue type mismatch (wrong queue kind, or file from an incompatible version)"`.
   In `map_rc`: `-5 => MmapError::SchemaMismatch,`.
3. No other Rust source changes: queue-type selection is implicit in which C
   functions the `queue` vs `batch` modules call, and open failures already
   surface as `Err` from participant creation.
4. Add one integration test (new file `tests/queue_type.rs`, own binary,
   PID-suffixed temp dir like `tests/runtime_uaf.rs`): create a normal queue
   via `IndexedQueue` + appender + one append, drop everything, then attempt
   `IndexedBatchQueue`-side participant creation on the same path and assert
   it returns `Err`. (Creation errors currently surface as
   `Internal`/`PoolExhausted`-style variants because create paths return
   NULL, not codes — assert `is_err()`, do NOT assert the specific variant.)

## 6. Python changes (`mmap-c-py`)

1. `src/mmap_c_py/_errors.py`: add

```python
class SchemaMismatchError(RuntimeError):
    """Schema version or queue type mismatch on open."""
```

   and the dict entry
   `-5: (SchemaMismatchError, "schema version or queue type mismatch"),`.
2. Export `SchemaMismatchError` from `__init__.py` alongside the existing
   error types.
3. Add two pytest tests: creating a single-message queue then constructing a
   batch appender on the same path raises (assert `pytest.raises(Exception)`
   — create-path errors are generic today); and
   `from mmap_c_py import SchemaMismatchError` works.

## 7. Documentation

- `mmap-c/README.md`: in the compatibility/interop section add:
  "Every open validates `schema_version` and the queue type stored in the
  meta file; a mismatch (including opening a batch queue with single-message
  participants, or vice versa) is refused. Fixed-8-byte queues created by
  Java are refused — that mode is not implemented. Batch queues are
  C-ecosystem only; do not open them from tools4j/Java."
- Release note (CHANGELOG or README "upgrading" note):
  "Batch queues created before this version carry queue_type 0 and will be
  refused on open. Recreate them; queue data is transient by design."

## 8. Complete file manifest (touch nothing else)

```
mmap-c/include/mmap_platform.h      (+1 define)
mmap-c/include/queue_meta.h         (constants, rename, accessor, signatures, docs)
mmap-c/src/queue_meta.c             (init ctx, cache, meta_validate, both opens)
mmap-c/src/indexed_appender.c       (call site)
mmap-c/src/indexed_poller.c         (call site)
mmap-c/src/indexed_reader.c         (call site)
mmap-c/src/indexed_queue.c          (call site)
mmap-c/src/indexed_batch_appender.c (call site)
mmap-c/src/indexed_batch_poller.c   (call site)
mmap-c/src/indexed_batch_reader.c   (call site)
mmap-c/src/<archiver/transfer files with queue_meta_open hits, if any>
mmap-c/test/test_indexed_queue.c    (+3 tests)
mmap-c/test/test_indexed_batch_queue.c (+2 tests)
mmap-c/README.md                    (interop + release note)
mmap-c-rs/c/...                     (synced mirror of the above C changes)
mmap-c-rs/src/error.rs              (variant + map_rc line)
mmap-c-rs/tests/queue_type.rs       (new test)
mmap-c-py/src/mmap_c_py/_errors.py  (exception + dict entry)
mmap-c-py/src/mmap_c_py/__init__.py (export)
mmap-c-py/tests/...                 (+2 tests)
```

## 9. Acceptance criteria — all must hold

1. `bash scripts/test.sh` in `mmap-c` exits 0: previous tests plus the five
   new ones, in BOTH the normal run and the release-flags loop.
2. `grep -rn "MMAP_ASSERT" mmap-c/src/queue_meta.c` shows NO assert on
   queue type or schema (other pre-existing asserts in the file are fine).
3. `cargo test` in `mmap-c-rs` fully green including `tests/queue_type.rs`.
4. `python -m pytest` in `mmap-c-py` fully green including the two new tests.
5. Manual check: create a normal queue, `printf '\x02' | dd of=<dir>/<q>/meta
   bs=1 seek=25 count=1 conv=notrunc`, then attempt to create a normal
   appender — must print the queue_type mismatch diagnostic to stderr and
   fail creation. Same file opened with batch participants must succeed.
6. `grep -rn "fixed_size_8bytes" mmap-c/src mmap-c/include` returns only the
   legacy alias `#define` in queue_meta.h (and comments), no live code.

## 10. Summary format required from the implementer

Report per repository: files changed, tests added with names, full test
counts before/after, and — explicitly — any place where you deviated from
this spec and why. An item reported as done that is not present in the diff
is the worst possible outcome; when in doubt, under-claim.
