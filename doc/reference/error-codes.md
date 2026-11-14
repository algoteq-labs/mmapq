# Error codes

Every fallible C API returns `0` (or a non-negative value) on success and a
negative `MMAPQ_*` code on failure. The codes are ABI: values never change
and new failure modes append new codes. Message text has a single source of
truth — `src/mmapq_strerror.c`; both bindings render the C text, so messages
cannot drift.

**Retryable** means the call may succeed if retried as-is (after a backoff
where noted). Anything not marked retryable is a programming error, a state
error, or on-disk corruption — retrying the identical call fails again.

| Code | Value | Meaning | Retryable | Rust variant (`mmapq-rs`) | Python exception (`mmapq-py`) |
|---|---|---|---|---|---|
| `MMAPQ_ETIMEDOUT` | -1 | Mapping timeout (a region was not mapped in time), or a transient OS condition (interrupted call, file-descriptor limit, lock contention) | **Yes** — transient | `MmapError::MapTimedOut` | `MmapqError` |
| `MMAPQ_EINVAL` | -2 | Invalid argument (NULL, zero length, index out of range) | No — caller bug | `MmapError::InvalidArgument` | `ValueError` |
| `MMAPQ_ECLOSED` | -3 | Operation on a closed handle | No | `MmapError::QueueClosed` | `QueueClosedError` |
| `MMAPQ_ECORRUPT` | -4 | On-disk corruption detected; also a malformed export stream. Note: a version-1 stream that fails to parse may be a legacy big-endian Java export — re-export it with a fixed Java (version 2) | No — recreate the queue (restore history via export/import) | `MmapError::Corrupt` | `QueueCorruptError` |
| `MMAPQ_ESCHEMA` | -5 | Schema version or queue-type mismatch, on-disk geometry not supported by this build/host (a queue is portable only to hosts whose page size divides its region size), or an export-stream version mismatch (1 and 2 accepted) | No — the file is refused, never misread | `MmapError::SchemaMismatch` | `SchemaMismatchError` |
| `MMAPQ_EEXIST` | -6 | Destination queue already exists (transfer without append mode) | No | `MmapError::Exists` | `QueueExistsError` |
| `MMAPQ_ENOENT` | -7 | File or queue does not exist (including a missing base directory) | No | `MmapError::FileNotFound` | `QueueFileNotFoundError` (subclass of both `MmapqError` and `FileNotFoundError`) |
| `MMAPQ_EIDCONFLICT` | -8 | Requested appender ID already in use (a killed writer's lane stays taken until `mmapq-ids --unset` on the dead process) | No — release the ID first | `MmapError::IdConflict` | `AppenderIdConflictError` |
| `MMAPQ_ENOMEM` | -9 | Internal allocation failure | Back off; do not retry in a hot loop | `MmapError::OutOfMemory` | `MmapOutOfMemoryError` |
| `MMAPQ_EIO` | -10 | I/O error on a queue file or directory: a write failed, or it cannot be created or opened (permission denied, a path component that is not a directory, a read-only or full filesystem, a filesystem without shared `mmap`) | After fixing the cause | `MmapError::Eio` | `MmapqError` |
| `MMAPQ_ENOTREADY` | -11 | Meta file exists but is not yet fully initialised (a concurrent creator is still writing it) | **Yes** — transient | `MmapError::NotReady` | `MmapqError` |
| `MMAPQ_ECALLBACK` | -12 | The appender-ID mapper callback aborted a transfer; cleanup completed | No — the transfer stopped cleanly | `MmapError::CallbackAborted` | `MmapqError` |

Notes:

- Poll paths do not return these codes directly: a poll returns
  `MMAPQ_POLL_ERROR` and the code is available via
  `mmapq_indexed_poller_last_error()`. A header-read mapping timeout is
  always `POLL_ERROR` + `MMAPQ_ETIMEDOUT`, never end-of-stream.
- An unknown negative code maps to `MmapError::UnexpectedCode` (Rust) or
  the base `MmapqError` (Python) — forward-compatible with codes added
  after the binding was built.
- The three-way iterators return `1` message / `0` exhausted / negative
  `MMAPQ_*` code. A negative is truthy in C — compare explicitly, never
  `while (mmapq_uncommitted_iterator_next(...))`.
- **A permanent failure is never `MMAPQ_ETIMEDOUT`.** Opening, creating
  or mapping a queue file or directory reports by cause: it does not exist
  → `MMAPQ_ENOENT`; out of memory → `MMAPQ_ENOMEM`; a transient OS
  condition → `MMAPQ_ETIMEDOUT`; anything else (permission denied, a path
  component that is not a directory, a read-only or full filesystem) →
  `MMAPQ_EIO`. See the access each participant needs in
  [chapter 9 of the guide](../guide/09-trade-offs-and-limits.md).
- A base directory that does not exist is `MMAPQ_ENOENT`: the library
  creates only the queue's own directory inside the base directory —
  create the base directory first. A reader or poller treats a queue that
  does not exist yet as end-of-stream (it may be created later).
- A payload file that a committed message points to and that is missing or
  too short — deleted by hand or damaged, outside the persistence contract
  — is `MMAPQ_ECORRUPT` on both lanes (single: `ENTRY_ERROR` /
  `POLL_CORRUPT`; batch: `BATCH_ENTRY_ERROR` / `POLL_CORRUPT`); it never
  recovers on retry: recreate the queue. A file reclaimed by archiving is
  reported as archived, not as corruption.

This table is pinned by `scripts/check_error_codes.sh`: every `MMAPQ_*`
code defined in `include/mmap_platform.h` must appear here (and in the C
strerror table, the Rust `map_rc`, and the Python `_RC_ERRORS`).
