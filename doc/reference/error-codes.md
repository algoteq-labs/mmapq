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
| `MMAPQ_ETIMEDOUT` | -1 | Mapping timeout or I/O wait exceeded (a region was not mapped in time) | **Yes** — transient | `MmapError::MapTimedOut` | `MmapqError` |
| `MMAPQ_EINVAL` | -2 | Invalid argument (NULL, zero length, index out of range) | No — caller bug | `MmapError::InvalidArgument` | `ValueError` |
| `MMAPQ_ECLOSED` | -3 | Operation on a closed handle | No | `MmapError::QueueClosed` | `QueueClosedError` |
| `MMAPQ_ECORRUPT` | -4 | On-disk corruption detected; also a malformed export stream. Note: a version-1 stream that fails to parse may be a legacy big-endian Java export — re-export it with a fixed Java (version 2) | No — recreate the queue (restore history via export/import) | `MmapError::Corrupt` | `QueueCorruptError` |
| `MMAPQ_ESCHEMA` | -5 | Schema version or queue-type mismatch, on-disk geometry not supported by this build/host (a queue is portable only to hosts whose page size divides its region size), or an export-stream version mismatch (1 and 2 accepted) | No — the file is refused, never misread | `MmapError::SchemaMismatch` | `SchemaMismatchError` |
| `MMAPQ_EEXIST` | -6 | Destination queue already exists (transfer without append mode) | No | `MmapError::Exists` | `QueueExistsError` |
| `MMAPQ_ENOENT` | -7 | File or queue does not exist | No | `MmapError::FileNotFound` | `FileNotFoundError` |
| `MMAPQ_EIDCONFLICT` | -8 | Requested appender ID already in use (a killed writer's lane stays taken until `mmapq-ids --unset` on the dead process) | No — release the ID first | `MmapError::IdConflict` | `AppenderIdConflictError` |
| `MMAPQ_ENOMEM` | -9 | Internal allocation failure | Back off; do not retry in a hot loop | `MmapError::OutOfMemory` | `MmapOutOfMemoryError` |
| `MMAPQ_EIO` | -10 | I/O error (metadata write failure) | **Yes** — retry | `MmapError::Eio` | `MmapqError` |
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
- A missing permission on the queue's files, or a queue path with a
  component that is not a directory, is currently reported as
  `MMAPQ_ETIMEDOUT` (`POLL_ERROR` on a poll) on every attempt, although
  retrying does not help — see the access each participant needs in
  [chapter 9 of the guide](../guide/09-trade-offs-and-limits.md).
- A base directory that does not exist is reported the same way: the first
  appender returns `MMAPQ_ETIMEDOUT` on every attempt. The library creates
  only the queue's own directory inside the base directory — create the
  base directory first.
- A payload file missing under a committed message — deleted by hand or
  damaged, outside the persistence contract — is reported by the
  single-message reader and poller as `MMAPQ_ETIMEDOUT` (`ENTRY_ERROR` /
  `POLL_ERROR`) and by the batch reader and poller as `MMAPQ_ECORRUPT`
  (`BATCH_ENTRY_ERROR` / `POLL_CORRUPT`). Neither recovers on retry:
  recreate the queue.

This table is pinned by `scripts/check_error_codes.sh`: every `MMAPQ_*`
code defined in `include/mmap_platform.h` must appear here (and in the C
strerror table, the Rust `map_rc`, and the Python `_RC_ERRORS`).
