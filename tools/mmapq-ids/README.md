# mmapq-ids (tools/mmapq-ids)

Operator tool for mmapq, imported from the standalone `mmapq-tools` repo
(commit `b740fd4`, "feat: add standalone appender ID inspection and manual
reclamation"; that repo is archived with a pointer here). It inspects
appender IDs and explicitly releases a selected ID, using a reusable C
module, `mmapq_appender_id_admin`, rather than linking the queue library.

The module takes mmapq's pool layout from a single source of truth: it
includes `src/internal/appender_id_pool.h` at compile time (constants only —
no mmapq object code is linked), and `_Static_assert`s pin the bitmap/ID-space
fit and the 64-byte region size (the Java binary-compatibility contract).
Pool-layout drift is a compile error in this tool, not silent divergence.

**Neither the module nor the CLI verifies ownership, process liveness, or
whether an ID is orphaned.** Before release, the operator must ensure the ID is
unused and prevent acquisition or restart during reclamation. Prefer a
maintenance window with producers stopped and automatic restarts disabled.
Clearing an active ID can allow two writers into the same payload lane and
corrupt the queue. CAS preserves neighboring bits; it does not verify ownership.

## Build and use

Linux x86-64 or ARM64, little-endian, with lock-free 32-bit atomics. Requires a C11
compiler and Make. No link to libmmapq, and no dependency on Rust, Python, or a
running service to build or use the module and CLI. Python 3 is used only by the
module's unit/CLI tests. Ordinary builds enable `-Wall -Wextra`; `make test`
rebuilds with `-Werror` in separate test directories so compiler warnings fail
validation, not user builds.

```sh
make mmapq-ids          # from the mmapq repo root (or: make -C tools/mmapq-ids)
tools/mmapq-ids/build/release/mmapq-ids /data orders
# Only after satisfying the operator preconditions above:
tools/mmapq-ids/build/release/mmapq-ids /data orders --unset 7
```

Listing prints acquired IDs and lanes with metadata/payload files, their sizes,
and latest modification time in UTC. These are observations, not a coherent
snapshot. mmap write activity is not reliably reflected in file mtimes; an old
mtime does not establish a dead writer. Listing is read-only and requires no
write access to the pool.

CLI exit codes: **0** for successful inspection/release, **1** for an already-free
ID, **2** for an error. Successful release prints the operator warning to stderr.
No prompt is used, so invocation is suitable for explicit operator automation.
The command does not restart producers or reacquire an ID.

## C module

The public API is in [mmapq_appender_id_admin.h](include/mmapq_appender_id_admin.h):

```c
#include <mmapq_appender_id_admin.h>

MmapqAppenderIdFacts facts[MMAPQ_APPENDER_ID_ADMIN_COUNT];
int error = mmapq_appender_id_admin_inspect("/data", "orders", facts);
/* error == 0: inspect facts; otherwise error is a positive errno value. */
```

`mmapq_appender_id_admin_release(directory, queue, id, &released)` returns zero
or a positive errno value. `released` becomes 1 immediately when CAS clears the
bit. It remains 1 even if `msync(MS_SYNC)` or cleanup subsequently fails. **An
error therefore does not necessarily mean nothing changed.** Handle that result
explicitly rather than blindly retrying after another owner could acquire the ID.
An already-free ID returns zero with `released == 0` and performs no writeback.

Link `build/release/libmmapq_appender_id_admin.a` or the corresponding `.so`;
add `-Iinclude`. The module has no printing, process-global mutable state, or
owner registry. Error codes are errno values, not the queue library's negative
`MMAPQ_*` codes. Inspection zeroes the output array on any error, rather than
returning incomplete information as success.

## File contract and boundaries

- Opens existing files only; never creates a pool or queue.
- Requires a regular, exactly 64-byte pool file. The first 32 bytes hold 256
  acquired-ID bits in eight little-endian 32-bit words. Reserved bytes are
  preserved. This matches `mmapq/src/internal/appender_id_pool.h`.
- Works for normal and batch queues without inspecting their queue metadata.
  A damaged unrelated metadata file does not prevent releasing a known ID.
- Refuses symlink queue entries, symlink pools, and nonregular canonical lane
  entries. The explicitly supplied base-directory path may resolve symlinks.
- Inspects only canonical `appender_meta_<id>` and `payload_<id>_<index>` names.
  Decimal components have no signs, suffixes, or redundant leading zeroes;
  IDs are 0–255 and file indices fit a signed 64-bit integer.
- Directory/stat failures, including a matching file disappearing mid-scan,
  are errors. Operators may repeat inspection after concurrent file activity
  settles. Inspection and release use a pinned directory descriptor, but no
  mechanism protects against external truncation/replacement of mapped files.
  Do not alter the pool's layout or replace/truncate it during either operation.
- Only the selected bit is changed. A changed-bit success includes checked
  synchronous writeback, not a general power-loss durability guarantee for the
  queue or its filesystem. An unrelated queue-file change is never needed.
- Release (`--unset` / `mmapq_appender_id_admin_release`) is valid only when
  the process holding the ID is **dead** (owner ruling, CAND-50 item 1;
  CAND-54 §5). A SIGSTOPped live process plus a release means two writers on
  the lane: the single-writer guarantee — on which the queue library's
  commit-intent protocol (CAND-54) rests — is void.

This is manual ID administration, not automatic crash recovery or queue repair.
PID ownership tracking, Rust/Python packages, and general queue verification are
outside this initial scope.

## Tests

```sh
make -C tools/mmapq-ids test    # module + CLI tests, debug and release
```

The module/CLI suite uses `PYTHON` (default `python3`, overridable, e.g.
`make test PYTHON=/path/to/python3`). It checks all 256 IDs, reserved bytes,
exact filename parsing, malformed/missing files, symlinks/FIFOs, read-only
access, CLI behavior, resource cleanup, and injected failures. Cross-process
tests race independent neighboring ID acquisitions against releases on a
synthetic pool. Those tests establish bit preservation, not safe release of
an actively owned ID.

The real-queue integration test is `tests/test_ids_integration.c` (no Python
or mmapq-py dependency), registered in mmapq's `scripts/test.sh` — which also
builds the CLI and runs the module/CLI suite above when `tools/mmapq-ids`
exists. On the CAND-51e fork/kill harness: a child holding ID 7 with an
uncommitted record is SIGKILLed; re-acquisition fails with
`MMAPQ_EIDCONFLICT`; `mmapq-ids --unset 7` exits 0 having changed exactly one
bit of exactly one file; re-attach then succeeds and the record is recovered
through the uncommitted iterators and committed. Both queue types, plus a
damaged-meta variant (the tool never opens queue metadata). These are
process-crash tests, not reboot or power-loss tests.

## License

BSD 3-Clause — see [LICENSE](LICENSE) — carried verbatim from the standalone
`mmapq-tools` repo at import (the surrounding mmapq repository is MIT; the
import keeps the tool's original license by owner ruling, 2026-10-22).
