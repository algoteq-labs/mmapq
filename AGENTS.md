# AGENTS.md — mmap-c

Instructions for AI coding agents working in this repository. Read fully
before making any change. These rules are not suggestions; they encode
lessons paid for in debugging time. Where these rules and your judgment
disagree, follow the rules and flag the disagreement in your summary.

## What this project is

A small, frozen-scope, low-latency C library: a persistent, indexed,
multi-appender shared-memory queue over memory-mapped files, binary-compatible
with the Java `tools4j/mmap` project. Cross-process correctness under
contention is the entire value of this code. Nanoseconds matter on the hot
path. The on-disk format is a compatibility contract.

Companion repositories `mmap-c-rs` (Rust) and `mmap-c-py` (Python) wrap this
library; `mmap-c-rs/c/` carries a vendored mirror of `src/` and `include/`.
This file is the upstream authority for the whole ecosystem: the wrappers'
AGENTS.md files defer to it, and where they differ, this file wins.

Before touching the header layout, payload framing, memory orderings, or
anything performance-adjacent, read `doc/maintainer_guide.md` — it is the
design authority, including an Anti-Pattern Ledger (§4) of optimizations
that were tried, measured, rejected, and must not be re-attempted. This
file governs conduct; that file governs design.

## Prime directives

1. **The feature set is frozen.** Do not add features, APIs, configuration
   knobs, convenience helpers, or "ecosystem" of any kind unless the task
   explicitly requests them. Useful higher-level functionality belongs in
   separate projects, never here. If a task seems to need a new public API,
   stop and ask.

2. **Never change the on-disk layout.** Byte offsets, file sizes, file
   naming, header encoding, payload framing, and the 64-byte meta layout are
   binary contracts shared with Java and with existing files on disk.
   `include/header_codec.h` and the layout block in `include/queue_meta.h`
   are the specification. If a task appears to require a layout change, stop
   and ask — do not improvise migration logic.

3. **Do only what the task says.** No drive-by refactors, no reformatting of
   surrounding code, no "while I was in there" improvements, no renames for
   taste. A change outside the task's file manifest is a defect even if it
   compiles and passes.

4. **Under-claim.** Your completion report must distinguish "implemented and
   verified by executing X" from "implemented, not verified." Reporting a
   change as done that is not in the diff is the worst possible failure mode
   in this repository's history. When in doubt, say less.

## Correctness rules (hard)

- **`MMAP_ASSERT` is for internal invariants only — never for validation.**
  It compiles to nothing under `-DNDEBUG`, which is how release builds ship.
  Anything checking caller input, on-disk data, file contents, allocation
  results on non-hot paths, or cross-process state must be a real runtime
  check that returns an error code. This rule exists because an assert-only
  length check once allowed silent data corruption in release builds.

- **Error codes are the `MMAPQ_*` family in `include/mmap_platform.h`**
  (`MMAPQ_ETIMEDOUT -1`, `MMAPQ_EINVAL -2`, `MMAPQ_ECLOSED -3`,
  `MMAPQ_ECORRUPT -4`, `MMAPQ_ESCHEMA -5`). New failure modes get a new code
  appended, never a reused one; existing values are ABI. Both language
  wrappers map these codes 1:1 — adding a code means adding one line to
  `map_rc` in `mmap-c-rs/src/error.rs` and one entry to `_RC_ERRORS` in
  `mmap-c-py/src/mmap_c_py/_errors.py` in the same change.

- **Concurrency and atomics changes require justification in comments.**
  This code uses C11 atomics with deliberate memory orderings (the payload
  write → acq_rel CAS header commit is the correctness core). Do not add,
  remove, weaken, or strengthen an ordering without stating in a comment why
  the ordering is what it is. Do not introduce GCC `__atomic` builtins; use
  C11 `atomic_*` consistently. If you cannot explain why `relaxed` is safe,
  it isn't — ask.

- **Hot path is sacred.** `indexed_appender_append`, the poll paths, header
  read/write, and payload framing must not gain allocations, locks, syscalls,
  string formatting, or branches beyond what a task explicitly requires.
  Validation branches at API boundaries are accepted; anything else on the
  hot path needs a stated justification.

- **Zero-length is the end-of-stream sentinel** in payload framing. Nothing
  may ever write a zero length prefix for a message. All append paths reject
  `length <= 0` with `MMAPQ_EINVAL`; keep it that way.

## Testing rules (hard)

- **Every bug fix ships with a regression test that fails before the fix.**
  Write the test first, run it, observe the failure, then fix. A fix whose
  test never failed proves nothing — this repository once accepted a
  reported fix that did not exist because this step was skipped.

- **Tests must pass under BOTH build configurations.** `scripts/test.sh`
  runs the suite at `-O0 -g` and re-runs NDEBUG-sensitive tests under the
  exact release flags (`make print-cflags-release`). Any test guarding a
  check that could be compiled out belongs in the release-flags loop. The
  release build is what users run; a test that only passes at `-O0` tests
  nothing that ships.

- **Never delete or weaken an existing test to make a change pass.** If a
  test fails after your change, the default assumption is your change is
  wrong. If you believe the test itself is wrong, stop and report it —
  removing it is not your call.

- Tests use the vendored Unity framework in `test/`; match the style of
  neighboring tests (setup/teardown, `/tmp` queue dirs cleaned in teardown,
  `RUN_TEST` registration — a test function without registration does not
  run and its "presence in the file" is worthless).

- Sanitizer targets exist (`test.sh` supports ASan/TSan runs). Concurrency
  changes are not done until TSan is clean.

## Build rules

- Build: `make` (release: `-O3 -flto -DNDEBUG`), tests: `bash scripts/test.sh`.
- Do not modify release CFLAGS, add dependencies, or introduce build systems.
  The library is dependency-free C11 (gnu11) + pthreads by design.
- Compile warnings are defects: the tree builds clean under
  `-Wall -Wextra`; a change that introduces a warning is not complete.
  Check return values (`fread`, `snprintf`, etc.) — `-Wunused-result` is not
  noise here, unchecked reads have concealed real test bugs.

## Documentation style (hard)

- Header comments use this project's house style: prose block comments with
  aligned `name: description` parameter lists. **No Javadoc/Doxygen tags** —
  no `@param`, `@return`, `@brief`. Match the surrounding file exactly.
- Comments carry design rationale and invariants ("only place that handles
  padding"), not restatements of the code. If a comment says nothing the
  signature doesn't, delete it before it's written.
- If your change makes an existing comment false, fixing that comment is in
  scope; rewriting comments that are still true is not.

## Mirror discipline

Any change under `src/` or `include/` must be synced verbatim into
`mmap-c-rs/c/src` and `mmap-c-rs/c/include` in the same task, and verified
mechanically, both directions:

```
diff -r src ../mmap-c-rs/c/src -x '*.o'
diff -r include ../mmap-c-rs/c/include
```

Exit code zero or the task is not done. Do not hand-eye it.

## Scope of the C port (do not "fix" these)

- `archivable` is a creation-time factory parameter (default 0). Existing
  queues keep their on-disk value on reopen. Archivable reads pay an
  acquire load on `start_index` — see doc/maintainer_guide.md before
  touching those paths.
- Fixed-8-byte payload mode (`queue_type == 1`) is Java-only; the C library
  refuses to open such queues. Do not implement it.
- Batch queues (`queue_type == 2`) are C-ecosystem only; Java cannot read
  them. This is documented, not a bug.
- Recovery detects on-disk corruption (`MMAPQ_ESCHEMA`/`MMAPQ_ECORRUPT`
  refusal) but never repairs. Queues are transient; durable data lives in
  the export format. Do not add repair or migration logic.
- Target platform is Linux x86-64/ARM64, little-endian, POSIX. Do not add
  Windows/macOS conditionals unless the task asks.

## Completion report format

For every task, report: files changed (must match the task's manifest);
tests added, by name; commands executed with their actual results
(test counts per binary, not aggregate totals); the mirror diff exit code;
and any deviation from the task or from this file, explicitly. Claims
without executed evidence will be independently re-verified — write the
report you'd want audited, because it will be.
