# Building and testing

## Building

Requires: Linux, gcc, pthread. No external dependencies.

```bash
# Build the static library and all binaries
make

# Or build specific targets
make bench echo_server            # single-message benchmark tools
make batch_bench batch_echo_server  # batch benchmark tools
```

CMake is also supported:

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

CMake builds the library and examples only; `scripts/test.sh` is the test
runner (below).

## Running the tests

```bash
./scripts/test.sh            # normal + release-flags loops
./scripts/test.sh --asan     # AddressSanitizer
./scripts/test.sh --tsan     # ThreadSanitizer (run with ASLR disabled:
                             # setarch $(uname -m) -R ./scripts/test.sh --tsan)
```

Tests run at `-O0 -g` and the NDEBUG-sensitive subset re-runs under the exact
release flags (`make print-cflags-release`) — the release build is what users
run, so guards that compile out are tested under the flags that compile them
out.

## Gate scripts

`scripts/check_sync.sh` (run from the workspace root with all three repos
checked out) verifies the generated documentation blocks, the Rust/Python
binding mirrors, the doc gates, and the vendored C mirrors. Doc-only changes
gate on `check_sync.sh` and `scripts/audit_asserts.sh --verify`, not on the
full test suite. The maintainer guide (`maintainer_guide.md`) covers the
rest of the development workflow.
