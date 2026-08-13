#!/usr/bin/env bash
# check_cmake_sources.sh — verify every src/*.c is in src/CMakeLists.txt's
# add_library list. Hard gate: exits non-zero when a C source exists on disk
# but is missing from the CMake source list (it would silently drop out of
# CMake builds), or when the list names a file that no longer exists.
#
# CMakeLists.txt is the last hand-maintained explicit source list (setup.py
# and mmapq-rs/build.rs glob). A CMake `file(GLOB)` is NOT the fix — glob
# results are baked at configure time and go stale; the explicit list is
# correct, it just must be kept in sync, which is what this gate enforces.
set -euo pipefail

cd "$(dirname "$0")/.."

CMAKE=src/CMakeLists.txt
[ -f "$CMAKE" ] || { echo "ERROR: $CMAKE not found — wrong working directory?" >&2; exit 1; }

# Extract every *.c token inside the add_library(...) block.
cmake_sources=$(sed -n '/add_library(mmapq STATIC/,/)/p' "$CMAKE" \
    | grep -oE '[A-Za-z0-9_./-]+\.c' | sort -u)

errors=0
total=0

for f in src/*.c; do
    [ -e "$f" ] || continue
    total=$((total + 1))
    base="${f#src/}"
    if ! grep -qx "$base" <<<"$cmake_sources"; then
        echo "ERROR: $base is on disk but missing from add_library in $CMAKE" >&2
        errors=$((errors + 1))
    fi
done

if [ "$total" -eq 0 ]; then
    echo "ERROR: no src/*.c files found — wrong working directory?" >&2
    exit 1
fi

# Reverse direction: a source listed in CMake that does not exist.
while IFS= read -r base; do
    [ -n "$base" ] || continue
    if [ ! -f "src/$base" ]; then
        echo "ERROR: $base listed in $CMAKE but no such file in src/" >&2
        errors=$((errors + 1))
    fi
done <<<"$cmake_sources"

if [ "$errors" -gt 0 ]; then
    echo "FAIL: $errors CMake source-list mismatch(es) across $total C sources" >&2
    exit 1
fi

echo "PASS: all $total src/*.c files present in $CMAKE (and no stale entries)"
