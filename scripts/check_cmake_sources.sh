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

# Reject file(GLOB) explicitly. Without this the gate still fails closed
# (no .c tokens are extracted, so every source reports missing), but it
# fails with no diagnostic and the reader is left guessing. Name it.
if grep -qE '^[[:space:]]*file[[:space:]]*\([[:space:]]*GLOB' "$CMAKE"; then
    echo "ERROR: $CMAKE uses file(GLOB) — this is NOT the fix for source-list drift." >&2
    echo "       CMake bakes glob results at configure time; a source added later" >&2
    echo "       is not picked up until someone re-runs cmake. CONFIGURE_DEPENDS" >&2
    echo "       only re-globs at build time on generators that support it and" >&2
    echo "       costs a directory stat on every build. Keep the explicit list;" >&2
    echo "       this gate is what keeps it honest." >&2
    exit 1
fi

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
