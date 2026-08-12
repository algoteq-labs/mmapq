#!/usr/bin/env bash
# api_surface.sh — cross-reference binding symbols against C headers.
#
# Three sections:
#   SECTION 1: ffi.rs declarations with call-site counts
#   SECTION 2: setup.py cdef declarations
#   SECTION 3: headers with zero binding usage
#
# The call-site count excludes ffi.rs itself. A function with
# declared-rs=1 and callers=0 is dead code.
#
# Portable (bash + awk, mawk 1.3.4 compatible).

set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
INCLUDE_DIR="$HERE/include"
FFI_RS="$HERE/../mmapq-rs/src/ffi.rs"
SETUP_PY="$HERE/../mmapq-py/setup.py"
RS_DIRS="$HERE/../mmapq-rs/src $HERE/../mmapq-rs/tests"

TMP_FFI=$(mktemp)
TMP_CDEF=$(mktemp)
TMP_CALLS=$(mktemp)
cleanup() { rm -f "$TMP_FFI" "$TMP_CDEF" "$TMP_CALLS"; }
trap cleanup EXIT

# Extract binding function names
grep -oP 'pub fn \K[a-zA-Z_][a-zA-Z0-9_]*' "$FFI_RS" | sort -u > "$TMP_FFI"
grep -oP '\b[a-z_][a-zA-Z0-9_]*\(' "$SETUP_PY" | sed 's/($//' | sort -u > "$TMP_CDEF"

# Count call sites in Rust source (excluding ffi.rs).
# Extract both fn( and path::fn( patterns; strip path prefixes.
find $RS_DIRS -name '*.rs' ! -path '*/ffi.rs' -print0 2>/dev/null \
    | xargs -0 grep -oPh '[a-zA-Z_][a-zA-Z0-9_]*::[a-zA-Z_][a-zA-Z0-9_]*\(|\b[a-zA-Z_][a-zA-Z0-9_]*\(' 2>/dev/null \
    | sed 's/($//' \
    | sed 's/.*:://' \
    | sort | uniq -c | awk '{print $2, $1}' > "$TMP_CALLS"

# SECTION 1: ffi.rs declarations with call-site counts
echo "=== ffi.rs declarations ==="
echo -e "function\tcallers-rs\tdeclared-py"
while IFS= read -r fn; do
    calls=$(awk -v fn="$fn" '$1 == fn {print $2}' "$TMP_CALLS" || echo 0)
    in_py=$(grep -Fx "$fn" "$TMP_CDEF" >/dev/null && echo 1 || echo 0)
    echo -e "$fn\t${calls:-0}\t$in_py"
done < "$TMP_FFI"

echo ""

# SECTION 2: setup.py declarations not in ffi.rs (Python-only)
echo "=== Python-only declarations ==="
echo -e "function"
comm -23 "$TMP_CDEF" "$TMP_FFI" | grep -v '^$' | while IFS= read -r fn; do
    echo "$fn"
done

echo ""

# SECTION 3: headers with zero binding usage (internal candidates)
echo "=== Headers with zero binding usage ==="
for hdr in "$INCLUDE_DIR"/*.h; do
    hdr_name=$(basename "$hdr")
    # Extract all identifier-like words from the header
    fns_in_hdr=$(grep -oPh '\b[a-z_][a-zA-Z0-9_]{2,}\(' "$hdr" 2>/dev/null | sed 's/($//' | sort -u)
    # Check if any of those functions appear in either binding
    hits=0
    while IFS= read -r fn; do
        [ -z "$fn" ] && continue
        if grep -Fx "$fn" "$TMP_FFI" >/dev/null 2>&1 || grep -Fx "$fn" "$TMP_CDEF" >/dev/null 2>&1; then
            hits=1
            break
        fi
    done <<< "$fns_in_hdr"
    if [ "$hits" -eq 0 ]; then
        # Double-check: look for the header name pattern in bindings
        # (some bindings reference types like IndexedAppender, not functions)
        base="${hdr_name%.h}"
        if ! grep -q "$base" "$FFI_RS" 2>/dev/null && ! grep -q "$base" "$SETUP_PY" 2>/dev/null; then
            echo "$hdr_name"
        fi
    fi
done
