#!/usr/bin/env bash
# check_strdup_guards.sh — verify every strdup() is followed by a NULL check.
# Hard gate: exits 0 when all strdup() calls have guards, non-zero otherwise.
set -euo pipefail

cd "$(dirname "$0")/.."

errors=0
total=0

while IFS= read -r match; do
    total=$((total + 1))
    file="${match%%:*}"
    rest="${match#*:}"
    linenum="${rest%%:*}"

    # Read the next non-blank, non-comment line after the strdup site
    next=$(tail -n "+$((linenum + 1))" "$file" \
        | grep -v -E '^\s*(//|/\*|\*[^/]|\s*$)' \
        | head -1 || true)

    if [ -z "$next" ] || ! echo "$next" | grep -q "== NULL"; then
        echo "ERROR: $file:$linenum: strdup() without NULL check on the following line" >&2
        errors=$((errors + 1))
    fi
done < <(grep -rn "strdup(" src/*.c)

if [ "$total" -eq 0 ]; then
    echo "ERROR: no strdup() call sites found — wrong working directory?" >&2
    exit 1
fi

if [ "$errors" -gt 0 ]; then
    echo "FAIL: $errors / $total strdup() calls lack NULL checks" >&2
    exit 1
fi

echo "PASS: all $total strdup() calls have NULL checks"
