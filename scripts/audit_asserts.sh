#!/usr/bin/env bash
# audit_asserts.sh — generate MMAP_ASSERT inventory from source
# Usage:
#   ./scripts/audit_asserts.sh
#   ./scripts/audit_asserts.sh --verify

set -euo pipefail
cd "$(dirname "$0")/.."

VERIFY="${1:-}"

total=0
files=0

for f in src/*.c; do
  matches=$(grep -n 'MMAP_ASSERT' "$f" | grep -vE '(//|/\*).*MMAP_ASSERT' || true)
  count=$(echo "$matches" | grep -c . || true)
  [ "$count" -gt 0 ] || continue
  files=$((files + 1))
  total=$((total + count))
  echo "## $(basename $f) — $count"
  echo "$matches" | while IFS=: read -r n rest; do
    echo "  Line $n: $rest"
  done
  echo ""
done

echo "---"
echo "Total: $total assert(s) across $files file(s)"

if [ "$VERIFY" = "--verify" ]; then
    claimed=$(sed -n 's/^\*\*\([0-9][0-9]*\) asserts.*$/\1/p' doc/ASSERT_AUDIT.md)
    if [ "$total" -ne "${claimed:-0}" ]; then
        echo "VERIFY FAILED: source has $total, ASSERT_AUDIT.md claims $claimed"
        exit 1
    fi
    echo "verification passed"
fi
