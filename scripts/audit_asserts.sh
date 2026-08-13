#!/usr/bin/env bash
# audit_asserts.sh — generate MMAP_ASSERT inventory from source
# Usage:
#   ./scripts/audit_asserts.sh
#   ./scripts/audit_asserts.sh --verify
set -euo pipefail
cd "$(dirname "$0")/.."

VERIFY="${1:-}"

# ── Parse a line-list string like "30, 269, 309, 323, 342, 358, 366"
#    or "81, 152-153, 157, 179-181" into sorted space-separated numbers.
parse_line_list() {
  local list="$1" out=""
  list="${list#Lines }"
  list="${list%%.*}"  # remove trailing comment after period
  # Rebuild into comma-separated from all digit/digit-range patterns
  list=$(echo "$list" | grep -oE '[0-9]+(-[0-9]+)?' | tr '\n' ',' | sed 's/,$//')

  IFS=',' read -ra parts <<< "$list"
  for part in "${parts[@]}"; do
    part="${part## }"
    part="${part%% }"
    if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      for ((i = ${BASH_REMATCH[1]}; i <= ${BASH_REMATCH[2]}; i++)); do
        out="$out $i"
      done
    else
      out="$out $part"
    fi
  done
  echo "$out" | tr ' ' '\n' | sort -n | tr '\n' ' '
}

# ── Extract claimed line numbers from ASSERT_AUDIT.md for a given file.
#    Handles both "Lines N, M, ..." format and table "| N ... |" format.
#    Returns sorted space-separated line numbers, or empty if not found.
get_claimed_lines() {
  local file="$1"
  local section
  section=$(sed -n "/^### $file — [0-9]/,/^### /p" doc/ASSERT_AUDIT.md)

  # Try "Lines N, M, ..." format first
  local line_list
  line_list=$(echo "$section" | grep '^Lines ' | head -1 || true)
  if [ -n "$line_list" ]; then
    echo "$line_list"
    return
  fi

  # Try table format: | N ...
  local table_lines
  table_lines=$(echo "$section" | grep '^| [0-9]' || true)
  if [ -n "$table_lines" ]; then
    local nums
    nums=$(echo "$table_lines" | sed -n 's/^| \([0-9]*\).*/\1/p' | tr '\n' ' ')
    echo "Lines ${nums% }."
    return
  fi

  true
}

# ── Compare per-file line numbers against ASSERT_AUDIT.md.
#    Exits 1 on any mismatch.
verify_per_file() {
  local errors=0

  for f in src/*.c; do
    local basename="${f#src/}"
    local raw matches count
    raw=$(grep -n 'MMAP_ASSERT' "$f" | grep -vE '(//|/\*).*MMAP_ASSERT' || true)
    matches=$(echo "$raw" | grep -c . || true)
    [ "$matches" -gt 0 ] || continue

    # Extract actual line numbers
    actual=""
    while IFS=: read -r n rest; do
      actual="$actual $n"
    done <<< "$raw"
    actual="${actual## }"
    actual="${actual%% }"

    # Get claimed line numbers from doc
    claimed_line=$(get_claimed_lines "$basename")
    if [ -z "$claimed_line" ]; then
      echo "  $basename: no line list found in ASSERT_AUDIT.md"
      errors=1
      continue
    fi
    claimed=$(parse_line_list "$claimed_line")
    claimed="${claimed## }"
    claimed="${claimed%% }"

    if [ "$actual" != "$claimed" ]; then
      echo "  $basename: line number mismatch"
      echo "    actual: $actual"
      echo "    doc:    $claimed"
      errors=1
    fi
  done

  return "$errors"
}

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
if [ "$total" -eq 0 ]; then
    echo "ERROR: no MMAP_ASSERT sites found — wrong working directory? (checked src/*.c)" >&2
    exit 1
fi
echo "Total: $total assert(s) across $files file(s)"

if [ "$VERIFY" = "--verify" ]; then
    claimed=$(sed -n 's/^\*\*\([0-9][0-9]*\) asserts.*$/\1/p' doc/ASSERT_AUDIT.md)
    if [ "$total" -ne "${claimed:-0}" ]; then
        echo "VERIFY FAILED: source has $total, ASSERT_AUDIT.md claims $claimed"
        exit 1
    fi
    if ! verify_per_file; then
        echo "VERIFY FAILED: per-file line number mismatches"
        exit 1
    fi
    echo "verification passed"
fi
