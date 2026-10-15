#!/usr/bin/env bash
# audit_asserts.sh — generate MMAP_ASSERT inventory from source
# Usage:
#   ./scripts/audit_asserts.sh
#   ./scripts/audit_asserts.sh --verify
set -euo pipefail
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$0")/.."

shopt -s nullglob
SRC_FILES=(src/*.c)
HDR_FILES=(src/internal/*.h)
shopt -u nullglob
if [ ${#SRC_FILES[@]} -eq 0 ] || [ ${#HDR_FILES[@]} -eq 0 ]; then
    echo "ERROR: scan scope missing (need src/*.c and src/internal/*.h) — wrong working directory?" >&2
    exit 1
fi

VERIFY="${1:-}"

# Doc path is parameterizable so --self-test can run against a mutated copy.
DOC="${ASSERT_AUDIT_DOC:-doc/ASSERT_AUDIT.md}"

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

# ── Inventory one file's MMAP_ASSERT sites.
#    A site is a line whose first non-whitespace token is MMAP_ASSERT( —
#    this excludes comment mentions (including block-comment continuation
#    lines, which the old (//|/\*) filter could not catch) and the macro's
#    own #ifndef/#define lines in mmap_assert.h.
assert_sites() {
  grep -nE '^[[:space:]]*MMAP_ASSERT\(' "$1" || true
}

# ── Extract claimed line numbers from ASSERT_AUDIT.md for a given file.
#    Handles both "Lines N, M, ..." format and table "| N ... |" format.
#    Returns sorted space-separated line numbers, or empty if not found.
get_claimed_lines() {
  local file="$1"
  local section
  section=$(sed -n "/^### $file — [0-9]/,/^### /p" "$DOC")

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

  for f in "${SRC_FILES[@]}" "${HDR_FILES[@]}"; do
    local basename raw matches count actual claimed claimed_line
    case "$f" in
      src/internal/*) basename="${f#src/internal/}" ;;
      *)              basename="${f#src/}" ;;
    esac
    raw=$(assert_sites "$f")
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

# ── Verify the I/V disposition split in the Summary table. ──────────────────
# The per-file inventory's disposition (I or V) is hand-maintained; a summary
# count that drifts from the actual rows is silent (this gate previously passed
# I=73/V=3 while the inventory had exactly 2 V rows). Checks:
#   1. I + V claims sum to the source total.
#   2. The V claim equals the actual number of `| V |` disposition rows.
verify_dispositions() {
  local errors=0
  local i_claim v_claim v_actual sum
  i_claim=$(sed -n 's/^| I (stays, re-verified) | \([0-9][0-9]*\) |.*/\1/p' "$DOC" | head -1)
  v_claim=$(sed -n 's/^| V (pending conversion) | \([0-9][0-9]*\) |.*/\1/p' "$DOC" | head -1)
  v_actual=$(grep -cE '^\| .* \| V \|' "$DOC" || true)

  sum=$(( ${i_claim:-0} + ${v_claim:-0} ))
  if [ "$sum" -ne "$total" ]; then
    echo "  disposition sum $sum != source total $total (I=$i_claim V=$v_claim)"
    errors=1
  fi
  if [ "${v_claim:-0}" -ne "$v_actual" ]; then
    echo "  V disposition claim $v_claim != actual V rows $v_actual"
    errors=1
  fi

  return "$errors"
}

total=0
files=0

for f in "${SRC_FILES[@]}" "${HDR_FILES[@]}"; do
  matches=$(assert_sites "$f")
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
    echo "ERROR: no MMAP_ASSERT sites found — wrong working directory? (checked src/*.c and src/internal/*.h)" >&2
    exit 1
fi
echo "Total: $total assert(s) across $files file(s)"

if [ "$VERIFY" = "--verify" ]; then
    claimed=$(sed -n 's/^\*\*\([0-9][0-9]*\) asserts.*$/\1/p' "$DOC")
    # Per-file first: a planted-site canary must be reported as a per-file
    # mismatch naming the file, not masked by the aggregate count check.
    if ! verify_per_file; then
        echo "VERIFY FAILED: per-file line number mismatches"
        exit 1
    fi
    if [ "$total" -ne "${claimed:-0}" ]; then
        echo "VERIFY FAILED: source has $total, ASSERT_AUDIT.md claims $claimed"
        exit 1
    fi
    if ! verify_dispositions; then
        echo "VERIFY FAILED: I/V disposition split inconsistent with inventory"
        exit 1
    fi
    echo "verification passed"
fi

if [ "$VERIFY" = "--self-test" ]; then
    STMP=$(mktemp -d)
    trap 'rm -rf "$STMP"' EXIT

    # Positive: pristine doc must pass. (SELF is absolute, resolved before the
    # top-level cd, so the recursive invocation lands on the script.)
    cp "$DOC" "$STMP/pass.md"
    if ASSERT_AUDIT_DOC="$STMP/pass.md" bash "$SELF" --verify >/dev/null 2>&1; then
        echo "  self-test pass case: OK"
    else
        echo "  self-test pass case: FAIL (pristine doc rejected)"
        exit 1
    fi

    # Negative: corrupt the V count (drift) and the disposition check must fail.
    sed 's/^| V (pending conversion) | [0-9][0-9]* |.*/| V (pending conversion) | 99 |/' "$DOC" > "$STMP/fail.md"
    if ASSERT_AUDIT_DOC="$STMP/fail.md" bash "$SELF" --verify >/dev/null 2>&1; then
        echo "  self-test fail case: FAIL (corrupted V count not detected)"
        exit 1
    else
        echo "  self-test fail case: OK"
    fi

    # Negative: drop one header site from the doc — the per-file header
    # check must catch it (header scope is D6; a doc-side mutation only,
    # the planted-source canary is manual acceptance, not self-test).
    sed 's/^Lines 138-139\./Lines 138./' "$DOC" > "$STMP/fail-hdr.md"
    if cmp -s "$DOC" "$STMP/fail-hdr.md"; then
        echo "  self-test setup error: header fail-case sed did not change the doc" >&2
        exit 1
    fi
    if ASSERT_AUDIT_DOC="$STMP/fail-hdr.md" bash "$SELF" --verify >/dev/null 2>&1; then
        echo "  self-test fail case (header line dropped): FAIL (not detected)"
        exit 1
    else
        echo "  self-test fail case (header line dropped): OK"
    fi

    echo "self-test: PASS"
    exit 0
fi
