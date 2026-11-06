#!/usr/bin/env bash
# check_api_coverage.sh — every public C function has at least one direct
# call in the C tests (exit criterion E2 / COV-01).
#
# Public surface, by the COV-01 method:
#   - functions declared in the include/PUBLIC_HEADERS headers AND present
#     as global text symbols in nm's dump of libmmapq.a (the intersection,
#     not the union — mmapqi_* internals and non-public exports drop out),
#   - plus the static-inline functions defined in those headers (they have
#     no archive symbol; the linker gate keeps them mmapq_-prefixed).
#
# A function counts as tested only when test/*.c CALLS it — the symbol
# followed by '(' — after C comments are stripped, so a header-doc quote
# in a comment is not coverage.
#
# Fail closed: a missing archive, an empty export list, an empty public
# function list, a missing test/ directory or a missing PUBLIC_HEADERS
# manifest is a FAIL, never a PASS.
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

ARCHIVE=libmmapq.a
[ -f "$ARCHIVE" ] || fail "$ARCHIVE not found — run make (the gate reads nm exports of the built archive)"
[ -f include/PUBLIC_HEADERS ] || fail "include/PUBLIC_HEADERS not found"
[ -d test ] || fail "test/ not found — no C test call sites to scan"

# ── Public function set ─────────────────────────────────────────────────────

# Global defined text symbols from the archive. Empty is a fail-closed
# condition, not a vacuous pass.
EXPORTS=$(nm -g --defined-only "$ARCHIVE" | awk '$2 == "T" { print $3 }' | sort -u)
[ -n "$EXPORTS" ] || fail "empty export list from $ARCHIVE (no defined text symbols)"

# Intersection: exported symbols named in a public header.
PUBLIC=""
while IFS= read -r sym; do
    while IFS= read -r h; do
        if grep -qw "$sym" "include/$h"; then
            PUBLIC="${PUBLIC}${sym}"$'\n'
            break
        fi
    done < include/PUBLIC_HEADERS
done <<< "$EXPORTS"

# Static-inline functions defined in the public headers (no archive symbol).
# The name is the mmapq_* identifier immediately before the parameter list,
# so multi-token return types ("static inline const char *mmapq_version")
# are captured; every .h in include/ is listed in PUBLIC_HEADERS (check_sync
# verifies the manifest matches include/*.h exactly).
INLINES=$(grep -h '^static inline' include/*.h \
    | grep -oE 'mmapq_[a-zA-Z0-9_]+[[:space:]]*\(' \
    | tr -d ' \t(' | sort -u)

FUNCS=$(printf '%s%s\n' "$PUBLIC" "$INLINES" | sort -u | grep -v '^$')
[ -n "$FUNCS" ] || fail "empty public function list (no exported header symbol, no static inline)"

# ── Call sites: test/*.c with C comments stripped ───────────────────────────

strip_c_comments() {
    # POSIX awk: removes /* ... */ (may span lines) and // to end of line.
    awk '
    {
        line = $0; out = ""; i = 1; n = length(line)
        while (i <= n) {
            two = substr(line, i, 2)
            if (in_block) {
                if (two == "*/") { in_block = 0; i += 2 } else { i++ }
                continue
            }
            if (two == "/*") { in_block = 1; i += 2; continue }
            if (two == "//") break
            out = out substr(line, i, 1); i++
        }
        print out
    }' "$@"
}

CALLS=$(strip_c_comments test/*.c)
[ -n "$(printf '%s' "$CALLS" | tr -d '[:space:]')" ] || fail "test/*.c stripped to nothing — no call sites to scan"

# ── Coverage ────────────────────────────────────────────────────────────────

uncovered=""
total=0
while IFS= read -r sym; do
    total=$((total + 1))
    # grep without -q: -q exits at the first match, the printf side of the
    # pipe dies of SIGPIPE, and pipefail reports the pipeline as failed —
    # every symbol would look uncovered. /dev/null keeps grep reading to
    # the end so both sides exit cleanly.
    if ! printf '%s\n' "$CALLS" | grep -E "\b${sym}[[:space:]]*\(" > /dev/null; then
        uncovered="${uncovered}  ${sym}"$'\n'
    fi
done <<< "$FUNCS"

if [ -n "$uncovered" ]; then
    echo "FAIL: public function(s) with no direct call in test/*.c (E2 / COV-01):" >&2
    printf '%s' "$uncovered" >&2
    exit 1
fi

echo "PASS: all $total public functions have at least one direct call in test/*.c"
