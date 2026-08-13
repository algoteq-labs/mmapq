#!/usr/bin/env bash
# check_gates_fail_closed.sh — verify every check_* gate exits non-zero when
# its inputs are absent.  Exits 0 if all gates pass the meta-test.
# Usage: bash scripts/check_gates_fail_closed.sh [--list-registry]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ── Registry: every gate script and its declared inputs ─────────────────────
# Format: repo_relative_path inputs_absent_cmd
# inputs_absent_cmd: a command that, when run from $ROOT, removes/hides the
#                    inputs the gate needs.  Uses the real gate script &
#                    expects non-zero exit.
declare -a REGISTRY
REGISTRY+=("mmapq/scripts/check_strdup_guards.sh|rm -f mmapq/src/*.c")
REGISTRY+=("mmapq/scripts/check_error_codes.sh|rm -f mmapq/include/mmap_platform.h")
REGISTRY+=("mmapq/scripts/audit_asserts.sh --verify|rm -f mmapq/src/indexed_appender.c")
REGISTRY+=("mmapq/scripts/check_readme_examples.sh|rm -f mmapq/README.md")
REGISTRY+=("mmapq-py/scripts/check_enum_sync.sh|rm -rf mmapq/include")
REGISTRY+=("mmapq-py/scripts/check_no_layout_mirrors.sh|rm -rf mmapq/include")
REGISTRY+=("mmapq/scripts/check_binding_symbols.sh|rm -rf mmapq/include")

if [ "${1:-}" = "--list-registry" ]; then
    echo "Registered gates:"
    for entry in "${REGISTRY[@]}"; do
        gate="${entry%%|*}"
        echo "  $gate"
    done
    exit 0
fi

# ── Discover unregistered gates ─────────────────────────────────────────────
# Glob every check_*.sh and audit_asserts.sh; flag anything not in the registry.
errors=0

check_registered() {
    local script="$1"
    local rel="${script#$ROOT/}"
    for entry in "${REGISTRY[@]}"; do
        gate="${entry%%|*}"
        if [ "$gate" = "$rel" ] || [ "$gate" = "$rel --verify" ]; then
            return 0
        fi
    done
    # Check without --verify suffix
    local base="${rel% --verify}"
    for entry in "${REGISTRY[@]}"; do
        gate="${entry%%|*}"
        if [ "$gate" = "$base" ]; then
            return 0
        fi
    done
    echo "ERROR: unregistered gate: $rel" >&2
    return 1
}

# Discover from mmapq (exclude aggregator and meta-gate itself)
while IFS= read -r script; do
    case "$script" in
        */check_sync.sh|*/check_gates_fail_closed.sh) continue ;;
    esac
    check_registered "$script" || errors=1
done < <(find "$ROOT/mmapq/scripts" "$ROOT/mmapq-py/scripts" -name 'check_*.sh' 2>/dev/null)
check_registered "$ROOT/mmapq/scripts/audit_asserts.sh --verify" || errors=1

if [ "$errors" -gt 0 ]; then
    echo "FAIL: unregistered gates found — add them to the registry or remove them" >&2
    exit 1
fi

echo "PASS: all $(find "$ROOT/mmapq/scripts" "$ROOT/mmapq-py/scripts" -name 'check_*.sh' 2>/dev/null | wc -l) check_ scripts are registered"

# ── Test each gate fails with absent inputs ─────────────────────────────────
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Copy the relevant trees
cp -a "$ROOT/mmapq" "$TMPDIR/mmapq"
cp -a "$ROOT/mmapq-py" "$TMPDIR/mmapq-py"

# Remove any c/ mirror that would satisfy the include check
rm -rf "$TMPDIR/mmapq-rs" 2>/dev/null || true

# Source repo-root env
cd "$TMPDIR"

for entry in "${REGISTRY[@]}"; do
    gate="${entry%%|*}"
    cmd="${entry#*|}"
    
    # Reset tree for each test
    git checkout -- "$TMPDIR/mmapq" 2>/dev/null || true
    git checkout -- "$TMPDIR/mmapq-py" 2>/dev/null || true
    
    # Apply the inputs-absent command
    eval "$cmd" 2>/dev/null || true
    
    set +e
    bash "$gate" > /dev/null 2>&1
    rc=$?
    set -e
    
    if [ "$rc" -eq 0 ]; then
        echo "FAIL: $gate exited 0 when inputs were absent" >&2
        errors=1
    else
        echo "  PASS: $gate -> exit $rc"
    fi
done

if [ "$errors" -gt 0 ]; then
    exit 1
fi

echo "PASS: all gates fail closed"
