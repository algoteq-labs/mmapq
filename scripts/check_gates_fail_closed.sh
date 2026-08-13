#!/usr/bin/env bash
# check_gates_fail_closed.sh — verify every check_* gate exits non-zero when
# its inputs are absent.  Exits 0 if all gates pass the meta-test.
# Usage: bash scripts/check_gates_fail_closed.sh [--list-registry]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# ── Registry: every gate script and its declared inputs ─────────────────────
# Format: path|args|input1 input2 ...
#   path   — relative to the workspace root; invoked as "bash $path $args"
#   args   — arguments passed to the gate
#   inputs — files/dirs whose ABSENCE must make the gate exit non-zero. One
#            meta-test per declared input. Granularity principle: declare the
#            input whose absence makes the gate's answer meaningless, not merely
#            narrower. Directories for total-zero gates, specific files for
#            cross-repo mirrors.
# Scope is derived from the inputs: any path under mmapq-rs/ or mmapq-py/ makes
# the gate cross-repo (needs sibling repos); otherwise local (runs from a bare
# mmapq clone).
declare -a REGISTRY
REGISTRY+=("mmapq/scripts/check_strdup_guards.sh||mmapq/src")
REGISTRY+=("mmapq/scripts/audit_asserts.sh|--verify|mmapq/src")
REGISTRY+=("mmapq/scripts/check_error_codes.sh||mmapq/include/mmap_platform.h mmapq-rs/src/error.rs mmapq-py/src/mmapq/_errors.py")
REGISTRY+=("mmapq/scripts/check_binding_symbols.sh||mmapq/include mmapq-rs/src/ffi.rs mmapq-py/setup.py")
REGISTRY+=("mmapq/scripts/check_readme_examples.sh|mmapq/README.md mmapq-rs/README.md mmapq-py/README.md|mmapq/README.md mmapq-rs/Cargo.toml mmapq-py/src/mmapq/__init__.py")
REGISTRY+=("mmapq-py/scripts/check_enum_sync.sh||mmapq/include mmapq-py/src/mmapq/types.py")
REGISTRY+=("mmapq-py/scripts/check_no_layout_mirrors.sh||mmapq/include mmapq-py/setup.py")

# ── --list-registry: print gates with their derived scope and inputs ─────────
if [ "${1:-}" = "--list-registry" ]; then
    echo "Registered gates (local = runs from a bare mmapq clone; cross = needs sibling repos):"
    for entry in "${REGISTRY[@]}"; do
        path="${entry%%|*}"
        rest="${entry#*|}"
        args="${rest%|*}"
        inputs="${rest##*|}"
        if echo "$inputs" | grep -q 'mmapq-rs/\|mmapq-py/'; then
            scope="cross"
        else
            scope="local"
        fi
        echo "  [$scope] $path${args:+ $args}"
        for input in $inputs; do
            echo "             input: $input"
        done
    done
    exit 0
fi

# ── Discover unregistered gates ─────────────────────────────────────────────
errors=0

check_registered() {
    local script="$1"
    local rel="${script#$ROOT/}"
    for entry in "${REGISTRY[@]}"; do
        path="${entry%%|*}"
        if [ "$path" = "$rel" ]; then
            return 0
        fi
    done
    echo "ERROR: unregistered gate: $rel" >&2
    return 1
}

while IFS= read -r script; do
    case "$script" in
        */check_sync.sh|*/check_gates_fail_closed.sh) continue ;;
    esac
    check_registered "$script" || errors=1
done < <(find "$ROOT/mmapq/scripts" "$ROOT/mmapq-py/scripts" -name 'check_*.sh' 2>/dev/null)
check_registered "$ROOT/mmapq/scripts/audit_asserts.sh" || errors=1

if [ "$errors" -gt 0 ]; then
    echo "FAIL: unregistered gates found — add them to the registry or remove them" >&2
    exit 1
fi

echo "PASS: all check_ scripts are registered"

# ── Working copy of the three repos (minimal subsets) ───────────────────────
# Copied FRESH per test. Never git-checkout-restored: cp -a does not carry .git,
# so `git checkout` fails (exit 128, swallowed) — the old harness never restored
# between tests and its results were order-dependent and cumulative.
# The mmapq-rs subset includes c/src and c/include because build.rs compiles the
# vendored C mirror; check_readme_examples compiles the crate against it.
WORK="$TMPDIR/mmapq-gate-work"

copy_subsets() {
    rm -rf "$WORK"
    mkdir -p "$WORK/mmapq/src" "$WORK/mmapq/include" "$WORK/mmapq/scripts" "$WORK/mmapq/doc"
    cp -a "$ROOT/mmapq/src/." "$WORK/mmapq/src/"
    cp -a "$ROOT/mmapq/include/." "$WORK/mmapq/include/"
    cp -a "$ROOT/mmapq/scripts/." "$WORK/mmapq/scripts/"
    cp -a "$ROOT/mmapq/doc/." "$WORK/mmapq/doc/"
    cp "$ROOT/mmapq/README.md" "$WORK/mmapq/README.md"

    mkdir -p "$WORK/mmapq-py/src/mmapq" "$WORK/mmapq-py/scripts"
    cp -a "$ROOT/mmapq-py/src/mmapq/." "$WORK/mmapq-py/src/mmapq/"
    cp "$ROOT/mmapq-py/setup.py" "$WORK/mmapq-py/setup.py"
    cp -a "$ROOT/mmapq-py/scripts/." "$WORK/mmapq-py/scripts/"
    cp "$ROOT/mmapq-py/README.md" "$WORK/mmapq-py/README.md"

    mkdir -p "$WORK/mmapq-rs/src" "$WORK/mmapq-rs/c/src" "$WORK/mmapq-rs/c/include"
    cp -a "$ROOT/mmapq-rs/src/." "$WORK/mmapq-rs/src/"
    cp -a "$ROOT/mmapq-rs/c/src/." "$WORK/mmapq-rs/c/src/"
    cp -a "$ROOT/mmapq-rs/c/include/." "$WORK/mmapq-rs/c/include/"
    cp "$ROOT/mmapq-rs/Cargo.toml" "$WORK/mmapq-rs/Cargo.toml"
    cp "$ROOT/mmapq-rs/README.md" "$WORK/mmapq-rs/README.md"
    cp "$ROOT/mmapq-rs/build.rs" "$WORK/mmapq-rs/build.rs"
}

run_gate() {
    local path="$1" args="$2"
    ( cd "$WORK" && if [ -n "$args" ]; then bash "$path" $args; else bash "$path"; fi ) \
        >/dev/null 2>&1
}

# ── Meta-test: baseline control, then one test per declared input ───────────
# Baseline control first: each gate must PASS on pristine input. If it does not,
# the subset is incomplete for that gate — report it and skip its input tests
# rather than record a string of vacuous PASSes. This is the pass-case that
# makes the fail-closed assertions meaningful (cf. check_error_codes.sh --self-test).
control_fails=0
tested_gates=0

for entry in "${REGISTRY[@]}"; do
    path="${entry%%|*}"
    rest="${entry#*|}"
    args="${rest%|*}"
    inputs="${rest##*|}"
    name="$path${args:+ $args}"

    # Baseline control: pristine copy must exit 0.
    copy_subsets
    if run_gate "$path" "$args"; then
        echo "  CONTROL-PASS: $name"
    else
        echo "  CONTROL-FAIL: $name exited non-zero on pristine input" >&2
        echo "                Possible causes: (a) subset incomplete for this gate," >&2
        echo "                (b) gate requires a toolchain the environment lacks" >&2
        echo "                (c) gate is genuinely broken. Its input tests are skipped." >&2
        control_fails=$((control_fails + 1))
        continue
    fi
    tested_gates=$((tested_gates + 1))

    # One test per declared input: that input absent must exit non-zero.
    for input in $inputs; do
        copy_subsets
        if [ -d "$WORK/$input" ]; then
            rm -rf "$WORK/$input"
        else
            rm -f "$WORK/$input"
        fi
        if run_gate "$path" "$args"; then
            echo "  FAIL: $name exited 0 when input '$input' was absent" >&2
            errors=1
        else
            echo "  PASS: $name (input '$input' absent)"
        fi
    done
done

if [ "$control_fails" -gt 0 ]; then
    echo "FAIL: $control_fails gate(s) failed the pristine-input control" >&2
    echo "      (incomplete subset, missing toolchain, or broken gate). Input tests skipped." >&2
    echo "      Fix the subset/gate/toolchain, not the registry." >&2
    errors=1
fi

if [ "$errors" -gt 0 ]; then
    exit 1
fi

echo "PASS: all $tested_gates gates fail closed on their declared inputs"
