#!/usr/bin/env bash
set -u

# check_readme_examples.sh — extract fenced code blocks from README files and
# verify they compile (C, Rust) or that mmapq imports resolve (Python).
#
# Usage:
#   ./scripts/check_readme_examples.sh [README.md ...]
#   ./scripts/check_readme_examples.sh --self-test
#
# If no arguments are given, checks mmapq/README.md (C blocks only, for
# backward compatibility).
#
# Language detection by fenced block marker:
#   ```c       → compile with gcc -I include
#   ```rust    → cargo check via temp project (with --examples)
#   ```python  → static import-name check: parses `from mmapq import X` lines
#                and verifies each X against mmapq.__all__ from the source tree
#                (no build required). This catches the Phase 21 defect class
#                (README references names that don't exist) but NOT wrong method
#                calls, module-attribute access, or argument mismatches.
#   ```ignore  → skipped (documented as non-compilable)
#   (other)    → skipped

FAIL=0
TOTAL=0
PASS=0
SKIP=0
# Coverage-gap skips (toolchain opt-outs). Distinct from intentional SKIPs
# (bash/ignore blocks): these represent incomplete verification and CI must
# assert GATE_SKIP == 0.
GATE_SKIP=0

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

check_c_block() {
    local readme="$1" block="$2" src="$3"
    local readme_dir
    readme_dir=$(cd "$(dirname "$readme")" && pwd)
    local cflags="-std=gnu11 -D_GNU_SOURCE -I $readme_dir/include"
    local pre="$TMPDIR/c_pre_${block}"
    local body="$TMPDIR/c_body_${block}"
    local out="$TMPDIR/c_out_${block}"

    # Classify each line: #include and static function definitions go to
    # file scope (pre), everything else goes inside main() (body).
    # A static function starts at "static ... (" and ends when brace
    # depth returns to the level at entry. Entry brace is counted from
    # the opening line itself (may contain "{").
    awk '
        /^#include/ { print > "'"$pre"'"; next }
        /^static / && /\(/ {
            in_static = 1; brace = 1
            for (i = 1; i <= length($0); i++) {
                c = substr($0, i, 1)
                if (c == "{") brace++
                if (c == "}") brace--
            }
            print > "'"$pre"'"; next
        }
        in_static {
            print > "'"$pre"'"
            for (i = 1; i <= length($0); i++) {
                c = substr($0, i, 1)
                if (c == "{") brace++
                if (c == "}") brace--
            }
            if (brace <= 1) in_static = 0
            next
        }
        { print > "'"$body"'" }
    ' "$src"

    {
        echo '#include <stdio.h>'
        echo '#include <assert.h>'
        cat "$pre" 2>/dev/null
        echo 'int main(void) {'
        cat "$body"
        echo 'return 0; }'
    } > "$out"

    if gcc $cflags -x c "$out" -c -o /dev/null 2>/dev/null; then
        echo "  PASS: block $block (C)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: block $block (C)"
        gcc $cflags -x c "$out" -c -o /dev/null 2>&1 | head -5
        FAIL=$((FAIL + 1))
    fi
}

check_rust_block() {
    local block="$1" src="$2"
    local rs_out="$TMPDIR/rust_examples/block_${block}.rs"

    mkdir -p "$TMPDIR/rust_examples"
    # Wrap in a main function if the block doesn't already start with fn main.
    # Strip leading blank lines, comment lines (//, //!, ///), and doc attrs (#![...]).
    first=$(awk '!/^[[:space:]]*$|^[[:space:]]*\/\/|^[[:space:]]*#!?\[/{print; exit}' "$src")
    if ! echo "$first" | grep -q '^fn main'; then
        {
            echo 'fn main() -> Result<(), Box<dyn std::error::Error>> {'
            cat "$src"
            echo 'Ok(())'
            echo '}'
        } > "$rs_out"
    else
        cp "$src" "$rs_out"
    fi
}

check_python_block() {
    local block="$1" src="$2"
    local script_dir
    script_dir=$(cd "$(dirname "$0")" && pwd)

    # Static import-name check: parse `from mmapq import ...` against __all__
    # from the source tree (no build required).
    local init_py="${script_dir}/../../mmapq-py/src/mmapq/__init__.py"
    local helper="${script_dir}/../../mmapq-py/scripts/check_py_readme_imports.py"
    if [ -f "$init_py" ] && [ -f "$helper" ]; then
        if python3 "$helper" "$src" "$init_py" 2>&1; then
            echo "  PASS: block $block (Python)"
            PASS=$((PASS + 1))
            return
        else
            echo "  FAIL: block $block (Python)"
            FAIL=$((FAIL + 1))
            return
        fi
    fi

    # Fallback: __init__.py not found — hard FAIL.
    # Set MMAPQ_SKIP_PYTHON_GATE=1 to skip Python blocks entirely
    # (e.g. in a bare mmapq-only checkout).
    if [ "${MMAPQ_SKIP_PYTHON_GATE:-0}" = "1" ]; then
        echo "  SKIP: block $block (Python, MMAPQ_SKIP_PYTHON_GATE=1 — coverage incomplete)"
        GATE_SKIP=$((GATE_SKIP + 1))
    else
        echo "  FAIL: block $block (Python) — mmapq-py/src/mmapq/__init__.py not found"
        FAIL=$((FAIL + 1))
    fi
}

# check_include_paths — verify that every `include/…` path a README mentions
# resolves against the INSTALLED header set (include/PUBLIC_HEADERS), not the
# source tree. This is 16e's prophylactic: after rewording the README away from
# internal headers, a future edit that re-introduces `include/header_codec.h`
# (internal, never installed) must fail. Self-tested below.
check_include_paths() {
    local readme="$1"
    local script_dir manifest
    script_dir=$(cd "$(dirname "$0")" && pwd)
    manifest="$script_dir/../include/PUBLIC_HEADERS"

    if [ ! -f "$manifest" ]; then
        echo "  FAIL: include/PUBLIC_HEADERS not found ($manifest) — cannot verify installed header paths"
        FAIL=$((FAIL + 1))
        return
    fi

    local refs
    refs=$(grep -oE 'include/[A-Za-z0-9_.-]+\.h' "$readme" | sed 's#^include/##' | sort -u || true)
    if [ -z "$refs" ]; then
        return
    fi

    local bad=0 h
    while IFS= read -r h; do
        if ! grep -qxF "$h" "$manifest"; then
            echo "  FAIL: README references include/$h, which is not an installed (PUBLIC_HEADERS) header"
            FAIL=$((FAIL + 1))
            bad=1
        fi
    done <<< "$refs"
    if [ "$bad" -eq 0 ]; then
        echo "  PASS: all include/… header references resolve against the installed set"
    fi
}

check_readme() {
    local readme="$1"
    if [ ! -f "$readme" ]; then
        echo "FAIL: $readme not found"
        FAIL=$((FAIL + 1))
        return
    fi

    echo "--- check_readme_examples: $readme ---"

    local block=0 lang="" src=""
    while IFS= read -r line; do
        if [ -n "$lang" ]; then
            # Inside a fenced block
            if [ "$line" = '```' ]; then
                # End of block
                TOTAL=$((TOTAL + 1))
                case "$lang" in
                    c)      check_c_block "$readme" "$block" "$src" ;;
                    rust)   check_rust_block "$block" "$src" ;;
                    python) check_python_block "$block" "$src" ;;
                    ignore) echo "  SKIP: block $block (ignore)"; SKIP=$((SKIP + 1)) ;;
                    *)      echo "  SKIP: block $block ($lang)"; SKIP=$((SKIP + 1)) ;;
                esac
                lang=""
                src=""
                block=$((block + 1))
            else
                # Accumulate block content
                if [ -z "$src" ]; then
                    src=$(mktemp "$TMPDIR/block_XXXX")
                fi
                echo "$line" >> "$src"
            fi
        elif [[ "$line" =~ ^'```' ]]; then
            # Start of fenced block — extract language
            lang="${line#\`\`\`}"
        fi
    done < "$readme"

    # Verify every include/… path mentioned in the README resolves against the
    # installed header set (16e path-resolution gate).
    check_include_paths "$readme"
}

# --- Self-test ---

self_test() {
    local fail=0
    local tmpdir
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' RETURN

    # Python arm: fake import should fail
    echo 'from mmapq import ThisClassDoesNotExist' > "$tmpdir/fake_import.py"
    local script_dir
    script_dir=$(cd "$(dirname "$0")" && pwd)
    local init_py="${script_dir}/../../mmapq-py/src/mmapq/__init__.py"
    local helper="${script_dir}/../../mmapq-py/scripts/check_py_readme_imports.py"
    if [ -f "$init_py" ] && [ -f "$helper" ]; then
        if python3 "$helper" "$tmpdir/fake_import.py" "$init_py" 2>/dev/null; then
            echo "  self-test Python arm: FAIL (false positive)"
            fail=1
        else
            echo "  self-test Python arm: OK"
        fi
    else
        echo "  self-test Python arm: SKIP (no __init__.py found)"
    fi

    # Python arm: missing __init__.py should also fail
    if [ -f "$init_py" ]; then
        local backup_init
        backup_init=$(mktemp "$tmpdir/init_backup_XXXX")
        cp "$init_py" "$backup_init"
        mv "$init_py" "$tmpdir/init_hidden"
        echo '```python' > "$tmpdir/py_readme.md"
        echo 'from mmapq import IndexedQueue' >> "$tmpdir/py_readme.md"
        echo '```' >> "$tmpdir/py_readme.md"
        if env -u MMAPQ_SKIP_PYTHON_GATE bash "$0" "$tmpdir/py_readme.md" 2>&1 | grep -q 'FAIL:.*__init__.py not found'; then
            echo "  self-test Python missing-init arm: OK"
        else
            echo "  self-test Python missing-init arm: FAIL (false positive)"
            fail=1
        fi
        mv "$tmpdir/init_hidden" "$init_py"
        rm -f "$backup_init"
    else
        echo "  self-test Python missing-init arm: SKIP (no __init__.py to hide)"
    fi

    # Rust arm: deliberately broken block must fail — either as a compile
    # error (cargo present) or as a toolchain FAIL (cargo absent). Both are
    # a correct FAIL, so assert on the overall exit code, not the message.
    echo '```rust' > "$tmpdir/fake_readme.md"
    echo 'fn main() { let x = }' >> "$tmpdir/fake_readme.md"
    echo '```' >> "$tmpdir/fake_readme.md"
    if env -u MMAPQ_SKIP_RUST_GATE bash "$0" "$tmpdir/fake_readme.md" >/dev/null 2>&1; then
        echo "  self-test Rust arm: FAIL (false positive — broken block passed)"
        env -u MMAPQ_SKIP_RUST_GATE bash "$0" "$tmpdir/fake_readme.md" 2>&1 | tail -5
        fail=1
    else
        echo "  self-test Rust arm: OK"
    fi

    # Path-resolution arm (16e): a README referencing a non-installed header
    # must FAIL; one referencing an installed header must PASS.
    local manifest
    manifest="$script_dir/../include/PUBLIC_HEADERS"
    if [ -f "$manifest" ]; then
        # Non-installed (internal) header → must fail.
        echo 'The spec lives in `include/header_codec.h`.' > "$tmpdir/bad_path.md"
        if bash "$0" "$tmpdir/bad_path.md" >/dev/null 2>&1; then
            echo "  self-test path-resolution (internal header) arm: FAIL (false positive)"
            fail=1
        else
            echo "  self-test path-resolution (internal header) arm: OK"
        fi
        # Installed header → must pass.
        local installed
        installed=$(grep -m1 -v '^$' "$manifest")
        echo "Consumers include \`include/${installed}\`." > "$tmpdir/good_path.md"
        if bash "$0" "$tmpdir/good_path.md" >/dev/null 2>&1; then
            echo "  self-test path-resolution (installed header) arm: OK"
        else
            echo "  self-test path-resolution (installed header) arm: FAIL (false negative)"
            fail=1
        fi
    else
        echo "  self-test path-resolution arm: SKIP (no PUBLIC_HEADERS manifest)"
    fi

    # Rust arm opt-out: without cargo, MMAPQ_SKIP_RUST_GATE=1 must SKIP, not FAIL.
    if command -v cargo >/dev/null 2>&1; then
        echo "  self-test Rust opt-out arm: SKIP (cargo present, cannot simulate absence)"
    else
        if MMAPQ_SKIP_RUST_GATE=1 bash "$0" "$tmpdir/fake_readme.md" >/dev/null 2>&1; then
            echo "  self-test Rust opt-out arm: OK"
        else
            echo "  self-test Rust opt-out arm: FAIL (opt-out did not skip)"
            fail=1
        fi
    fi

    if [ "$fail" -eq 0 ]; then
        echo "self-test: PASS"
        exit 0
    else
        echo "self-test: FAIL"
        exit 1
    fi
}

# --- Main ---

if [ "${1:-}" = "--self-test" ]; then
    self_test
fi

if [ $# -eq 0 ]; then
    check_readme "README.md"
else
    for f in "$@"; do
        check_readme "$f"
    done
fi

# Rust blocks: compile all at once via temp Cargo project
if [ -d "$TMPDIR/rust_examples" ] && [ "$(ls -A "$TMPDIR/rust_examples" 2>/dev/null)" ]; then
    echo "--- Compiling Rust blocks ---"
    # Distinguish "cargo absent" (a toolchain gap, reported as such) from a
    # genuine compile failure. Hard FAIL by default (P5); MMAPQ_SKIP_RUST_GATE=1
    # is an explicit, counted opt-out for C-only hosts without a Rust toolchain.
    if ! command -v cargo >/dev/null 2>&1; then
        if [ "${MMAPQ_SKIP_RUST_GATE:-0}" = "1" ]; then
            echo "  SKIP: Rust blocks (MMAPQ_SKIP_RUST_GATE=1, cargo not found — coverage incomplete)"
            for f in "$TMPDIR/rust_examples"/*.rs; do
                GATE_SKIP=$((GATE_SKIP + 1))
            done
        else
            echo "  FAIL: cargo not found — cannot compile Rust blocks"
            echo "        Set MMAPQ_SKIP_RUST_GATE=1 to skip (coverage incomplete)"
            FAIL=$((FAIL + 1))
        fi
    else
        # Find the mmapq-rs directory by checking common locations
        RUST_README_DIR=""
        for f in "$@"; do
            if echo "$f" | grep -q 'mmapq-rs'; then
                RUST_README_DIR=$(cd "$(dirname "$f")" && pwd)
                break
            fi
        done
        if [ -z "$RUST_README_DIR" ]; then
            # Try default location
            RUST_README_DIR="$(cd "$(dirname "$0")/../../mmapq-rs" && pwd 2>/dev/null || true)"
        fi

        if [ -n "$RUST_README_DIR" ] && [ -f "$RUST_README_DIR/Cargo.toml" ]; then
            local_crate="$RUST_README_DIR"

            # Use a shared target dir so per-block re-checks don't recompile
            shared_target="$TMPDIR/rust_target"

            # Create a temp Cargo project
            mkdir -p "$TMPDIR/rust_project/src"
            cat > "$TMPDIR/rust_project/Cargo.toml" << EOF
[package]
name = "readme_check"
version = "0.1.0"
edition = "2021"

[dependencies]
mmapq = { path = "$local_crate" }
EOF
            echo 'fn main() {}' > "$TMPDIR/rust_project/src/main.rs"

            # Copy each block as a separate example
            mkdir -p "$TMPDIR/rust_project/examples"
            for f in "$TMPDIR/rust_examples"/*.rs; do
                base=$(basename "$f")
                cp "$f" "$TMPDIR/rust_project/examples/$base"
            done

            if cargo check --manifest-path "$TMPDIR/rust_project/Cargo.toml" --examples --target-dir "$shared_target" 2>/dev/null; then
                echo "  All Rust blocks compiled successfully"
                # Count all Rust blocks as PASS
                for f in "$TMPDIR/rust_examples"/*.rs; do
                    PASS=$((PASS + 1))
                done
            else
                echo "  FAIL: some Rust blocks failed to compile"
                cargo check --manifest-path "$TMPDIR/rust_project/Cargo.toml" --examples --target-dir "$shared_target" 2>&1 | tail -10
                # Count failures per block
                for f in "$TMPDIR/rust_examples"/*.rs; do
                    base=$(basename "$f" .rs)
                    block_num="${base#block_}"
                    # Try compiling just this example with shared target dir
                    if ! cargo check --manifest-path "$TMPDIR/rust_project/Cargo.toml" --example "$base" --target-dir "$shared_target" 2>/dev/null; then
                        echo "  FAIL: block $block_num (Rust)"
                        FAIL=$((FAIL + 1))
                    fi
                done
            fi
        else
            echo "  FAIL: mmapq-rs crate not found — cannot compile Rust blocks"
            FAIL=$((FAIL + 1))
        fi
    fi
fi

echo "---"
echo "PASS=$PASS FAIL=$FAIL SKIP=$SKIP GATE_SKIP=$GATE_SKIP TOTAL=$TOTAL"
if [ "$GATE_SKIP" -gt 0 ]; then
    echo "WARNING: $GATE_SKIP block(s) skipped due to missing toolchains. Coverage is incomplete."
    echo "         CI must assert GATE_SKIP == 0; install the Rust toolchain or drop the opt-out."
fi
if [ "$FAIL" -eq 0 ]; then
    echo "PASS: all compilable blocks passed"
    exit 0
else
    echo "FAIL: $FAIL block(s) failed"
    exit 1
fi
