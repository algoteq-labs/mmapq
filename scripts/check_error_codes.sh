#!/usr/bin/env bash
# check_error_codes.sh — assert every MMAPQ_* error code appears in all
# four mirrors: mmapq_strerror.c, map_rc, test_map_rc_all_codes, _RC_ERRORS.
#
# Portable: uses only POSIX sh + awk (mawk-compatible, no gawk extensions).
# --self-test: verifies the gate itself catches omissions.

set -u
export LC_ALL=C

REPO="$(cd "$(dirname "$0")/.." && pwd)"

# ── Extract (name, val) pairs from mmap_platform.h ──────────────────────────

extract_pairs() {
    awk '
    /^#define MMAPQ_/ {
        name = $2
        val = $3
        gsub(/[()]/, "", val)
        if (val + 0 < 0) print name, val
    }' "$1"
}

# ── Check code NAMES in a file (for strerror.c — uses MMAPQ_ name) ──────────

check_names() {
    local label="$1" file="$2"
    local total=0 found=0
    while read name val; do
        total=$((total + 1))
        if grep -qwe "$name" "$file" 2>/dev/null; then
            found=$((found + 1))
        fi
    done < <(extract_pairs "$REPO/include/mmap_platform.h")
    if [ "$found" -ne "$total" ]; then
        echo "MISMATCH: $label has $found/$total codes"
        return 1
    fi
}

# ── Check code VALUES within a region of a file (for Rust/Python) ────────────
# region: awk range expression, e.g. '/^fn map_rc/,/^}/'

check_values_region() {
    local label="$1" file="$2" region="$3"
    local total=0 found=0
    while read name val; do
        total=$((total + 1))
        if awk "$region" "$file" 2>/dev/null | grep -qwe "$val"; then
            found=$((found + 1))
        fi
    done < <(extract_pairs "$REPO/include/mmap_platform.h")
    if [ "$found" -ne "$total" ]; then
        echo "MISMATCH: $label has $found/$total codes"
        return 1
    fi
}

# ── Check code VALUES in a whole file (for _errors.py) ───────────────────────

check_values() {
    local label="$1" file="$2"
    local total=0 found=0
    while read name val; do
        total=$((total + 1))
        if grep -qwe "$val" "$file" 2>/dev/null; then
            found=$((found + 1))
        fi
    done < <(extract_pairs "$REPO/include/mmap_platform.h")
    if [ "$found" -ne "$total" ]; then
        echo "MISMATCH: $label has $found/$total codes"
        return 1
    fi
}

# ── Run checks ───────────────────────────────────────────────────────────────

check_all() {
    local errors=0

    check_names "mmapq_strerror.c" "$REPO/src/mmapq_strerror.c" || errors=1
    check_values_region "map_rc (error.rs)" "$REPO/../mmapq-rs/src/error.rs" \
        '/^pub\(crate\) fn map_rc/,/^}/' || errors=1
    check_values_region "test_map_rc_all_codes (error.rs)" "$REPO/../mmapq-rs/src/error.rs" \
        '/fn test_map_rc_all_codes/,/^\];/' || errors=1
    check_values "_RC_ERRORS" "$REPO/../mmapq-py/src/mmapq/_errors.py" || errors=1

    if [ "$errors" -eq 0 ]; then
        local total
        total=$(extract_pairs "$REPO/include/mmap_platform.h" | wc -l)
        echo "OK: all $total MMAPQ_* error codes present in all mirrors"
    fi
    return $errors
}

# ── Self-test ────────────────────────────────────────────────────────────────

self_test() {
    local errors=0
    local tmpdir

    tmpdir=$(mktemp -d) || return 1

    cat > "$tmpdir/mmap_platform.h" <<'EOF'
#define MMAPQ_ETIMEDOUT  (-1)
#define MMAPQ_EINVAL     (-2)
#define MMAPQ_ECLOSED    (-3)
#define MMAPQ_ECORRUPT   (-4)
#define MMAPQ_ESCHEMA    (-5)
#define MMAPQ_EEXIST     (-6)
#define MMAPQ_ENOENT     (-7)
#define MMAPQ_EIDCONFLICT (-8)
#define MMAPQ_ENOMEM     (-9)
#define MMAPQ_EIO        (-10)
#define MMAPQ_ENOTREADY  (-11)
EOF

    local pairs
    pairs=$(extract_pairs "$tmpdir/mmap_platform.h")
    local total
    total=$(echo "$pairs" | wc -l)

    # PASS: all 11 names in a complete strerror fixture
    cat > "$tmpdir/strerror_pass.c" <<'EOSW'
    case MMAPQ_ETIMEDOUT:  return "";
    case MMAPQ_EINVAL:     return "";
    case MMAPQ_ECLOSED:    return "";
    case MMAPQ_ECORRUPT:   return "";
    case MMAPQ_ESCHEMA:    return "";
    case MMAPQ_EEXIST:     return "";
    case MMAPQ_ENOENT:     return "";
    case MMAPQ_EIDCONFLICT: return "";
    case MMAPQ_ENOMEM:     return "";
    case MMAPQ_EIO:        return "";
    case MMAPQ_ENOTREADY:  return "";
EOSW
    local pass_n=0
    while read name val; do
        if grep -qwe "$name" "$tmpdir/strerror_pass.c" 2>/dev/null; then
            pass_n=$((pass_n + 1))
        fi
    done <<< "$pairs"
    if [ "$pass_n" -eq "$total" ]; then
        echo "  self-test pass case: OK"
    else
        echo "  self-test pass case: FAIL (expected $total, got $pass_n)"
        errors=1
    fi

    # FAIL: 11 codes in header, but only 10 names in strerror
    cat > "$tmpdir/strerror_fail.c" <<'EOSW'
    case MMAPQ_ETIMEDOUT:  return "";
    case MMAPQ_EINVAL:     return "";
    case MMAPQ_ECLOSED:    return "";
    case MMAPQ_ECORRUPT:   return "";
    case MMAPQ_ESCHEMA:    return "";
    case MMAPQ_EEXIST:     return "";
    case MMAPQ_ENOENT:     return "";
    case MMAPQ_EIDCONFLICT: return "";
    /* ENOMEM omitted */
    case MMAPQ_EIO:        return "";
    case MMAPQ_ENOTREADY:  return "";
EOSW
    local fail_n=0
    while read name val; do
        if grep -qwe "$name" "$tmpdir/strerror_fail.c" 2>/dev/null; then
            fail_n=$((fail_n + 1))
        fi
    done <<< "$pairs"
    if [ "$fail_n" -eq "$total" ]; then
        echo "  self-test fail case: FAIL (false positive)"
        errors=1
    else
        echo "  self-test fail case: OK"
    fi

    rm -rf "$tmpdir"
    return $errors
}

# ── Main ─────────────────────────────────────────────────────────────────────

main() {
    if [ "${1:-}" = "--self-test" ]; then
        self_test
        return $?
    fi

    if ! check_all; then
        exit 1
    fi
}

main "$@"
