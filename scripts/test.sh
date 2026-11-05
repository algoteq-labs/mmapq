#!/usr/bin/env bash
# test.sh — build and run the mmapq test suite
# Usage:
#   ./scripts/test.sh            # normal build + tests
#   ./scripts/test.sh --asan     # AddressSanitizer (leak + memory error detection)
#   ./scripts/test.sh --tsan     # ThreadSanitizer (data race detection)

set -euo pipefail
cd "$(dirname "$0")/.."

TESTS=(
    test_file_initialiser
    test_file_mapper_single_ro
    test_file_mapper_single_rw
    test_file_mapper_rolled
    test_region_sync
    test_region_async
    test_region_ring
    test_region_ring_accessor
    test_fixed_size_region
    test_runtime
    test_header_codec
    test_appender_id_pool
    test_queue_meta
    test_payload_walk
    test_indexed_queue
    test_shared_queue
    test_queue_transfer
    test_transfer_failures
    test_indexed_archiver
    test_indexed_batch_queue
    test_indexed_batch_archiver
    test_alloc_faults
    test_crash_composition
    test_cand54_pins
    test_c58_walk_pins
    test_c59_commit_floor_pins
    test_poll_arch_pins
)

# Hook-only suites (TEST-01): main() wraps every RUN_TEST in
# `#ifdef MMAPQ_TEST_HOOKS`, so under the release flags — which deliberately
# omit -DMMAPQ_TEST_HOOKS — they compile to ZERO tests. That is by design
# (CAND-58/CAND-59), so the release loop SKIPs them instead of failing. Any
# other suite that runs 0 tests is a defect and FAILs.
HOOK_ONLY_SUITES=(
    test_c58_walk_pins
    test_c59_commit_floor_pins
    test_poll_arch_pins
)

# MMAPQ_TEST_HOOKS enables the deterministic race/failure-injection hooks
# (CAND-30 pins P9/P15) in the normal loop only; the release loop below
# compiles without it, and the shipped library never contains the hooks
# (nm check at acceptance).
CFLAGS="-std=gnu11 -g -O0 -D_GNU_SOURCE -DMMAPQ_TEST_HOOKS -I include -I src"
LDFLAGS="-pthread -lm"
SRCS="$(ls src/*.c)"
UNITY="test/unity.c"

case "${1:-}" in
    --asan)
        MODE="ASAN"
        CFLAGS="$CFLAGS -fsanitize=address"
        export ASAN_OPTIONS="detect_leaks=1:abort_on_error=0"
        ;;
    --tsan)
        MODE="TSAN"
        CFLAGS="$CFLAGS -fsanitize=thread"
        export TSAN_OPTIONS="abort_on_error=0"
        ;;
    "")
        MODE="normal"
        ;;
    *)
        echo "Usage: $0 [--asan|--tsan]" >&2
        exit 1
        ;;
esac

echo "=== mmapq test suite ($MODE) ==="

# ── Gate A: umbrella header compiles with only -I include (no -I src) ────────
GATE_CFLAGS="-std=gnu11 -D_GNU_SOURCE -I include"
echo "--- Gate A: mmapq.h umbrella ---"
echo '#include "mmapq.h"' | gcc $GATE_CFLAGS -x c - -c -o /dev/null 2>&1 && \
    echo "PASS: mmapq.h compiles with -I include only" || \
    { echo "FAIL: mmapq.h umbrella header did not compile"; exit 1; }

PASS=0
FAIL=0
SKIP=0

# ── Tools block (CAND-50 import): runs only when tools/mmapq-ids exists ────
# Conditional so the mirrored copy in the wrappers' c/ trees (which carry no
# tools/) is a provable no-op there. Fail-closed: a present tools dir with a
# broken build, a missing python3, or a failing suite is a counted FAIL,
# never a skip.
IDS_BIN=""
if [ -d tools/mmapq-ids ]; then
    echo "--- tools: mmapq-ids build ---"
    if make -C tools/mmapq-ids > /dev/null 2>&1 && \
       [ -x tools/mmapq-ids/build/release/mmapq-ids ]; then
        IDS_BIN="$PWD/tools/mmapq-ids/build/release/mmapq-ids"
        echo "PASS ($MODE): mmapq-ids build"
        PASS=$((PASS+1))
    else
        echo "FAIL ($MODE): mmapq-ids build"
        FAIL=$((FAIL+1))
    fi

    echo "--- tools: mmapq-ids unit/CLI suite (python3) ---"
    if ! command -v "${PYTHON:-python3}" >/dev/null 2>&1; then
        echo "FAIL ($MODE): mmapq-ids unit/CLI suite (python3 missing)"
        FAIL=$((FAIL+1))
    elif make -C tools/mmapq-ids test PYTHON="${PYTHON:-python3}" > /tmp/mmapq_ids_pytest.log 2>&1; then
        echo "PASS ($MODE): mmapq-ids unit/CLI suite"
        PASS=$((PASS+1))
    else
        echo "FAIL ($MODE): mmapq-ids unit/CLI suite"
        tail -20 /tmp/mmapq_ids_pytest.log
        FAIL=$((FAIL+1))
    fi
fi

# ── Shared compile + run + verify ──────────────────────────────────────────

# is_hook_only — is this suite in the by-design 0-test-in-release allow-list?
is_hook_only() {
    local t="$1" h
    for h in "${HOOK_ONLY_SUITES[@]}"; do
        if [ "$h" = "$t" ]; then
            return 0
        fi
    done
    return 1
}

# classify_tests — TEST-01: a run that exits 0 is a PASS only if it actually
# ran tests. Parse the Unity summary; decide 0-test handling from the BUILD
# FLAGS (not the loop label), so the release loop is judged on what it
# compiled, not on what the caller thinks it is. Echoes the summary line on
# PASS (as before); prints its own FAIL/SKIP diagnostics. Returns 0 PASS,
# 1 FAIL, 2 SKIP.
classify_tests() {
    local test="$1" label="$2" cflags="$3" out="$4"
    local summary n_tests hooks_on=0
    summary=$(echo "$out" | grep -E '^[0-9]+ Tests [0-9]+ Failures [0-9]+ Ignored' | tail -1 || true)
    if [ -z "$summary" ]; then
        echo "FAIL ($label): $test (no Unity summary)"
        echo "$out" | tail -20
        return 1
    fi
    n_tests=$(echo "$summary" | awk '{print $1}')
    case "$cflags" in *-DMMAPQ_TEST_HOOKS*) hooks_on=1 ;; esac
    if is_hook_only "$test"; then
        if [ "$hooks_on" -eq 0 ] && [ "$n_tests" -eq 0 ]; then
            echo "SKIP ($label): $test (hook-only suite, 0 tests without MMAPQ_TEST_HOOKS)"
            return 2
        fi
        if [ "$n_tests" -eq 0 ]; then
            echo "FAIL ($label): $test (hook-only suite produced 0 tests with MMAPQ_TEST_HOOKS)"
            echo "$out" | tail -20
            return 1
        fi
        if [ "$hooks_on" -eq 0 ]; then
            echo "FAIL ($label): $test (allow-list stale: hook-only suite ran $n_tests tests)"
            echo "$out" | tail -20
            return 1
        fi
    elif [ "$n_tests" -eq 0 ]; then
        echo "FAIL ($label): $test (0 tests)"
        echo "$out" | tail -20
        return 1
    fi
    echo "$summary"
    return 0
}

run_test() {
    local test="$1"
    local label="$2"
    local cflags="$3"
    local bin="/tmp/mmapq_${test}_${label}"

    local sources="$SRCS"
    if [ "$test" = test_transfer_failures ]; then
        sources="${sources//src\/queue_transfer.c/}"
        sources="${sources//src\/batch_queue_transfer.c/}"
        sources="$sources test/transfer_fault_normal.c test/transfer_fault_batch.c"
    fi
    # Fault-injection tests route every calloc through __wrap_calloc.
    local extra_ldflags=""
    if [ "$test" = test_alloc_faults ]; then
        extra_ldflags="-Wl,--wrap=calloc"
    fi
    if ! gcc $cflags -Wno-missing-prototypes -Wno-strict-prototypes $sources "test/${test}.c" $UNITY -o "$bin" $LDFLAGS $extra_ldflags 2>&1; then
        echo "FAIL ($label): $test (compile error)"
        return 1
    fi

    set +e
    OUT=$(timeout 300 "$bin" 2>&1)
    STATUS=$?
    set -e

    if [ "$STATUS" -ne 0 ] || echo "$OUT" | grep -qE \
        "FAIL|ASSERTION FAILED|Aborted|AddressSanitizer|LeakSanitizer|ThreadSanitizer|DATA RACE|heap-use-after-free|heap-buffer-overflow"; then
        echo "FAIL ($label): $test (exit=$STATUS)"
        echo "$OUT" | tail -20
        rm -f "$bin"
        return 1
    fi

    rm -f "$bin"
    classify_tests "$test" "$label" "$cflags" "$OUT"
}

# ── Tools integration suite (CAND-50): the C fork/kill real-queue test for
# the mmapq-ids CLI. Lives in tools/mmapq-ids/tests/, so it needs -I test
# for unity.h and the freshly built CLI path via MMAPQ_IDS_BIN. ────────────
run_ids_integration() {
    local label="$1"
    local cflags="$2"
    local bin="/tmp/mmapq_test_ids_integration_${label}"

    if ! gcc $cflags -I test -Wno-missing-prototypes -Wno-strict-prototypes \
         $SRCS tools/mmapq-ids/tests/test_ids_integration.c $UNITY \
         -o "$bin" $LDFLAGS 2>&1; then
        echo "FAIL ($label): test_ids_integration (compile error)"
        return 1
    fi

    set +e
    OUT=$(MMAPQ_IDS_BIN="$IDS_BIN" timeout 300 "$bin" 2>&1)
    STATUS=$?
    set -e

    if [ "$STATUS" -ne 0 ] || echo "$OUT" | grep -qE \
        "FAIL|ASSERTION FAILED|Aborted|AddressSanitizer|LeakSanitizer|ThreadSanitizer|DATA RACE|heap-use-after-free|heap-buffer-overflow"; then
        echo "FAIL ($label): test_ids_integration (exit=$STATUS)"
        echo "$OUT" | tail -20
        rm -f "$bin"
        return 1
    fi

    rm -f "$bin"
    classify_tests test_ids_integration "$label" "$cflags" "$OUT"
}

# ── Normal mode ────────────────────────────────────────────────────────────
for TEST in "${TESTS[@]}"; do
    rc=0
    run_test "$TEST" "$MODE" "$CFLAGS" || rc=$?
    case "$rc" in
        0) echo "PASS ($MODE): $TEST"; PASS=$((PASS+1)) ;;
        2) SKIP=$((SKIP+1)) ;;
        *) FAIL=$((FAIL+1)) ;;
    esac
done

if [ -n "$IDS_BIN" ]; then
    rc=0
    run_ids_integration "$MODE" "$CFLAGS" || rc=$?
    case "$rc" in
        0) echo "PASS ($MODE): test_ids_integration"; PASS=$((PASS+1)) ;;
        2) SKIP=$((SKIP+1)) ;;
        *) FAIL=$((FAIL+1)) ;;
    esac
fi

# ── Release-flags validation (every suite in TESTS + gates under exact release flags) ──
echo ""
echo "=== release-flags validation ==="
CFLAGS_RELEASE="$(make -s print-cflags-release)"
REL_PASS=0
REL_FAIL=0
REL_SKIP=0

# Gate A under release flags (no -I src — must resolve everything from include/)
GATE_CFLAGS_RELEASE="-std=gnu11 -O3 -DNDEBUG -D_GNU_SOURCE -I include"
echo "--- Gate A (release): mmapq.h umbrella ---"
echo '#include "mmapq.h"' | gcc $GATE_CFLAGS_RELEASE -x c - -c -o /dev/null 2>&1 && \
    echo "PASS (release): mmapq.h umbrella" || \
    { echo "FAIL (release): mmapq.h umbrella header did not compile"; ((REL_FAIL++)); }

for TEST in "${TESTS[@]}"; do
    rc=0
    run_test "$TEST" "release" "$CFLAGS_RELEASE" || rc=$?
    case "$rc" in
        0) echo "PASS (release): $TEST"; REL_PASS=$((REL_PASS+1)) ;;
        2) REL_SKIP=$((REL_SKIP+1)) ;;
        *) echo "FAIL (release): $TEST"; REL_FAIL=$((REL_FAIL+1)) ;;
    esac
done

if [ -n "$IDS_BIN" ]; then
    rc=0
    run_ids_integration "release" "$CFLAGS_RELEASE" || rc=$?
    case "$rc" in
        0) echo "PASS (release): test_ids_integration"; REL_PASS=$((REL_PASS+1)) ;;
        2) REL_SKIP=$((REL_SKIP+1)) ;;
        *) echo "FAIL (release): test_ids_integration"; REL_FAIL=$((REL_FAIL+1)) ;;
    esac
fi

echo ""
echo "=== Results (normal): $PASS passed, $FAIL failed, $SKIP skipped ==="
echo "=== Results (release): $REL_PASS passed, $REL_FAIL failed, $REL_SKIP skipped ==="
[ "$FAIL" -eq 0 ] && [ "$REL_FAIL" -eq 0 ]
