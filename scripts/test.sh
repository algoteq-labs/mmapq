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
    echo "$OUT" | grep -E '^[0-9]+ Tests [0-9]+ Failures [0-9]+ Ignored'
    return 0
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
    echo "$OUT" | grep -E '^[0-9]+ Tests [0-9]+ Failures [0-9]+ Ignored'
    return 0
}

# ── Normal mode ────────────────────────────────────────────────────────────
for TEST in "${TESTS[@]}"; do
    if run_test "$TEST" "$MODE" "$CFLAGS"; then
        echo "PASS ($MODE): $TEST"
        PASS=$((PASS+1))
    else
        FAIL=$((FAIL+1))
    fi
done

if [ -n "$IDS_BIN" ]; then
    if run_ids_integration "$MODE" "$CFLAGS"; then
        echo "PASS ($MODE): test_ids_integration"
        PASS=$((PASS+1))
    else
        FAIL=$((FAIL+1))
    fi
fi

# ── Release-flags validation (all 21 tests + gates under exact release flags) ──
echo ""
echo "=== release-flags validation ==="
CFLAGS_RELEASE="$(make -s print-cflags-release)"
REL_PASS=0
REL_FAIL=0

# Gate A under release flags (no -I src — must resolve everything from include/)
GATE_CFLAGS_RELEASE="-std=gnu11 -O3 -DNDEBUG -D_GNU_SOURCE -I include"
echo "--- Gate A (release): mmapq.h umbrella ---"
echo '#include "mmapq.h"' | gcc $GATE_CFLAGS_RELEASE -x c - -c -o /dev/null 2>&1 && \
    echo "PASS (release): mmapq.h umbrella" || \
    { echo "FAIL (release): mmapq.h umbrella header did not compile"; ((REL_FAIL++)); }

for TEST in "${TESTS[@]}"; do
    if run_test "$TEST" "release" "$CFLAGS_RELEASE"; then
        echo "PASS (release): $TEST"
        REL_PASS=$((REL_PASS+1))
    else
        echo "FAIL (release): $TEST"
        REL_FAIL=$((REL_FAIL+1))
    fi
done

if [ -n "$IDS_BIN" ]; then
    if run_ids_integration "release" "$CFLAGS_RELEASE"; then
        echo "PASS (release): test_ids_integration"
        REL_PASS=$((REL_PASS+1))
    else
        echo "FAIL (release): test_ids_integration"
        REL_FAIL=$((REL_FAIL+1))
    fi
fi

echo ""
echo "=== Results (normal): $PASS passed, $FAIL failed ==="
echo "=== Results (release): $REL_PASS passed, $REL_FAIL failed ==="
[ "$FAIL" -eq 0 ] && [ "$REL_FAIL" -eq 0 ]
