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
    test_queue_transfer
    test_indexed_archiver
    test_indexed_batch_queue
    test_indexed_batch_archiver
)

CFLAGS="-std=gnu11 -g -O0 -D_GNU_SOURCE -I include -I src"
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

# ── Shared compile + run + verify ──────────────────────────────────────────
run_test() {
    local test="$1"
    local label="$2"
    local cflags="$3"
    local bin="/tmp/mmapq_${test}_${label}"

    if ! gcc $cflags $SRCS "test/${test}.c" $UNITY -o "$bin" $LDFLAGS 2>&1; then
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

# ── Release-flags validation (all 19 tests + gates under exact release flags) ──
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

echo ""
echo "=== Results (normal): $PASS passed, $FAIL failed ==="
echo "=== Results (release): $REL_PASS passed, $REL_FAIL failed ==="
[ "$FAIL" -eq 0 ] && [ "$REL_FAIL" -eq 0 ]
