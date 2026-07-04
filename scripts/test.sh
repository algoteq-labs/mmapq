#!/usr/bin/env bash
# test.sh — build and run the mmap-c test suite
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

CFLAGS="-std=gnu11 -g -O0 -D_GNU_SOURCE -I include"
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

echo "=== mmap-c test suite ($MODE) ==="

PASS=0
FAIL=0

for TEST in "${TESTS[@]}"; do
    BIN="/tmp/mmap_c_${TEST}_${MODE}"

    # Compile
    if ! gcc $CFLAGS $SRCS "test/${TEST}.c" $UNITY -o "$BIN" $LDFLAGS 2>&1; then
        echo "FAIL: $TEST (compile error)"
        FAIL=$((FAIL+1))
        continue
    fi

    # Run — treat non-zero exit OR any sanitiser/assertion output as failure
    set +e
    OUT=$(timeout 60 "$BIN" 2>&1)
    STATUS=$?
    set -e

    if [ "$STATUS" -ne 0 ] || echo "$OUT" | grep -qE \
        "FAIL|ASSERTION FAILED|Aborted|AddressSanitizer|LeakSanitizer|ThreadSanitizer|DATA RACE|heap-use-after-free|heap-buffer-overflow"; then
        echo "FAIL: $TEST (exit=$STATUS)"
        echo "$OUT" | tail -20
        FAIL=$((FAIL+1))
    else
        echo "PASS: $TEST"
        PASS=$((PASS+1))
    fi

    rm -f "$BIN"
done

# ── Release-flags validation (the 3 regression tests that guard NDEBUG bugs) ─
echo ""
echo "=== release-flags validation ==="
CFLAGS_RELEASE="$(make -s print-cflags-release)"
REL_FAILED=0
for TEST in test_indexed_queue test_indexed_batch_queue test_indexed_archiver test_indexed_batch_archiver test_runtime; do
    BIN="/tmp/mmap_c_${TEST}_release"
    if gcc $CFLAGS_RELEASE $SRCS "test/${TEST}.c" $UNITY -o "$BIN" $LDFLAGS 2>&1 && "$BIN" 2>&1; then
        echo "PASS (release): $TEST"
    else
        echo "FAIL (release): $TEST"
        REL_FAILED=1
    fi
    rm -f "$BIN"
done

echo ""
echo "=== Results ($MODE): $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && [ "$REL_FAILED" -eq 0 ]
