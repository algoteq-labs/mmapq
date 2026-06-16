#!/usr/bin/env bash
# test_multiprocess.sh — multiprocess queue correctness test
#
# Compiles producer and consumer, then runs three scenarios:
#   1. Producer starts first, consumer reads after
#   2. Consumer starts first, producer writes second (consumer waits)
#   3. Two concurrent appenders, one consumer (interleaved messages)
#
# Usage: ./test/multiprocess/test_multiprocess.sh [--keep-binaries]

set -euo pipefail
cd "$(dirname "$0")/../.."

QUEUE_DIR="/tmp/mmap_multiprocess_test"
CFLAGS="-std=gnu11 -O2 -D_GNU_SOURCE -I include"
SRCS="$(ls src/*.c)"
LDFLAGS="-pthread -lm"

PRODUCER="/tmp/mmap_mp_producer"
CONSUMER="/tmp/mmap_mp_consumer"

PASS=0
FAIL=0

# ── Build ─────────────────────────────────────────────────────────────────── #

echo "=== building multiprocess binaries ==="
gcc $CFLAGS $SRCS test/multiprocess/producer.c -o "$PRODUCER" $LDFLAGS
gcc $CFLAGS $SRCS test/multiprocess/consumer.c -o "$CONSUMER" $LDFLAGS
echo "OK"

cleanup_queue() {
    rm -rf "$QUEUE_DIR"
}

run_test() {
    local name="$1"
    local result=0

    if eval "$2"; then
        echo "PASS: $name"
        PASS=$((PASS+1))
    else
        echo "FAIL: $name"
        FAIL=$((FAIL+1))
    fi

    cleanup_queue
}

# ── Test 1: producer first, consumer reads after ───────────────────────────── #

test_producer_first() {
    cleanup_queue
    mkdir -p "$QUEUE_DIR"

    local count=10000
    local timeout_ms=10000

    # Producer runs to completion
    "$PRODUCER" "$QUEUE_DIR" q "$count" 2>&1 | sed 's/^/    /'

    # Consumer reads all messages
    "$CONSUMER" "$QUEUE_DIR" q "$count" "$timeout_ms" 2>&1 | sed 's/^/    /'
}

run_test "producer_first_consumer_after" test_producer_first

# ── Test 2: consumer starts first, waits for producer ─────────────────────── #

test_consumer_first() {
    cleanup_queue
    mkdir -p "$QUEUE_DIR"

    local count=10000
    local timeout_ms=15000

    # Consumer starts first — will wait for messages
    "$CONSUMER" "$QUEUE_DIR" q "$count" "$timeout_ms" 2>&1 | sed 's/^/    /' &
    CONSUMER_PID=$!

    # Give consumer a moment to start
    sleep 0.1

    # Producer writes
    "$PRODUCER" "$QUEUE_DIR" q "$count" 2>&1 | sed 's/^/    /'

    # Wait for consumer
    wait "$CONSUMER_PID"
}

run_test "consumer_first_producer_after" test_consumer_first

# ── Test 3: concurrent producer and consumer ───────────────────────────────── #

test_concurrent() {
    cleanup_queue
    mkdir -p "$QUEUE_DIR"

    local count=50000
    local timeout_ms=30000

    # Start consumer in background
    "$CONSUMER" "$QUEUE_DIR" q "$count" "$timeout_ms" 2>&1 | sed 's/^/    /' &
    CONSUMER_PID=$!

    # Start producer in background
    "$PRODUCER" "$QUEUE_DIR" q "$count" 2>&1 | sed 's/^/    /' &
    PRODUCER_PID=$!

    wait "$PRODUCER_PID"
    wait "$CONSUMER_PID"
}

run_test "concurrent_producer_and_consumer" test_concurrent

# ── Test 4: two producers, one consumer ─────────────────────────────────────── #
#
# Two independent appender processes write to the same queue.
# The consumer reads all messages from both appenders, verifying that
# no messages are lost or corrupted (sequence within each appender's stream).

test_two_producers() {
    cleanup_queue
    mkdir -p "$QUEUE_DIR"

    local count_each=10000
    local total=$((count_each * 2))
    local timeout_ms=30000

    # Start consumer expecting total messages
    "$CONSUMER" "$QUEUE_DIR" q "$total" "$timeout_ms" 2>&1 | sed 's/^/    /' &
    CONSUMER_PID=$!

    # Two producers concurrently with explicit IDs
    "$PRODUCER" "$QUEUE_DIR" q "$count_each" 0 2>&1 | sed 's/^/    /' &
    P1=$!
    "$PRODUCER" "$QUEUE_DIR" q "$count_each" 1 2>&1 | sed 's/^/    /' &
    P2=$!

    wait "$P1"
    wait "$P2"
    wait "$CONSUMER_PID"
}

run_test "two_producers_one_consumer" test_two_producers

# ── Summary ──────────────────────────────────────────────────────────────── #

if [ "${1:-}" != "--keep-binaries" ]; then
    rm -f "$PRODUCER" "$CONSUMER"
fi
cleanup_queue

echo ""
echo "=== multiprocess results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
