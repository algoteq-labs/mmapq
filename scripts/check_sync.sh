#!/usr/bin/env bash
# check_sync.sh — verify mirrors and invariants across all three repos.
# Hard gate: exits 0 on sync, non-zero on any difference.
set -e

cd "$(dirname "$0")/../.."

diff -rq mmapq/src mmapq-rs/c/src -x '*.o'
diff -rq mmapq/include mmapq-rs/c/include
diff -rq mmapq/test mmapq-rs/c/test -x '*.o'
diff -rq mmapq/scripts mmapq-rs/c/scripts

# Verify that no private C struct layout is mirrored in the Python cdef
bash mmapq-py/scripts/check_no_layout_mirrors.sh --self-test
bash mmapq-py/scripts/check_no_layout_mirrors.sh

echo "mirrors in sync"
