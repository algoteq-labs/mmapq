#!/usr/bin/env bash
# check_sync.sh — verify that mmapq-rs/c/ C mirrors match mmapq/ exactly.
# Hard gate: exits 0 on sync, non-zero on any difference.
set -e

cd "$(dirname "$0")/../.."

diff -rq mmapq/src mmapq-rs/c/src -x '*.o'
diff -rq mmapq/include mmapq-rs/c/include
diff -rq mmapq/test mmapq-rs/c/test -x '*.o'
diff -rq mmapq/scripts mmapq-rs/c/scripts

echo "mirrors in sync"
