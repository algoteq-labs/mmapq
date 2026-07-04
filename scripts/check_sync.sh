#!/usr/bin/env bash
# check_sync.sh — verify that mmap-c-rs/c/ C mirrors match mmap-c/ exactly.
# Hard gate: exits 0 on sync, non-zero on any difference.
set -e

cd "$(dirname "$0")/../.."

diff -rq mmap-c/src mmap-c-rs/c/src -x '*.o'
diff -rq mmap-c/include mmap-c-rs/c/include
diff -rq mmap-c/test mmap-c-rs/c/test -x '*.o'
diff -rq mmap-c/scripts mmap-c-rs/c/scripts

echo "mirrors in sync"
