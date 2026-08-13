#!/usr/bin/env bash
# check_mirrors.sh — verify the vendored C mirrors (mmapq-rs/c and mmapq-py/c)
# are in sync with the authoritative mmapq tree, on src/include/test/scripts.
# Hard gate: exits 0 when all mirrors match, non-zero on any difference.
#
# Both mmapq-rs/c and mmapq-py/c are submodules of the mmapq repo (the Python
# one ships in the sdist, 11b). The two copies must never diverge from the
# source of truth. A missing sibling directory is a FAIL, not a pass — a stale
# or absent mirror is exactly the drift this gate exists to catch (P5).
#
# This is the mirror check extracted from check_sync.sh (17d) so that
# check_sync.sh's cross-scope gating is uniform and the meta-harness gains
# fail-closed coverage of the mirror check itself.
set -euo pipefail

cd "$(dirname "$0")/../.."

errors=0

for pair in \
    "mmapq/src mmapq-rs/c/src" \
    "mmapq/include mmapq-rs/c/include" \
    "mmapq/test mmapq-rs/c/test" \
    "mmapq/scripts mmapq-rs/c/scripts" \
    "mmapq/src mmapq-py/c/src" \
    "mmapq/include mmapq-py/c/include" \
    "mmapq/test mmapq-py/c/test" \
    "mmapq/scripts mmapq-py/c/scripts"
do
    a="${pair%% *}"
    b="${pair#* }"
    if [ ! -d "$a" ]; then
        echo "ERROR: source tree $a not found — wrong working directory?" >&2
        exit 1
    fi
    if [ ! -d "$b" ]; then
        echo "ERROR: mirror $b is absent — the vendored submodule is missing or not checked out" >&2
        errors=$((errors + 1))
        continue
    fi
    if ! diff -rq "$a" "$b" -x '*.o' >/dev/null 2>&1; then
        echo "ERROR: $a and $b differ" >&2
        errors=$((errors + 1))
    fi
done

if [ "$errors" -gt 0 ]; then
    echo "FAIL: $errors mirror mismatch(es) between mmapq and mmapq-rs/c + mmapq-py/c" >&2
    exit 1
fi

echo "PASS: all 8 mirrors (mmapq-rs/c + mmapq-py/c) in sync with mmapq"
