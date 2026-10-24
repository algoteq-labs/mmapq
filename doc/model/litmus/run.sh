#!/usr/bin/env bash
# run.sh — CAND-54 herd7 litmus checks (audit/CAND-54-spec.md §9.2).
#
#   HERD7=/path/to/herd7 ./run.sh
#
# herd7 is not pinned by check.sh; any 7.5x build with the C11 model
# works.  For each test the expected observation is listed below; the
# script exits non-zero on any mismatch.
#
#   c54_case1                  the forbidden combination is NEVER observed
#   c54_case3                  NEVER
#   c54_aba                    NEVER
#   c54_linefirst              NEVER (L(iii): line visible before boundary)
#   c54_case1_relaxed_store    observed (SOMETIMES) — the seq_cst intent
#                              store is load-bearing
#   c54_case3_relaxed_clear    observed (SOMETIMES) — the release clear
#                              is load-bearing
set -euo pipefail
cd "$(dirname "$0")"
HERD7="${HERD7:-herd7}"
command -v "$HERD7" >/dev/null || { echo "ERROR: herd7 not found (set HERD7=/path/to/herd7)" >&2; exit 2; }
C11LIB="${C11LIB:-/tmp/opencode/herd7/opamroot/herd/share/herdtools7/herd}"
# RC11 (the repaired C11, Lahav et al. 2017) + the c11 bell/def files;
# herd7 7.58 built scratch-local (not installed system-wide).
HERD7FLAGS="-macros $C11LIB/c11.def -model $C11LIB/rc11.cat"

fail=0
check() { # name  expected(Never|Sometimes)
    local name="$1" want="$2" out got
    out=$("$HERD7" $HERD7FLAGS "$name.litmus" 2>&1)
    if printf '%s\n' "$out" | grep -q "^Observation.*\bNever\b"; then
        got=Never
    elif printf '%s\n' "$out" | grep -q "^Observation.*\bSometimes\b"; then
        got=Sometimes
    else
        got=ERROR
    fi
    if [ "$got" = "$want" ]; then mark=ok; else mark=MISMATCH; fail=1; fi
    printf '%-28s expected %-9s got %-9s %s\n' "$name" "$want" "$got" "$mark"
}

check c54_case1                 Never
check c54_case3                 Never
check c54_aba                   Never
check c54_linefirst             Never
check c54_case1_relaxed_store   Sometimes
check c54_case3_relaxed_clear   Sometimes
exit $fail
