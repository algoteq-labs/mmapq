#!/usr/bin/env bash
# run_soak.sh — DEMO-01 stage (e) E6 soak, the release gate.
#
# Runs many seeds of the churn with archiving on, across sync/async/mixed and
# both cadences (--snap-every 4096 and 1), plus the hand-deletion run and the
# two gateway kill hooks. No plants: every run must be green. Target wall time
# 30-60 minutes on the house machine.
#
# Per run it reports messages, kills, undercuts, snapshot cycles, peak live
# disk, and that every survivor reached the live digest. At the end it prints
# the totals. Exit non-zero if any run failed.
#
# Churn runs are soak evidence — "ran N messages under M kills without a check
# failure" — never proof of race-freedom; that claim belongs to the TLA+ model
# (doc/model/). See README.md.
#
# Usage:  torture/mktsim/run_soak.sh [workdir]
# Env:    SOAK_SEEDS (default 12), SOAK_KILLS (default 60),
#         SOAK_TRADERS (default 8), SOAK_ONLY (regex filter on run name),
#         MKTSIM_BIN.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${MKTSIM_BIN:-$HERE/mktsim}"
CHURN="$HERE/churn.py"
SOAKRUN="$HERE/scenarios/_soak.py"
TMP="${1:-${TMPDIR:-/tmp}/mktsim_soak.$$}"
SEEDS="${SOAK_SEEDS:-12}"
KILLS="${SOAK_KILLS:-60}"
TRADERS="${SOAK_TRADERS:-8}"
ONLY="${SOAK_ONLY:-}"

if [ ! -x "$BIN" ]; then
    echo "run_soak.sh: $BIN not built; run 'make mktsim' first" >&2
    exit 2
fi

rm -rf "$TMP"
mkdir -p "$TMP"
LOG="$TMP/soak.log"
: >"$LOG"
START=$(date +%s)
RUNS=0
FAILS=0

run_one() { # name mode snap_every seed extra...
    local name="$1" mode="$2" snap="$3" seed="$4"; shift 4
    if [ -n "$ONLY" ] && ! printf '%s' "$name" | grep -Eq "$ONLY"; then return; fi
    RUNS=$((RUNS + 1))
    printf '[%3d] %-28s mode=%-5s snap=%-5s seed=%-4s ... ' \
        "$RUNS" "$name" "$mode" "$snap" "$seed" >&2
    local out="$TMP/$name.out"
    python3 "$SOAKRUN" --name "$name" --bin "$BIN" --churn "$CHURN" \
        --dir "$TMP/$name" --mode "$mode" --seed "$seed" \
        --snap-every "$snap" --traders "$TRADERS" --kills "$KILLS" \
        --survivors 2 "$@" >>"$LOG" 2>&1
    local rc=$?
    local line; line=$(grep "^SOAKRUN $name " "$LOG" | tail -1)
    printf '%s\n' "$line" >&2
    if [ "$rc" -ne 0 ]; then FAILS=$((FAILS + 1)); fi
}

echo "run_soak: seeds=$SEEDS kills=$KILLS traders=$TRADERS dir=$TMP" >&2
for mode in sync async mixed; do
    for snap in 4096 1; do
        for seed in $(seq 1 "$SEEDS"); do
            run_one "$mode-$snap-$seed" "$mode" "$snap" "$seed"
        done
    done
done
run_one hand-delete-sync  sync  4096 101 --hand-delete
run_one hand-delete-async async 4096 102 --hand-delete
run_one gw-kill-after-append sync 4096 103 --gw-hook MKTSIM_GW_KILL_AFTER_APPEND
run_one gw-kill-after-commit sync 4096 104 --gw-hook MKTSIM_GW_KILL_AFTER_COMMIT

ELAPSED=$(( $(date +%s) - START ))

echo ""
echo "================ run_soak summary ================"
echo "--- per run ---"
grep '^SOAKRUN ' "$LOG"
echo "--- totals ---"
awk '
/^SOAKRUN / {
    for (i = 1; i <= NF; i++) {
        split($i, kv, "=")
        if (kv[1] == "messages") m += kv[2]
        if (kv[1] == "kills")    k += kv[2]
        if (kv[1] == "undercuts") u += kv[2]
        if (kv[1] == "snaps")    s += kv[2]
        if (kv[1] == "peak_kb" && kv[2] > p) p = kv[2]
        if (kv[1] == "finals")   f += kv[2]
        if (kv[1] == "at_digest") d += kv[2]
        if (kv[1] == "result" && kv[2] == "PASS") pass++
        if (kv[1] == "result") n++
    }
}
END {
    printf "runs=%d pass=%d fail=%d messages=%d kills=%d undercuts=%d snaps=%d peak_kb=%d finals=%d at_digest=%d\n",
           n, pass, n - pass, m, k, u, s, p, f, d
}' "$LOG"
printf 'elapsed=%ds (%dm%ds)\n' "$ELAPSED" "$((ELAPSED / 60))" "$((ELAPSED % 60))"
echo "=================================================="

if [ "$FAILS" -ne 0 ]; then
    echo "run_soak: FAIL ($FAILS run(s)); evidence kept in $TMP" >&2
    exit 1
fi
rm -rf "$TMP"
echo "run_soak: OK ($RUNS runs)"
exit 0
