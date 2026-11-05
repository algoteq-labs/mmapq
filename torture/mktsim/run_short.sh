#!/usr/bin/env bash
# run_short.sh — DEMO-01 stage (e) E6 short gate.
#
# For CI once CI exists; run by hand until then (DEMO-01-spec §6, Q4). It is
# NOT part of test.sh. Target wall time: <= 5 minutes.
#
# Three sections, one summary table, exit non-zero if any row is MISMATCH:
#   1. every plant (current and inherited) run once and expected RED;
#   2. every stage-(e) scenario harness, checked against its declared outcome
#      (a harness that never reached its point reports got=VACUOUS reason=<…>,
#      which is a MISMATCH, never a silent green);
#   3. one clean churn per mode (sync, async, mixed) at the churn default
#      cadence, plus one at --snap-every 1, all expected GREEN.
#
# Failure evidence is kept: every run and every scenario directory lives under
# the work directory, and on any MISMATCH the work directory is left in place
# and its path printed. On success it is removed.
#
# Usage:  torture/mktsim/run_short.sh [workdir]
# Env:    MKTSIM_BIN overrides the binary path; MKTSIM_SHORT_DIR the workdir.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${MKTSIM_BIN:-$HERE/mktsim}"
CHURN="$HERE/churn.py"
SCEN="$HERE/scenarios"
CODE="$HERE/mktsim.c"
TMP="${1:-${MKTSIM_SHORT_DIR:-${TMPDIR:-/tmp}/mktsim_short.$$}}"

if [ ! -x "$BIN" ]; then
    echo "run_short.sh: $BIN not built; run 'make mktsim' first" >&2
    exit 2
fi

rm -rf "$TMP"
mkdir -p "$TMP"

ROWS=()
FAILS=0
START=$(date +%s)

record() { # name expect got [reason]
    local name="$1" expect="$2" got="$3" reason="${4:-}" res row
    if [ "$expect" = "$got" ]; then res=OK; else res=MISMATCH; FAILS=$((FAILS + 1)); fi
    row=$(printf '%-38s expect=%-4s got=%-7s result=%s' "$name" "$expect" "$got" "$res")
    [ -n "$reason" ] && row="$row reason=$reason"
    printf '  %s\n' "$row" >&2
    ROWS+=("$row")
}

# Run churn once. A plant row must prove its plant fired: the process that
# changes behaviour prints a `PLANT <name> …` line (in a run log, or in churn's
# captured stdout for the export step). If no such line appears the row is
# got=VACUOUS reason=plant_not_fired — never a silent green. Otherwise the row
# is PASS/FAIL from churn's RESULT. `want` is the expected got (FAIL for a
# plant that must make the run red).
churn_run() { # dirname mode want marker env...
    local name="$1" mode="$2" want="$3" marker="$4"; shift 4
    local out="$TMP/$name.out" got
    env "$@" python3 "$CHURN" --bin "$BIN" --dir "$TMP/$name" --mode "$mode" \
        --seed 1 --traders 4 --kills 6 --survivors 2 --archive >"$out" 2>&1
    if grep -q '^RESULT: PASS' "$out"; then got=PASS; else got=FAIL; fi
    if [ -n "$marker" ] && ! grep -rqs "$marker" "$out" "$TMP/$name/logs"; then
        record "$name" "$want" "VACUOUS" "plant_not_fired"
        return
    fi
    record "$name" "$want" "$got"
}

scenario_run() { # name script
    local name="$1" script="$2"
    local out="$TMP/$name.scen.out" line expect got reason
    MKTSIM_SCENARIO_DIR="$TMP/scen_$name" python3 "$script" >"$out" 2>&1
    line=$(grep '^SCENARIO ' "$out" | tail -1)
    expect=$(printf '%s' "$line" | sed -n 's/.*expect=\([A-Z]*\).*/\1/p')
    got=$(printf '%s' "$line" | sed -n 's/.*got=\([A-Z]*\).*/\1/p')
    reason=$(printf '%s' "$line" | sed -n 's/.*reason=\([^ ]*\).*/\1/p')
    [ -n "$expect" ] || expect="?"
    [ -n "$got" ] || got="?"
    record "$name" "$expect" "$got" "$reason"
}

# ── plant inventory (rule: check the list against the code with grep) ───────
echo "plant inventory (grep $CODE):" >&2
STAGE_E_PLANTS="MKTSIM_SNAPSHOTTER_OFF_BY_ONE MKTSIM_GC_KEEP_ONE MKTSIM_SNAPSHOTTER_DIE_AFTER_WRITE MKTSIM_ARCH_BEFORE_SNAP MKTSIM_SNAP_BODY_CORRUPT MKTSIM_GW_NO_SNAPSEQ MKTSIM_SNAP_OLD_MAGIC"
STAGE_BD_PLANTS="MKTSIM_RECLAIM_AHEAD_OF_MARK MKTSIM_BATCH_REORDER MKTSIM_BATCH_PARTIAL_READ MKTSIM_TIEBREAK_BUG MKTSIM_REJOIN_STALE_SEQ MKTSIM_COMMIT_BEFORE_GAP MKTSIM_RUNTIME_MISCLASSIFY MKTSIM_EXPORT_TRUNCATE"
REBUILD_PLANTS="MKTSIM_REBUILD_RACE MKTSIM_REBUILD_TIMEOUT MKTSIM_REBUILD_STUCK MKTSIM_REBUILD_EOS"
MISSING=""
for p in $STAGE_E_PLANTS $STAGE_BD_PLANTS $REBUILD_PLANTS; do
    if grep -q "$p" "$CODE"; then
        printf '  present  %s\n' "$p" >&2
    else
        printf '  ABSENT   %s   <-- plant-list mismatch\n' "$p" >&2
        MISSING="$MISSING $p"
    fi
done
[ -n "$MISSING" ] && echo "WARN: plants named in the E6 list but absent from the code:$MISSING" >&2

# ── 1. plants (expected red) ────────────────────────────────────────────────
echo "" >&2
echo "== plants (expected red) ==" >&2
churn_run MKTSIM_SNAPSHOTTER_OFF_BY_ONE       sync FAIL "PLANT snapshotter-off-by-one" MKTSIM_SNAPSHOTTER_OFF_BY_ONE=1
churn_run MKTSIM_GC_KEEP_ONE                  sync FAIL "PLANT gc-keep-one"            MKTSIM_GC_KEEP_ONE=1
churn_run MKTSIM_SNAPSHOTTER_DIE_AFTER_WRITE  sync FAIL "PLANT die-after-write"        MKTSIM_SNAPSHOTTER_DIE_AFTER_WRITE=2
churn_run MKTSIM_ARCH_BEFORE_SNAP             sync FAIL "PLANT arch-before-snap"       MKTSIM_ARCH_BEFORE_SNAP=1
churn_run MKTSIM_SNAP_BODY_CORRUPT            sync FAIL "PLANT snap-body-corrupt"      MKTSIM_SNAP_BODY_CORRUPT=1
churn_run MKTSIM_SNAP_OLD_MAGIC               sync FAIL "PLANT snap-old-magic"         MKTSIM_SNAP_OLD_MAGIC=1
churn_run MKTSIM_RECLAIM_AHEAD_OF_MARK        sync FAIL "PLANT reclaim-ahead-of-mark"  MKTSIM_RECLAIM_AHEAD_OF_MARK=1
churn_run MKTSIM_BATCH_REORDER                sync FAIL "PLANT batch-reorder"          MKTSIM_BATCH_REORDER=1
churn_run MKTSIM_BATCH_PARTIAL_READ           sync FAIL "PLANT batch-partial-read"     MKTSIM_BATCH_PARTIAL_READ=1
churn_run MKTSIM_COMMIT_BEFORE_GAP            sync FAIL "PLANT commit-before-gap"      MKTSIM_COMMIT_BEFORE_GAP=1
churn_run MKTSIM_EXPORT_TRUNCATE              sync FAIL "PLANT export-truncate"        MKTSIM_EXPORT_TRUNCATE=1
scenario_run MKTSIM_TIEBREAK_BUG        "$SCEN/tiebreak.py"
scenario_run MKTSIM_REJOIN_STALE_SEQ    "$SCEN/rejoin_stale_seq.py"
scenario_run MKTSIM_RUNTIME_MISCLASSIFY "$SCEN/runtime_misclassify.py"
scenario_run MKTSIM_GW_NO_SNAPSEQ       "$SCEN/gw_no_snapseq.py"

# ── 2. scenario harnesses ───────────────────────────────────────────────────
echo "" >&2
echo "== scenario harnesses ==" >&2
scenario_run undercut            "$SCEN/undercut.py"
scenario_run rejoin-delay        "$SCEN/rejoin_delay.py"
scenario_run gwsnap              "$SCEN/gwsnap.py"
scenario_run die-after-write     "$SCEN/die_after_write.py"
scenario_run lock                "$SCEN/lock.py"
scenario_run removed-snapshot    "$SCEN/removed_snapshot.py"
scenario_run corrupt-snapshot    "$SCEN/corrupt_snapshot.py"
scenario_run arch-before-snap    "$SCEN/arch_before_snap.py"
scenario_run rebuild-race        "$SCEN/rebuild_race.py"
scenario_run rebuild-timeout     "$SCEN/rebuild_timeout.py"
scenario_run rebuild-eos         "$SCEN/rebuild_eos.py"
scenario_run rebuild-stuck       "$SCEN/rebuild_stuck.py"

# ── 3. clean churn (expected green) ─────────────────────────────────────────
echo "" >&2
echo "== clean churn (expected green) ==" >&2
churn_clean() { # name mode extra...
    local name="$1" mode="$2"; shift 2
    local out="$TMP/$name.out"
    python3 "$CHURN" --bin "$BIN" --dir "$TMP/$name" --mode "$mode" \
        --seed 1 --traders 4 --kills 6 --survivors 2 --archive "$@" >"$out" 2>&1
    if grep -q '^RESULT: PASS' "$out"; then got=PASS; else got=FAIL; fi
    record "$name" PASS "$got"
}
churn_clean clean-sync  sync
churn_clean clean-async async
churn_clean clean-mixed mixed
churn_clean clean-snap1 sync --snap-every 1

# ── summary table ───────────────────────────────────────────────────────────
ELAPSED=$(( $(date +%s) - START ))
echo ""
echo "================ run_short summary ================"
for r in "${ROWS[@]}"; do echo "$r"; done
echo "---------------------------------------------------"
printf 'rows=%d mismatches=%d elapsed=%ds\n' "${#ROWS[@]}" "$FAILS" "$ELAPSED"
[ -n "$MISSING" ] && printf 'plant-list mismatch:%s\n' "$MISSING"
echo "==================================================="

if [ "$FAILS" -ne 0 ]; then
    echo "run_short: FAIL ($FAILS mismatch(es)); evidence kept in $TMP" >&2
    exit 1
fi
rm -rf "$TMP"
echo "run_short: OK"
exit 0
