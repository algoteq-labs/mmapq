#!/usr/bin/env bash
# check.sh — run the mmapq protocol model checks and compare with cfg/EXPECTED.
#
#   ./check.sh                 fast + minutes-long checks (~25 min on 2 cores)
#   ./check.sh --quick         counterexample checks only (seconds to ~1 min each)
#   ./check.sh --all           everything, incl. the ~70 min crash/listing-cap run
#   ./check.sh NAME [NAME..]   named checks (see cfg/EXPECTED)
#
# Exit status 0 iff every selected check matches its expected PASS/FAIL.
# An expected-FAIL check must violate the invariant named for it in
# EXPECTED_VIOLATIONS, and any TLC Warning fails the check.
# Requires Java 11+. Downloads tla2tools 1.7.4 once (pinned by SHA-256).
set -euo pipefail
cd "$(dirname "$0")"

JAR=tla2tools.jar
JAR_URL=https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
JAR_SHA=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88

if [ ! -f "$JAR" ]; then
    echo "downloading tla2tools 1.7.4 ..."
    curl -fsSL -o "$JAR.tmp" "$JAR_URL"
    mv "$JAR.tmp" "$JAR"
fi
echo "$JAR_SHA  $JAR" | sha256sum -c --quiet - || { echo "ERROR: $JAR checksum mismatch" >&2; exit 1; }

# ── EXPECTED_VIOLATIONS: the invariant each expected-FAIL check must violate ──
# Read before any TLC run, and fail closed: a new FAIL row cannot be added to
# cfg/EXPECTED without naming its invariant here. One ERROR line per problem.
declare -A exp_viol=()
ev_errors=0
EV=EXPECTED_VIOLATIONS
if [ ! -f "$EV" ]; then
    echo "ERROR: $EV is missing" >&2
    exit 1
fi
while IFS= read -r ev_line || [ -n "$ev_line" ]; do
    [[ -z "${ev_line//[[:space:]]/}" || "$ev_line" == \#* ]] && continue
    read -r ev_name ev_inv ev_extra <<< "$ev_line"
    if [ -z "$ev_name" ] || [ -z "$ev_inv" ] || [ -n "$ev_extra" ]; then
        echo "ERROR: $EV: line is not exactly two fields: '$ev_line'" >&2
        ev_errors=1
        continue
    fi
    if [ -n "${exp_viol[$ev_name]+x}" ]; then
        echo "ERROR: $EV: duplicate check name: $ev_name" >&2
        ev_errors=1
        continue
    fi
    exp_viol[$ev_name]="$ev_inv"
done < "$EV"

declare -A exp_verdict=()
while read -r name expected speed; do
    [[ -z "$name" || "$name" == \#* ]] && continue
    exp_verdict[$name]="$expected"
done < cfg/EXPECTED

for ev_name in "${!exp_viol[@]}"; do
    if [ -z "${exp_verdict[$ev_name]+x}" ]; then
        echo "ERROR: $EV: '$ev_name' is not a row of cfg/EXPECTED" >&2
        ev_errors=1
    elif [ "${exp_verdict[$ev_name]}" != "FAIL" ]; then
        echo "ERROR: $EV: '$ev_name' is not a FAIL row of cfg/EXPECTED (is ${exp_verdict[$ev_name]})" >&2
        ev_errors=1
    fi
done
for name in "${!exp_verdict[@]}"; do
    [ "${exp_verdict[$name]}" = "FAIL" ] || continue
    if [ -z "${exp_viol[$name]+x}" ]; then
        echo "ERROR: $EV: FAIL row '$name' of cfg/EXPECTED has no line" >&2
        ev_errors=1
    fi
done
[ "$ev_errors" -eq 0 ] || exit 1

max_speed=1; names=()
case "${1:-}" in
    --quick) max_speed=0 ;;
    --all)   max_speed=2 ;;
    "")      ;;
    *)       names=("$@"); max_speed=2 ;;
esac

mkdir -p out
fail=0
while read -r name expected speed; do
    [[ -z "$name" || "$name" == \#* ]] && continue
    if [ ${#names[@]} -gt 0 ]; then
        [[ " ${names[*]} " == *" $name "* ]] || continue
    elif [ "$speed" -gt "$max_speed" ]; then
        continue
    fi
    log="out/$name.out"
    java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -deadlock -workers auto \
        -config "cfg/$name.cfg" -metadir "out/states/$name" MmapqArchive.tla > "$log" 2>&1 || true
    if grep -q "Model checking completed. No error has been found." "$log"; then
        got=PASS
    elif grep -q "is violated" "$log"; then
        got=FAIL
    else
        got=ERROR
    fi
    detail=$(grep -oE "Invariant [A-Za-z0-9]+ is violated|[0-9,]+ distinct states found, 0 states left" "$log" | head -1 || true)
    if [ "$got" = "$expected" ]; then mark=ok; else mark=MISMATCH; fail=1; fi
    if [ "$got" = "FAIL" ]; then
        got_inv=$(grep -oE '^Error: Invariant [A-Za-z0-9_]+ is violated' "$log" | head -1 | sed -E 's/^Error: Invariant ([A-Za-z0-9_]+) is violated$/\1/' || true)
        if [ -z "$got_inv" ]; then
            mark=MISMATCH; fail=1; detail="violation is not an invariant"
        elif [ "$expected" = "FAIL" ] && [ "$got_inv" != "${exp_viol[$name]}" ]; then
            mark=MISMATCH; fail=1; detail="violated $got_inv, expected ${exp_viol[$name]}"
        fi
    fi
    if grep -q "Warning" "$log"; then
        mark=MISMATCH; fail=1
        detail="TLC Warning: $(grep -m1 "Warning" "$log" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    fi
    printf '%-28s expected %-4s got %-5s %-8s %s\n' "$name" "$expected" "$got" "$mark" "$detail"
    rm -rf "out/states/$name"
done < cfg/EXPECTED
exit $fail
