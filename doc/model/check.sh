#!/usr/bin/env bash
# check.sh — run the mmapq protocol model checks and compare with cfg/EXPECTED.
#
#   ./check.sh                 fast + minutes-long checks (~25 min on 2 cores)
#   ./check.sh --quick         counterexample checks only (seconds each)
#   ./check.sh --all           everything, incl. the ~70 min crash/listing-cap run
#   ./check.sh NAME [NAME..]   named checks (see cfg/EXPECTED)
#
# Exit status 0 iff every selected check matches its expected PASS/FAIL.
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
    printf '%-28s expected %-4s got %-5s %-8s %s\n' "$name" "$expected" "$got" "$mark" "$detail"
    rm -rf "out/states/$name"
done < cfg/EXPECTED
exit $fail
