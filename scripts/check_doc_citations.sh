#!/usr/bin/env bash
# check_doc_citations.sh — ban line-number citations in docs (17f).
# Hard gate: exits non-zero if any doc contains `path/file.ext:NNN`.
#
# `path/file.ext:NNN` in a doc is itself the violation: line numbers drift
# with every edit and nothing used to catch it (16e checks include/ header
# paths only; 12g checks presence-matrix freshness, not doc line refs). A
# sweep found 9 stale citations with drift from 4 to 147 lines. Replace
# `file.ext:NNN` with `symbol in file.ext`.
#
# ROADMAP.md is EXCLUDED by design: it is a point-in-time working document
# whose line refs (e.g. `region_ring.c:200`) are legitimately ephemeral.
# This exclusion is a decision written here, not an accident of the scanned
# path set.
#
# Scope: the line-number ban is the actionable defect (the 9 stale
# citations). A generic "cited file exists" check is not included: citation
# path roots vary (src/, include/, mmapq/, repo-absolute) and URLs
# (https://…/file.c) produce false positives that would make the gate noisy
# rather than zero-tolerance.
set -euo pipefail

cd "$(dirname "$0")/../.."

# DOC-02: doc/ is now layered (guide/reference/design/contributing/model) —
# the glob must be recursive or the moved pages escape the scan.
shopt -s globstar
DOCS=(
    mmapq/doc/**/*.md
    mmapq-py/docs/*.md
    mmapq/README.md
    mmapq-py/README.md
    mmapq-rs/README.md
)

# Fail closed: if the mmapq doc tree is absent (bare/broken checkout), the scan
# would trivially find nothing and pass — the gate exists to protect these
# docs, so their absence is a failure, not a pass (P5).
if [ ! -d mmapq/doc ]; then
    echo "ERROR: mmapq/doc not found — wrong working directory?" >&2
    exit 1
fi

# Check 1: no line-number citations.
violations=$(grep -rnE '\.(c|h|py|rs|sh):[0-9]+' "${DOCS[@]}" 2>/dev/null || true)
if [ -n "$violations" ]; then
    echo "ERROR: line-number citations found — replace file.ext:NNN with 'symbol in file.ext':" >&2
    echo "$violations" >&2
    exit 1
fi

echo "PASS: no line-number citations in docs"
