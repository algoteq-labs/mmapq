#!/usr/bin/env bash
# check_published_urls.sh — two properties over the URL surface that ships to
# consumers, both fail-closed (P5):
#
#   1. Every submodule URL is RELATIVE (url = ../<name>.git). An absolute URL
#      in .gitmodules breaks the day the repo moves or gets forked, regardless
#      of which host it names. This is the property: relative, always.
#   2. No internal hostname appears in any file that is
#      published to consumers (PyPI / crates.io metadata, submodule config).
#      A bare hostname in src/ or README prose is not a violation; a hostname
#      shipped in package metadata is.
#
# FAILS-CLOSED: if any declared input file is absent, this exits non-zero.
# The two arms share one registry entry so both get the same absent-input
# coverage from check_gates_fail_closed.sh (the fail-open shape — "grep over
# an absent file finds nothing and reports clean" — is exactly what P5 exists
# to catch, and is why a missing file is fatal for BOTH arms).
#
# NOTE (P1): the published-file list below is hand-maintained, not derived.
# It will be wrong the first time metadata moves — a [package.metadata] block,
# a new homepage field, an added manifest — so extend it when that happens;
# do not assume it is generated.
#
# NOTE (hostname sweeps): a detector necessarily contains its own target
# pattern. This file's regex is composed from adjacent string literals
# (INTERNAL_HOST_PATTERNS, below) so the internal hostname never appears as a
# contiguous literal in this public repo. Exclude detector scripts from
# hostname sweeps — they scan only the PUBLISHED files and never their own
# source.
#
# Usage: bash scripts/check_published_urls.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Published files that must not carry an internal hostname.
PUBLISHED=(
    "$ROOT/mmapq-rs/.gitmodules"
    "$ROOT/mmapq-rs/Cargo.toml"
    "$ROOT/mmapq-py/.gitmodules"
    "$ROOT/mmapq-py/pyproject.toml"
    "$ROOT/mmapq-py/setup.py"
)

bad=0

# ── Arm 1: every submodule URL must be relative ────────────────────────────
# The .gitmodules files are explicit inputs (fail-closed if missing).
for gm in "$ROOT/mmapq-rs/.gitmodules" "$ROOT/mmapq-py/.gitmodules"; do
    if [ ! -f "$gm" ]; then
        echo "ERROR: missing .gitmodules at $gm (cannot verify submodule URLs)" >&2
        bad=1
        continue
    fi
    while IFS= read -r url; do
        if ! echo "$url" | grep -q '=\s*\.\./'; then
            echo "ERROR: absolute submodule URL in $gm: $url" >&2
            bad=1
        fi
    done < <(grep -hE '^\s*url\s*=' "$gm" || true)
done

# ── Arm 2: no internal hostname in published files ─────────────────────────
# Each input must exist (fail-closed); grep over an absent file would find
# nothing and report clean, which is the fail-open shape this arm rejects.
#
# Internal host tokens that must never appear in published files. Assembled
# from adjacent string literals so the internal hostname is not present as a
# contiguous literal in this repo (see NOTE above).
INTERNAL_HOST_PATTERNS='echi''dna|algoteq''.com'

for f in "${PUBLISHED[@]}"; do
    if [ ! -f "$f" ]; then
        echo "ERROR: missing published file $f (cannot verify it has no internal hostname)" >&2
        bad=1
        continue
    fi
    hit=$(grep -nE "$INTERNAL_HOST_PATTERNS" "$f" || true)
    if [ -n "$hit" ]; then
        echo "ERROR: internal hostname in published file $f:" >&2
        echo "$hit" >&2
        bad=1
    fi
done

if [ "$bad" -eq 0 ]; then
    echo "PASS: all submodule URLs are relative; no internal hostname in published files"
    exit 0
fi
exit 1
