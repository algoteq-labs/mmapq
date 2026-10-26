#!/usr/bin/env bash
# check_doc_links.sh — every relative Markdown link in the docs resolves
# to an existing file (DOC-02 §10b, new gate).
#
# A moved or removed page leaves dangling links behind; nothing else in
# the gate set checks link targets (check_doc_citations bans line-number
# citations; check_readme_examples compiles code blocks). This gate scans
# [text](target) links in all Markdown of the three repos and fails on
# any relative target that does not exist. Absolute URLs, mailto:, and
# in-page #anchors are out of scope (no network in a gate).
#
# FAILS-CLOSED (P5): if the mmapq doc tree is absent the scan would
# trivially pass, so a missing tree is an error, not a pass.
set -euo pipefail

cd "$(dirname "$0")/../.."

# Declared inputs (check_gates_fail_closed registry): absent input is a
# failure, never a pass — a gate that scans nothing reports nothing.
for f in mmapq/doc mmapq/README.md mmapq-rs/README.md mmapq-py/README.md; do
    if [ ! -e "$f" ]; then
        echo "ERROR: $f not found — wrong working directory?" >&2
        exit 1
    fi
done

python3 - <<'PYEOF'
import os, re, sys

LINK = re.compile(r'\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)')
roots = ["mmapq", "mmapq-rs", "mmapq-py"]
skip_dirs = {".git", ".codeloom", "target", "build", ".venv", "node_modules",
             "cmake-build-debug", "out", "egg-info"}
bad = []
scanned = 0
for root in roots:
    if not os.path.isdir(root):
        print(f"ERROR: {root}/ not found — wrong working directory?", file=sys.stderr)
        sys.exit(1)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs
                       and not d.endswith(".egg-info")]
        for fn in filenames:
            if not fn.endswith(".md"):
                continue
            path = os.path.join(dirpath, fn)
            scanned += 1
            try:
                text = open(path, encoding="utf-8").read()
            except OSError as e:
                bad.append(f"{path}: unreadable: {e}")
                continue
            for m in LINK.finditer(text):
                target = m.group(1)
                if target.startswith(("http://", "https://", "mailto:", "#")):
                    continue
                target = target.split("#", 1)[0]
                if not target:
                    continue
                if not os.path.exists(os.path.join(dirpath, target)):
                    bad.append(f"{path}: broken relative link -> {target}")

if bad:
    print("ERROR: broken relative links:", file=sys.stderr)
    for b in bad:
        print("  " + b, file=sys.stderr)
    sys.exit(1)
print(f"PASS: all relative Markdown links resolve ({scanned} files scanned)")
PYEOF
