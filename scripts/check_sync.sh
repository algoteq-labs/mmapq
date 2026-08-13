#!/usr/bin/env bash
# check_sync.sh — verify mirrors and invariants across all three repos.
# Hard gate: exits 0 on sync, non-zero on any difference.
set -e

cd "$(dirname "$0")/../.."

diff -rq mmapq/src mmapq-rs/c/src -x '*.o'
diff -rq mmapq/include mmapq-rs/c/include
diff -rq mmapq/test mmapq-rs/c/test -x '*.o'
diff -rq mmapq/scripts mmapq-rs/c/scripts

# Verify that no private C struct layout is mirrored in the Python cdef
bash mmapq-py/scripts/check_no_layout_mirrors.sh --self-test
bash mmapq-py/scripts/check_no_layout_mirrors.sh

# Verify disjointness: no header in both public include/ and private src/internal/
MMAPQ_INC=$(ls mmapq/include/*.h | xargs -n1 basename | sort)
MMAPQ_INT=$(ls mmapq/src/internal/*.h 2>/dev/null | xargs -n1 basename | sort)
DUPES=$(comm -12 <(echo "$MMAPQ_INC") <(echo "$MMAPQ_INT"))
if [ -n "$DUPES" ]; then
    echo "ERROR: headers present in both include/ and src/internal/:" >&2
    echo "$DUPES" >&2
    exit 1
fi

# Verify manifest completeness: every include/*.h in PUBLIC_HEADERS and vice versa
PUB=$(cat mmapq/include/PUBLIC_HEADERS | sort)
MISSING_FROM_MANIFEST=$(comm -23 <(echo "$MMAPQ_INC") <(echo "$PUB"))
EXTRA_IN_MANIFEST=$(comm -13 <(echo "$MMAPQ_INC") <(echo "$PUB"))
if [ -n "$MISSING_FROM_MANIFEST" ]; then
    echo "ERROR: headers in include/ but not in PUBLIC_HEADERS:" >&2
    echo "$MISSING_FROM_MANIFEST" >&2
    exit 1
fi
if [ -n "$EXTRA_IN_MANIFEST" ]; then
    echo "ERROR: entries in PUBLIC_HEADERS with no matching include/*.h:" >&2
    echo "$EXTRA_IN_MANIFEST" >&2
    exit 1
fi

# Verify that every MMAPQ_* error code appears in all mirrors
bash mmapq/scripts/check_error_codes.sh --self-test
bash mmapq/scripts/check_error_codes.sh

# Verify ASSERT_AUDIT.md line numbers match source
bash mmapq/scripts/audit_asserts.sh --verify

# Verify README fenced C blocks compile against -I include only
bash mmapq/scripts/check_readme_examples.sh mmapq/README.md

# Verify the generated binding-coverage section in feature-matrix.md is current
GENERATED=$(bash mmapq/scripts/api_surface.sh --presence-matrix | grep -A100 '^## ' | head -n -1)
CHECKED_IN=$(awk '/^## Auto-generated binding coverage$/{p=1} /^<!-- \/GENERATED -->$/{p=0} p' mmapq/doc/feature-matrix.md)
if [ "$GENERATED" != "$CHECKED_IN" ]; then
    echo "ERROR: feature-matrix.md auto-generated section is stale. Regenerate with:" >&2
    echo "  bash scripts/api_surface.sh --presence-matrix > /tmp/gen &&" >&2
    echo "  # replace the section between <!-- GENERATED --> and <!-- /GENERATED -->" >&2
    exit 1
fi
echo "PASS: feature-matrix.md binding-coverage section is current"

# Verify every strdup() is followed by a NULL check
bash mmapq/scripts/check_strdup_guards.sh

# Verify binding symbols are declared in public headers
bash mmapq/scripts/check_binding_symbols.sh

# Verify no unprefixed symbols in the archive (strong linker gate)
bash mmapq/scripts/api_surface.sh --linker

# Verify static inline functions in public headers have mmapq_ prefix
BAD_INLINES=$(grep -rn '^static inline' mmapq/include/ 2>/dev/null \
    | grep -v 'mmapq_\|MMAPQ_\|mmapqi_' || true)
if [ -n "$BAD_INLINES" ]; then
    echo "ERROR: static inline functions in public headers without mmapq_ prefix:" >&2
    echo "$BAD_INLINES" >&2
    exit 1
fi

# Verify every gate fails closed when its inputs are absent (P5)
bash mmapq/scripts/check_gates_fail_closed.sh

echo "mirrors in sync"
