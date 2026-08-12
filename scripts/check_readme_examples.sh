#!/usr/bin/env bash
set -u

# check_readme_examples.sh — extract every fenced C block from README.md and
# compile against -I include only. Catches API signature drift in quick-starts.
#
# Each block is assembled into a compilable TU. Lines are classified as:
#   file-scope — includes, static function definitions (handler callbacks)
#   body      — executable statements (go inside main())
#
# Usage: ./scripts/check_readme_examples.sh [path/to/README.md]
#
# Returns 0 if all blocks compile, 1 otherwise.

README="${1:-README.md}"
FAIL=0
TOTAL=0
README_DIR=$(cd "$(dirname "$README")" && pwd)
CFLAGS="-std=gnu11 -D_GNU_SOURCE -I $README_DIR/include"

if [ ! -f "$README" ]; then
    echo "FAIL: $README not found"
    exit 1
fi

echo "--- check_readme_examples: $README ---"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

BLOCK=0
IN_C=0
while IFS= read -r line; do
    if [ "$IN_C" -eq 1 ]; then
        if [ "$line" = '```' ]; then
            IN_C=0
            TOTAL=$((TOTAL + 1))
            src="$TMPDIR/block_$BLOCK.c"
            pre="$TMPDIR/block_${BLOCK}_pre.c"
            body="$TMPDIR/block_${BLOCK}_body.c"
            out="$TMPDIR/block_${BLOCK}_out.c"

            # Classify each line: #include and static function definitions go to
            # file scope (pre), everything else goes inside main() (body).
            # A static function starts at "static ... (" and ends when brace
            # depth returns to the level at entry. Entry brace is counted from
            # the opening line itself (may contain "{").
            awk '
                /^#include/ { print > "'"$pre"'"; next }
                /^static / && /\(/ {
                    in_static = 1
                    brace = 1
                    for (i = 1; i <= length($0); i++) {
                        c = substr($0, i, 1)
                        if (c == "{") brace++
                        if (c == "}") brace--
                    }
                    print > "'"$pre"'"
                    next
                }
                in_static {
                    print > "'"$pre"'"
                    for (i = 1; i <= length($0); i++) {
                        c = substr($0, i, 1)
                        if (c == "{") brace++
                        if (c == "}") brace--
                    }
                    if (brace <= 1) { in_static = 0 }
                    next
                }
                { print > "'"$body"'" }
            ' "$src"

            {
                echo '#include <stdio.h>'
                echo '#include <assert.h>'
                cat "$pre" 2>/dev/null
                echo 'int main(void) {'
                cat "$body"
                echo 'return 0; }'
            } > "$out"

            if gcc $CFLAGS -x c "$out" -c -o /dev/null 2>/dev/null; then
                echo "  PASS: block $BLOCK"
            else
                echo "  FAIL: block $BLOCK"
                gcc $CFLAGS -x c "$out" -c -o /dev/null 2>&1 | head -5
                FAIL=$((FAIL + 1))
            fi
            BLOCK=$((BLOCK + 1))
        else
            echo "$line" >> "$TMPDIR/block_$BLOCK.c"
        fi
    elif [ "$line" = '```c' ]; then
        IN_C=1
        : > "$TMPDIR/block_$BLOCK.c"
    fi
done < "$README"

echo "---"
if [ "$FAIL" -eq 0 ]; then
    echo "PASS: $TOTAL block(s) compiled cleanly"
    exit 0
else
    echo "FAIL: $FAIL/$TOTAL block(s) failed to compile"
    exit 1
fi
