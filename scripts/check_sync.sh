#!/usr/bin/env bash
# check_sync.sh — verify mirrors and invariants across all three repos.
# Hard gate: exits 0 on sync, non-zero on any difference.
set -e

cd "$(dirname "$0")/../.."

# ── 17d: sibling-presence detection ─────────────────────────────────────────
# "Bare clone" is decided by the presence of the sibling directories, never by
# watching a diff fail — that conflates "sibling absent" with "sibling present
# and drifted", which is the failure mode this refactor exists to remove.
# Cross-repo checks (those that read mmapq-rs/ or mmapq-py/) are skipped here
# with an explicit, counted GATE_SKIP line. CI asserts GATE_SKIP == 0, so a
# bare-clone-shaped run can never be mistaken for full coverage.
HAVE_SIBLINGS=1
GATE_SKIP=0
missing=""
[ -d mmapq-rs ] || missing="$missing mmapq-rs"
[ -d mmapq-py ] || missing="$missing mmapq-py"
if [ -n "$missing" ]; then
    HAVE_SIBLINGS=0
    echo "GATE_SKIP: cross-repo checks skipped — bare clone (missing:$missing)" >&2
fi

run_cross() {
    if [ "$HAVE_SIBLINGS" -eq 0 ]; then
        GATE_SKIP=$((GATE_SKIP + 1))
        echo "GATE_SKIP: $* (bare clone — mmapq-rs/mmapq-py not present)" >&2
        return 0
    fi
    "$@"
}

# ── Structural guard (GATE-01c): cross-scope gates must go through run_cross ─
# check_gates_fail_closed.sh --list-registry=csv derives each gate's scope from
# its declared inputs; a gate whose inputs include mmapq-rs/ or mmapq-py/ is
# cross-repo and must be invoked via run_cross so a bare clone counts the skip
# (17d). Nothing called --list-registry=csv before, so the bare-clone path could
# silently rot — check_doc_links.sh was invoked unwrapped. This guard makes the
# classification mechanical: it reads check_sync.sh and fails if any cross-scope
# gate is invoked outside run_cross. It reads only mmapq/ files, so it runs (and
# passes) on a bare clone. It sits before every gate so its diagnosis comes
# first.
#
# Fail closed on the query. The output is captured into a variable (not read
# through `done < <(...)`, whose failure set -e cannot see); a failed query, or
# a registry that names no cross-scope gate, is itself an ERROR — a registry
# that cannot name its cross gates makes this guard meaningless.
if ! REG_CSV="$(bash mmapq/scripts/check_gates_fail_closed.sh --list-registry=csv)"; then
    echo "ERROR: check_gates_fail_closed.sh --list-registry=csv failed — cannot classify cross-scope gates" >&2
    exit 1
fi
if ! printf '%s\n' "$REG_CSV" | awk -F'\t' '$1 == "cross" { n++ } END { exit !(n > 0) }'; then
    echo "ERROR: check_gates_fail_closed.sh --list-registry=csv named no cross-scope gates" >&2
    exit 1
fi
#
# Exemption (explicit, one entry, with its reason): check_readme_examples.sh
# --self-test. That self-test DOES read mmapq-py (its self_test() function)
# and temporarily moves mmapq-py/src/mmapq/__init__.py; it is safe on a bare
# clone only because each sibling-reading arm checks for the sibling and SKIPs
# when it is absent (F3). Any other cross-gate --self-test must go through
# run_cross or be added here with its own reason.
GUARD_BAD=""
while IFS=$'\t' read -r _scope _gpath _gargs; do
    [ "$_scope" = "cross" ] || continue
    _gfile=$(basename "$_gpath")
    _in_run_cross=0
    while IFS= read -r _line; do
        case "$_line" in
            \#*) _in_run_cross=0; continue ;;
            '')  _in_run_cross=0; continue ;;
        esac
        _invoked=0
        case "$_line" in *"$_gfile"*) _invoked=1 ;; esac
        case "$_line" in
            *check_readme_examples.sh*--self-test*) _invoked=0 ;;
        esac
        if [ "$_invoked" -eq 1 ]; then
            _wrapped=0
            case "$_line" in *run_cross*) _wrapped=1 ;; esac
            if [ "$_in_run_cross" -eq 1 ]; then _wrapped=1; fi
            if [ "$_wrapped" -eq 0 ]; then
                GUARD_BAD="$GUARD_BAD $_gfile"
            fi
        fi
        case "$_line" in
            *run_cross*\\) _in_run_cross=1 ;;
            *\\) : ;;
            *) _in_run_cross=0 ;;
        esac
    done < mmapq/scripts/check_sync.sh
done <<< "$REG_CSV"
if [ -n "$GUARD_BAD" ]; then
    echo "ERROR: cross-scope gate(s) invoked outside run_cross in check_sync.sh:$GUARD_BAD" >&2
    echo "       A gate whose declared inputs include mmapq-rs/ or mmapq-py/ must be" >&2
    echo "       wrapped in run_cross (17d) so a bare clone counts the skip." >&2
    exit 1
fi
echo "PASS: every cross-scope registry gate is wrapped in run_cross"

# ── Local checks (valid on a bare mmapq clone) ──────────────────────────────

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

# Self-test the ASSERT_AUDIT disposition check, then verify counts + line numbers
bash mmapq/scripts/audit_asserts.sh --self-test
bash mmapq/scripts/audit_asserts.sh --verify

# Verify the GENERATED constants block in doc/reference/wire-format.md is current
GEN_WF=$(bash mmapq/scripts/gen_wire_format.sh)
CHECKED_WF=$(awk '/^<!-- GENERATED by scripts\/gen_wire_format.sh;/{p=1} /^<!-- \/GENERATED -->$/{p=0} p' mmapq/doc/reference/wire-format.md)
if [ "$GEN_WF" != "$CHECKED_WF" ]; then
    echo "ERROR: doc/reference/wire-format.md GENERATED constants block is stale. Regenerate with:" >&2
    echo "  bash scripts/gen_wire_format.sh" >&2
    echo "  # replace the section between <!-- GENERATED --> and <!-- /GENERATED -->" >&2
    exit 1
fi
echo "PASS: doc/reference/wire-format.md GENERATED constants block is current"

# Verify every strdup() is followed by a NULL check
bash mmapq/scripts/check_strdup_guards.sh

# ── CAND-54 §6(a)/§6(b): grep-checked allowlists ────────────────────────────
# §6(a): every start_offset writer in src/ is one of: the sanctioned
# accessor bodies (internal/appender_meta.h), the creation init pwrite
# (appender_meta.c), or these call sites — the archivers' forward publish
# CAS, live-pass/rollback/undo mins, and the commit re-check mins (single +
# batch). §6(b): payload-file deletion only via unlink() in the two
# reclaim_files. A new writer or deleter fails the build. Counts are per
# file, line-number-free; comments are excluded (lines whose first
# non-whitespace token is *, /* or //).
C54_COMMENT_RE='^[^:]+:[0-9]+:[[:space:]]*(\*|/\*|//)'

C54_SO_WRITES=$(grep -rnE 'appender_meta_start_offset_(cas|min)\s*\(' \
    mmapq/src --include='*.c' --include='*.h' \
    | grep -vE "$C54_COMMENT_RE" \
    | awk -F: '{print $1}' | sort | uniq -c | awk '{print $2 ":" $1}' \
    | tr '\n' ' ' | sed 's/ $//')
C54_SO_WRITERS_EXPECTED="mmapq/src/indexed_appender.c:1 mmapq/src/indexed_archiver.c:4 mmapq/src/indexed_batch_appender.c:1 mmapq/src/indexed_batch_archiver.c:4 mmapq/src/internal/appender_meta.h:2"
if [ "$C54_SO_WRITES" != "$C54_SO_WRITERS_EXPECTED" ]; then
    echo "ERROR: CAND-54 §6(a) start_offset-writer allowlist drifted" >&2
    echo "  expected: $C54_SO_WRITERS_EXPECTED" >&2
    echo "  actual:   $C54_SO_WRITES" >&2
    exit 1
fi
# Every mention of the offset constant is allowlisted per file: the define +
# the accessor bodies (appender_meta.h) and the creation init pwrite
# (appender_meta.c). A raw atomic_store through the constant, or a new
# accessor, is caught here even if it never calls the two accessors above.
C54_SO_REFS=$(grep -rn 'APPENDER_META_START_OFFSET_OFFSET' \
    mmapq/src --include='*.c' --include='*.h' \
    | grep -vE "$C54_COMMENT_RE" \
    | awk -F: '{print $1}' | sort | uniq -c | awk '{print $2 ":" $1}' \
    | tr '\n' ' ' | sed 's/ $//')
C54_SO_REFS_EXPECTED="mmapq/src/appender_meta.c:1 mmapq/src/internal/appender_meta.h:5"
if [ "$C54_SO_REFS" != "$C54_SO_REFS_EXPECTED" ]; then
    echo "ERROR: CAND-54 §6(a) start_offset offset-constant allowlist drifted" >&2
    echo "  expected: $C54_SO_REFS_EXPECTED" >&2
    echo "  actual:   $C54_SO_REFS" >&2
    exit 1
fi
# §6(b): file-deletion/rename call sites (unlink/unlinkat/remove/rename/
# renameat — a payload file can be "deleted" by any of them).
C54_DELETES=$(grep -rnE '\b(unlink|unlinkat|remove|rename|renameat)\s*\(' \
    mmapq/src --include='*.c' --include='*.h' \
    | grep -vE "$C54_COMMENT_RE" \
    | awk -F: '{print $1}' | sort | uniq -c | awk '{print $2 ":" $1}' \
    | tr '\n' ' ' | sed 's/ $//')
C54_DELETES_EXPECTED="mmapq/src/indexed_archiver.c:1 mmapq/src/indexed_batch_archiver.c:1"
if [ "$C54_DELETES" != "$C54_DELETES_EXPECTED" ]; then
    echo "ERROR: CAND-54 §6(b) payload-deletion allowlist drifted" >&2
    echo "  expected: $C54_DELETES_EXPECTED" >&2
    echo "  actual:   $C54_DELETES" >&2
    exit 1
fi
echo "PASS: CAND-54 §6(a)/§6(b) writer/unlink allowlists current"

# Verify every src/*.c appears in src/CMakeLists.txt's add_library list
bash mmapq/scripts/check_cmake_sources.sh

# Verify no unprefixed symbols in the archive (strong linker gate)
bash mmapq/scripts/api_surface.sh --linker

# Verify no stale macOS/Darwin/__APPLE__ references remain in the C tree (16a).
# The platform decision is "Linux only"; allow-list is zero.
MMAP_MACOS=$(grep -rn "__APPLE__\|Darwin\|macOS" mmapq/src mmapq/include mmapq/CMakeLists.txt mmapq/Makefile 2>/dev/null || true)
if [ -n "$MMAP_MACOS" ]; then
    echo "ERROR: stale macOS/Darwin/__APPLE__ references in the C tree:" >&2
    echo "$MMAP_MACOS" >&2
    exit 1
fi

# Verify static inline functions in public headers have mmapq_ prefix
BAD_INLINES=$(grep -rn '^static inline' mmapq/include/ 2>/dev/null \
    | grep -v 'mmapq_\|MMAPQ_\|mmapqi_' || true)
if [ -n "$BAD_INLINES" ]; then
    echo "ERROR: static inline functions in public headers without mmapq_ prefix:" >&2
    echo "$BAD_INLINES" >&2
    exit 1
fi

# Verify no doc cites a source file by line number, and cited files exist (17f)
bash mmapq/scripts/check_doc_citations.sh

# Verify every relative Markdown link resolves (DOC-02). Its registry inputs
# include mmapq-rs/README.md and mmapq-py/README.md, so it is cross-scope by the
# registry's own rule; unwrapped, a bare clone would die here and the 17d
# counted-skip path would be unreachable (GATE-01b).
run_cross bash mmapq/scripts/check_doc_links.sh

# ── Cross-repo checks (need mmapq-rs/ and mmapq-py/) ────────────────────────

# Verify every cdef header appears in the set_source include list (F3b follow-on).
# A missing #include causes implicit-declaration warnings, which under gcc < 14
# are mere warnings but silently truncate pointer returns (the mmapq_strerror
# bug: 64-bit pointer truncated to 32 bits by implicit int return).
run_cross bash -c '
IFS="|" read -ra INCS <<< "$(grep -oP "#include\s+\"[^\"]+" mmapq-py/setup.py | sed "s/#include \"//" | sort -u | tr "\n" "|")"
for h in mmap_platform.h; do
    if ! printf "%s\n" "${INCS[@]}" | grep -qx "$h"; then
        echo "ERROR: $h used in cdef but missing from set_source include list" >&2
        echo "       in mmapq-py/setup.py. Implicit declaration truncates pointers." >&2
        exit 1
    fi
done
'

# Mirror sync of the vendored C mirrors (mmapq-rs/c + mmapq-py/c)
run_cross bash mmapq/scripts/check_mirrors.sh

# Verify that no private C struct layout is mirrored in the Python cdef
run_cross bash mmapq-py/scripts/check_no_layout_mirrors.sh --self-test
run_cross bash mmapq-py/scripts/check_no_layout_mirrors.sh

# Verify that every MMAPQ_* error code appears in all mirrors
run_cross bash mmapq/scripts/check_error_codes.sh --self-test
run_cross bash mmapq/scripts/check_error_codes.sh

# Verify Python IntEnum members match C header enums (17e). This was reachable
# only via the fail-closed meta-harness baseline control; wire it here so a
# harness reorder cannot silently stop it running.
run_cross bash mmapq-py/scripts/check_enum_sync.sh

# Self-test the README checker before running it (local; fake inputs)
bash mmapq/scripts/check_readme_examples.sh --self-test

# Verify README fenced code blocks compile (C/Rust) or parse (Python)
run_cross bash mmapq/scripts/check_readme_examples.sh \
    mmapq/README.md \
    mmapq-rs/README.md \
    mmapq-py/README.md

# Verify the generated binding-coverage section in feature-matrix.md is current
# (the presence matrix reads mmapq-rs/src/ffi.rs and mmapq-py/setup.py → cross).
presence_fresh() {
    GENERATED=$(bash mmapq/scripts/api_surface.sh --presence-matrix | grep -A100 '^## ' | head -n -1)
    CHECKED_IN=$(awk '/^## Auto-generated binding coverage$/{p=1} /^<!-- \/GENERATED -->$/{p=0} p' mmapq/doc/reference/feature-matrix.md)
    if [ "$GENERATED" != "$CHECKED_IN" ]; then
        echo "ERROR: reference/feature-matrix.md auto-generated section is stale. Regenerate with:" >&2
        echo "  bash scripts/api_surface.sh --presence-matrix > /tmp/gen &&" >&2
        echo "  # replace the section between <!-- GENERATED --> and <!-- /GENERATED -->" >&2
        exit 1
    fi
    echo "PASS: reference/feature-matrix.md binding-coverage section is current"
}
run_cross presence_fresh

# Verify submodule URLs are relative and published files carry no internal host
run_cross bash mmapq/scripts/check_published_urls.sh

# Verify binding symbols are declared in public headers
run_cross bash mmapq/scripts/check_binding_symbols.sh

# Verify every gate fails closed when its inputs are absent (P5). Reads the
# sibling inputs, so cross-scope.
run_cross bash mmapq/scripts/check_gates_fail_closed.sh

# ── Final ───────────────────────────────────────────────────────────────────
if [ "$GATE_SKIP" -gt 0 ]; then
    echo "local checks passed ($GATE_SKIP cross-repo check(s) skipped — bare clone). CI must assert GATE_SKIP == 0." >&2
else
    echo "mirrors in sync"
fi
