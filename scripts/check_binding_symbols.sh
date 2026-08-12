#!/usr/bin/env bash
# check_binding_symbols.sh — verify bindings only reference public symbols.
#
# Extracts every function name from mmapq-rs/src/ffi.rs and
# mmapq-py/setup.py cdef, and confirms each is declared in a public
# header (include/*.h). A function declared in bindings but not in any
# public header is a regression — it means the binding depends on an
# internal symbol.
#
# Usage:
#   bash scripts/check_binding_symbols.sh
#   bash scripts/check_binding_symbols.sh --self-test
#
# Exit code: 0 = all binding symbols found in public headers
#            1 = one or more symbols not found in public headers
#            2 = self-test failed
#
# Portable: bash + awk (mawk 1.3.4 compatible).

set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
INCLUDE_DIR="$HERE/include"
FFI_RS="$HERE/../mmapq-rs/src/ffi.rs"
SETUP_PY="$HERE/../mmapq-py/setup.py"

TMP_FFI=$(mktemp)
TMP_CDEF=$(mktemp)
SELF_DIR=$(mktemp -d)
cleanup() { rm -f "$TMP_FFI" "$TMP_CDEF"; rm -rf "$SELF_DIR"; }
trap cleanup EXIT

# Extract function names from ffi.rs 'extern "C"' blocks
extract_ffi_fns() {
    awk '
    /^[[:space:]]*pub fn [a-zA-Z_][a-zA-Z0-9_]*\(/ {
        n = $3
        sub(/\(.*/, "", n)
        print n
    }
    ' "$1"
}

# Extract function names from setup.py cdef
extract_cdef_fns() {
    awk '
    /^[[:space:]]*(int|void|int32_t|int64_t|uint8_t|uint16_t|uint32_t|uint64_t|char|unsigned|NextMove|PollResult|BatchPollResult|BatchEntryStatus|EntryStatus|ssize_t|size_t|uintptr_t|const|int32_t|int64_t)[ *]+[a-zA-Z_][a-zA-Z0-9_]*\(/ {
        line = $0
        sub(/^[[:space:]]*/, "", line)
        idx = index(line, "(")
        if (idx > 0) {
            before = substr(line, 1, idx - 1)
            split(before, words, /[ *]+/)
            name = words[1]
            for (i = 2; i <= length(words); i++) {
                if (words[i] != "") name = words[i]
            }
            gsub(/[^a-zA-Z0-9_]/, "", name)
            if (name != "" && name !~ /^(if|while|for|return|typedef|struct|union|enum)$/)
                print name
        }
    }
    ' "$1"
}

# Check if a function is declared in any public header
is_in_public_headers() {
    local fn="$1"
    for hdr in "$INCLUDE_DIR"/*.h; do
        if grep -qP '\b'"$fn"'\(' "$hdr" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}

main() {
    local exit_code=0

    extract_ffi_fns "$FFI_RS" | sort -u > "$TMP_FFI"
    extract_cdef_fns "$SETUP_PY" | sort -u > "$TMP_CDEF"

    # Check ffi.rs symbols
    while IFS= read -r fn; do
        [ -z "$fn" ] && continue
        if ! is_in_public_headers "$fn"; then
            echo "ERROR: ffi.rs declares '$fn' which is not in any public header" >&2
            exit_code=1
        fi
    done < "$TMP_FFI"

    # Check setup.py symbols (skip stdlib functions not part of mmapq API)
    while IFS= read -r fn; do
        [ -z "$fn" ] && continue
        case "$fn" in
            fopen|fclose|fread|fwrite|printf|fprintf|sprintf|snprintf|memcpy|memmove|memset|calloc|malloc|realloc|free|exit|abort|abs|close|open|read|write|pwrite|pread|ftruncate|lseek|stat|fstat|lstat|mmap|munmap|mprotect|msync|pipe|dup2|fcntl|ioctl|mkdir|rmdir|unlink|remove|rename|opendir|readdir|closedir|system|getenv|setenv|realpath|strlen|strcmp|strncmp|strcpy|strncpy|strdup|strndup|strcat|strncat|strchr|strrchr|strstr|strtok|strtol|strtoll|strtoul|strtoull|atoi|atol|atoll|qsort|bsearch|time|clock_gettime|gettimeofday|nanosleep|sleep|usleep|sched_yield|pthread_create|pthread_join|pthread_detach|pthread_self|pthread_mutex_lock|pthread_mutex_unlock|pthread_mutex_init|pthread_mutex_destroy|assert|signal|sigaction|kill)
                continue
                ;;
        esac
        if ! is_in_public_headers "$fn"; then
            echo "ERROR: setup.py cdef declares '$fn' which is not in any public header" >&2
            exit_code=1
        fi
    done < "$TMP_CDEF"

    if [ "$exit_code" -eq 0 ]; then
        echo "OK: all binding symbols found in public headers"
    fi
    return "$exit_code"
}

self_test() {
    local td="$SELF_DIR"
    mkdir -p "$td/include" "$td/ffi"

    # Create a public header declaring one function
    cat > "$td/include/public.h" << 'EOF'
int indexed_queue_create(void *a, void *b);
EOF

    # Pass case: ffi.rs declares a function that IS in a public header
    cat > "$td/ffi/pass.rs" << 'EOF'
extern "C" {
    pub fn indexed_queue_create(a: *mut std::ffi::c_void, b: *mut std::ffi::c_void) -> i32;
}
EOF

    # Fail case: ffi.rs declares a function NOT in any public header
    cat > "$td/ffi/fail.rs" << 'EOF'
extern "C" {
    pub fn indexed_queue_create(a: *mut std::ffi::c_void, b: *mut std::ffi::c_void) -> i32;
    pub fn internal_only_function();
}
EOF

    local pass_exit=0 fail_exit=0

    INCLUDE_DIR="$td/include" FFI_RS="$td/ffi/pass.rs" SETUP_PY=/dev/null main && pass_exit=0 || pass_exit=1
    INCLUDE_DIR="$td/include" FFI_RS="$td/ffi/fail.rs" SETUP_PY=/dev/null main && fail_exit=0 || fail_exit=1

    echo "  self-test pass case: $([ "$pass_exit" -eq 0 ] && echo 'OK' || echo 'FAIL')"
    echo "  self-test fail case: $([ "$fail_exit" -eq 1 ] && echo 'OK' || echo 'FAIL')"

    if [ "$pass_exit" -eq 0 ] && [ "$fail_exit" -eq 1 ]; then
        echo "self-test: PASS"
        return 0
    else
        echo "self-test: FAIL" >&2
        return 2
    fi
}

case "${1:-}" in
    --self-test) self_test ;;
    *) main ;;
esac
