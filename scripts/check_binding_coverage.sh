#!/usr/bin/env bash
# check_binding_coverage.sh — BIND-COV-01: every public method of the Rust
# and Python bindings has at least one direct test.
#
# Modes:
#   --rust <repo>     gate the Rust binding of <repo> (mmapq-rs layout).
#   --python <repo>   gate the Python binding of <repo> (mmapq-py layout).
#   --list            modifier: print every item with covered/uncovered and
#                     the covering test (exit code unchanged: still fails on
#                     uncovered items).
#   --self-test       canaries on synthetic miniature repos in a temp dir —
#                     never reads or mutates the real trees.
#
# Item definitions (BIND-COV-01, decided):
#   Rust item    = (Type, method) for every `pub fn` / `pub unsafe fn` in an
#                  inherent `impl Type` / `impl<..> Type<..>` block of a pub
#                  type in a pub module, plus free `pub fn`s in pub modules.
#                  Excluded: src/ffi.rs; anything under #[cfg(test)];
#                  pub(crate)/pub(super)/pub(in ..); trait impls.
#   Python item  = (Class, method) for every method defined in mmapq's own
#                  source (not inherited from enum/int/Exception) whose name
#                  has no leading underscore, of every class in
#                  mmapq.__all__; plus every function in mmapq.__all__.
#                  Dunders excluded.
#
# Matching (decided):
#   Rust:    explicit tags. A covering test fn carries `// covers: Type::method`
#            comment line(s) directly above its #[test] attribute. The gate
#            fails when an item has no tag; a tag names no inventory item
#            (stale/typo); the tagged fn's body contains no `.method(` or
#            `Type::method(` call.
#   Python:  runtime trace. The unittest suite runs under sys.setprofile +
#            threading.setprofile; every 'call' event whose code object is in
#            src/mmapq and whose caller frame's file is under tests/ records
#            the item, mapped by (co_filename, co_firstlineno). Methods whose
#            class attribute is a functools.wraps decorator wrapper (the
#            _reader_operation guard) are recorded via their own code object
#            when the caller chain reaches tests/ through wrapper frames of
#            the item itself only — a wrapper entered from another binding
#            method does not count ("not only via another binding method").
#            NOTE for test authors: passing a method as a callable to
#            assertRaises(E, obj.m) is NOT a direct call — the caller is
#            unittest machinery, so the trace does not count it. Write
#            `with self.assertRaises(E): obj.m()` instead.
#
# Exclusions (scripts/binding_coverage_exclusions.txt, mirrored with this
# script): one line per item — `<binding> <item> <one-line reason>`, `#`
# comments and blank lines ignored. A line naming no current inventory item
# is an error: exclusions must be removed when the item disappears. An
# exclusion is a last resort — prefer a real test.
#
# Fail closed: exit 1 naming each uncovered item; exit non-zero (not 0)
# whenever the gate cannot run (missing python, missing repo, unimportable
# package, failing traced suite). A pass is claimed only from a full run.

set -u
export LC_ALL=C

usage() {
    cat >&2 <<'EOF'
usage: check_binding_coverage.sh --rust <repo> [--list]
       check_binding_coverage.sh --python <repo> [--list]
       check_binding_coverage.sh --self-test
MMAPQ_PYTHON may override the python3 used (default: python3 on PATH).
MMAPQ_BINDCOV_EXCLUSIONS may override the exclusions file
(default: binding_coverage_exclusions.txt next to this script).
EOF
}

# ── Python resolution: first, before any external command, so an empty PATH
# fails here rather than halfway through (self-test canary d).
PY="${MMAPQ_PYTHON:-}"
if [ -z "$PY" ]; then
    if command -v python3 >/dev/null 2>&1; then
        PY=python3
    fi
fi
if [ -z "$PY" ] || ! "$PY" -c 'import sys' >/dev/null 2>&1; then
    echo "ERROR: cannot run: no usable python3 (set MMAPQ_PYTHON)" >&2
    exit 1
fi

EXCL="${MMAPQ_BINDCOV_EXCLUSIONS:-$(cd "$(dirname "$0")" && pwd)/binding_coverage_exclusions.txt}"

RUST_REPO=""
PY_REPO=""
LIST=0
SELFTEST=0
while [ $# -gt 0 ]; do
    case "$1" in
        --rust)   [ $# -ge 2 ] || { usage; exit 2; }; RUST_REPO="$2"; shift 2 ;;
        --python) [ $# -ge 2 ] || { usage; exit 2; }; PY_REPO="$2"; shift 2 ;;
        --list)   LIST=1; shift ;;
        --self-test) SELFTEST=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done

if [ "$SELFTEST" -eq 1 ]; then
    if [ -n "$RUST_REPO" ] || [ -n "$PY_REPO" ] || [ "$LIST" -eq 1 ]; then
        usage; exit 2
    fi
elif [ -z "$RUST_REPO" ] && [ -z "$PY_REPO" ]; then
    usage; exit 2
elif [ "$LIST" -eq 1 ] && [ -z "$RUST_REPO" ] && [ -z "$PY_REPO" ]; then
    usage; exit 2
fi

# ── Exclusions parsing (shared by both modes) ────────────────────────────────
# Emits "<binding> <item>" pairs, one per line; validates the file shape.
parse_exclusions() {
    "$PY" - "$EXCL" <<'PYXEOF'
import sys
path = sys.argv[1]
try:
    lines = open(path).read().splitlines()
except FileNotFoundError:
    print(f'ERROR: exclusions file not found: {path}', file=sys.stderr)
    sys.exit(1)
for n, raw in enumerate(lines, 1):
    line = raw.strip()
    if not line or line.startswith('#'):
        continue
    parts = line.split(None, 2)
    if len(parts) < 3 or parts[0] not in ('rust', 'python'):
        print(f'ERROR: exclusions file {path}:{n}: expected '
              f'"<rust|python> <item> <one-line reason>", got: {raw}',
              file=sys.stderr)
        sys.exit(1)
    print(f'{parts[0]} {parts[1]}')
PYXEOF
}

EXCLUSIONS="$(parse_exclusions)" || { echo "ERROR: exclusions file invalid" >&2; exit 1; }
export BINDCOV_EXCLUSIONS="$EXCLUSIONS"

RC=0

# ── Rust mode ────────────────────────────────────────────────────────────────
if [ -n "$RUST_REPO" ]; then
    "$PY" - "$RUST_REPO" "$LIST" <<'PYRUST' || RC=1
import os
import re
import sys

repo = sys.argv[1]
list_mode = sys.argv[2] == '1'
exclusions = {
    line.split()[1]
    for line in os.environ.get('BINDCOV_EXCLUSIONS', '').splitlines()
    if line.startswith('rust ')
}

IMPL_RE = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?impl\b')
INHERENT_HEAD_RE = re.compile(
    r'^\s*impl\s*(?:<[^>]*>\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^{]*>)?'
    r'\s*(?:where\s[^{]*)?\{\s*$'
)
TRAIT_HEAD_RE = re.compile(r'^\s*impl\s*(?:<[^>]*>\s*)?.*\s+for\s+.*\{')
PUBFN_RE = re.compile(r'^\s*pub\s+(unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)')
CFGMOD_RE = re.compile(r'^\s*#\[[^\]]*cfg\(test\)[^\]]*\]')
MODHEAD_RE = re.compile(r'^\s*(pub(?:\([^)]*\))?\s+)?mod\b')
COVERS_RE = re.compile(r'^\s*//\s*covers:\s*(\S+)\s*$')
TESTATTR_RE = re.compile(r'^\s*#\[test\]')
ATTR_RE = re.compile(r'^\s*#\[')
IGNORE_RE = re.compile(r'^\s*#\[ignore')
FN_RE = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)')


def die(msg):
    print(f'ERROR: {msg}', file=sys.stderr)
    sys.exit(1)


def strip_comments(lines):
    """Comment-stripped copies of lines (// and /* */); string literals are
    not tracked — src/ and tests/ contain no raw strings holding these
    markers, and structure detection only needs approximate text."""
    out = []
    in_block = False
    for raw in lines:
        s = []
        i = 0
        n = len(raw)
        while i < n:
            if in_block:
                j = raw.find('*/', i)
                if j == -1:
                    i = n
                    break
                i = j + 2
                in_block = False
                continue
            c = raw.find('//', i)
            b = raw.find('/*', i)
            if c != -1 and (b == -1 or c < b):
                s.append(raw[i:c])
                break
            if b != -1:
                s.append(raw[i:b])
                i = b + 2
                in_block = True
                continue
            s.append(raw[i:])
            break
        out.append(''.join(s))
    return out


def cfg_test_ranges(lines, stripped):
    """(start, end) line-index ranges of `#[cfg(test)] mod NAME { ... }`
    blocks. A test-only mod declared `mod x;` in a file contributes nothing
    here (its body is another file, scanned on its own)."""
    ranges = []
    i = 0
    while i < len(lines):
        if CFGMOD_RE.match(lines[i]):
            j = i + 1
            while j < len(stripped) and not MODHEAD_RE.match(stripped[j]):
                if stripped[j].strip() and not ATTR_RE.match(stripped[j]):
                    break
                j += 1
            if j < len(stripped) and MODHEAD_RE.match(stripped[j]):
                m = re.match(r'\s*(pub(?:\([^)]*\))?\s+)?mod\s+'
                             r'[A-Za-z_][A-Za-z0-9_]*\s*(\{|;)', stripped[j])
                if m and m.group(2) == '{':
                    depth = 0
                    started = False
                    k = j
                    while k < len(stripped):
                        depth += stripped[k].count('{') - stripped[k].count('}')
                        if '{' in stripped[k]:
                            started = True
                        if started and depth == 0:
                            break
                        k += 1
                    ranges.append((j, k))
                    i = k + 1
                    continue
        i += 1
    return ranges


def scan_inventory(repo):
    src = os.path.join(repo, 'src')
    if not os.path.isdir(src):
        die(f'cannot run: {src} not found (wrong repo layout?)')
    items = {}   # item name -> (file, line); "Type::method" or "name"
    for path in sorted(os.path.join(dp, f) for dp, _, fs in os.walk(src)
                       for f in fs if f.endswith('.rs')):
        rel = os.path.relpath(path, src)
        if rel == 'ffi.rs':
            continue
        lines = open(path, encoding='utf-8').read().splitlines()
        stripped = strip_comments(lines)
        ranges = cfg_test_ranges(lines, stripped)

        def in_cfg_test(idx):
            return any(a <= idx <= b for a, b in ranges)

        impl_stack = []   # (type_or_None, depth_after_open)
        depth = 0
        for idx, s in enumerate(stripped):
            before = depth
            depth += s.count('{') - s.count('}')
            if in_cfg_test(idx):
                continue
            if IMPL_RE.match(s) and s.rstrip().endswith('{'):
                if TRAIT_HEAD_RE.match(s):
                    impl_stack.append((None, depth))
                else:
                    hm = INHERENT_HEAD_RE.match(s)
                    impl_stack.append((hm.group(1) if hm else None, depth))
            for entry in list(impl_stack):
                if depth < entry[1]:
                    impl_stack.remove(entry)
            fm = PUBFN_RE.match(s)
            if not fm:
                continue
            name = fm.group(2)
            if not impl_stack:
                items.setdefault(name, (rel, idx + 1))
            elif impl_stack[-1][0] is not None and before == impl_stack[-1][1]:
                items.setdefault(f'{impl_stack[-1][0]}::{name}', (rel, idx + 1))
    return items


def scan_tags(repo, items):
    """Collect tagged tests. Fails on orphan covers lines (a covers comment
    not immediately above #[test]), stale tags, and tagged bodies without
    the call."""
    errors = []
    tests = []   # (relfile, fn_name, [item names], body_text)
    roots = []
    tdir = os.path.join(repo, 'tests')
    if os.path.isdir(tdir):
        roots.append(tdir)
    if os.path.isdir(os.path.join(repo, 'src')):
        roots.append(os.path.join(repo, 'src'))
    for root in roots:
        for path in sorted(os.path.join(dp, f) for dp, _, fs in os.walk(root)
                           for f in fs if f.endswith('.rs')):
            rel = os.path.relpath(path, repo)
            lines = open(path, encoding='utf-8').read().splitlines()
            stripped = strip_comments(lines)
            is_src = root.endswith('src')
            ranges = cfg_test_ranges(lines, stripped) if is_src else \
                [(0, len(lines) - 1)]

            def valid(idx):
                return any(a <= idx <= b for a, b in ranges)

            i = 0
            while i < len(lines):
                m = COVERS_RE.match(lines[i])
                if not m:
                    i += 1
                    continue
                tags = [m.group(1)]
                j = i + 1
                while j < len(lines):
                    m2 = COVERS_RE.match(lines[j])
                    if not m2:
                        break
                    tags.append(m2.group(1))
                    j += 1
                # The covers block must sit directly above #[test].
                if j >= len(lines) or not TESTATTR_RE.match(lines[j]):
                    if valid(i):
                        errors.append(
                            f'{rel}:{i + 1}: covers tag not directly above '
                            f'#[test] — orphan or misplaced tag: '
                            f'"{tags[0]}"')
                    i = j
                    continue
                # fn header: skip further attributes after #[test]. An
                # #[ignore]d test never runs, so its covers tag covers
                # nothing at runtime — error rather than count it.
                k = j + 1
                ignored = False
                while k < len(lines) and ATTR_RE.match(lines[k]):
                    if IGNORE_RE.match(lines[k]):
                        ignored = True
                    k += 1
                fnm = FN_RE.match(stripped[k]) if k < len(stripped) else None
                if not fnm:
                    errors.append(
                        f'{rel}:{j + 1}: #[test] with covers tag but no fn '
                        f'found')
                    i = j
                    continue
                if ignored:
                    errors.append(
                        f'{rel}:{j + 1}: test {fnm.group(1)} is tagged but '
                        f'carries #[ignore] — an ignored test covers nothing '
                        f'at runtime')
                # body: brace-count from the fn line to its matching close.
                depth = 0
                started = False
                end = k
                while end < len(stripped):
                    depth += stripped[end].count('{') - stripped[end].count('}')
                    if '{' in stripped[end]:
                        started = True
                    if started and depth == 0:
                        break
                    end += 1
                body = '\n'.join(stripped[k:end + 1])
                if not valid(i):
                    # covers block outside any test region: orphan.
                    errors.append(
                        f'{rel}:{i + 1}: covers tag outside test code '
                        f'(tests/ or #[cfg(test)]): "{tags[0]}"')
                tests.append((rel, fnm.group(1), tags, body))
                i = end + 1
    # Validate tags against the inventory and bodies against the calls.
    covered = {}   # item -> (relfile, fn_name)
    for rel, fn, tags, body in tests:
        for tag in tags:
            if tag not in items:
                errors.append(
                    f'{rel}: test {fn} tags "{tag}" which is not an '
                    f'inventory item (stale or typo)')
                continue
            if '::' in tag:
                typ, meth = tag.split('::', 1)
                call_re = re.compile(
                    rf'\.{re.escape(meth)}\s*\(|{re.escape(typ)}::'
                    rf'{re.escape(meth)}\s*\(')
            else:
                call_re = re.compile(rf'\b{re.escape(tag)}\s*\(')
            if not call_re.search(body):
                errors.append(
                    f'{rel}: test {fn} is tagged "{tag}" but its body '
                    f'contains no direct call of it')
                continue
            covered.setdefault(tag, (rel, fn))
    return covered, errors


items = scan_inventory(repo)
covered, errors = scan_tags(repo, items)

for item in sorted(exclusions):
    if item not in items:
        errors.append(f'exclusion names no inventory item: rust {item}')

uncovered = sorted(
    it for it in items
    if it not in covered and it not in exclusions)

if list_mode:
    print(f'rust binding inventory: {len(items)} item(s)')
    for it in sorted(items):
        where = f'{items[it][0]}:{items[it][1]}'
        if it in exclusions:
            state = 'EXCLUDED'
            by = '-'
        elif it in covered:
            state = 'covered'
            by = f'{covered[it][0]}::{covered[it][1]}'
        else:
            state = 'UNCOVERED'
            by = '-'
        print(f'  {it:<50} {state:<10} {where:<28} {by}')

for e in errors:
    print(f'ERROR: {e}', file=sys.stderr)

if errors:
    print(f'FAIL: rust binding coverage — {len(errors)} tag/exclusion '
          f'error(s) (see above)', file=sys.stderr)
    sys.exit(1)
if uncovered:
    print(f'FAIL: rust binding coverage — {len(uncovered)} uncovered '
          f'item(s):', file=sys.stderr)
    for it in uncovered:
        print(f'  {it}', file=sys.stderr)
    sys.exit(1)
print(f'PASS: rust binding coverage — all {len(items)} item(s) covered '
      f'(tags verified: direct call in every tagged test body)')
PYRUST
fi

# ── Python mode ──────────────────────────────────────────────────────────────
if [ -n "$PY_REPO" ]; then
    "$PY" - "$PY_REPO" "$LIST" <<'PYPY' || RC=1
import ast
import os
import sys
import threading
import time
import unittest

repo = os.path.realpath(os.path.abspath(sys.argv[1]))
list_mode = sys.argv[2] == '1'
exclusions = {
    line.split()[1]
    for line in os.environ.get('BINDCOV_EXCLUSIONS', '').splitlines()
    if line.startswith('python ')
}
src_dir = os.path.join(repo, 'src', 'mmapq')
tests_dir = os.path.join(repo, 'tests')


def die(msg):
    print(f'ERROR: {msg}', file=sys.stderr)
    sys.exit(1)


if not os.path.isdir(src_dir):
    die(f'cannot run: {src_dir} not found (wrong repo layout?)')
if not os.path.isdir(tests_dir):
    die(f'cannot run: {tests_dir} not found (no test suite to trace)')


def node_first_lineno(node):
    # ast.FunctionDef/.lineno points at the first decorator (py3.8+),
    # matching co_firstlineno of the compiled code object.
    if node.decorator_list:
        return node.decorator_list[0].lineno
    return node.lineno


def build_inventory(src_dir):
    """AST inventory: every class in mmapq.__all__ (methods defined in
    mmapq's own source, no leading underscore, no dunders) plus every
    function in __all__. Returns (items, all_names) where items is a list
    of (class_or_None, name, relfile, first_lineno)."""
    init = os.path.join(src_dir, '__init__.py')
    tree = ast.parse(open(init, encoding='utf-8').read())
    all_names = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == '__all__':
                    all_names = [ast.literal_eval(e) for e in node.value.elts]
    if all_names is None:
        die('no __all__ in src/mmapq/__init__.py')
    classes, funcs, aliases = {}, {}, {}
    files = ['__init__.py'] + sorted(
        f for f in os.listdir(src_dir)
        if f.endswith('.py') and f != '__init__.py')
    for fn in files:
        tree = ast.parse(open(os.path.join(src_dir, fn), encoding='utf-8').read())
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                classes.setdefault(node.name, (fn, node))
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                funcs.setdefault(node.name, fn)
            elif (isinstance(node, ast.Assign) and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and isinstance(node.value, ast.Name)):
                aliases.setdefault(node.targets[0].id, node.value.id)

    def resolve(name, seen=()):
        if name in seen:
            return None
        if name in classes or name in funcs:
            return name
        if name in aliases:
            return resolve(aliases[name], seen + (name,))
        return None

    items = []
    for name in all_names:
        target = resolve(name)
        if target is None:
            continue  # constant / foreign re-export: neither class nor fn
        if target in funcs:
            items.append((None, name, funcs[target], None))
        else:
            cfile, cnode = classes[target]
            for sub in cnode.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if sub.name.startswith('_'):
                        continue
                    if sub.name.startswith('__') and sub.name.endswith('__'):
                        continue
                    items.append((name, sub.name, cfile, node_first_lineno(sub)))
    return items, all_names


items, all_names = build_inventory(src_dir)
item_ids = ['.'.join(filter(None, p[:2])) if p[0] else p[1] for p in items]

# ── Import the repo's own package (shadow any installed copy) ────────────────
sys.path.insert(0, os.path.join(repo, 'src'))
os.chdir(repo)
try:
    import mmapq
except Exception as exc:
    die(f'cannot run: import mmapq failed from {repo}/src ({exc!r}); '
        f'build the extension first (pip install -e .)')

# ── Map each item to its live code object ────────────────────────────────────
# The trace records (co_filename, co_firstlineno); mapping via the live
# attribute gives exact keys and ties each AST item to the object that fires
# the call event. A functools.wraps decorator wrapper on the class attribute
# is call machinery of the item itself, recorded separately.
import inspect

item_keys = {}     # (co_filename, co_first_lineno) -> item id
wrapper_codes = set()
for (cls, name, cfile, _ln), item_id in zip(items, item_ids):
    try:
        if cls is None:
            attr = getattr(mmapq, name)
        else:
            attr = getattr(getattr(mmapq, cls), name)
    except AttributeError:
        die(f'cannot run: item {item_id} not reachable on the imported '
            f'mmapq package')
    def _unfunc(x):
        return x.__func__ if hasattr(x, '__func__') else x

    orig = _unfunc(inspect.unwrap(attr))
    if not hasattr(orig, '__code__'):
        die(f'cannot run: item {item_id} resolved to a non-function')
    code = orig.__code__
    if os.path.basename(code.co_filename) != cfile:
        die(f'cannot run: item {item_id} maps to {code.co_filename} '
            f'but is defined in {cfile}')
    item_keys[(code.co_filename, code.co_firstlineno)] = item_id
    # A functools.wraps decorator wrapper is call machinery of the item
    # itself; remember its code object so the trace can step past it.
    if hasattr(attr, '__wrapped__'):
        wrapper_codes.add(_unfunc(attr).__code__)

# ── Reconciliation: every public method found live on an __all__ class that
# is defined in mmapq's own source must be an inventory item (the AST parser
# must not have missed it). Inherited enum/int/Exception surface is skipped
# by the co_filename check.
live_ids = set()
for cls_name in all_names:
    cls = getattr(mmapq, cls_name, None)
    if not inspect.isclass(cls):
        continue
    for m, attr in vars(cls).items():
        if m.startswith('_'):
            continue
        fn = attr
        if isinstance(fn, (classmethod, staticmethod)):
            fn = fn.__func__
        fn = inspect.unwrap(fn) if hasattr(fn, '__wrapped__') else fn
        if not hasattr(fn, '__code__'):
            continue
        if not os.path.realpath(fn.__code__.co_filename).startswith(
                os.path.realpath(src_dir) + os.sep):
            continue
        live_ids.add(f'{cls_name}.{m}')
for lid in sorted(live_ids - set(item_ids)):
    die(f'cannot run: live public method {lid} is missing from the AST '
        f'inventory — parser drift, fix the gate')

# ── Runtime trace ────────────────────────────────────────────────────────────
covered = {}   # item id -> test label
_tests_real = os.path.realpath(tests_dir)
_cache = {}


def _in_tests(fname):
    v = _cache.get(fname)
    if v is None:
        v = os.path.realpath(fname).startswith(_tests_real + os.sep)
        _cache[fname] = v
    return v


def profiler(frame, event, arg):
    if event != 'call':
        return
    item = item_keys.get((frame.f_code.co_filename,
                          frame.f_code.co_firstlineno))
    if item is None:
        return
    f = frame.f_back
    # Skip decorator-wrapper frames of the item itself (functools.wraps
    # call machinery); stop at the first non-wrapper frame so a wrapper
    # entered from another binding method does not count.
    while f is not None and f.f_code in wrapper_codes:
        f = f.f_back
    if f is not None and _in_tests(f.f_code.co_filename):
        label = (os.path.basename(f.f_code.co_filename) + '::'
                 + f.f_code.co_name)
        if item not in covered:
            covered[item] = label


sys.setprofile(profiler)
threading.setprofile(profiler)
t0 = time.monotonic()
loader = unittest.TestLoader()
suite = loader.discover('tests')
runner = unittest.TextTestRunner(verbosity=0, stream=sys.stderr)
result = runner.run(suite)
elapsed = time.monotonic() - t0
sys.setprofile(None)
threading.setprofile(None)

if not result.wasSuccessful():
    die(f'traced test suite failed ({len(result.failures)} failure(s), '
        f'{len(result.errors)} error(s)) — coverage result invalid')

# ── Exclusions, report ───────────────────────────────────────────────────────
errors = []
for item in sorted(exclusions):
    if item not in item_ids:
        errors.append(f'exclusion names no inventory item: python {item}')

uncovered = sorted(
    it for it in item_ids if it not in covered and it not in exclusions)

if list_mode:
    where = {}
    for (cls, name, cfile, ln), item_id in zip(items, item_ids):
        where[item_id] = f'{cfile}:{ln}'
    print(f'python binding inventory: {len(item_ids)} item(s); '
          f'traced suite {elapsed:.1f}s')
    for it in sorted(item_ids):
        if it in exclusions:
            state, by = 'EXCLUDED', '-'
        elif it in covered:
            state, by = 'covered', covered[it]
        else:
            state, by = 'UNCOVERED', '-'
        print(f'  {it:<50} {state:<10} {where.get(it, "-"):<28} {by}')

for e in errors:
    print(f'ERROR: {e}', file=sys.stderr)
if errors:
    print(f'FAIL: python binding coverage — {len(errors)} exclusion '
          f'error(s) (see above)', file=sys.stderr)
    sys.exit(1)
if uncovered:
    print(f'FAIL: python binding coverage — {len(uncovered)} uncovered '
          f'item(s):', file=sys.stderr)
    for it in uncovered:
        print(f'  {it}', file=sys.stderr)
    sys.exit(1)
print(f'PASS: python binding coverage — all {len(item_ids)} item(s) '
      f'covered (runtime trace of the unittest suite, {elapsed:.1f}s)')
PYPY
fi

# ── Self-test ────────────────────────────────────────────────────────────────
# Canaries on synthetic miniature repos in a temp dir. The real trees are
# never read (beyond this script and its exclusions file, overridden to an
# empty one) and never mutated. Every canary runs against a fresh copy of
# the pristine miniature.
if [ "$SELFTEST" -eq 1 ]; then
    T="$(mktemp -d)"
    trap 'rm -rf "$T"' EXIT
    SELF="$(cd "$(dirname "$0")" && pwd)/check_binding_coverage.sh"
    EMPTY_EXCL="$T/empty-exclusions.txt"
    : > "$EMPTY_EXCL"
    export MMAPQ_BINDCOV_EXCLUSIONS="$EMPTY_EXCL"

    ST_FAIL=0
    canary() {
        # canary <name> <expect: fail|pass> <output-must-name> -- cmd...
        local name="$1" expect="$2" must_name="$3"
        shift 4   # drop the '--'
        local out rc=0
        out="$("$@" 2>&1)" || rc=$?
        if [ "$expect" = fail ] && [ "$rc" -eq 0 ]; then
            echo "  FAIL: $name — exited 0, expected non-zero" >&2
            ST_FAIL=1
            return
        fi
        if [ "$expect" = pass ] && [ "$rc" -ne 0 ]; then
            echo "  FAIL: $name — exited $rc, expected 0" >&2
            echo "$out" >&2
            ST_FAIL=1
            return
        fi
        if [ -n "$must_name" ] && ! printf '%s' "$out" | grep -q "$must_name"; then
            echo "  FAIL: $name — output did not name '$must_name'" >&2
            printf '%s\n' "$out" >&2
            ST_FAIL=1
            return
        fi
        echo "  PASS: $name"
    }

    # ── Rust miniature ───────────────────────────────────────────────────────
    make_rust_mini() {
        local d="$1"
        mkdir -p "$d/src" "$d/tests"
        cat > "$d/src/lib.rs" <<'EOF'
pub struct QueueConfig {
    pub dir: String,
}

impl QueueConfig {
    pub fn new(dir: &str) -> Self {
        QueueConfig { dir: dir.to_string() }
    }
}

pub struct IndexedPoller {
    pub idx: i64,
}

impl IndexedPoller {
    pub fn poll(&self) -> i64 { self.idx }
    pub fn move_to_start(&mut self) -> bool { self.idx = 0; true }
}

pub struct IndexedBatchPoller {
    pub idx: i64,
}

impl IndexedBatchPoller {
    pub fn poll(&self) -> i64 { self.idx }
    pub fn move_to_start(&mut self) -> bool { self.idx = 0; true }
}

#[cfg(test)]
mod tests {
    use super::*;

    // covers: QueueConfig::new
    #[test]
    fn config_new_in_src() {
        let c = QueueConfig::new("d");
        assert_eq!(c.dir, "d");
    }
}
EOF
        cat > "$d/tests/poller_tests.rs" <<'EOF'
use mymini::{IndexedPoller, IndexedBatchPoller, QueueConfig};

// covers: QueueConfig::new
#[test]
fn config_new() {
    let c = QueueConfig::new("d");
    assert!(!c.dir.is_empty());
}

// covers: IndexedPoller::poll
// covers: IndexedPoller::move_to_start
#[test]
fn single_poller_moves() {
    let mut p = IndexedPoller { idx: 5 };
    assert_eq!(p.poll(), 5);
    assert!(p.move_to_start());
}

// covers: IndexedBatchPoller::poll
// covers: IndexedBatchPoller::move_to_start
#[test]
fn batch_poller_moves() {
    let mut p = IndexedBatchPoller { idx: 5 };
    assert_eq!(p.poll(), 5);
    assert!(p.move_to_start());
}
EOF
    }

    make_rust_mini "$T/rust-pristine"

    # R0 control: pristine miniature must PASS.
    canary "R0 rust control (pristine miniature passes)" pass "" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/rust-pristine", "$T/rust-ctl", dirs_exist_ok=True)
rc = subprocess.call(["bash", "$SELF", "--rust", "$T/rust-ctl"])
sys.exit(rc)
PYEOF

    # Ra planted untested pub method -> FAIL naming it (the batch poller —
    # the single poller keeps the same method name covered, so this canary
    # also proves the failure names the right TYPE).
    canary "Ra rust planted untested pub method -> FAIL naming it" \
        fail "IndexedBatchPoller::planted_untested_method" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/rust-pristine", "$T/rust-a", dirs_exist_ok=True)
p = "$T/rust-a/src/lib.rs"
s = open(p).read()
head, sep, tail = s.partition("pub struct IndexedBatchPoller")
assert sep
old = "    pub fn move_to_start(&mut self) -> bool { self.idx = 0; true }"
new = (old + "\n    pub fn planted_untested_method(&mut self) -> bool "
       "{ false }")
assert old in tail
open(p, "w").write(head + sep + tail.replace(old, new, 1))
out = subprocess.run(["bash", "$SELF", "--rust", "$T/rust-a"],
                     capture_output=True, text=True)
sys.stderr.write(out.stderr + out.stdout)
ok = ("IndexedBatchPoller::planted_untested_method" in out.stderr
      and "IndexedPoller::planted_untested_method" not in out.stderr)
sys.exit(out.returncode if ok else (0 if out.returncode else 1))
PYEOF

    # Rb only test of IndexedBatchPoller::move_to_start removed while
    # IndexedPoller::move_to_start stays tested -> FAIL naming the batch item.
    canary "Rb rust batch move_to_start test removed -> FAIL naming batch item only" \
        fail "IndexedBatchPoller::move_to_start" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/rust-pristine", "$T/rust-b", dirs_exist_ok=True)
p = "$T/rust-b/tests/poller_tests.rs"
s = open(p).read()
block = """// covers: IndexedBatchPoller::poll
// covers: IndexedBatchPoller::move_to_start
#[test]
fn batch_poller_moves() {
    let mut p = IndexedBatchPoller { idx: 5 };
    assert_eq!(p.poll(), 5);
    assert!(p.move_to_start());
}
"""
assert block in s
# remove the whole test; re-cover poll from a new test without move_to_start
s = s.replace(block, """// covers: IndexedBatchPoller::poll
#[test]
fn batch_poller_polls() {
    let p = IndexedBatchPoller { idx: 5 };
    assert_eq!(p.poll(), 5);
}
""")
open(p, "w").write(s)
out = subprocess.run(["bash", "$SELF", "--rust", "$T/rust-b"],
                     capture_output=True, text=True)
sys.stderr.write(out.stderr + out.stdout)
# the single poller item must remain covered: it must NOT be named uncovered
bad = "IndexedPoller::move_to_start" in out.stderr and "uncovered" in out.stderr
if bad:
    sys.exit(0)  # single item wrongly named uncovered -> force canary FAIL
sys.exit(out.returncode)
PYEOF

    # Rc tag on a fn whose body lacks the call -> FAIL.
    canary "Rc rust tag without call in body -> FAIL" \
        fail "tagged" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/rust-pristine", "$T/rust-c", dirs_exist_ok=True)
p = "$T/rust-c/tests/poller_tests.rs"
s = open(p).read()
s += """
// covers: QueueConfig::new
#[test]
fn does_not_call_new() {
    assert_eq!(1 + 1, 2);
}
"""
open(p, "w").write(s)
sys.exit(subprocess.call(["bash", "$SELF", "--rust", "$T/rust-c"]))
PYEOF

    # Rd tag naming a nonexistent item -> FAIL.
    canary "Rd rust stale tag naming no item -> FAIL" \
        fail "not an" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/rust-pristine", "$T/rust-d", dirs_exist_ok=True)
p = "$T/rust-d/tests/poller_tests.rs"
s = open(p).read()
s += """
// covers: QueueConfig::bogus_getter
#[test]
fn stale_tag_test() {
    let c = QueueConfig::new("d");
    assert!(!c.dir.is_empty());
}
"""
open(p, "w").write(s)
sys.exit(subprocess.call(["bash", "$SELF", "--rust", "$T/rust-d"]))
PYEOF

    # ── Python miniature ─────────────────────────────────────────────────────
    make_py_mini() {
        local d="$1"
        mkdir -p "$d/src/mmapq" "$d/tests"
        cat > "$d/src/mmapq/__init__.py" <<'EOF'
"""miniature mmapq package for gate self-test."""

from .pollers import IndexedPoller, IndexedBatchPoller
from .config import open_queue

__all__ = ["IndexedPoller", "IndexedBatchPoller", "open_queue"]
EOF
        cat > "$d/src/mmapq/pollers.py" <<'EOF'
"""miniature pollers."""


class IndexedPoller:
    def __init__(self, idx=0):
        self._idx = idx

    def poll(self):
        return self._idx

    def move_to_start(self):
        self._idx = 0
        return True

    def close(self):
        return None


class IndexedBatchPoller:
    def __init__(self, idx=0):
        self._idx = idx

    def poll(self):
        return self._idx

    def move_to_start(self):
        self._idx = 0
        return True

    def close(self):
        return None
EOF
        cat > "$d/src/mmapq/config.py" <<'EOF'
"""miniature factory function."""


def open_queue(name):
    return name
EOF
        cat > "$d/tests/test_cov.py" <<'EOF'
import unittest

import mmapq


class TestCoverage(unittest.TestCase):
    def test_single_poller(self):
        p = mmapq.IndexedPoller(idx=5)
        self.assertEqual(p.poll(), 5)
        self.assertTrue(p.move_to_start())
        p.close()

    def test_batch_poller(self):
        p = mmapq.IndexedBatchPoller(idx=5)
        self.assertEqual(p.poll(), 5)
        self.assertTrue(p.move_to_start())
        p.close()

    def test_open_queue(self):
        self.assertEqual(mmapq.open_queue("q"), "q")
EOF
    }

    make_py_mini "$T/py-pristine"

    # P0 control: pristine miniature must PASS (traced run).
    canary "P0 python control (pristine miniature passes)" pass "" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-ctl", dirs_exist_ok=True)
rc = subprocess.call(["bash", "$SELF", "--python", "$T/py-ctl"])
sys.exit(rc)
PYEOF

    # Pa planted untested public method -> FAIL naming it (the batch
    # poller — same-name precision as Ra).
    canary "Pa python planted untested public method -> FAIL naming it" \
        fail "IndexedBatchPoller.planted_untested_method" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-a", dirs_exist_ok=True)
p = "$T/py-a/src/mmapq/pollers.py"
s = open(p).read()
head, sep, tail = s.partition("class IndexedBatchPoller")
assert sep
old = "    def close(self):\n        return None\n"
new = old + "\n    def planted_untested_method(self):\n        return 1\n"
assert old in tail
open(p, "w").write(head + sep + tail.replace(old, new, 1))
out = subprocess.run(["bash", "$SELF", "--python", "$T/py-a"],
                     capture_output=True, text=True)
sys.stderr.write(out.stderr + out.stdout)
ok = ("IndexedBatchPoller.planted_untested_method" in out.stderr
      and "IndexedPoller.planted_untested_method" not in out.stderr)
sys.exit(out.returncode if ok else (0 if out.returncode else 1))
PYEOF

    # Pb only test of IndexedBatchPoller.move_to_start removed while
    # IndexedPoller.move_to_start stays tested -> FAIL naming the batch item.
    canary "Pb python batch move_to_start test removed -> FAIL naming batch item only" \
        fail "IndexedBatchPoller.move_to_start" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-b", dirs_exist_ok=True)
p = "$T/py-b/tests/test_cov.py"
s = open(p).read()
s = s.replace("""    def test_batch_poller(self):
        p = mmapq.IndexedBatchPoller(idx=5)
        self.assertEqual(p.poll(), 5)
        self.assertTrue(p.move_to_start())
        p.close()
""", """    def test_batch_poller(self):
        p = mmapq.IndexedBatchPoller(idx=5)
        self.assertEqual(p.poll(), 5)
        p.close()
""")
open(p, "w").write(s)
out = subprocess.run(["bash", "$SELF", "--python", "$T/py-b"],
                     capture_output=True, text=True)
sys.stderr.write(out.stderr + out.stdout)
bad = ("IndexedPoller.move_to_start" in out.stderr
       and "uncovered" in out.stderr)
if bad:
    sys.exit(0)  # single item wrongly named uncovered -> force canary FAIL
sys.exit(out.returncode)
PYEOF

    # D cannot run: python missing (empty PATH, no MMAPQ_PYTHON) -> non-zero
    # naming the python-resolution failure. /bin/bash is absolute so the
    # child PATH (empty dir) cannot break the interpreter lookup itself.
    mkdir -p "$T/emptybin"
    canary "D gate cannot run (python missing) -> non-zero naming it" fail \
        "no usable python3" -- \
        env PATH="$T/emptybin" MMAPQ_PYTHON= /bin/bash "$SELF" \
        --python "$T/py-pristine"

    # Dm mutation check: canary D's pinned message must come from the
    # python-resolution check itself, not from some later failure. Mutate
    # the check away (exit 1 -> :) in a copy; the pinned canary must then
    # FAIL (the run no longer names "no usable python3").
    if "$PY" - <<PYEOF
import os
import subprocess
import sys
s = open("$SELF", encoding='utf-8').read()
needle = ('    echo "ERROR: cannot run: no usable python3 '
          '(set MMAPQ_PYTHON)" >&2\n    exit 1\n')
if needle not in s:
    print('self-test: python-check needle not found (script reformatted?)',
          file=sys.stderr)
    sys.exit(1)
mutant = "$T/gate-mutant.sh"
open(mutant, 'w', encoding='utf-8').write(
    s.replace(needle, '    :\n', 1))
os.chmod(mutant, 0o755)
out = subprocess.run(
    ['env', 'PATH=$T/emptybin', 'MMAPQ_PYTHON=', '/bin/bash', mutant,
     '--python', '$T/py-pristine'],
    capture_output=True, text=True)
combined = out.stdout + out.stderr
pinned = out.returncode != 0 and 'no usable python3' in combined
if pinned:
    print('mutant still shows the pinned message from elsewhere',
          file=sys.stderr)
sys.exit(0 if not pinned else 1)
PYEOF
    then
        echo "  PASS: Dm python-check mutated away -> pinned canary fails"
    else
        echo "  FAIL: Dm python-check mutated away -> pinned canary fails" >&2
        ST_FAIL=1
    fi

    # D2 cannot run: repo missing -> non-zero.
    canary "D2 gate cannot run (repo missing) -> non-zero" fail "" -- \
        bash "$SELF" --python "$T/no-such-repo"

    # ── Python-mode fail-closed canaries (miniature copies) ──────────────────
    # Pe package not importable -> non-zero naming the import failure.
    canary "Pe python package not importable -> non-zero naming it" fail \
        "import mmapq failed" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-e", dirs_exist_ok=True)
p = "$T/py-e/src/mmapq/__init__.py"
open(p, 'a', encoding='utf-8').write("\nraise ImportError('boom')\n")
sys.exit(subprocess.call(["bash", "$SELF", "--python", "$T/py-e"]))
PYEOF

    # Pf src/mmapq missing -> non-zero naming it.
    canary "Pf python src/mmapq missing -> non-zero naming it" fail \
        "src/mmapq not found" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-f", dirs_exist_ok=True)
shutil.rmtree("$T/py-f/src/mmapq")
sys.exit(subprocess.call(["bash", "$SELF", "--python", "$T/py-f"]))
PYEOF

    # Pg tests/ missing -> non-zero naming it.
    canary "Pg python tests/ missing -> non-zero naming it" fail \
        "tests not found" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-g", dirs_exist_ok=True)
shutil.rmtree("$T/py-g/tests")
sys.exit(subprocess.call(["bash", "$SELF", "--python", "$T/py-g"]))
PYEOF

    # Ph a failing test in the traced suite -> non-zero naming it (the
    # coverage result is invalid when the suite fails).
    canary "Ph python failing test in suite -> non-zero naming it" fail \
        "traced test suite failed" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/py-pristine", "$T/py-h", dirs_exist_ok=True)
p = "$T/py-h/tests/test_cov.py"
open(p, 'a', encoding='utf-8').write(
    "\n\nclass TestBroken(unittest.TestCase):\n"
    "    def test_always_fails(self):\n"
    "        self.fail('planted failure')\n")
sys.exit(subprocess.call(["bash", "$SELF", "--python", "$T/py-h"]))
PYEOF

    # Re covers tag on an #[ignore]d test -> FAIL (covers nothing at runtime).
    canary "Re rust tag on #[ignore]d test -> FAIL naming it" fail \
        "ignored test covers nothing" -- \
        "$PY" - <<PYEOF
import shutil, subprocess, sys
shutil.copytree("$T/rust-pristine", "$T/rust-e", dirs_exist_ok=True)
p = "$T/rust-e/tests/poller_tests.rs"
s = open(p).read()
s += """
// covers: QueueConfig::new
#[test]
#[ignore]
fn ignored_new() {
    let c = QueueConfig::new("d");
    assert!(!c.dir.is_empty());
}
"""
open(p, "w").write(s)
sys.exit(subprocess.call(["bash", "$SELF", "--rust", "$T/rust-e"]))
PYEOF

    if [ "$ST_FAIL" -ne 0 ]; then
        echo "FAIL: check_binding_coverage.sh --self-test" >&2
        exit 1
    fi
    echo "PASS: check_binding_coverage.sh --self-test (all canaries)"
fi

exit "$RC"
