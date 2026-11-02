# Strip comments and docstrings from tracked Python, honouring a keep-list.
#
#     strip.py <keepfile> [--apply] [paths...]
#
# Without --apply it reports only. With no paths it sweeps every tracked `.py`
# outside EXCLUDE_PREFIXES/EXCLUDE_SUBSTRINGS -- which is why the committed
# keep-list anchors the whole tree and not just the files last swept: a bare run
# must be a no-op, or it silently deletes what an earlier pass adjudicated.
#
# The bar is AGENTS.md's: a comment earns its place by naming something invisible
# from the file. What that means in practice is `research/dev/keep.txt`, which is
# the adjudication itself and not a filter over one.
from __future__ import annotations

import ast
import hashlib
import io
import re
import subprocess
import sys
import tokenize
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve()
ROOT = Path.cwd()

EXCLUDE_PREFIXES = (
    "research/aspire/upstream/",
    "data/",
    "tests/metasmith_libraries/test_msm_home/",
    "MetasmithLibraries/",
)

# Programs a transform stages to a worker, not code this repo imports. Out of
# scope by the same argument that puts `research/aspire/upstream/` out of it: the
# comments are the staged program's, read where it runs.
#
# The previous pass left them alone for a different and much weaker reason -- they
# were still inside `DRIVER = r'''...'''` string literals then, so the tokenizer
# could not see them. That was not a decision, so do not read it as precedent.
EXCLUDE_SUBSTRINGS = (
    "/resources/lib/",
    "/resources/buildlib/",
)

PRAGMA = re.compile(
    r"(noqa|type:\s*ignore|^#\s*type:|pragma|fmt:\s*(on|off)|pylint|mypy|nosec"
    r"|isort|ruff|^#\s*-\*-|^#SBATCH|\bTODO\b|\bFIXME\b|RECONCILE-ME)"
)


def load_keeps(path):
    # `{path: {line, ...}}` and `{path: {digest, ...}}` from the keep-list.
    #
    # A `path:12` anchor names a line; a `path:@<hex>` anchor names the TEXT of a
    # comment block or docstring. Both are honoured, but only the digest survives
    # this tool's own edits -- every line below a deletion moves, so a keep-list
    # written against the unswept tree points somewhere else on the second run.
    lines = defaultdict(set)
    digests = defaultdict(set)
    for raw in Path(path).read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fpath, _, spec = line.rpartition(":")
        if spec.startswith("@"):
            digests[fpath].add(spec[1:])
        elif "-" in spec:
            a, b = spec.split("-", 1)
            lines[fpath].update(range(int(a), int(b) + 1))
        else:
            lines[fpath].add(int(spec))
    return lines, digests


def digest(text: str) -> str:
    # A keep anchor that moves with the text it names, not with its line.
    normalised = " ".join(text.split())
    return hashlib.sha1(normalised.encode("utf-8")).hexdigest()[:16]


def own_line(lines, lineno, col):
    return lines[lineno - 1][:col].strip() == ""


def docstring_nodes(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if not body:
                continue
            first = body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                yield node, first, len(body)


def comment_blocks(comment_lines, own_comment):
    # The contiguous runs of own-line comments, as sorted line lists.
    out, cur = [], []
    for ln in sorted(own_comment):
        if cur and ln == cur[-1] + 1:
            cur.append(ln)
        else:
            if cur:
                out.append(cur)
            cur = [ln]
    if cur:
        out.append(cur)
    return out


def process(text, keeps, keep_digests, keep_module_doc):
    lines = text.splitlines(keepends=True)
    n = len(lines)
    tree = ast.parse(text)

    comment_lines = {}
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError) as e:
        raise RuntimeError(f"tokenize failed: {e}")
    for tok in toks:
        if tok.type == tokenize.COMMENT:
            comment_lines[tok.start[0]] = tok

    # a keep anchor holds the whole contiguous own-line comment block it lands in
    expanded = set(keeps)
    own_comment = {
        ln for ln, t in comment_lines.items() if own_line(lines, ln, t.start[1])
    }
    fired = set()
    for block in comment_blocks(comment_lines, own_comment):
        body = "".join(comment_lines[ln].string for ln in block)
        d = digest(body)
        if set(block) & set(keeps) or d in keep_digests:
            expanded.update(block)
            fired.add(d)
    # A trailing comment belongs to no block, so it is addressed by its own text.
    # Without this a `@digest` anchor on one silently matches nothing -- and the
    # head of `X = 1  # units...` is truncated while the own-line continuation
    # under it is kept, leaving a fragment with no antecedent.
    for ln in set(comment_lines) - own_comment:
        d = digest(comment_lines[ln].string)
        if d in keep_digests:
            expanded.add(ln)
            fired.add(d)

    delete = set()
    truncate = {}
    replace = {}
    stats = {"comments": 0, "docstrings": 0, "kept_comments": 0, "kept_docstrings": 0}

    for ln, tok in sorted(comment_lines.items()):
        txt = tok.string
        if ln in expanded or (ln == 1 and txt.startswith("#!")) or PRAGMA.search(txt):
            stats["kept_comments"] += 1
            continue
        stats["comments"] += 1
        if own_line(lines, ln, tok.start[1]):
            delete.add(ln)
        else:
            prefix = lines[ln - 1][: tok.start[1]].rstrip()
            truncate[ln] = prefix + "\n"

    for node, expr, bodylen in docstring_nodes(tree):
        s, e = expr.lineno, expr.end_lineno
        d = digest(expr.value.value)
        if expanded & set(range(s, e + 1)) or d in keep_digests:
            stats["kept_docstrings"] += 1
            fired.add(d)
            continue
        if isinstance(node, ast.Module) and keep_module_doc:
            stats["kept_docstrings"] += 1
            continue
        if not own_line(lines, s, expr.col_offset):
            stats["kept_docstrings"] += 1
            continue
        tail = lines[e - 1][expr.end_col_offset:]
        if tail.strip() not in ("", ):
            stats["kept_docstrings"] += 1
            continue
        stats["docstrings"] += 1
        if bodylen == 1 and not isinstance(node, ast.Module):
            indent = " " * expr.col_offset
            replace[s] = indent + "pass\n"
            delete.update(range(s + 1, e + 1))
        else:
            delete.update(range(s, e + 1))
        # Swallow the blank lines that followed the docstring -- but only the ones
        # still INSIDE the body. Past the end of the definition they are the
        # separator before the next one, and eating those runs two top-level
        # definitions together.
        k = e + 1
        while k <= n and lines[k - 1].strip() == "":
            nxt = next((lines[j] for j in range(k, n) if lines[j].strip()), None)
            if nxt is not None and len(nxt) - len(nxt.lstrip()) < expr.col_offset:
                break
            delete.add(k)
            k += 1

    out = []
    for i in range(1, n + 1):
        if i in replace:
            out.append(replace[i])
        elif i in delete:
            continue
        elif i in truncate:
            out.append(truncate[i])
        else:
            out.append(lines[i - 1])

    # collapse runs of 3+ blank lines and strip leading/trailing blanks
    cleaned = []
    blanks = 0
    for line in out:
        if line.strip() == "":
            blanks += 1
            if blanks > 2 or not cleaned:
                continue
        else:
            blanks = 0
        cleaned.append(line)
    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()
    result = "".join(cleaned)
    if result and not result.endswith("\n"):
        result += "\n"
    anchored = set(comment_lines)
    for _node, expr, _bl in docstring_nodes(tree):
        anchored.update(range(expr.lineno, expr.end_lineno + 1))
    return result, stats, expanded, anchored, fired


def main():
    keepfile = sys.argv[1]
    apply = "--apply" in sys.argv[2:]
    explicit = [a for a in sys.argv[2:] if not a.startswith("--")]
    keeps, keep_digests = load_keeps(keepfile)

    if explicit:
        files = explicit
    else:
        files = subprocess.run(
            ["git", "ls-files", "*.py"], capture_output=True, text=True, check=True
        ).stdout.split()
        files = [
            f for f in files
            if not f.startswith(EXCLUDE_PREFIXES)
            and not any(s in "/" + f for s in EXCLUDE_SUBSTRINGS)
        ]

    total = defaultdict(int)
    failures = []
    unmatched = []
    changed = 0
    for f in files:
        p = Path(f)
        try:
            text = p.read_text(encoding="utf-8")
        except Exception as e:
            failures.append((f, f"read: {e}"))
            continue
        # `__doc__` appearing anywhere in the file, because that is what an argparse
        # parser reads it through (`ArgumentParser(description=__doc__)`). A substring
        # test, so a module whose `__doc__` some OTHER file displays is not spared,
        # and one that merely mentions the name is. Both are checkable by hand and
        # neither is true in this tree today.
        keep_module_doc = "__doc__" in text
        try:
            new, stats, expanded, comment_line_set, fired = process(
                text, keeps.get(f, set()), keep_digests.get(f, set()), keep_module_doc)
        except Exception as e:
            failures.append((f, str(e)))
            continue
        for k, v in stats.items():
            total[k] += v
        for anchor in keeps.get(f, set()):
            if anchor not in comment_line_set:
                unmatched.append(f"{f}:{anchor}")
        # A digest that matched nothing is a keep that silently lapsed, which is
        # how a comment the list was written to protect gets deleted anyway.
        for d in keep_digests.get(f, set()) - fired:
            unmatched.append(f"{f}:@{d}")
        if new != text:
            changed += 1
            if apply:
                p.write_text(new, encoding="utf-8")
                try:
                    compile(new, f, "exec")
                except SyntaxError as e:
                    failures.append((f, f"POST-COMPILE: {e}"))
            else:
                try:
                    compile(new, f, "exec")
                except SyntaxError as e:
                    failures.append((f, f"would-not-compile: {e}"))

    print(f"files considered: {len(files)}  changed: {changed}")
    print(dict(total))
    if unmatched:
        print(f"\nkeep anchors not on a comment line ({len(unmatched)}):")
        for u in sorted(unmatched):
            print("  ", u)
    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for f, e in failures:
            print("  ", f, "->", e)


if __name__ == "__main__":
    main()
