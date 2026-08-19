# Rewrite a kept docstring as the comment block it should have been.
#
#     undocstring.py <keepfile> [--apply] <paths...>
#
# AGENTS.md: do not write docstrings, except a module `__doc__` an argparse parser
# displays. The keep-list protects content that clears the comment bar, and some of
# that content happens to live in a docstring -- so the content stays and the shape
# changes, which is what `6041e96` did by hand for the handful it kept.
from __future__ import annotations

import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from strip import digest, docstring_nodes, load_keeps, own_line


def as_comment(body: str, indent: int) -> list[str]:
    # The docstring's text as a comment block, with its shape intact.
    #
    # Relative indentation is carried across, not flattened. A labelled block's
    # continuation lines and a bullet's hanging indent are how it reads, and a
    # trailing `\` flattened to column zero turns the line after it into a comment
    # the shell swallows -- which silently breaks a documented invocation.
    pad = " " * indent
    raw_lines = body.strip("\n").rstrip().splitlines()
    bodies = [l for l in raw_lines[1:] if l.strip()]
    common = min((len(l) - len(l.lstrip()) for l in bodies), default=0)
    out = []
    for i, raw in enumerate(raw_lines):
        text = raw.rstrip()
        if not text.strip():
            out.append(f"{pad}#\n")
            continue
        rel = (text if i == 0 else text[common:]).rstrip()
        out.append(f"{pad}# {rel}\n")
    return splice_continuations(out, pad)


def splice_continuations(block: list[str], pad: str) -> list[str]:
    # Join a line ending in `\` onto the one below it.
    #
    # The backslash was continuing a shell command across two docstring lines. A
    # comment cannot carry that: left in place it continues into the next `#`, so
    # the command pasted into a shell is one long comment that runs nothing, and
    # simply dropping it leaves two lines that no longer compose. Splicing gives
    # back the single command the docstring was describing.
    out = []
    for line in block:
        if out and out[-1].rstrip().endswith("\\"):
            out[-1] = out[-1].rstrip()[:-1].rstrip() + " " + line.strip().lstrip("#").strip() + "\n"
            continue
        out.append(line)
    return out


def convert(text: str, keep_digests: set[str], keep_module_doc: bool):
    lines = text.splitlines(keepends=True)
    tree = ast.parse(text)
    edits = []
    for node, expr, bodylen in docstring_nodes(tree):
        if isinstance(node, ast.Module) and keep_module_doc:
            continue                       # argparse displays it; it must stay a docstring
        if digest(expr.value.value) not in keep_digests:
            continue                       # the sweep deletes it; nothing to convert
        if not own_line(lines, expr.lineno, expr.col_offset):
            continue
        if lines[expr.end_lineno - 1][expr.end_col_offset:].strip():
            continue
        block = as_comment(expr.value.value, expr.col_offset)
        if bodylen == 1 and not isinstance(node, ast.Module):
            block.append(" " * expr.col_offset + "pass\n")
        edits.append((expr.lineno, expr.end_lineno, block))
    if not edits:
        return None
    out, prev = [], 1
    for start, end, block in sorted(edits):
        out.extend(lines[prev - 1:start - 1])
        out.extend(block)
        prev = end + 1
    out.extend(lines[prev - 1:])
    return "".join(out)


def main():
    keepfile, apply = sys.argv[1], "--apply" in sys.argv
    files = [a for a in sys.argv[2:] if not a.startswith("--")]
    _, digests = load_keeps(keepfile)
    changed = failures = 0
    for f in files:
        p = Path(f)
        text = p.read_text(encoding="utf-8")
        new = convert(text, digests.get(f, set()), "__doc__" in text)
        if new is None or new == text:
            continue
        try:
            ast.parse(new)
        except SyntaxError as e:
            failures += 1
            print(f"  WOULD NOT PARSE {f}: {e}")
            continue
        changed += 1
        if apply:
            p.write_text(new, encoding="utf-8")
    print(f"converted: {changed}  failures: {failures}")


if __name__ == "__main__":
    main()
