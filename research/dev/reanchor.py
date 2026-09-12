# Anchor every comment and docstring that survived, against the tree it survived in.
#
#     reanchor.py <file-list>            > keep.txt
#
# The keep-list is adjudication, and after a sweep everything still standing has
# been adjudicated -- including the blocks that were docstrings before
# `undocstring.py` reshaped them. An anchor is over the TEXT, so reshaping a block
# changes it: re-anchoring after the fact is what makes the tool a no-op on its own
# output, and without it the next run deletes its predecessor's keeps.
import ast, io, sys, tokenize
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from strip import PRAGMA, comment_blocks, digest, docstring_nodes, own_line

for f in Path(sys.argv[1]).read_text().split():
    text = Path(f).read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    toks = list(tokenize.generate_tokens(io.StringIO(text).readline))
    com = {t.start[0]: t for t in toks if t.type == tokenize.COMMENT}
    own = {ln for ln, t in com.items() if own_line(lines, ln, t.start[1])}
    for block in comment_blocks(com, own):
        if all(PRAGMA.search(com[ln].string) for ln in block):
            continue                      # the tool keeps pragmas by rule, not by list
        print(f"{f}:@{digest(''.join(com[ln].string for ln in block))}")
    for ln in sorted(set(com) - own):
        if not PRAGMA.search(com[ln].string):
            print(f"{f}:@{digest(com[ln].string)}")
    # Only the MODULE docstring is kept by rule in an argparse module. Skipping the
    # whole file here spared its function and class docstrings from being anchored,
    # so a later run deleted them -- which is how this tool's own docstrings, which
    # mention `__doc__` only because the heuristic is written here, survived a pass
    # that was supposed to convert them.
    argparse_module = "__doc__" in text
    for node, expr, _bl in docstring_nodes(ast.parse(text)):
        if argparse_module and isinstance(node, ast.Module):
            continue
        print(f"{f}:@{digest(expr.value.value)}")
