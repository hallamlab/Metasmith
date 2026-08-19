# Every comment 6041e96 left standing that is still in HEAD, as a digest anchor.
#
# Comments are read by TOKENIZING the blessed blob, never by testing a line for a
# leading `#`: a trailing comment sits on a line that starts with code, so a prefix
# test cannot see one -- and a verifier written the same way agrees with the bug.
import io, subprocess, sys, tokenize
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from strip import comment_blocks, digest, own_line


def comments_of(text):
    toks = tokenize.generate_tokens(io.StringIO(text).readline)
    return {t.string.strip() for t in toks if t.type == tokenize.COMMENT and len(t.string.strip()) > 3}


for f in Path(sys.argv[1]).read_text().split():
    r = subprocess.run(["git", "show", f"6041e96:{f}"], capture_output=True, text=True)
    if r.returncode:
        continue
    try:
        blessed = comments_of(r.stdout)
    except tokenize.TokenError:
        continue
    if not blessed:
        continue
    text = Path(f).read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    com = {t.start[0]: t for t in tokenize.generate_tokens(io.StringIO(text).readline)
           if t.type == tokenize.COMMENT}
    own = {ln for ln, t in com.items() if own_line(lines, ln, t.start[1])}
    for block in comment_blocks(com, own):
        if any(com[ln].string.strip() in blessed for ln in block):
            print(f"{f}:@{digest(''.join(com[ln].string for ln in block))}")
    for ln in sorted(set(com) - own):
        if com[ln].string.strip() in blessed:
            print(f"{f}:@{digest(com[ln].string)}")
