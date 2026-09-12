# Rust-aware comment stripper. Usage: rust_strip.py [--apply] <file>...
#
# Comments inside string literals, raw strings and char literals are left alone.
# A `# keep <line>` list is not supported: pass files you intend to sweep whole.
import sys
from pathlib import Path


def comment_spans(src):
    # Yield (start, end) offsets of every comment token.
    i, n = 0, len(src)
    spans = []
    while i < n:
        c = src[i]
        if c == '"':
            i += 1
            while i < n:
                if src[i] == '\\':
                    i += 2
                    continue
                if src[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == 'r' and i + 1 < n and src[i + 1] in '#"':
            j = i + 1
            hashes = 0
            while j < n and src[j] == '#':
                hashes += 1
                j += 1
            if j < n and src[j] == '"':
                close = '"' + '#' * hashes
                k = src.find(close, j + 1)
                i = n if k < 0 else k + len(close)
                continue
        if c == "'":
            # char literal or lifetime; a lifetime has no closing quote nearby
            j = i + 1
            if j < n and src[j] == '\\':
                j += 2
                while j < n and src[j] != "'":
                    j += 1
                i = j + 1
                continue
            if j + 1 < n and src[j + 1] == "'":
                i = j + 2
                continue
            i += 1
            continue
        if c == '/' and i + 1 < n and src[i + 1] == '/':
            j = src.find('\n', i)
            j = n if j < 0 else j
            spans.append((i, j))
            i = j
            continue
        if c == '/' and i + 1 < n and src[i + 1] == '*':
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith('/*', j):
                    depth += 1
                    j += 2
                elif src.startswith('*/', j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            spans.append((i, j))
            i = j
            continue
        i += 1
    return spans


def strip(src):
    spans = comment_spans(src)
    if not spans:
        return src, 0
    out = []
    prev = 0
    for s, e in spans:
        out.append(src[prev:s])
        prev = e
    out.append(src[prev:])
    text = ''.join(out)
    lines = text.split('\n')
    cleaned = []
    blanks = 0
    for line in lines:
        stripped = line.rstrip()
        if stripped == '':
            blanks += 1
            if blanks > 1 or not cleaned:
                continue
        else:
            blanks = 0
        cleaned.append(stripped)
    while cleaned and cleaned[-1] == '':
        cleaned.pop()
    return '\n'.join(cleaned) + '\n', len(spans)


def main():
    apply = '--apply' in sys.argv
    files = [a for a in sys.argv[1:] if not a.startswith('--')]
    for f in files:
        p = Path(f)
        src = p.read_text()
        new, n = strip(src)
        print(f"{f}: {n} comments, {len(src.splitlines())} -> {len(new.splitlines())} lines")
        if apply:
            p.write_text(new)


if __name__ == '__main__':
    main()
