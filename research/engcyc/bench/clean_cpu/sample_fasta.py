"""Write the same seeded ORF sample embed.py draws, as a sanitised FASTA (first header token, ESM alphabet)."""
import random
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from embed import read_fasta  # noqa: E402

src, n, seed, dst = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
items = list(read_fasta(src))
if n and n < len(items):
    items = random.Random(seed).sample(items, n)
with open(dst, "w") as f:
    for name, seq in items:
        f.write(f">{name}\n{seq}\n")
