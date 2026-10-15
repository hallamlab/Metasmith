#!/usr/bin/env python3
# unaligned.py <paf> <megahit.fa> <base.fa> <out.fa>: base contigs plus every MEGAHIT contig the base does not already hold.
# A MEGAHIT contig counts as held when alignments at >= 90% identity cover >= 90% of its length.
import sys
from collections import defaultdict

paf, megahit, base, out = sys.argv[1:]
spans, qlen = defaultdict(list), {}
for line in open(paf):
    f = line.split("\t")
    if int(f[10]) and int(f[9]) / int(f[10]) >= 0.90:
        spans[f[0]].append((int(f[2]), int(f[3])))
        qlen[f[0]] = int(f[1])


def covered(iv):
    total, end = 0, -1
    for a, b in sorted(iv):
        if b > end:
            total += b - max(a, end)
            end = b
    return total


held = {q for q, iv in spans.items() if covered(iv) >= 0.90 * qlen[q]}
kept = dropped = 0
with open(out, "w") as o:
    with open(base) as b:
        for line in b:
            o.write(line)
    keep = False
    for line in open(megahit):
        if line.startswith(">"):
            name = line[1:].split()[0]
            keep = name not in held
            kept += keep
            dropped += not keep
            if keep:
                o.write(f">megahit_{name}\n")
        elif keep:
            o.write(line)
print(f"megahit contigs kept {kept}, dropped as held {dropped}", file=sys.stderr)
