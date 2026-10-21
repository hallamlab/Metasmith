"""Score CLEAN calls against the DH10B truth set, and against the GPU production calls.

Truth is ref/cohort_dh10b.tsv: the 1,288 adjudicable DH10B ORFs (exact Swiss-Prot K-12 md5 matches),
built by research/fabfos/annotation_lanes/pbert/build_cohort.py. The GPU reference is
ref/dh10b.clean.tsv, the production (GPU) CLEAN run copied from data/fabfos/benchmarks/lane_dh10b.
ORF ids are mapped to the bundle's ECDH10B_* ids by sequence md5, so a run over another DH10B
FASTA (the e4_mm RefSeq proteome) scores on the same cohort.

Usage: python score.py <bench dir> <calls.tsv> [<calls.tsv> ...]
"""
import collections
import hashlib
import json
import os
import re
import sys

B = sys.argv[1]
L4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")
MIN_SCORE = 0.02
E4 = f"{B}/inputs/dh10b.faa"  # the e4_mm RefSeq DH10B proteome, 4,468 seqs


def fasta(path):
    name, seq = None, []
    for line in open(path):
        if line.startswith(">"):
            if name:
                yield name, "".join(seq)
            name, seq = line[1:].split()[0], []
        else:
            seq.append(line.strip())
    if name:
        yield name, "".join(seq)


def md5(s):
    return hashlib.md5(s.encode()).hexdigest()


truth = {}
for line in open(f"{B}/ref/cohort_dh10b.tsv"):
    orf, ec = line.rstrip("\n").split("\t")[:2]
    if orf != "orf":
        truth[orf] = frozenset(e for e in ec.split(";") if L4.match(e))
by_md5 = collections.defaultdict(list)
for n, s in fasta(f"{B}/ref/dh10b.faa"):
    by_md5[md5(s)].append(n)
alias, in_e4 = {}, set()
for path in (f"{B}/ref/dh10b.faa", E4):
    for n, s in fasta(path):
        for k in (s, s.rstrip("*")):
            if md5(k) in by_md5:
                alias[n] = by_md5[md5(k)]
                if path == E4:
                    in_e4.update(by_md5[md5(k)])
# The first benchmark rows embedded the e4_mm RefSeq proteome, which lacks some cohort sequences;
# every run is scored on the cohort ORFs both proteomes carry, so the rows share one denominator.
truth = {o: t for o, t in truth.items() if o in in_e4}
print(json.dumps({"cohort_scored": len(truth)}))


def load(path):
    calls = collections.defaultdict(list)
    for line in open(path):
        f = line.rstrip("\n").split("\t")
        if f[0] == "Query ID":
            continue
        ec = f[1].replace("EC:", "").strip()
        try:
            sc = float(f[2])
        except ValueError:
            sc = 0.0
        for o in alias.get(f[0], []):
            calls[o].append((sc, ec))
    return calls


def prf(calls, thr):
    pred = {o: {e for s, e in v if s >= thr and L4.match(e)} for o, v in calls.items()}
    fired = {o for o in truth if pred.get(o)}
    correct = {o for o in fired if pred[o] & truth[o]}
    P = len(correct) / len(fired) if fired else 0.0
    R = len(correct) / len(truth)
    return dict(P=round(P, 4), R=round(R, 4), F1=round(2 * P * R / (P + R), 4) if P + R else 0.0,
                cov=round(len(fired) / len(truth), 4))


def auc(calls):
    pos, neg = [], []
    for o in truth:
        for s, e in calls.get(o, []):
            if L4.match(e):
                (pos if e in truth[o] else neg).append(s)
    ranked = sorted([(s, 1) for s in pos] + [(s, 0) for s in neg])
    rank_sum, i = 0.0, 0
    while i < len(ranked):
        j = i
        while j < len(ranked) and ranked[j][0] == ranked[i][0]:
            j += 1
        r = (i + j + 1) / 2
        rank_sum += r * sum(x[1] for x in ranked[i:j])
        i = j
    n1, n0 = len(pos), len(neg)
    return round((rank_sum - n1 * (n1 + 1) / 2) / (n1 * n0), 4) if n1 and n0 else None


def agree(a, b):
    common = [o for o in by_md5_ids if o in a and o in b]
    top = sum(max(a[o])[1] == max(b[o])[1] for o in common)
    eset = sum({e for _, e in a[o]} == {e for _, e in b[o]} for o in common)
    return dict(n=len(common), top_ec=round(top / len(common), 4), ec_set=round(eset / len(common), 4))


by_md5_ids = sorted({o for v in by_md5.values() for o in v})
gpu = load(f"{B}/ref/dh10b.clean.tsv")
for path in [f"{B}/ref/dh10b.clean.tsv"] + sys.argv[2:]:
    c = load(path)
    row = {"calls": os.path.basename(path), "orfs_mapped": len(c), "auc": auc(c),
           "thr0": prf(c, 0.0), f"thr{MIN_SCORE}": prf(c, MIN_SCORE), "vs_gpu": agree(c, gpu)}
    print(json.dumps(row), flush=True)
