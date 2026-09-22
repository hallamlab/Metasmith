"""Gather one CheckM2 row per scored bin for E1 and E2 into per-arm tables.

  e1  nf-core's checkm2_summary.tsv (short and long), with the E1 close-out's own reports replacing
      the sets it redid: strain_sample_49 MetaBAT2 and DAS Tool (gapfill), and every long sample's
      b19 MetaBAT2 and three-binner DAS Tool (in place of nf-core's two-binner long DAS Tool).
  e2  the one-row CheckM2 files the archive manifest lists, read from the task cache and relabelled.
"""
import argparse
import csv
import glob
import gzip
import os
import sys

CHECKM2 = ["Completeness", "Contamination", "Completeness_Model_Used", "Translation_Table_Used",
           "Coding_Density", "Contig_N50", "Average_Gene_Length", "Genome_Size", "GC_Content",
           "Total_Coding_Sequences", "Total_Contigs", "Max_Contig_Length"]
HEAD = ["arm", "sample", "dataset", "binner", "source", "bin"] + CHECKM2
NFCORE_TOOL = {"COMEBin": "COMEBin", "MetaBAT2": "MetaBAT2", "SemiBin2": "SemiBin2",
               "COMEBinRefined": "DASTool", "MetaBAT2Refined": "DASTool", "SemiBin2Refined": "DASTool"}


def read_tsv(path):
    op = gzip.open if path.endswith(".gz") else open
    with op(path, "rt", newline="") as f:
        yield from csv.DictReader(f, delimiter="\t")


def sheet(path):
    return {r["sample"]: r for r in read_tsv(path)}


def owner(name, samples):
    asm, tool, rest = name.split("-", 2)
    hits = [s for s in samples if rest == s or rest.startswith(s + ".") or rest.startswith(s + "_")]
    assert len(hits) == 1, (name, hits)
    return tool, hits[0]


def e1(a):
    samples = sheet(a.sheet)
    redone = {("strain_sample_49", "MetaBAT2"), ("strain_sample_49", "DASTool")}
    redone |= {(s, b) for s, r in samples.items() if r["arm"] == "long" for b in ("MetaBAT2", "DASTool")}
    rows = []
    for summary in a.nfcore:
        for r in read_tsv(summary):
            tool, s = owner(r["Name"], samples)
            if tool == "DASToolUnbinned":
                continue
            b = NFCORE_TOOL[tool]
            if (s, b) in redone:
                continue
            rows.append((s, b, "nfcore", r))
    reports = glob.glob(os.path.join(a.gapfill, "*", "*.quality_report.tsv")) + glob.glob(os.path.join(a.checkm2, "*.quality_report.tsv"))
    for path in reports:
        source = "gapfill" if path.startswith(a.gapfill) else "b19_rerun"
        tool, = [b for b in ("MetaBAT2", "DASTool") if f"-{b}-" in os.path.basename(path)]
        for r in read_tsv(path):
            _, s = owner(r["Name"], samples)
            assert (s, tool) in redone, path
            rows.append((s, tool, source, r))
    write(a.out, ([samples[s]["arm"], s, samples[s]["dataset"], b, src, r["Name"]] + [r[c] for c in CHECKM2]
                  for s, b, src, r in rows))


def e2(a):
    out = []
    for m in read_tsv(a.manifest):
        if m["kind"] != "checkm2":
            continue
        path = os.path.join(a.cache, m["shard"][:2], m["shard"][2:], m["relpath"])
        body = list(read_tsv(path))
        assert len(body) == 1, path
        r = body[0]
        out.append([m["arm"], m["sample"], m["dataset"], m["binner"], "metasmith", m["label"]] + [r[c] for c in CHECKM2])
    write(a.out, out)


def write(path, rows):
    rows = sorted(rows, key=lambda r: (r[0], r[1], r[3], r[5]))
    with gzip.open(path, "wt", newline="") if path.endswith(".gz") else open(path, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(HEAD)
        w.writerows(rows)
    counts = {}
    for r in rows:
        counts[(r[0], r[3], r[4])] = counts.get((r[0], r[3], r[4]), 0) + 1
    for k in sorted(counts):
        print(*k, counts[k], sep="\t", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="verb", required=True)
    q = sub.add_parser("e1")
    q.add_argument("--sheet", required=True)
    q.add_argument("--nfcore", nargs="+", required=True)
    q.add_argument("--gapfill", required=True)
    q.add_argument("--checkm2", required=True)
    q.add_argument("--out", required=True)
    q = sub.add_parser("e2")
    q.add_argument("--manifest", required=True)
    q.add_argument("--cache", required=True)
    q.add_argument("--out", required=True)
    a = p.parse_args()
    {"e1": e1, "e2": e2}[a.verb](a)


if __name__ == "__main__":
    main()
