#!/usr/bin/env python3
"""Cut the contaminant screen by each entry's blank prevalence, and tag every ASV it matched.

An entry's prevalence is the `blanks=<seen>/<total>` field of its header. An entry without one
counts at every cut. A BLAST hit reports one entry per row, so the cut drops rows before
mito_checker.py reads them. The tag table holds every ASV with a qualifying hit in the uncut
screen and its highest matched prevalence: the ASV is removed at a cut t exactly when that
prevalence is at least t, so a study can retune the cut from the table without a rerun.
"""

import argparse
import gzip
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import filter_nontarget as fn  # noqa: E402
from mito_checker import BLAST6_COLUMNS, read_possibly_gz  # noqa: E402

SUFFIXES = (".fasta.gz", ".fasta")
COLUMNS = ["asv", "list", "entry", "pident", "prevalence", "studies", "blanks", "sources",
           "reads", "removed"]


def list_stem(path: Path) -> str:
    return next(path.name[: -len(s)] for s in SUFFIXES if path.name.endswith(s))


def read_support(set_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(p for p in set_dir.iterdir() if p.name.endswith(SUFFIXES)):
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt") as f:
            for line in f:
                if line.startswith(">"):
                    seqid, _, desc = line[1:].rstrip("\n").partition(" ")
                    fields = dict(kv.split("=", 1) for kv in desc.split(";") if "=" in kv)
                    rows.append({"sseqid": f"{list_stem(path)}|{seqid}",
                                 **{k: fields.get(k, "") for k in ("studies", "blanks", "sources")},
                                 "prevalence": prevalence(fields.get("blanks", ""))})
    return pd.DataFrame(rows, columns=["sseqid", "studies", "blanks", "sources", "prevalence"]) \
        .set_index("sseqid")


def prevalence(blanks: str) -> float:
    seen, _, total = blanks.partition("/")
    return int(seen) / int(total) if seen.isdigit() and total.isdigit() and int(total) else 1.0


def read_screen(path: Path) -> pd.DataFrame:
    return read_possibly_gz(path, sep="\t", header=None, names=BLAST6_COLUMNS,
                            dtype={"qseqid": str, "sseqid": str})


def cut(args):
    support = read_support(args.set)
    hits = read_screen(args.blast6)
    prevalence = hits["sseqid"].map(support["prevalence"]).fillna(1.0)
    kept = hits[prevalence >= args.min_prevalence]
    kept.to_csv(args.out, sep="\t", header=False, index=False)
    print(f"[INFO] {len(kept)} of {len(hits)} contaminant hits are to entries seen in at least "
          f"{args.min_prevalence:.0%} of their blanks")


def tag(args):
    support = read_support(args.set)
    hits = read_screen(args.blast6)
    hits = hits[hits["qlen"] > 0]
    hits = hits[(hits["pident"] >= args.min_pident)
                & (hits["length"] / hits["qlen"] * 100.0 >= args.min_percov)].copy()
    hits["asv"] = hits["qseqid"].str.split(";", n=1).str[0]
    hits = hits.join(support, on="sseqid")
    hits["prevalence"] = hits["prevalence"].fillna(1.0)
    best = (hits.sort_values(["prevalence", "bitscore"], ascending=False)
                .drop_duplicates("asv").set_index("asv"))

    counts = fn.load_table(args.counts)
    reads = counts.sum(axis=1)
    reads.index = reads.index.astype(str).str.split(";", n=1).str[0]
    removed = pd.read_csv(args.removed, sep="\t", index_col=0, usecols=[0, 1])
    removed.index = removed.index.astype(str).str.split(";", n=1).str[0]
    contaminant = set(removed.index[removed["reason"] == "contaminant"])

    listed = best["sseqid"].str.partition("|")
    table = pd.DataFrame({
        "asv": best.index, "list": listed[0].values, "entry": listed[2].values,
        "pident": best["pident"].values, "prevalence": best["prevalence"].round(4).values,
        "studies": best["studies"].values, "blanks": best["blanks"].values,
        "sources": best["sources"].values,
        "reads": [int(reads.get(a, 0)) for a in best.index],
        "removed": [a in contaminant for a in best.index],
    }, columns=COLUMNS).sort_values(["prevalence", "reads"], ascending=False)
    table.to_csv(args.out, sep="\t", index=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="verb", required=True)
    p = sub.add_parser("cut", help="keep the screen's hits to entries at or above the prevalence cut")
    p.add_argument("--blast6", type=Path, required=True, help="the uncut contaminant screen")
    p.add_argument("--set", type=Path, required=True, help="the contaminant reference set directory")
    p.add_argument("--min-prevalence", type=float, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(run=cut)
    p = sub.add_parser("tag", help="tag every ASV with a qualifying hit by its highest matched prevalence")
    p.add_argument("--blast6", type=Path, required=True, help="the uncut contaminant screen")
    p.add_argument("--set", type=Path, required=True, help="the contaminant reference set directory")
    p.add_argument("--min-pident", type=float, required=True)
    p.add_argument("--min-percov", type=float, required=True)
    p.add_argument("--counts", type=Path, required=True, help="the counts curation read")
    p.add_argument("--removed", type=Path, required=True, help="curate.py's removed counts")
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(run=tag)
    args = ap.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
