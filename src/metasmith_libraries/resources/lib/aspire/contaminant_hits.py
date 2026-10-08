#!/usr/bin/env python3
"""Name the contaminant list, entry and support behind every ASV curation removed as a contaminant."""

import argparse
import gzip
from pathlib import Path

import pandas as pd

SUFFIXES = (".fasta.gz", ".fasta")
COLUMNS = ["asv", "list", "entry", "pident", "studies", "blanks", "sources", "reads"]


def list_stem(path: Path) -> str:
    return next(path.name[: -len(s)] for s in SUFFIXES if path.name.endswith(s))


def read_headers(set_dir: Path) -> dict[str, str]:
    descriptions = {}
    for path in sorted(p for p in set_dir.iterdir() if p.name.endswith(SUFFIXES)):
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt") as f:
            for line in f:
                if line.startswith(">"):
                    seqid, _, desc = line[1:].rstrip("\n").partition(" ")
                    descriptions[f"{list_stem(path)}|{seqid}"] = desc
    return descriptions


def support(desc: str) -> dict[str, str]:
    fields = dict(kv.split("=", 1) for kv in desc.split(";") if "=" in kv)
    return {k: fields.get(k, "") for k in ("studies", "blanks", "sources")}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--removed", type=Path, required=True, help="curate.py's removed counts")
    ap.add_argument("--master", type=Path, required=True, help="mito_checker.py's master table")
    ap.add_argument("--set", type=Path, required=True, help="the contaminant reference set directory")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    removed = pd.read_csv(args.removed, sep="\t", index_col=0)
    removed = removed[removed["reason"] == "contaminant"].drop(columns="reason")
    master = pd.read_csv(args.master, sep="\t", dtype={"Sequence_ID": str}).set_index("Sequence_ID")
    descriptions = read_headers(args.set)

    rows = []
    for asv, counts in removed.iterrows():
        hit = master.loc[str(asv).split(";", 1)[0]]
        tagged = str(hit["BF_ID"])
        listed, _, entry = tagged.partition("|")
        rows.append({"asv": asv, "list": listed, "entry": entry,
                     "pident": hit["BF_pid"],
                     **support(descriptions.get(tagged, "")),
                     "reads": int(counts.sum())})
    pd.DataFrame(rows, columns=COLUMNS).to_csv(args.out, sep="\t", index=False)


if __name__ == "__main__":
    main()
