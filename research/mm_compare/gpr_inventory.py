#!/usr/bin/env python3
"""Per-subject counts from the fabfos 4-lane mapper tables, in the mapper's own terms.

    gpr_inventory.py <repo root>

Reads data/fabfos/runs/e_coli_<strain>/gpr/gpr_denovo.parquet and, once present,
data/mm_compare/gpr_4lane/<e5_sample>.parquet (one row per call, with a `mag` column). Writes
data/mm_compare/inventory/gpr_models.tsv: ORFs in the input, ORFs with any call, calls, distinct MNXR,
and per lane the ORFs with a call, the calls and the distinct MNXR.
"""
import sys
from pathlib import Path

import pandas as pd

STRAINS = ("k12", "dh10b", "dh1", "w3110", "bw25113", "epi300")
LANES = ("clean", "kofam", "pbert", "uniref50")


def counts(calls: pd.DataFrame, n_orfs: int) -> dict:
    row = {"orfs": n_orfs, "orfs_with_call": calls["orf"].nunique(), "calls": len(calls),
           "mnxr": calls["mnxr"].nunique()}
    for lane in LANES:
        c = calls[calls["channel"] == lane]
        row |= {f"{lane}_orfs": c["orf"].nunique(), f"{lane}_calls": len(c), f"{lane}_mnxr": c["mnxr"].nunique()}
    return row


def n_fasta(path: Path) -> int:
    with open(path) as f:
        return sum(line.startswith(">") for line in f)


def main(repo: Path):
    mm = repo / "data" / "mm_compare"
    rows = []
    fabfos = repo / "data" / "fabfos"
    for st in STRAINS:
        calls = pd.read_parquet(fabfos / "runs" / f"e_coli_{st}" / "gpr" / "gpr_denovo.parquet")
        proteome = next((fabfos / "originals" / "genomes" / f"e_coli_{st}" / "genome").glob("*.faa"))
        rows.append({"dataset": "ecoli", "sample": st, "subject": st, **counts(calls, n_fasta(proteome))})
    gpr = mm / "gpr_4lane"
    if gpr.exists():
        o2m = pd.read_csv(mm / "e5_hmp_orf_to_mag.tsv", sep="\t")
        per_mag = o2m.groupby(["e5_sample", "mag"]).size()
        for f in sorted(gpr.glob("toy_hmp_*.parquet")):
            calls = pd.read_parquet(f)
            for mag, c in calls.groupby("mag"):
                rows.append({"dataset": "cami_hmp", "sample": f.stem, "subject": mag,
                             **counts(c, int(per_mag[(f.stem, mag)]))})
    out = mm / "inventory" / "gpr_models.tsv"
    out.unlink(missing_ok=True)
    pd.DataFrame(rows).to_csv(out, sep="\t", index=False)
    print(len(rows), "subjects", file=sys.stderr)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
