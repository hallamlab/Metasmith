#!/usr/bin/env python3
"""Check a retrieved cyano_r1 run against its acceptance criteria, and summarise it.

    python research/aspire/campaigns/cyano_r1/check_results.py [RESULTS_DIR]

RESULTS_DIR defaults to data/aspire/cyano_r1. Exits non-zero on the first failed criterion
group, after printing every failure in it.
"""

import csv
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
STAGES = ["raw", "qc", "merged", "filtered", "mapped", "table_filtered", "kept"]


def product(results: Path, dtype: str) -> Path:
    hits = sorted((results / dtype.replace("::", "-")).glob("*"))
    if not hits:
        sys.exit(f"FAIL {dtype}: absent under {results}")
    return hits[0]


def phylum(taxon: str) -> str:
    for rank in str(taxon).split(";"):
        rank = rank.strip()
        if rank.startswith("p__"):
            return rank[3:] or "unassigned"
    return "unassigned"


def main():
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO / "data" / "aspire" / "cyano_r1"
    with open(HERE / "samples.tsv") as f:
        ena = {r["sample"]: int(r["read_count"]) for r in csv.DictReader(f, delimiter="\t")}
    failures = []

    clean = pd.read_csv(product(results, "aspire::counts_clean"), sep="\t", index_col=0)
    if sorted(clean.columns) != sorted(ena):
        failures.append(f"counts_clean has {len(clean.columns)} sample columns, want {len(ena)}: "
                        f"missing {sorted(set(ena) - set(clean.columns))}")

    fate = pd.read_csv(product(results, "aspire::read_fate"), sep="\t", dtype={"sample": str})
    fate = fate.set_index("sample")
    for sid, want in ena.items():
        if sid not in fate.index:
            failures.append(f"read_fate: no row for {sid}")
            continue
        row = fate.loc[sid]
        if int(row["raw"]) != want:
            failures.append(f"read_fate {sid}: raw {row['raw']} != ENA read_count {want}")
        for a, b in zip(STAGES, STAGES[1:]):
            if int(row[b]) > int(row[a]):
                failures.append(f"read_fate {sid}: {b} {row[b]} > {a} {row[a]}")

    tax = pd.read_csv(product(results, "amplicon::asv_taxonomy"), sep="\t", index_col=0)
    tax.index = [i.split(";")[0] for i in tax.index]
    phyla = tax["Taxon"].map(phylum)
    by_phylum = clean.groupby(phyla.reindex(clean.index).fillna("unassigned")).sum()
    top = by_phylum.idxmax()
    for sid, p in top.items():
        if p != "Cyanobacteria":
            failures.append(f"{sid}: top phylum is {p}, not Cyanobacteria")

    isa = product(results, "aspire::indicspecies_results")
    for kind in ("results", "summary"):
        if not (isa / f"culture_indicator_species_{kind}.tsv").exists():
            failures.append(f"indicspecies_results: no culture_indicator_species_{kind}.tsv")

    print(f"ASVs in the clean table: {len(clean)} over {len(clean.columns)} samples")
    print("reads kept per stage, summed over samples:")
    for stage in STAGES:
        print(f"  {stage:<15}{int(fate[stage].sum()):>10}")
    share = by_phylum.div(by_phylum.sum()).mul(100).round(1)
    print("Cyanobacteria share of each sample's clean reads (%):")
    print("  " + ", ".join(f"{s} {share.loc['Cyanobacteria', s]}" for s in share.columns)
          if "Cyanobacteria" in share.index else "  none")
    summary = isa / "culture_indicator_species_summary.tsv"
    if summary.exists():
        isa_df = pd.read_csv(summary, sep="\t")
        sig = isa_df.loc[isa_df.get("significant", False) == True]  # noqa: E712
        print(f"indicators of culture, q < 0.05: {len(sig)}")
        for level in [c for c in isa_df.columns if c.startswith("s.")]:
            best = sig.loc[sig[level] == 1].sort_values("stat", ascending=False).head(5)
            names = [f"{a} ({tax['Taxon'].get(a, '?').split(';')[-1].strip()})" for a in best["ASV"]]
            print(f"  {level[2:]}: {', '.join(names) or 'none'}")

    if failures:
        print(f"\n{len(failures)} FAILED:", *failures, sep="\n  ")
        return 1
    print("\nall criteria met")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
