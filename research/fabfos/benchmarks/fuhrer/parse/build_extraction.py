#!/usr/bin/env python3
"""The Fuhrer cohort as a study-tier extraction: one row per screened deletion, on the
column set `buildlib/benchmark/study_tier.py`'s `gene_del` reader consumes.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fuhrer/parse/build_extraction.py
    ... --publish        # writes data/fabfos/benchmarks/_extractions/fuhrer/extraction.tsv

WHY THIS FILE IS SEPARATE FROM `lof.csv`. `lof.csv` is the arm's own working table and its
column names are the campaign's; the extraction is the BUILD's input and its column names
are the reference build's. They carry the same facts and neither is derived from the other
at build time -- this script derives the extraction FROM `lof.csv` once, so the two cannot
disagree about which genes the screen measured.

`gene_del` ASSERTS `measured=down` FOR EVERY ROW and that stays true here, though for a
subtler reason than in the Keio arm. A knockout removes a route, so the CONDUCTANCE to
anything downstream falls; that is what the column claims and it is claim about the graph,
not about the metabolite. This screen measures the metabolite, and 40% of its significant
responses are metabolites RISING -- which is a real result about metabolism and not a
contradiction of the conductance claim. The per-metabolite direction lives in the Y
sidecars, where it can be keyed on the metabolite it is about.

ONE REACTION PER GENE, THE ONE `build_lof_reactions.py` ALREADY PICKED. `del_mnxr` takes
the primary reaction rather than the full candidate list, because the candidate list is
already published beside it in `lof_reaction_edges.csv` and duplicating it here would let
the two drift.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]

LOF = REPO / "data/fabfos/runs/fuhrer_clones/parse/lof/lof.csv"
RESOLUTION = REPO / "data/fabfos/runs/fuhrer_clones/parse/lof/lof_resolution.tsv"
OUT = REPO / "data/fabfos/benchmarks/_extractions/fuhrer"

COHORT = "fuhrer"
HOST = "BW25113"
HOST_GEM = "iML1515"
DOI = "10.15252/msb.20167150"
CITATION = ("Fuhrer et al. Genomewide landscape of gene-metabolome associations in "
            "Escherichia coli. Mol Syst Biol 13:907 (2017)")

COLS = ("obs_id", "host", "host_gem", "gene_set", "gene", "gene_norm", "b_number",
        "locus_tag", "subsystem", "del_mnxr", "mapping_provenance", "n_differential_ions",
        "phenotype", "growth_rate_mean", "dataset_source", "doi", "citation", "note")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}/")
    a = ap.parse_args()

    lof = list(csv.DictReader(LOF.open()))
    res = {r["gene"]: r for r in csv.DictReader(RESOLUTION.open(), delimiter="\t")}
    if len(lof) != len(res):
        raise SystemExit(f"[extraction] {len(lof)} lof rows against {len(res)} resolution "
                         f"rows -- the two disagree about the cohort")

    rows = []
    for r in lof:
        g = r["gene"]
        m = res[g]
        rows.append({
            "obs_id": f"{COHORT}:{g}", "host": HOST, "host_gem": HOST_GEM,
            "gene_set": f"{g}:del", "gene": g, "gene_norm": m["gene_norm"],
            "b_number": m["b_number"], "locus_tag": r["locus_tag"],
            "subsystem": r["product"], "del_mnxr": r["mnxr"],
            "mapping_provenance": r["mnxr_mapping_method"] or r["no_mapping_reason"],
            "n_differential_ions": r["n_differential_ions"],
            "phenotype": m["fuhrer_category"], "growth_rate_mean": m["growth_rate_mean"],
            "dataset_source": "BioStudies S-BSST5 + Table EV1A", "doi": DOI,
            "citation": CITATION,
            "note": (f"Keio deletion profiled by flow-injection MS on 7,534 ions; "
                     f"{r['n_differential_ions']} of them differential at the paper's "
                     f"0.1-percentile cutoff"),
        })

    mapped = sum(1 for r in rows if r["del_mnxr"])
    print(f"{len(rows):,} deletions | {mapped:,} carry a reaction "
          f"({len(rows) - mapped:,} do not, with the reason in mapping_provenance)")
    cats: dict[str, int] = {}
    for r in rows:
        cats[r["phenotype"]] = cats.get(r["phenotype"], 0) + 1
    print(f"   Fuhrer's own categories: {cats}")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write "
              f"{OUT.relative_to(REPO)}/extraction.tsv)")
        return 0
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "extraction.tsv"
    path.unlink(missing_ok=True)
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(COLS), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"\n-> {path.relative_to(REPO)}: {len(rows):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
