#!/usr/bin/env python3
"""The Fuhrer cohort as one flat table: every deletion the screen measured, placed on
BW25113, with the gene-scalar phenotype the paper itself classifies mutants by.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fuhrer/parse/build_lof_table.py
    ... --publish        # writes data/fabfos/runs/fuhrer_clones/parse/lof/

SAME SCHEMA AS THE 2007 EYDALLIN ARM, and that arm is the direct template rather than a
loose analogy: same collection (Keio), same strain (BW25113), same host model (iML1515),
same perturbation (deletion). Only the readout differs. The five identity columns and the
resolution table's column set are that arm's, unchanged.

THE PHENOTYPE COLUMN IS `n_differential_ions`, WHICH IS THE PAPER'S OWN GENE-SCALAR. Fuhrer
classifies each mutant silent / rare / moderate / global by how many ions it moves past an
absolute 0.1-percentile cutoff on the z-score distribution (Methods, "Categorization of
mutants based on number of differential ions"). That cutoff is computed HERE from the
matrices rather than transcribed, because the paper states the percentile and not the
resulting z -- and the classification thresholds it then quotes (0, 1-4, 5-10, >10 hits)
are reproduced as `fuhrer_category` so the count can be checked against something.

**CAUTION** THE 0.1-PERCENTILE CUTOFF IS NOT THE 2.765 REPRODUCIBILITY THRESHOLD, and the two
are used for different things throughout this arm. 2.765 is the |z| above which a change
repeats between biological replicates with 1% false-positive probability -- it defines a
POSITIVE on one ion, and it is what `sinks/declare_axes.py` and the Y sidecars use. The 0.1
percentile is a per-dataset tail cutoff that defines how BUSY a mutant is across all ions.
Swapping them silently changes what every downstream count means.

RESOLUTION IS BY BLATTNER IDENTIFIER, TAKEN FROM THE PAPER, NOT LOOKED UP FROM THE NAME.
Table EV1A carries each strain's JW identifier and its b-number, which is the Keio
collection's own record of which locus was deleted. That is strictly better evidence than
the 2007 arm's name-to-b-number walk through MG1655's GenBank synonyms, and it is
cross-checked against Baba 2006's roster rather than trusted: a strain whose two sources
disagree is reported, and a strain EV1A cannot name falls back to the name walk.

`wt` IS NOT A MUTANT. The z-score matrices carry a wild-type control column and the deposit
does not flag it. It is excluded here by name, which is why this table has 3,806 rows
against the matrices' 3,807 columns.

NOTHING IS SILENTLY DROPPED. A gene that resolves no way keeps its row with blank identity
cells and its reason in `lof_resolution.tsv`.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/fang/parse"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin/gpr_build"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/keio"))
from build_extraction import gene_to_bnumber                             # noqa: E402
from build_clone_orfs import one_faa, read_faa                           # noqa: E402
import baba_roster                                                       # noqa: E402

SCREEN = REPO / "data/fabfos/runs/fuhrer_clones/parse/screen"
MG1655 = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome"
BW25113 = REPO / "data/fabfos/originals/genomes/e_coli_bw25113/genome"
OUT = REPO / "data/fabfos/runs/fuhrer_clones/parse/lof"

OUT_COLS = ("gene", "source_genome", "locus_tag", "product", "n_differential_ions")

RESOLUTION_COLS = ("gene", "gene_norm", "b_number", "b_number_source",
                   "mg1655_accession", "bw25113_accession", "bw25113_gene", "locus_tag",
                   "method", "identical_to_mg1655", "aa_len", "jw_id",
                   "growth_rate_mean", "n_differential_ions", "fuhrer_category", "note")

GENOME_LABEL = "BW25113"
WT_COLUMN = "wt"
DIFFERENTIAL_PERCENTILE = 0.1        # Fuhrer 2017 Methods, per ionization mode
# Fuhrer 2017 Fig EV7C: the four bands the differential-ion count is read as.
CATEGORIES = ((0, "silent"), (4, "rare"), (10, "moderate"))

PRODUCT_RE = re.compile(r"\[protein=([^\]]+)\]")


def product_of(rec: dict) -> str:
    m = PRODUCT_RE.search(rec["header"])
    return m.group(1).strip() if m else ""


def category(n: int) -> str:
    for bound, name in CATEGORIES:
        if n <= bound:
            return name
    return "global"


def differential_counts(genes: list[str]) -> tuple[dict[str, int], dict[str, float]]:
    """How many ions each deletion moves past the 0.1-percentile tail, summed over modes.

    The cutoff is taken on the WHOLE mode's z distribution, deletions and wild type alike,
    which is what "the distribution of the z-scores" in the Methods refers to -- a cutoff
    recomputed per gene would move with the thing it is measuring.
    """
    total = {g: 0 for g in genes}
    cutoffs: dict[str, float] = {}
    for mode in ("neg", "pos"):
        z = pd.read_parquet(SCREEN / f"zscore_{mode}.parquet")
        arr = np.abs(z.to_numpy())
        cut = float(np.percentile(arr, 100 - DIFFERENTIAL_PERCENTILE))
        cutoffs[mode] = cut
        hits = dict(zip(z.columns, (arr > cut).sum(axis=0)))
        for g in genes:
            total[g] += int(hits[g])
        del z, arr
    return total, cutoffs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}/")
    a = ap.parse_args()

    roster = list(csv.DictReader((SCREEN / "gene_roster.tsv").open(), delimiter="\t"))
    screen = [r for r in roster if r["gene"] != WT_COLUMN]
    if len(screen) != len(roster) - 1:
        raise SystemExit(f"[fuhrer] the roster carries {len(roster) - len(screen)} `wt` "
                         f"columns, expected exactly one")

    n_diff, cutoffs = differential_counts([r["gene"] for r in roster])
    print(f"{len(screen)} deletions ({WT_COLUMN} excluded) | "
          + " ".join(f"{m} 0.1-percentile |z| cutoff {c:.4f}" for m, c in cutoffs.items()))
    print(f"   wild-type control moves {n_diff[WT_COLUMN]} ions past that cutoff -- "
          f"the floor any mutant's count has to be read against")
    bands = {}
    for r in screen:
        bands[category(n_diff[r["gene"]])] = bands.get(category(n_diff[r["gene"]]), 0) + 1
    print(f"   {bands} (Fuhrer's own four bands, Fig EV7C)")

    assayed, _essential = baba_roster.collection()
    baba_b = {g: b for b, g in assayed.items()}
    to_bnum = gene_to_bnumber(one_faa(MG1655).with_suffix(".gbk"))
    mg = read_faa(one_faa(MG1655))
    bw = read_faa(one_faa(BW25113))
    print(f"MG1655 {len(mg):,} proteins, BW25113 {len(bw):,} proteins, "
          f"Baba roster names {len(baba_b):,} b-numbers")

    mg_by_locus = {r["locus_tag"]: r for r in mg if r["locus_tag"]}
    bw_by_seq: dict[str, dict] = {}
    for r in bw:
        bw_by_seq.setdefault(r["seq"], r)
    bw_by_gene: dict[str, dict] = {}
    for r in sorted(bw, key=lambda x: x["pseudo"]):
        if r["gene"]:
            bw_by_gene.setdefault(r["gene"].lower(), r)

    out_rows, table, methods, sources, disagree = [], [], {}, {}, []
    for r in screen:
        gene = r["gene"].strip()
        norm = gene.lower()
        rec = dict.fromkeys(RESOLUTION_COLS, "")
        n = n_diff[gene]
        rec.update(gene=gene, gene_norm=norm, jw_id=r["jw_id"],
                   growth_rate_mean=r["growth_mean"],
                   n_differential_ions=str(n), fuhrer_category=category(n))

        b, source = r["b_number"], "table_EV1A"
        if not b:
            b, source = to_bnum.get(gene) or to_bnum.get(norm) or "", "mg1655_genbank"
        if b and baba_b.get(gene) and baba_b[gene] != b:
            disagree.append((gene, b, baba_b[gene]))
        rec["b_number"], rec["b_number_source"] = b, source if b else ""
        sources[source if b else "none"] = sources.get(source if b else "none", 0) + 1

        hit, method = None, "unresolved"
        if not b:
            rec["note"] = "no b-number in Table EV1A and none for this name in MG1655"
        elif (src := mg_by_locus.get(b)) is None:
            rec["note"] = (f"{b} has no protein in the MG1655 proteome -- non-coding, "
                           f"or a pseudogene")
        else:
            rec["mg1655_accession"] = src["accession"]
            hit, method = bw_by_seq.get(src["seq"]), "bw25113_exact_protein"
            if hit is None:
                for key in (src["gene"], norm, gene):
                    if key and key.lower() in bw_by_gene:
                        hit, method = bw_by_gene[key.lower()], "bw25113_symbol"
                        break
            if hit is not None and hit["pseudo"]:
                rec["note"] = (f"BW25113's {hit['gene'] or hit['locus_tag']} is a "
                               f"pseudogene; no protein to describe the deleted locus")
                hit, method = None, "unresolved"
            elif hit is None:
                method = "unresolved"
                rec["note"] = (f"{b} / {src['gene'] or '?'} has neither an identical "
                               f"protein nor a matching symbol in BW25113")
        if hit is not None:
            rec.update(bw25113_accession=hit["accession"], bw25113_gene=hit["gene"],
                       locus_tag=hit["locus_tag"], aa_len=str(len(hit["seq"])),
                       identical_to_mg1655="yes" if method.endswith("exact_protein")
                                           else "no")
        rec["method"] = method
        methods[method] = methods.get(method, 0) + 1
        table.append(rec)
        out_rows.append({
            "gene": gene,
            "source_genome": GENOME_LABEL if hit else "",
            "locus_tag": hit["locus_tag"] if hit else "",
            "product": product_of(hit) if hit else "",
            "n_differential_ions": str(n),
        })

    print("\nb-number source: " + ", ".join(f"{n} {s}" for s, n in sorted(sources.items())))
    print("resolution     : " + ", ".join(f"{n} {m}" for m, n in sorted(methods.items())))
    if disagree:
        print(f"!! {len(disagree)} strains where Table EV1A and Baba 2006 name different "
              f"b-numbers for the same gene: {disagree[:8]}")
    renamed = [(r["gene"], r["bw25113_gene"]) for r in table
               if r["bw25113_gene"] and r["bw25113_gene"] != r["gene"]]
    print(f"{len(renamed)} of the paper's gene names have been retired since 2017: "
          + ", ".join(f"{x}->{y}" for x, y in sorted(renamed)[:20])
          + (" ..." if len(renamed) > 20 else ""))

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(REPO)}/lof.csv)")
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    for path, cols, rows in ((OUT / "lof.csv", OUT_COLS, out_rows),
                             (OUT / "lof_resolution.tsv", RESOLUTION_COLS, table)):
        path.unlink(missing_ok=True)
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(cols),
                               delimiter="\t" if path.suffix == ".tsv" else ",")
            w.writeheader()
            w.writerows(rows)
        print(f"-> {path.relative_to(REPO)}: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
