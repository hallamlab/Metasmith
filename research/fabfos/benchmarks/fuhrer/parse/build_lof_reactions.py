#!/usr/bin/env python3
"""Attach reaction chemistry to the Fuhrer `lof.csv`: which MetaNetX reaction each deleted
gene was scored against by the ECSPr GEM channel, its equation, its baked direction ratio,
and whether it changes a carbon skeleton -- plus the candidate list that choosing one
reaction per gene throws away.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fuhrer/parse/build_lof_reactions.py
    ... --publish        # rewrites lof.csv in place, writes lof_reaction_edges.csv beside it

SAME FOUR OUTCOMES PER GENE as both eydallin halves, and `mnxr_mapping_method` says which:
`GEM`, `denovo`, or blank with `no_mapping_reason` saying why. A blank `mnxr` and a blank
`no_mapping_reason` never both happen, and neither do both filled. The invariant is checked
with an exit rather than a warning.

THE JOIN KEY IS THE B-NUMBER FOR THE CURATED CHANNEL AND THE BW25113 ACCESSION FOR THE
DE-NOVO ONE, and neither is the gene name. `gpr_gem.parquet` keys `orf` on iML1515's
b-numbers; `gpr_denovo.parquet` keys it on BW25113 protein accessions. **646 of this
screen's 3,806 gene names have been retired** since 2017 -- `cspC` is `cspE`, `citA` is
`dpiB`, `cld` is `wzzB` -- so a join on `feature_name` would drop one gene in six. Both
identifiers are already resolved in `lof_resolution.tsv`.

**CAUTION** THIS ARM DOES NOT ADJUDICATE ITS MISSES ONE AT A TIME, AND THAT IS A REAL
DIFFERENCE FROM THE EYDALLIN ARMS. Those cohorts were 65 and 86 genes, so every gene without
a reaction was read and given its own functional category -- `regulatory`, `proteolysis`,
`electron_carrier` -- and the categories were evidence. A genome-wide deletion screen cannot
be handled that way and pretending otherwise would be a hand-written list dressed as
curation. So the two reasons here are STRUCTURAL and derived, and the report must not read
them as functional classes:

    not_in_curated_model      the gene is on BW25113 and iML1515 gives it no reaction
    not_in_bw25113_proteome   `lof_resolution.tsv` could not place it on a protein at all

THE DE-NOVO GAP-FILL RUNS OVER EVERY GEM MISS, not over a curated shortlist. The eydallin
arm restricted it to the genes it had already read and called uncharacterized, which is
sound at 65 genes and impossible here. `denovo_gapfill`'s own rule is what does the work:
at least two independent projection methods must land on the same reaction, out of a
candidate pool of ten or fewer. That rule was written to survive exactly this -- being
pointed at genes nobody has read.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[5]

sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin/parse"))
import bake_pairs                                                             # noqa: E402
from reaction_chemistry import (carbon_bond_change, denovo_gapfill,           # noqa: E402
                                legible_equation, pick_primary)

LOF_DIR = REPO / "data/fabfos/runs/fuhrer_clones/parse/lof"
LOF_CSV = LOF_DIR / "lof.csv"
RESOLUTION = LOF_DIR / "lof_resolution.tsv"
EDGES_CSV = LOF_DIR / "lof_reaction_edges.csv"
GPR_GEM = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
GPR_DENOVO = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_denovo.parquet"
REACTIONS = REPO / "data/fabfos/processed/lookups/reactions.parquet"
METABOLITES = REPO / "data/fabfos/processed/lookups/metabolites.parquet"

LOF_IDENTITY_COLS = ("gene", "source_genome", "locus_tag", "product",
                     "n_differential_ions")
LOF_REACTION_COLS = ("mnxr", "mnxr_mapping_method", "reaction_equation",
                     "direction_ratio", "carbon_bond_change", "no_mapping_reason")
EDGE_COLS = ("gene", "channel", "model_gene_symbol", "mnxr", "is_primary",
             "primary_reason", "intermediate_id", "intermediate_name", "raw_score",
             "evidence_quality", "in_atom_universe", "gpr_rule", "reaction_equation",
             "direction_ratio", "carbon_bond_change")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"rewrite {LOF_CSV.relative_to(REPO)} and write "
                         f"{EDGES_CSV.relative_to(REPO)}")
    a = ap.parse_args()

    lof_rows = [{k: row[k] for k in LOF_IDENTITY_COLS}
                for row in csv.DictReader(LOF_CSV.open())]
    resolution = {r["gene"]: r for r in csv.DictReader(RESOLUTION.open(), delimiter="\t")}
    b_number = {g: r["b_number"] for g, r in resolution.items() if r["b_number"]}
    accession = {g: r["bw25113_accession"] for g, r in resolution.items()
                 if r["bw25113_accession"]}

    gpr = pd.read_parquet(GPR_GEM)
    orf_to_gene = {b: g for g, b in b_number.items()}
    gpr = gpr[gpr["orf"].isin(orf_to_gene)].copy()
    gpr["gene"] = gpr["orf"].map(orf_to_gene)

    gpr_denovo = pd.read_parquet(GPR_DENOVO)
    acc_to_gene = {acc: g for g, acc in accession.items()}
    gpr_denovo = gpr_denovo[gpr_denovo["orf"].isin(acc_to_gene)].copy()
    gpr_denovo["gene"] = gpr_denovo["orf"].map(acc_to_gene)

    met_names = pd.read_parquet(METABOLITES).set_index("mnxm")["name"].to_dict()
    reactions = {m: legible_equation(eq, met_names) for m, eq in
                 pd.read_parquet(REACTIONS).set_index("mnxr")["equation"].to_dict().items()}
    ratios = (pd.read_parquet(bake_pairs.direction_ratios())
              .set_index("mnxr")["ratio"].to_dict())
    atom_pairs_by_mnxr = {m: g for m, g in
                          pd.read_parquet(bake_pairs.atom_pairs()).groupby("mnxr")}

    gem_hit = set(gpr["gene"])
    misses = {r["gene"] for r in lof_rows} - gem_hit
    denovo_picks = denovo_gapfill(gpr_denovo, misses)

    print(f"{len(lof_rows):,} genes in lof.csv, {len(gpr):,} gene-reaction rows in "
          f"{GPR_GEM.name} covering {len(gem_hit):,} genes, {len(reactions):,} reaction "
          f"equations, {len(ratios):,} direction ratios")
    print(f"{len(misses):,} genes get no curated reaction; the de-novo channel gap-fills "
          f"{len(denovo_picks):,} of them on >=2-method agreement over a pool of <=10")

    bond_cache: dict[str, str] = {}

    def bond_for(mnxr: str) -> str:
        if mnxr not in bond_cache:
            pairs = atom_pairs_by_mnxr.get(mnxr)
            bond_cache[mnxr] = carbon_bond_change(pairs) if pairs is not None else ""
        return bond_cache[mnxr]

    by_gene = {g: d for g, d in gpr.groupby("gene")}
    edge_rows, n_multi, n_gem, n_denovo, n_no_reaction = [], 0, 0, 0, 0
    for r in lof_rows:
        gene = r["gene"]
        candidates = by_gene.get(gene, gpr.iloc[0:0])
        primary, reason = pick_primary(candidates)
        if len(candidates) > 1:
            n_multi += 1
        for _, c in candidates.iterrows():
            is_primary = (primary is not None and c["mnxr"] == primary["mnxr"]
                          and c["intermediate_id"] == primary["intermediate_id"])
            edge_rows.append({
                "gene": gene, "channel": "GEM", "model_gene_symbol": c["feature_name"],
                "mnxr": c["mnxr"], "is_primary": "yes" if is_primary else "no",
                "primary_reason": reason if is_primary else "",
                "intermediate_id": c["intermediate_id"],
                "intermediate_name": c["intermediate_name"], "raw_score": c["raw_score"],
                "evidence_quality": c["evidence_quality"],
                "in_atom_universe": c["in_atom_universe"], "gpr_rule": c["gpr_rule"],
                "reaction_equation": reactions.get(c["mnxr"], ""),
                "direction_ratio": ratios.get(c["mnxr"], ""),
                "carbon_bond_change": bond_for(c["mnxr"]),
            })

        channel, no_mapping_reason = "", ""
        if primary is not None:
            channel, n_gem = "GEM", n_gem + 1
        elif gene in denovo_picks:
            mnxr, note = denovo_picks[gene]
            channel, n_denovo = "denovo", n_denovo + 1
            primary = pd.Series({"mnxr": mnxr})
            edge_rows.append({
                "gene": gene, "channel": "denovo", "model_gene_symbol": gene,
                "mnxr": mnxr, "is_primary": "yes", "primary_reason": note,
                "intermediate_id": "", "intermediate_name": "", "raw_score": "",
                "evidence_quality": "denovo", "in_atom_universe": "", "gpr_rule": "",
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })
        else:
            no_mapping_reason = ("not_in_curated_model" if gene in accession
                                 else "not_in_bw25113_proteome")
            n_no_reaction += 1

        r["mnxr"] = primary["mnxr"] if primary is not None else ""
        r["mnxr_mapping_method"] = channel
        r["reaction_equation"] = reactions.get(r["mnxr"], "") if primary is not None else ""
        r["direction_ratio"] = ratios.get(r["mnxr"], "") if primary is not None else ""
        r["carbon_bond_change"] = bond_for(r["mnxr"]) if primary is not None else ""
        r["no_mapping_reason"] = no_mapping_reason

    both_blank = [r["gene"] for r in lof_rows
                  if not r["mnxr"] and not r["no_mapping_reason"]]
    both_filled = [r["gene"] for r in lof_rows
                   if r["mnxr"] and r["no_mapping_reason"]]
    if both_blank or both_filled:
        raise SystemExit(f"[fuhrer] the four-outcome invariant broke: both blank "
                         f"{both_blank[:10]}, both filled {both_filled[:10]}")

    print(f"\n{n_gem:,}/{len(lof_rows):,} genes resolved via gpr_gem.parquet, "
          f"{n_denovo:,} more via de-novo gap-fill, {n_no_reaction:,} have no reaction "
          f"-> {n_gem + n_denovo:,}/{len(lof_rows):,} covered")
    print(f"{n_multi:,} GEM-channel genes had more than one candidate reaction -> "
          f"{len(edge_rows):,} edge rows")
    n_bond = sum(1 for r in edge_rows if r["carbon_bond_change"])
    print(f"{n_bond:,}/{len(edge_rows):,} edges have an atom-mapped carbon-bond call")
    print("no_mapping_reason:", dict(Counter(r["no_mapping_reason"] for r in lof_rows
                                             if r["no_mapping_reason"])))

    if not a.publish:
        print(f"\n(dry run -- pass --publish to rewrite {LOF_CSV.relative_to(REPO)})")
        return 0

    for path, cols, rows in (
            (LOF_CSV, list(LOF_IDENTITY_COLS) + list(LOF_REACTION_COLS), lof_rows),
            (EDGES_CSV, list(EDGE_COLS), edge_rows)):
        path.unlink(missing_ok=True)
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        print(f"\n-> {path.relative_to(REPO)}: {len(rows):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
