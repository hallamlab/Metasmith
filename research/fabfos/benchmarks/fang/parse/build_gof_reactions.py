#!/usr/bin/env python3
"""Attach reaction chemistry to Fang's `gof.csv`/`gof_round2.csv`: which MetaNetX reaction
each gene's ORF was scored against by the ECSPr GEM channel, its equation, its baked
direction ratio, and whether it changes a carbon skeleton -- plus the candidate list that
choosing one reaction per gene throws away.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fang/parse/build_gof_reactions.py
    ... --publish        # rewrites both tables in place, writes *_reaction_edges.csv beside them

FOUR OUTCOMES PER GENE, the same four `build_gof_reactions.py` established on the eydallin
cohort and with the same rules, imported rather than restated: `GEM` (iML1515 has it),
`LLM review` (it does not, but a specific MetaNetX reaction was hand-picked and cited),
`denovo` (neither, but >=2 independent de-novo projection methods agree on one reaction), or
blank with `no_mapping_reason` saying why. A blank `mnxr` and a blank `no_mapping_reason`
never both happen.

THE JOIN KEY IS THE b-NUMBER, AND THIS IS THE TRAP THIS COHORT ADDS. Eydallin's arm learned
that joining on `feature_name` drops every renamed gene and joined on `condition_id`
instead. Here `condition_id` is not enough either: the cohort GPR is keyed on the ASKA
ROSTER's 2005 gene names, and the paper writes 2024 ones. Ten of the fifty-nine genes
disagree -- every `waa*` gene is `rfa*` on the roster, `lptA/B/D/E` are `yhbN`/`yhbG`/`imp`/
`rlpB`, `gppA` is `gpp`, `opgD` is `mdoD` -- and a name join silently writes "no reaction"
for the entire LPS core module, which is the half of this study the primary axis is about.
So the join runs paper name -> b-number (MG1655's GenBank, synonyms included) -> the roster
gene of that b-number -> `condition_id`.

THE CARBON-BOND CALL IS READ OFF THE BAKED ATOM MAP, never off the equation string.
`reaction_chemistry.carbon_bond_change` groups the reaction's mapped carbon pairs into
connected components; a component spanning more than one substrate means carbons became
bonded, more than one product means they came apart. An equation-string reader would call
rfaY's own reaction a change because ATP becomes ADP; the atom map calls it `no_change`,
which is correct and is exactly why the primary axis had to be defined by rfaY's PRODUCT
rather than by its reaction.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin/parse"))

import bake_pairs                                                             # noqa: E402
from build_extraction import gene_to_bnumber                                  # noqa: E402
from reaction_chemistry import (carbon_bond_change, denovo_gapfill,           # noqa: E402
                                legible_equation, pick_primary)

GOF_DIR = REPO / "data/fabfos/runs/fang_clones/parse/gof"
TABLES = ("gof.csv", "gof_round2.csv")
GPR_GEM = REPO / "data/fabfos/runs/fang/gpr/gpr_gem.parquet"
GPR_DENOVO = REPO / "data/fabfos/runs/fang/gpr/gpr_denovo.parquet"
CENSUS = REPO / "data/fabfos/runs/fang/gpr/clone_census.tsv"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
REACTIONS = REPO / "data/fabfos/processed/lookups/reactions.parquet"
METABOLITES = REPO / "data/fabfos/processed/lookups/metabolites.parquet"

# LLM REVIEW, THE THIRD CHANNEL, for genes iML1515 carries no trace of. Each entry is one
# `mnxr` chosen by hand -- product identity checked against the characterised activity, not
# by grepping a keyword and taking the first hit -- and is the only route outside the
# GEM/de-novo channels that may write a gene straight to a reaction.
# IT IS EMPTY FOR THIS COHORT, AND THAT IS THE FINDING RATHER THAN AN OVERSIGHT. Eydallin's
# arm promoted six genes this way. Here the thirteen genes iML1515 misses split into
# regulators, transporters, envelope machinery and DUF proteins, none of which has a
# characterised reaction to pick; the one candidate that looked promotable, `opgD`, was
# checked and rejected. OpgD processes the osmoregulated periplasmic glucan, MetaNetX does
# carry that neighbourhood (`MNXM148913` and its transferase reactions), but every entry
# there belongs to OpgB/OpgE's phosphoglycerol and phosphoethanolamine transfers rather than
# to OpgD's branching step, and the polymer's chain length is undefined on both sides. A
# reaction picked for the right pathway and the wrong enzyme is the fabrication this column
# exists to prevent, so `opgD` is categorised below instead.
MANUAL_MNXR: dict[str, tuple[str, str]] = {}

# FOR EVERYTHING ELSE, ONE CATEGORY, so a blank `mnxr` says WHY rather than looking
# unchecked. These are the genes' own established functional classes read off the product
# string in the table beside them, not a paraphrase of "no reaction" invented here. The
# `uncharacterized` bucket is the only one the de-novo gap-fill is allowed to reach into --
# a gene the literature has already placed outside metabolism needs a citation to move, not
# an algorithmic coincidence, and eydallin's arm showed `clpA` and `recQ` converging on the
# same generic ATP hydrolysis when that rule was relaxed.
NO_MAPPING_REASON = {
    "lacI": "regulatory", "norR": "regulatory", "ybeF": "regulatory",

    "bcr": "transport", "nepI": "transport", "yoaE": "transport", "yggR": "transport",

    # lptA/B/D/E are NOT here: iML1515 does carry them, all four on the same LPS-transport
    # reaction `MNXR100900`, so they resolve on the GEM channel and a reason for them would
    # describe a gene that has one.
    "yafL": "proteolysis",
    "rimM": "translation",
    "dps": "dna_binding_stress_protection",
    "rsxG": "electron_transfer", "hydN": "electron_transfer",

    "yaaA": "uncharacterized", "yafZ": "uncharacterized", "yeeS": "uncharacterized",
    "yegL": "uncharacterized", "ygdD": "uncharacterized", "yjcO": "uncharacterized",
    "yjdF": "uncharacterized", "ytfK": "uncharacterized",

    "wcaA": "ambiguous_metabolic_reaction", "tas": "ambiguous_metabolic_reaction",
    "opgD": "ambiguous_metabolic_reaction",
}

IDENTITY_COLS = ("gene", "source_genome", "locus_tag", "product", "ffa_titer_mg_l")
REACTION_COLS = ("mnxr", "mnxr_mapping_method", "reaction_equation", "direction_ratio",
                 "carbon_bond_change", "no_mapping_reason")
EDGE_COLS = ("gene", "channel", "model_gene_symbol", "mnxr", "is_primary", "primary_reason",
             "intermediate_id", "intermediate_name", "raw_score", "evidence_quality",
             "in_atom_universe", "gpr_rule", "reaction_equation", "direction_ratio",
             "carbon_bond_change")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"rewrite {GOF_DIR.relative_to(REPO)}/*.csv and write the edge files")
    a = ap.parse_args()

    to_bnum = gene_to_bnumber(MG1655_GBK)
    census = pd.read_csv(CENSUS, sep="\t", dtype=str)
    roster_of_b = {}
    for g, b in zip(census.gene, census.b_number.fillna("")):
        if b:
            roster_of_b.setdefault(b, g)

    gpr = pd.read_parquet(GPR_GEM)
    gpr["cohort_gene"] = gpr["condition_id"].str.split(":").str[1]
    gpr_denovo = pd.read_parquet(GPR_DENOVO)
    gpr_denovo["cohort_gene"] = gpr_denovo["condition_id"].str.split(":").str[1]
    met_names = pd.read_parquet(METABOLITES).set_index("mnxm")["name"].to_dict()
    reactions = {m: legible_equation(eq, met_names) for m, eq in
                 pd.read_parquet(REACTIONS).set_index("mnxr")["equation"].to_dict().items()}
    ratios = pd.read_parquet(bake_pairs.direction_ratios()).set_index("mnxr")["ratio"].to_dict()
    atom_pairs_by_mnxr = {m: g for m, g in
                          pd.read_parquet(bake_pairs.atom_pairs()).groupby("mnxr")}

    bond_cache: dict[str, str] = {}

    def bond_for(mnxr: str) -> str:
        if mnxr not in bond_cache:
            pairs = atom_pairs_by_mnxr.get(mnxr)
            bond_cache[mnxr] = carbon_bond_change(pairs) if pairs is not None else ""
        return bond_cache[mnxr]

    all_genes = set()
    for t in TABLES:
        all_genes |= {r["gene"] for r in csv.DictReader((GOF_DIR / t).open())}
    cohort_of = {g: roster_of_b.get(to_bnum.get(g, ""), "") for g in sorted(all_genes)}
    renamed = {g: c for g, c in cohort_of.items() if c and c != g}
    unmatched = sorted(g for g, c in cohort_of.items() if not c)
    print(f"{len(all_genes)} genes | {len(renamed)} carry a different name on the ASKA "
          f"roster and would be lost by a name join: {renamed}")
    if unmatched:
        print(f"!! {len(unmatched)} gene(s) have no roster clone at all: {unmatched}")

    uncharacterized = {g for g, cat in NO_MAPPING_REASON.items() if cat == "uncharacterized"}
    denovo_picks = denovo_gapfill(
        gpr_denovo.assign(gene=gpr_denovo.cohort_gene.map(
            {c: g for g, c in cohort_of.items() if c})),
        uncharacterized)
    print(f"{len(uncharacterized)} genes are uncharacterized; the de-novo channel gap-fills "
          f"{len(denovo_picks)} with >=2-method agreement on one reaction: "
          f"{sorted(denovo_picks)}")

    for table in TABLES:
        path = GOF_DIR / table
        rows = [{k: r[k] for k in IDENTITY_COLS} for r in csv.DictReader(path.open())]
        edge_rows, counts = [], Counter()
        for r in rows:
            gene = r["gene"]
            candidates = gpr[gpr["cohort_gene"] == cohort_of.get(gene, "\0")]
            primary, reason = pick_primary(candidates)
            if len(candidates) > 1:
                counts["multi_candidate"] += 1

            for _, c in candidates.iterrows():
                is_primary = (primary is not None and c["mnxr"] == primary["mnxr"]
                              and c["intermediate_id"] == primary["intermediate_id"])
                edge_rows.append(dict(
                    gene=gene, channel="GEM", model_gene_symbol=c["feature_name"],
                    mnxr=c["mnxr"], is_primary="yes" if is_primary else "no",
                    primary_reason=reason if is_primary else "",
                    intermediate_id=c["intermediate_id"],
                    intermediate_name=c["intermediate_name"], raw_score=c["raw_score"],
                    evidence_quality=c["evidence_quality"],
                    in_atom_universe=c["in_atom_universe"], gpr_rule=c["gpr_rule"],
                    reaction_equation=reactions.get(c["mnxr"], ""),
                    direction_ratio=ratios.get(c["mnxr"], ""),
                    carbon_bond_change=bond_for(c["mnxr"])))

            channel, no_mapping_reason = "", ""
            if primary is not None:
                channel = "GEM"
            elif gene in MANUAL_MNXR or gene in denovo_picks:
                mnxr, note = (MANUAL_MNXR[gene] if gene in MANUAL_MNXR
                              else denovo_picks[gene])
                channel = "LLM review" if gene in MANUAL_MNXR else "denovo"
                primary = pd.Series({"mnxr": mnxr})
                edge_rows.append(dict(
                    gene=gene, channel=channel, model_gene_symbol=gene, mnxr=mnxr,
                    is_primary="yes", primary_reason=note, intermediate_id="",
                    intermediate_name="", raw_score="", evidence_quality=channel,
                    in_atom_universe="", gpr_rule="",
                    reaction_equation=reactions.get(mnxr, ""),
                    direction_ratio=ratios.get(mnxr, ""), carbon_bond_change=bond_for(mnxr)))
            else:
                no_mapping_reason = NO_MAPPING_REASON.get(gene, "")
                if not no_mapping_reason:
                    raise SystemExit(
                        f"[reactions] {gene} resolves no reaction on any channel and has no "
                        f"NO_MAPPING_REASON entry -- a blank `mnxr` beside a blank reason is "
                        f"the one state this table may not carry ({r['product']})")
            counts[channel or "none"] += 1

            r["mnxr"] = primary["mnxr"] if primary is not None else ""
            r["mnxr_mapping_method"] = channel
            r["reaction_equation"] = reactions.get(r["mnxr"], "") if primary is not None else ""
            r["direction_ratio"] = ratios.get(r["mnxr"], "") if primary is not None else ""
            r["carbon_bond_change"] = bond_for(r["mnxr"]) if primary is not None else ""
            r["no_mapping_reason"] = no_mapping_reason

        # A reaction the GEM nominates that `reactions.parquet` does not carry leaves an
        # empty equation cell beside a populated `mnxr`, which reads like a formatting slip
        # rather than the gap it is. iML1515's LPS-transport reaction `MNXR100900` is the
        # case here, and it is the primary for all four `lpt*` genes.
        no_eq = sorted({(r["gene"], r["mnxr"]) for r in rows
                        if r["mnxr"] and not r["reaction_equation"]})
        if no_eq:
            print(f"    !! {len(no_eq)} primary reaction(s) are absent from "
                  f"{REACTIONS.name} and carry no equation: {no_eq}")
        covered = counts["GEM"] + counts["LLM review"] + counts["denovo"]
        print(f"\n{table}: {counts['GEM']}/{len(rows)} via iML1515, "
              f"{counts['LLM review']} via LLM review, {counts['denovo']} via de-novo "
              f"gap-fill, {counts['none']} with no reaction -> {covered}/{len(rows)} covered")
        print(f"    carbon_bond_change: "
              f"{dict(Counter(r['carbon_bond_change'] for r in rows))}")
        print(f"    no_mapping_reason:  "
              f"{dict(Counter(r['no_mapping_reason'] for r in rows if r['no_mapping_reason']))}")

        if not a.publish:
            continue
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(IDENTITY_COLS) + list(REACTION_COLS))
            w.writeheader()
            w.writerows(rows)
        edges = GOF_DIR / table.replace(".csv", "_reaction_edges.csv")
        with edges.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(EDGE_COLS))
            w.writeheader()
            w.writerows(edge_rows)
        print(f"    -> {path.name}: {len(rows)} rows | {edges.name}: {len(edge_rows)} rows")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to rewrite {GOF_DIR.relative_to(REPO)}/*.csv)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
