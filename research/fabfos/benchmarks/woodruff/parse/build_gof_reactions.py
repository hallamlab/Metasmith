#!/usr/bin/env python3
"""Attach reaction chemistry to `gof.csv`: which MetaNetX reaction each SCALEs gene was
scored against by the ECSPr GEM channel, its equation, its baked direction ratio, and
whether it changes a carbon skeleton -- plus the full candidate list that choosing one
reaction per gene throws away.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/woodruff/parse/build_gof_reactions.py
    ... --publish        # rewrites gof.csv in place, writes gof_reaction_edges.csv beside it

THE RULES ARE THE EYDALLIN BENCHMARK'S, IMPORTED RATHER THAN COPIED.
`carbon_bond_change`, `legible_equation`, `pick_primary` and `denovo_gapfill` all come
from `benchmarks/eydallin/parse/reaction_chemistry.py`, and the atom-pair/direction
tables from `benchmarks/eydallin/bake_pairs.py` -- the same import `sweeps/sweep_scales.py`
already makes. Two copies of those rules is how two benchmarks in one tree come to
disagree about what `breaks` means. In particular `carbon_bond_change` is read off the
baked atom map by connected components, never guessed from the equation string; the two
give different answers and only the first is a measurement.

FOUR OUTCOMES PER GENE, as in eydallin. `mnxr_mapping_method` says which: `GEM` (the
curated model has it), `LLM review` (the model does not, but a specific MetaNetX
reaction was hand-picked and cited), `denovo` (neither, but the de-novo GPR channel's
independent projection methods agree on one reaction), or blank, in which case
`no_mapping_reason` says why. A blank `mnxr` and a blank `no_mapping_reason` never both
happen; every one of the 4,225 genes lands in exactly one of the four outcomes.

THE LLM-REVIEW CHANNEL IS EYDALLIN'S OWN, REUSED, AND NOTHING NEW WAS CURATED HERE.
`MANUAL_MNXR` below is a verbatim subset of the eydallin benchmark's hand-picked table
-- same organism, same reasoning, already cited there -- kept only for genes this screen
also measures and the curated GEM also misses. It is NOT extended. Hand-reviewing a
4,225-gene screen is not something that was done, and a partial hand-review would put
curator attention exactly where the reader cannot see it. Six genes clear the bar by
inheritance; the rest of the miss list is left to the mechanical channels and to
`no_mapping_reason`.

`no_mapping_reason` IS MECHANICAL HERE, AND THAT IS A WEAKER COLUMN THAN EYDALLIN'S.
Eydallin's eleven-way split (`regulatory`, `transport`, `dna_repair`, ...) is a hand
adjudication of 52 genes against each gene's own literature. It does not scale to 2,700,
and inventing it by keyword would be exactly the fabrication the column exists to
prevent. So this column reports WHICH CHANNEL FAILED, not WHY THE BIOLOGY HAS NO
REACTION -- three categories, each read straight off the tables:

  no_candidate_in_either_channel -- neither the curated GEM nor any de-novo annotation
                                    lane nominated a reaction for this gene at all
  denovo_candidates_disagree     -- de-novo lanes nominated candidates, but no single
                                    reaction cleared `denovo_gapfill`'s bar (>=2
                                    independent methods on one `mnxr`, pool <= 10)
  denovo_pick_is_nonspecific     -- a reaction cleared that bar and was then dropped
                                    (see below)

DROPPING A GAP-FILL REACTION NOMINATED FOR MORE THAN ONE GENE IS THE SCALE VERSION OF
EYDALLIN'S HAND REJECTION. Eydallin refused `ucpA`, `yabI`, `ylcG` and `yqjA` because a
strong de-novo hit on a catalytic FOLD is not evidence the gene does that fold's
specific reaction. At 4,225 genes that non-specificity has a mechanical signature: the
same reaction gets nominated for many unrelated genes. Run unfiltered, this cascade
hands `MNXR153054` (`ADP + phosphate -> ATP`) to 35 genes, a generic DNA polymerase
(`MNXR166212`) to 13 and a generic RNA polymerase (`MNXR166137`) to 5 -- the identical
artifact eydallin names, at scale. A gene-specific hit is nominated once. So a gap-fill
reaction claimed by more than one gene is dropped from every one of them and those genes
are recorded as `denovo_pick_is_nonspecific`, which is a checked decision rather than an
omission.

GENE -> REACTION IS MANY-TO-MANY AND gof.csv IS ONE ROW PER GENE. The GEM channel
nominates more than one `mnxr` for 642 of its 1,503 covered genes. `gof.csv` keeps
exactly one PRIMARY reaction per gene (still 4,225 rows) under `pick_primary`'s
`in_atom_universe`-first tie-break, and every candidate that lost -- with the evidence it
lost on -- goes to `gof_reaction_edges.csv` beside it, so the pick is checkable rather
than silently swallowed.

THE JOIN KEY IS `condition_id`, NOT `feature_name`, for the reason eydallin gives:
`feature_name` is the model's current symbol after b-number resolution, so joining on it
silently drops every renamed gene. Here `condition_id` is `scales_tol:<gene_norm>` --
the screen's own lowercase spelling -- while `gof.csv` carries the sheet spelling in
`gene`. The two are bridged through `gene_census.tsv`, which is the same table
`build_gof_table.py` read, rather than by lowercasing `gene` and hoping.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[5]
EYDALLIN = REPO / "research/fabfos/benchmarks/eydallin"

sys.path.insert(0, str(EYDALLIN))
sys.path.insert(0, str(EYDALLIN / "parse"))
import bake_pairs                                                             # noqa: E402
from reaction_chemistry import (carbon_bond_change, denovo_gapfill,           # noqa: E402
                                legible_equation, pick_primary)

GOF_DIR = REPO / "data/fabfos/runs/woodruff_clones/parse/gof"
GOF_CSV = GOF_DIR / "gof.csv"
EDGES_CSV = GOF_DIR / "gof_reaction_edges.csv"
CENSUS = REPO / "data/fabfos/runs/woodruff_clones/gpr/gene_census.tsv"
GPR_GEM = REPO / "data/fabfos/runs/woodruff_clones/gpr/gpr_gem.parquet"
GPR_DENOVO = REPO / "data/fabfos/runs/woodruff_clones/gpr/gpr_denovo.parquet"
REACTIONS = REPO / "data/fabfos/processed/lookups/reactions.parquet"
METABOLITES = REPO / "data/fabfos/processed/lookups/metabolites.parquet"

# Inherited verbatim from `benchmarks/eydallin/parse/build_gof_reactions.py`. Each entry
# was chosen there by hand for the same organism -- product identity checked by name
# against the characterized activity -- and the citation is that file. Nothing was added.
MANUAL_MNXR = {
    "yjcC": ("MNXR96524", "c-di-GMP phosphodiesterase: c-di-GMP + H2O -> pGpG"),
    "yeaP": ("MNXR97353", "diguanylate cyclase: 2 GTP -> c-di-GMP + 2 diphosphate"),
    "yafV": ("MNXR105306", "2-oxoglutaramate amidase: 2-oxoglutaramate + H2O -> "
             "2-oxoglutarate + NH4+"),
    "hyuA": ("MNXR160637", "D-phenylhydantoinase: phenylhydantoin + H2O -> "
             "phenylureidoacetic acid"),
    "cpdB": ("MNXR148152", "2',3'-cyclic-nucleotide 2'-phosphodiesterase: "
             "2',3'-cyclic nucleotide + H2O -> nucleoside 2'-phosphate"),
    "yncG": ("MNXR165804", "glutathione S-transferase (EC 2.5.1.18): RX + "
             "glutathione -> a halide anion + an S-substituted glutathione"),
}
MANUAL_MNXR = {g.lower(): v for g, v in MANUAL_MNXR.items()}

NO_CANDIDATE = "no_candidate_in_either_channel"
DISAGREE = "denovo_candidates_disagree"
NONSPECIFIC = "denovo_pick_is_nonspecific"

GOF_IDENTITY_COLS = ("gene", "source_genome", "locus_tag", "product",
                     "fitness_30gL_ethanol")
GOF_REACTION_COLS = ("mnxr", "mnxr_mapping_method", "reaction_equation",
                     "direction_ratio", "carbon_bond_change", "no_mapping_reason")
EDGE_COLS = ("gene", "channel", "model_gene_symbol", "mnxr", "is_primary",
             "primary_reason", "intermediate_id", "intermediate_name", "raw_score",
             "evidence_quality", "in_atom_universe", "gpr_rule", "reaction_equation",
             "direction_ratio", "carbon_bond_change")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"rewrite {GOF_CSV.relative_to(REPO)} and write "
                         f"{EDGES_CSV.relative_to(REPO)}")
    a = ap.parse_args()

    gof_rows = [{k: row[k] for k in GOF_IDENTITY_COLS}
                for row in csv.DictReader(GOF_CSV.open())]
    census = pd.read_csv(CENSUS, sep="\t", dtype=str).fillna("")
    norm_of = dict(zip(census.gene, census.gene_norm))

    gpr = pd.read_parquet(GPR_GEM)
    gpr["gene"] = gpr["condition_id"].str.split(":").str[1]
    gpr_denovo = pd.read_parquet(GPR_DENOVO)
    gpr_denovo["gene"] = gpr_denovo["condition_id"].str.split(":").str[1]
    met_names = pd.read_parquet(METABOLITES).set_index("mnxm")["name"].to_dict()
    raw_equations = pd.read_parquet(REACTIONS).set_index("mnxr")["equation"].to_dict()
    reactions = {mnxr: legible_equation(eq, met_names) for mnxr, eq in raw_equations.items()}
    ratios = pd.read_parquet(bake_pairs.direction_ratios()).set_index("mnxr")["ratio"].to_dict()
    atom_pairs = pd.read_parquet(bake_pairs.atom_pairs())
    atom_pairs_by_mnxr = {m: g for m, g in atom_pairs.groupby("mnxr")}

    gem_covered = set(gpr["gene"])
    uncovered = {norm_of[r["gene"]] for r in gof_rows} - gem_covered - set(MANUAL_MNXR)
    raw_picks = denovo_gapfill(gpr_denovo, uncovered)
    claimed = Counter(mnxr for mnxr, _ in raw_picks.values())
    denovo_picks = {g: v for g, v in raw_picks.items() if claimed[v[0]] == 1}
    nonspecific = {g for g in raw_picks if g not in denovo_picks}

    print(f"{len(gof_rows)} genes in gof.csv, {len(gpr)} gene-reaction rows in "
          f"{GPR_GEM.name} covering {len(gem_covered)} genes, "
          f"{len(reactions):,} reaction equations, {len(ratios):,} direction "
          f"ratios, {len(atom_pairs_by_mnxr):,} reactions with atom-mapped pairs")
    dropped = sorted(((n, m) for m, n in claimed.items() if n > 1), reverse=True)[:5]
    print(f"de-novo gap-fill over {len(uncovered)} GEM-uncovered genes: {len(raw_picks)} "
          f"cleared the >=2-method bar, {len(nonspecific)} of them dropped because their "
          f"reaction was claimed by more than one gene "
          f"({', '.join(f'{m} x{n}' for n, m in dropped)}) -> {len(denovo_picks)} kept")

    bond_cache: dict[str, str] = {}

    def bond_for(mnxr: str) -> str:
        if mnxr not in bond_cache:
            pairs = atom_pairs_by_mnxr.get(mnxr)
            bond_cache[mnxr] = carbon_bond_change(pairs) if pairs is not None else ""
        return bond_cache[mnxr]

    edge_rows, n_multi, n_gem, n_manual, n_denovo, n_no_reaction = [], 0, 0, 0, 0, 0
    for r in gof_rows:
        gene = norm_of[r["gene"]]
        candidates = gpr[gpr["gene"] == gene]
        primary, reason = pick_primary(candidates)
        if len(candidates) > 1:
            n_multi += 1

        for _, c in candidates.iterrows():
            mnxr = c["mnxr"]
            is_primary = primary is not None and c["mnxr"] == primary["mnxr"] and \
                c["intermediate_id"] == primary["intermediate_id"]
            edge_rows.append({
                "gene": r["gene"],
                "channel": "GEM",
                "model_gene_symbol": c["feature_name"],
                "mnxr": mnxr,
                "is_primary": "yes" if is_primary else "no",
                "primary_reason": reason if is_primary else "",
                "intermediate_id": c["intermediate_id"],
                "intermediate_name": c["intermediate_name"],
                "raw_score": c["raw_score"],
                "evidence_quality": c["evidence_quality"],
                "in_atom_universe": c["in_atom_universe"],
                "gpr_rule": c["gpr_rule"],
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })

        channel, no_mapping_reason = "", ""
        if primary is not None:
            channel = "GEM"
            n_gem += 1
        elif gene in MANUAL_MNXR or gene in denovo_picks:
            if gene in MANUAL_MNXR:
                mnxr, note = MANUAL_MNXR[gene]
                channel, n_manual = "LLM review", n_manual + 1
            else:
                mnxr, note = denovo_picks[gene]
                channel, n_denovo = "denovo", n_denovo + 1
            primary = pd.Series({"mnxr": mnxr})
            edge_rows.append({
                "gene": r["gene"], "channel": channel, "model_gene_symbol": gene,
                "mnxr": mnxr, "is_primary": "yes", "primary_reason": note,
                "intermediate_id": "", "intermediate_name": "", "raw_score": "",
                "evidence_quality": channel, "in_atom_universe": "", "gpr_rule": "",
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })
        else:
            no_mapping_reason = (NONSPECIFIC if gene in nonspecific else
                                 DISAGREE if gene in set(gpr_denovo["gene"]) else
                                 NO_CANDIDATE)
            n_no_reaction += 1

        r["mnxr"] = primary["mnxr"] if primary is not None else ""
        r["mnxr_mapping_method"] = channel
        r["reaction_equation"] = reactions.get(r["mnxr"], "") if primary is not None else ""
        r["direction_ratio"] = ratios.get(r["mnxr"], "") if primary is not None else ""
        r["carbon_bond_change"] = bond_for(r["mnxr"]) if primary is not None else ""
        r["no_mapping_reason"] = no_mapping_reason

    print(f"\n{n_gem}/{len(gof_rows)} genes resolved via {GPR_GEM.name}, "
          f"{n_manual} more via LLM review, {n_denovo} more via de-novo gap-fill, "
          f"{n_no_reaction} have no reaction (see no_mapping_reason) -> "
          f"{n_gem + n_manual + n_denovo}/{len(gof_rows)} covered")
    print(f"{n_multi} GEM-channel genes had more than one candidate reaction -> "
          f"{len(edge_rows)} edge rows")
    n_bond = sum(1 for r in edge_rows if r["carbon_bond_change"])
    print(f"{n_bond}/{len(edge_rows)} edges have an atom-mapped carbon-bond call "
          f"({len(edge_rows) - n_bond} have no carbon atom-mapping to call it from)")
    print("carbon_bond_change over gof.csv:",
          dict(Counter(r["carbon_bond_change"] or "(unknown)" for r in gof_rows)))
    print("no_mapping_reason breakdown:",
          dict(Counter(r["no_mapping_reason"] for r in gof_rows if r["no_mapping_reason"])))

    if not a.publish:
        print(f"\n(dry run -- pass --publish to rewrite {GOF_CSV.relative_to(REPO)} "
              f"and write {EDGES_CSV.relative_to(REPO)})")
        return 0

    with GOF_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(GOF_IDENTITY_COLS) + list(GOF_REACTION_COLS))
        w.writeheader()
        w.writerows(gof_rows)
    with EDGES_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(EDGE_COLS))
        w.writeheader()
        w.writerows(edge_rows)
    print(f"\n-> {GOF_CSV}: {len(gof_rows)} rows")
    print(f"-> {EDGES_CSV}: {len(edge_rows)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
