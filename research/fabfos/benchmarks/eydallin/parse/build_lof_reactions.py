#!/usr/bin/env python3
"""Attach reaction chemistry to `lof.csv`: which MetaNetX reaction each deleted gene was
scored against by the ECSPr GEM channel, its equation, its baked direction ratio, and
whether it changes a carbon skeleton -- plus the full candidate list that choosing one
reaction per gene throws away. The LOF counterpart to `build_gof_reactions.py`.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/parse/build_lof_reactions.py
    ... --publish        # rewrites lof.csv in place, writes lof_reaction_edges.csv beside it

SAME FOUR OUTCOMES PER GENE as the gof half, and `mnxr_mapping_method` says which: `GEM`,
`LLM review`, `denovo`, or blank with `no_mapping_reason` saying why. A blank `mnxr` and a
blank `no_mapping_reason` never both happen, and neither do both filled.

THE JOIN KEY IS THE B-NUMBER, AND THAT IS THE WHOLE REASON `build_lof_table.py` CARRIES
ONE. `gpr_gem.parquet`'s `feature_name` is iML1515's current gene symbol, and six of this
cohort's 2007 names have been retired since -- `ybhE` is `pgl`, `yhbG` is `lptB`, `yobG`
is `mgrB`, `cspC` is `cspE`, `cysU` is `cysT`, `deoT` is `yciT`. Joining the paper's
spelling against `feature_name` silently drops all six. The b-number is stable across both
renames and substrains, it is what `orf` holds in an iML1515-derived table, and
`lof_resolution.tsv` already resolved it for all 65 genes.

WHY THIS HALF HAS NO `LLM review` AND NO `denovo` ENTRIES, WHICH IS A FINDING RATHER THAN
AN OMISSION. In the gof half those two channels rescued 8 of 52 misses, because that
cohort's misses were plasmid-borne enzymes of uncharacterized function -- genes with real
metabolite chemistry that DH1's small curated model simply lacked. This cohort's 27 misses
are not that. They are sigma factors, response regulators, ribosomal proteins, proteases
and RNA-acting enzymes: genes with no metabolite substrate for a curated model to have
missed. An overexpression screen surfaces enzymes; a deletion screen of the same phenotype
surfaces the regulatory network around it, and that asymmetry is visible right here in the
channel counts.

Two of the 27 were checked properly before being categorised, because both ARE enzymes and
both have their textbook reaction in MetaNetX, nominated independently by three de-novo
methods:

  glnD  PII uridylyltransferase, EC 2.7.7.59. `MNXR165615` is exactly its characterized
        activity ([protein-PII]-L-tyrosine + UTP -> [protein-PII]-uridylyl-L-tyrosine +
        PPi), agreed by blast_bsr, clean_confidence and knn_vote.
  miaA  tRNA dimethylallyltransferase, EC 2.5.1.75. `MNXR166489` likewise (adenosine(37)
        in tRNA + DMAPP -> N(6)-dimethylallyladenosine(37) in tRNA + PPi), same three.

Neither is promoted, for the reason the gof half did not promote `clpA`: the substrate is a
macromolecule, not a metabolite. Both reactions are absent from the atom universe, so they
carry no carbon-bond call and no direction ratio -- writing the id would add an identifier
nothing downstream can read, dressed as coverage. The de-novo channel's own rule rejects
them independently and for a different reason: each gene has TWO reactions clearing the
two-method bar (`MNXR169247` for glnD, `MNXR147164` for miaA), and the rule admits a gene
only when exactly one does.

Consistency check worth keeping: `rpoS` is in both cohorts, and both halves categorise it
`regulatory`. `glgP` is in both and both resolve it through the GEM.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import bake_pairs                                                             # noqa: E402
from reaction_chemistry import (carbon_bond_change, denovo_gapfill,           # noqa: E402
                                legible_equation, pick_primary)

LOF_DIR = REPO / "data/fabfos/runs/eydallin_clones/parse/lof"
LOF_CSV = LOF_DIR / "lof.csv"
RESOLUTION = LOF_DIR / "lof_resolution.tsv"
EDGES_CSV = LOF_DIR / "lof_reaction_edges.csv"
GPR_GEM = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
GPR_DENOVO = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_denovo.parquet"
REACTIONS = REPO / "data/fabfos/processed/lookups/reactions.parquet"
METABOLITES = REPO / "data/fabfos/processed/lookups/metabolites.parquet"

# No entries. See the module docstring -- this cohort's misses have no metabolite
# substrate, and the two that do (glnD, miaA) act on macromolecules and sit outside the
# atom universe. The channel stays wired so that a later gene with real chemistry has
# somewhere to go, and so that "empty" reads as a checked result rather than a missing
# feature.
MANUAL_MNXR: dict[str, tuple[str, str]] = {}

# THE ELEVEN CATEGORIES `build_gof_reactions.py` ESTABLISHED, PLUS TWO THIS COHORT NEEDS.
# Each is the gene's own established functional category, not a paraphrase of "no reaction
# found" invented for the column, and a class of one is still the right class -- that
# precedent is the gof half's and it holds here.
#
#   regulatory              -- a sigma factor, response regulator, transcription factor or
#                              antiterminator, or an RNA chaperone acting as a
#                              post-transcriptional regulator: a regulatory component with
#                              no substrate to react (11 genes)
#   protein_modification    -- NEW. A covalent protein-modifying enzyme: real catalysis,
#                              but the substrate is a signalling protein rather than a
#                              metabolite pool, so no reaction in a metabolic model
#                              corresponds to it (2 genes: `glnD`, `phoQ`). Distinct from
#                              `regulatory`, which has no catalysis at all, and from
#                              `proteolysis`, which destroys rather than modifies.
#   proteolysis             -- degrades protein substrates (2 genes: `clpP`, `lon`)
#   translation             -- a ribosomal protein or ribosome assembly factor (3 genes:
#                              `rbfA`, `rpmJ`, `rpsF`)
#   rna_processing          -- acts covalently on RNA, degrading or modifying it (2 genes:
#                              `miaA`, `rnt`)
#   transport               -- a transporter with no single defined substrate (1 gene:
#                              `mdtH`, a predicted multidrug efflux system)
#   envelope_assembly_or_secretion -- structural or assembly machinery for the cell
#                              envelope, no metabolite substrate (1 gene: `nlpD`)
#   electron_carrier        -- NEW. A redox protein that shuttles electrons rather than
#                              transforming a metabolite; it has no substrate of its own
#                              (1 gene: `fdx`, whose own supplement entry reads
#                              "Ferredoxin, an iron-sulfur protein"). Its Fe-S assembly
#                              partners `iscS` and `iscU` both resolve through the GEM, so
#                              this is a gap in what a ferredoxin IS, not in the pathway.
#   uncharacterized         -- function or substrate not established in the literature
#                              (4 genes; the paper's own supplement calls two of them
#                              "predicted protein" and two "conserved inner membrane
#                              protein")
NO_MAPPING_REASON = {
    "cspC": "regulatory", "deoT": "regulatory", "greA": "regulatory",
    "hfq": "regulatory", "nusB": "regulatory", "phoB": "regulatory",
    "phoP": "regulatory", "rfaH": "regulatory", "rpoN": "regulatory",
    "rpoS": "regulatory", "yobG": "regulatory",

    "glnD": "protein_modification", "phoQ": "protein_modification",

    "clpP": "proteolysis", "lon": "proteolysis",

    "rbfA": "translation", "rpmJ": "translation", "rpsF": "translation",

    "miaA": "rna_processing", "rnt": "rna_processing",

    "mdtH": "transport",
    "nlpD": "envelope_assembly_or_secretion",
    "fdx": "electron_carrier",

    "yciU": "uncharacterized", "ygdD": "uncharacterized",
    "ynfA": "uncharacterized", "yqgB": "uncharacterized",
}

LOF_IDENTITY_COLS = ("gene", "source_genome", "locus_tag", "product",
                     "pct_glycogen_production")
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

    # keep only the identity columns from whatever's on disk -- a prior run of this script
    # may have written a now-retired reaction column, and re-reading its output must not
    # carry that stale column into DictWriter's strict fieldnames.
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
    raw_equations = pd.read_parquet(REACTIONS).set_index("mnxr")["equation"].to_dict()
    reactions = {mnxr: legible_equation(eq, met_names)
                 for mnxr, eq in raw_equations.items()}
    ratios = pd.read_parquet(bake_pairs.direction_ratios()).set_index("mnxr")["ratio"].to_dict()
    atom_pairs = pd.read_parquet(bake_pairs.atom_pairs())
    atom_pairs_by_mnxr = {m: g for m, g in atom_pairs.groupby("mnxr")}

    uncharacterized = {g for g, cat in NO_MAPPING_REASON.items()
                       if cat == "uncharacterized"}
    denovo_picks = denovo_gapfill(gpr_denovo, uncharacterized)

    print(f"{len(lof_rows)} genes in lof.csv, {len(gpr)} gene-reaction rows in "
          f"{GPR_GEM.name} covering {gpr['gene'].nunique()} genes, "
          f"{len(reactions):,} reaction equations, {len(ratios):,} direction ratios, "
          f"{len(atom_pairs_by_mnxr):,} reactions with atom-mapped pairs")
    print(f"{len(uncharacterized)} genes are uncharacterized; the de-novo channel "
          f"gap-fills {len(denovo_picks)} of those with >=2-method agreement on one "
          f"reaction: {sorted(denovo_picks)}")

    bond_cache: dict[str, str] = {}

    def bond_for(mnxr: str) -> str:
        if mnxr not in bond_cache:
            pairs = atom_pairs_by_mnxr.get(mnxr)
            bond_cache[mnxr] = carbon_bond_change(pairs) if pairs is not None else ""
        return bond_cache[mnxr]

    edge_rows, n_multi, n_gem, n_manual, n_denovo, n_no_reaction = [], 0, 0, 0, 0, 0
    for r in lof_rows:
        gene = r["gene"]
        candidates = gpr[gpr["gene"] == gene]
        primary, reason = pick_primary(candidates)
        if len(candidates) > 1:
            n_multi += 1

        for _, c in candidates.iterrows():
            mnxr = c["mnxr"]
            is_primary = primary is not None and c["mnxr"] == primary["mnxr"] and \
                c["intermediate_id"] == primary["intermediate_id"]
            edge_rows.append({
                "gene": gene,
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
        elif gene in MANUAL_MNXR:
            mnxr, note = MANUAL_MNXR[gene]
            channel = "LLM review"
            n_manual += 1
            primary = pd.Series({"mnxr": mnxr})
            edge_rows.append({
                "gene": gene, "channel": "LLM review", "model_gene_symbol": gene,
                "mnxr": mnxr, "is_primary": "yes", "primary_reason": note,
                "intermediate_id": "", "intermediate_name": "", "raw_score": "",
                "evidence_quality": "LLM review", "in_atom_universe": "", "gpr_rule": "",
                "reaction_equation": reactions.get(mnxr, ""),
                "direction_ratio": ratios.get(mnxr, ""),
                "carbon_bond_change": bond_for(mnxr),
            })
        elif gene in denovo_picks:
            mnxr, note = denovo_picks[gene]
            channel = "denovo"
            n_denovo += 1
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
            if gene not in NO_MAPPING_REASON:
                raise SystemExit(
                    f"[lof] {gene} resolves no reaction and has no no_mapping_reason -- "
                    f"add its own functional category rather than letting it default")
            no_mapping_reason = NO_MAPPING_REASON[gene]
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
        raise SystemExit(f"[lof] the four-outcome invariant broke: both blank "
                         f"{both_blank}, both filled {both_filled}")

    print(f"\n{n_gem}/{len(lof_rows)} genes resolved via gpr_gem.parquet, "
          f"{n_manual} more via LLM review, {n_denovo} more via de-novo gap-fill, "
          f"{n_no_reaction} have no reaction (see no_mapping_reason) -> "
          f"{n_gem + n_manual + n_denovo}/{len(lof_rows)} covered")
    print(f"{n_multi} GEM-channel genes had more than one candidate reaction -> "
          f"{len(edge_rows)} edge rows")
    n_bond = sum(1 for r in edge_rows if r["carbon_bond_change"])
    print(f"{n_bond}/{len(edge_rows)} edges have an atom-mapped carbon-bond call "
          f"({len(edge_rows) - n_bond} have no carbon atom-mapping to call it from)")
    reason_counts = Counter(r["no_mapping_reason"] for r in lof_rows
                            if r["no_mapping_reason"])
    print("no_mapping_reason breakdown:", dict(reason_counts))

    if not a.publish:
        print(f"\n(dry run -- pass --publish to rewrite {LOF_CSV.relative_to(REPO)} "
              f"and write {EDGES_CSV.relative_to(REPO)})")
        return 0

    with LOF_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(LOF_IDENTITY_COLS) + list(LOF_REACTION_COLS))
        w.writeheader()
        w.writerows(lof_rows)
    with EDGES_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(EDGE_COLS))
        w.writeheader()
        w.writerows(edge_rows)
    print(f"\n-> {LOF_CSV}: {len(lof_rows)} rows")
    print(f"-> {EDGES_CSV}: {len(edge_rows)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
