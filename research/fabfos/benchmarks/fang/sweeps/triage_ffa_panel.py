#!/usr/bin/env python3
"""What can ECSPr see of this study at all, before it is asked to predict anything.

A benchmark is only worth running if the phenotype and the method's reach are not
anti-correlated. If every gene that moved the titer is one the network has no edge
for, no score computed afterwards can mean anything, and the honest deliverable is
that sentence rather than a table of z-values. So the 2x2 -- moved the phenotype x
carries an atom-mapped edge -- is computed first and reported whatever it says.

REACH IS THE TIER'S OWN DEFINITION, NOT THE HOST GEM'S COLUMN. The two disagree
and the difference is the whole story here: the study tier excludes TRANSPORT from
the atom universe, because a transporter's only atom pairs are the ATP hydrolysis
every transporter shares, so "did the method move this" has the same answer for
all of them and none of it is about the species that crossed the membrane. The
ASKA winners are largely transporters -- ydeA, nepI, setA, setB, msbA and the
whole lpt operon -- so reading reach off `gpr_gem.parquet`, whose universe keeps
transport, would count exactly those as visible and overstate the study's ceiling.

Reads the built study tier and writes, beside this script:

    out/triage_2x2.tsv        the contingency table, over each condition subset
    out/condition_reach.tsv   per condition: clones, measured direction, reach
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(HERE)
TIER = ROOT / "data/scratch/bench_conditions_local/study_tier/fang"
EXTRACT = ROOT / "data/fabfos/benchmarks/_extractions/fang/extraction.tsv"
PAIRS = ROOT / "data/fabfos/benchmark/reference_tier4/atom_pairs_tier4.parquet"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tier", type=Path, default=TIER)
    ap.add_argument("--out", type=Path, default=HERE / "out")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    conds = pd.read_csv(args.tier / "conditions.tsv", sep="\t", keep_default_na=False)
    conds = conds[conds["element"] == "C"]
    gpr = pd.read_parquet(args.tier / "gpr_manual.parquet")
    ext = pd.read_csv(EXTRACT, sep="\t", keep_default_na=False)

    add = gpr[gpr["action"] == "add"]
    reach = add.groupby("condition_id")["in_atom_universe"].apply(
        lambda s: bool(s.astype("boolean").fillna(False).any()))
    n_rxn = add.groupby("condition_id")["mnxr"].nunique()
    n_mapped = (add[add["in_atom_universe"].astype("boolean").fillna(False)]
                .groupby("condition_id")["mnxr"].nunique())

    carriable = set(pd.read_parquet(PAIRS, columns=["mnxr", "element"])
                    .query("element == 'C'")["mnxr"].unique())
    n_ecspr = (add[add["mnxr"].isin(carriable)]
               .groupby("condition_id")["mnxr"].nunique())

    per_obs = ext[ext["role"] != "control"].groupby("obs_id")
    clones = per_obs["gene"].apply(lambda s: "|".join(sorted(set(s) - {""})))
    fold = per_obs["fold_change"].first().astype(float)
    ffa = per_obs["ffa_mg_L"].first().astype(float)
    figure = per_obs["figure"].first()

    df = conds[conds["tier"] == "primary"].copy()
    df["clones"] = df["condition_id"].map(clones).fillna("")
    df["n_clones"] = df["clones"].map(lambda s: len([g for g in s.split("|") if g]))
    df["figure"] = df["condition_id"].map(figure).fillna("")
    df["ffa_mg_L"] = df["condition_id"].map(ffa)
    df["fold_change"] = df["condition_id"].map(fold)
    df["n_reactions"] = df["condition_id"].map(n_rxn).fillna(0).astype(int)
    df["n_atom_mapped"] = df["condition_id"].map(n_mapped).fillna(0).astype(int)
    df["n_ecspr_reactions"] = df["condition_id"].map(n_ecspr).fillna(0).astype(int)
    df["mappable"] = df["condition_id"].map(reach).astype("boolean").fillna(False)
    df["mappable_ecspr"] = df["n_ecspr_reactions"] > 0
    df["moved"] = df["measured_dir"].isin(["+", "-"])

    cols = ["condition_id", "figure", "clones", "n_clones", "ffa_mg_L",
            "fold_change", "measured_dir", "n_reactions", "n_atom_mapped",
            "n_ecspr_reactions", "mappable", "mappable_ecspr", "is_control",
            "control_kind"]
    df[cols].sort_values("condition_id").to_csv(
        args.out / "condition_reach.tsv", sep="\t", index=False)

    subsets = {
        "all_primary": df,
        "single_clone": df[df["n_clones"] == 1],
        "single_clone_F": df[(df["n_clones"] == 1) & (df["figure"].isin(
            ["fig1c", "fig3b", "figS5"]))],
    }
    rows = []
    for name, sub in subsets.items():
        for moved in (True, False):
            for mappable in (True, False):
                rows.append(dict(subset=name, moved=moved, mappable=mappable,
                                 n=int(((sub["moved"] == moved) &
                                        (sub["mappable"] == mappable)).sum())))
    tab = pd.DataFrame(rows)
    tab.to_csv(args.out / "triage_2x2.tsv", sep="\t", index=False)

    for name, sub in subsets.items():
        ct = pd.crosstab(sub["moved"], sub["mappable"],
                         rownames=["moved"], colnames=["mappable"])
        mv, nm = sub[sub["moved"]], sub[~sub["moved"]]
        rate_moved = mv["mappable"].mean() if len(mv) else float("nan")
        rate_still = nm["mappable"].mean() if len(nm) else float("nan")
        print(f"\n=== {name}: {len(sub)} conditions ===")
        print(ct.to_string())
        print(f"mappable among moved {rate_moved:.1%} vs among unmoved "
              f"{rate_still:.1%}")

    up = df[df["measured_dir"] == "+"].sort_values("fold_change", ascending=False)
    print(f"\n=== the {len(up)} conditions the paper scores as an increase ===")
    print(up[["condition_id", "clones", "fold_change", "n_reactions",
              "n_atom_mapped", "mappable"]].to_string(index=False))
    print(f"\nwrote {args.out}/triage_2x2.tsv and {args.out}/condition_reach.tsv")


if __name__ == "__main__":
    main()
