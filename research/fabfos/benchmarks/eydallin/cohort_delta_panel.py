#!/usr/bin/env python3
"""Cohort-wide GOF delta panel: does ECSPr's glycogen delta track the measured phenotype?

Extends `delta_panel.py` from the two hand-picked controls (glgC/ddg) to every eydallin
condition whose gene has a reaction the curated host GEM (iECDH1ME8569_1439, GPR channel
`gem_gpr`) already carries in the atom universe -- 25 of the cohort's 86, per
`data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet`. Each gene's reaction(s) are folded
x2 together (the GOF/overexpression model `run_pilot_glycogen.py` established), one base
solve is shared across the whole cohort (only the perturbed reactions' weights change
between conditions), and glycogen's delta is correlated against `Y/measured_glycogen.tsv`
-- the digitised Fig. 1 phenotype, joined case-insensitively on condition_id (the GPR
table's `ppK` vs the measured table's `ppk` is the one mismatch this catches).

The other 61 genes are NOT in this run: they have no GEM-asserted reaction in the atom
universe, so a fold-change has nothing to multiply. The denovo GPR channel
(`gpr_denovo.parquet`) covers 81 of them, but 774 of its 934 in-universe rows name a
reaction the host GEM does not carry at all -- an ADDITION (`--weight`, per
run_pilot_glycogen.py), not a fold, and there is no principled default weight for "a clone
adds this reaction" the way there is for "this reaction's dosage doubled". That is future
work, not a number this script should invent.

    mamba run -n ecspr python main/benchmarks/eydallin/cohort_delta_panel.py --fold 2.0
"""
from __future__ import annotations

import sys
import argparse
from pathlib import Path

import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, measure_leak                               # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bake_pairs  # noqa: E402

ATOM_PAIRS = bake_pairs.atom_pairs()
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = Path(__file__).resolve().parent / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    args = p.parse_args()

    pairs = load_pairs(ATOM_PAIRS, element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}

    clone = pd.read_parquet(CLONE_GPR)
    clone = clone[clone.in_atom_universe & (clone.channel == "gem_gpr")]
    not_in_host = sorted(set(clone.mnxr) - set(base_w))
    if not_in_host:
        raise SystemExit(f"[cohort] {not_in_host} named by the clone GPR but absent from "
                         f"the host GEM's own reaction set -- the fold model doesn't cover "
                         f"this, add --weight addition handling before running")

    genes = (clone.groupby("condition_id")
             .agg(gene=("feature_name", "first"), rxns=("mnxr", lambda s: sorted(set(s))))
             .reset_index())
    print(f"[cohort] {len(genes)} conditions with >=1 in-universe host-GEM reaction "
          f"(of {clone.condition_id.nunique()} distinct condition_ids in the filtered "
          f"clone GPR table)", file=sys.stderr)

    def solve_(weights, tag):
        g = graph_from_pairs(pairs, args.element, weights, ratios)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="source")
        if src.missing:
            raise SystemExit(f"[cohort] source {SOURCE_MNXM} absent from the built graph")
        r = measure_leak(g, src, [], leak=args.leak)
        print(f"[cohort] {tag}: {g.n:,} nodes / {g.m:,} edges from "
              f"{g.meta['n_reactions_used']:,} reactions (AAM gap {g.meta['n_aam_gap']:,})",
              file=sys.stderr)
        return g, r

    g_base, r_base = solve_(base_w, "base")
    if GLYCOGEN_MNXM not in r_base["draw"]:
        raise SystemExit(f"[cohort] {GLYCOGEN_MNXM} (glycogen) never became a node in the "
                         f"base build")
    draw_base = r_base["draw"][GLYCOGEN_MNXM]

    rows = []
    for i, row in genes.iterrows():
        pert_w = dict(base_w)
        for r in row.rxns:
            pert_w[r] = base_w[r] * args.fold
        _, r_pert = solve_(pert_w, row.condition_id)
        draw_pert = r_pert["draw"].get(GLYCOGEN_MNXM)
        if draw_pert is None:
            print(f"[cohort] WARNING: {row.condition_id} perturbed build drops glycogen "
                  f"as a node -- skipping", file=sys.stderr)
            continue
        rows.append(dict(condition_id=row.condition_id, gene=row.gene,
                         rxns=",".join(row.rxns), draw_base=draw_base, draw_pert=draw_pert,
                         delta=draw_pert - draw_base))

    df = pd.DataFrame(rows)
    df["condition_key"] = df.condition_id.str.lower()

    meas = pd.read_csv(MEASURED, sep="\t")
    meas["condition_key"] = meas.condition_id.str.lower()
    merged = df.merge(meas[["condition_key", "pct_wt", "nmol_glucose_per_mg_protein"]],
                      on="condition_key", how="left")

    unmatched = merged[merged.pct_wt.isna()]
    if len(unmatched):
        print(f"[cohort] {len(unmatched)} condition(s) have no measured phenotype: "
              f"{list(unmatched.condition_id)}", file=sys.stderr)
    scored = merged.dropna(subset=["pct_wt"])

    out = OUT_DIR / f"cohort_delta_panel_gem_fold{args.fold}_{args.element}.parquet"
    merged.to_parquet(out)
    print(f"\n[cohort] {len(merged)} conditions run, {len(scored)} scored against the "
          f"measured phenotype; wrote {out}", file=sys.stderr)

    rho_d, p_d = spearmanr(scored.delta, scored.pct_wt)
    rho_p, p_p = spearmanr(scored.draw_pert, scored.pct_wt)
    print(f"[cohort] Spearman(delta, pct_wt)      rho={rho_d:+.4f}  p={p_d:.3g}  n={len(scored)}",
          file=sys.stderr)
    print(f"[cohort] Spearman(draw_pert, pct_wt)  rho={rho_p:+.4f}  p={p_p:.3g}  n={len(scored)}",
          file=sys.stderr)

    print(scored.sort_values("pct_wt", ascending=False)
          [["condition_id", "gene", "pct_wt", "delta", "draw_pert"]]
          .to_string(index=False), file=sys.stderr)


if __name__ == "__main__":
    main()
