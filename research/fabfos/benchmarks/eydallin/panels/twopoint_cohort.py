#!/usr/bin/env python3
"""Cohort-wide two-point panel, scored on MAGNITUDE rather than direction.

`cohort_delta_panel.py` asked whether the universal-leak draw delta tracks the signed
phenotype and got rho=+0.21. Two things are different here. The probe grounds AT glycogen
(a real glucose -> glycogen measurement) instead of at OMEGA, and the score is
|log2FC| against |log2FC|.

The magnitude framing is forced, not chosen. Effective conductance is a non-decreasing
function of every edge conductance (Rayleigh), so a fold > 1 can only raise this readout
-- verified in `twopoint_panel.py`, where all eight fold-2.0 deltas are >= 0 and all eight
fold-0.5 deltas <= 0. A signed correlation against a phenotype that goes both ways is
therefore unanswerable by construction. What remains askable: does a clone that moves
glycogen FAR from wild-type, in either direction, sit on an edge that carries more of the
glucose -> glycogen current?

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/panels/twopoint_cohort.py
    ... --host e_coli_bw25113 --fold 0 \
        --clone-gpr data/fabfos/runs/eydallin_clones/gpr/lof/gpr_gem.parquet \
        --measured data/fabfos/benchmarks/eydallin_2007/Y/measured_glycogen.tsv

THE COHORT AND THE HOST ARE FLAGS, and their defaults are the 2010 ASKA run this script
was written for, so an unchanged command line still reproduces that output byte for byte.
The 2007 deletion arm is the same probe over a different cohort against a different host:
`--fold 0` deletes rather than doubles, and the output filename carries the host, which is
what keeps the two arms' results from landing on the same path.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr, pearsonr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                      # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--clone-gpr", type=Path, default=CLONE_GPR,
                   help="the cohort's own GPR table, a subset of the host's")
    p.add_argument("--measured", type=Path, default=MEASURED,
                   help="the digitised phenotype table, joined on `condition_id`")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}

    clone = pd.read_parquet(args.clone_gpr)
    clone = clone[clone.in_atom_universe & (clone.channel == "gem_gpr")]
    absent = sorted(set(clone.mnxr.astype(str)) - set(base_w))
    if absent:
        raise SystemExit(f"[2pt-cohort] {absent} named by the clone GPR but not host edges")

    genes = (clone.groupby("condition_id")
             .agg(gene=("feature_name", "first"),
                  rxns=("mnxr", lambda s: sorted(set(s.astype(str)))))
             .reset_index())
    print(f"[2pt-cohort] {len(genes)} conditions with >=1 in-universe host reaction",
          file=sys.stderr)

    def ieff(weights):
        g = graph_from_pairs(pairs, args.element, weights, ratios)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
        snk = Terminal.metabolite(g, GLYCOGEN_MNXM, label="glycogen")
        if src.missing or snk.missing:
            raise SystemExit("[2pt-cohort] terminal missing")
        return float(solve(g, src, snk).total)

    base = ieff(base_w)
    print(f"[2pt-cohort] base glucose->glycogen conductance = {base:.6f}", file=sys.stderr)

    rows = []
    for i, row in genes.iterrows():
        v = ieff(dict(base_w, **{r: base_w[r] * args.fold for r in row.rxns}))
        rows.append(dict(condition_id=row.condition_id, gene=row.gene,
                         n_rxn=len(row.rxns), rxns=",".join(row.rxns),
                         ieff_base=base, ieff_pert=v, delta=v - base))
        print(f"  {row.condition_id:22} {v:.6f}  {v - base:+.6f}", file=sys.stderr)

    df = pd.DataFrame(rows)
    meas = pd.read_csv(args.measured, sep="\t")
    df["k"] = df.condition_id.str.lower()
    meas["k"] = meas.condition_id.str.lower()
    df = df.merge(meas[["k", "pct_wt"]], on="k", how="left").drop(columns="k")

    unmatched = df[df.pct_wt.isna()]
    if len(unmatched):
        print(f"[2pt-cohort] no phenotype for {list(unmatched.condition_id)}", file=sys.stderr)
    df = df.dropna(subset=["pct_wt"]).copy()

    df["log2fc_meas"] = np.log2(df.pct_wt / 100.0)
    df["abs_log2fc_meas"] = df.log2fc_meas.abs()
    df["log2fc_ieff"] = np.log2(df.ieff_pert / df.ieff_base)
    df["direction"] = np.where(df.pct_wt >= 100, "excess", "deficient")

    out = args.out_dir / f"twopoint_cohort_{args.host}_fold{args.fold}_{args.element}.tsv"
    df.to_csv(out, sep="\t", index=False)
    print(f"\n[2pt-cohort] wrote {out}\n", file=sys.stderr)

    pd.set_option("display.width", 220)
    print(df.sort_values("log2fc_ieff", ascending=False)
          [["gene", "direction", "pct_wt", "log2fc_meas", "abs_log2fc_meas",
            "log2fc_ieff", "n_rxn"]]
          .to_string(index=False, formatters={"log2fc_meas": "{:+.3f}".format,
                                              "abs_log2fc_meas": "{:.3f}".format,
                                              "log2fc_ieff": "{:.5f}".format}),
          file=sys.stderr)

    def report(label, x, y, n=None):
        if len(x) < 3:
            print(f"\n{label}: n={len(x)}, too few", file=sys.stderr)
            return
        rs, ps = spearmanr(x, y)
        rp, pp = pearsonr(x, y)
        print(f"\n{label}  (n={len(x)})\n"
              f"   Spearman rho={rs:+.4f}  p={ps:.3g}\n"
              f"   Pearson    r={rp:+.4f}  p={pp:.3g}", file=sys.stderr)

    report("|log2FC| measured  vs  log2FC Ieff", df.abs_log2fc_meas, df.log2fc_ieff)
    report("SIGNED log2FC measured  vs  log2FC Ieff", df.log2fc_meas, df.log2fc_ieff)

    for d, sub in df.groupby("direction"):
        report(f"|log2FC| vs log2FC Ieff -- {d} only",
               sub.abs_log2fc_meas, sub.log2fc_ieff)

    report("n_rxn  vs  log2FC Ieff", df.n_rxn.astype(float), df.log2fc_ieff)
    report("n_rxn  vs  |log2FC| measured", df.n_rxn.astype(float), df.abs_log2fc_meas)


if __name__ == "__main__":
    main()
