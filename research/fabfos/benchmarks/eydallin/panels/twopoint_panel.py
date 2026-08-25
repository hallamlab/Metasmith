#!/usr/bin/env python3
"""Two-point glucose -> glycogen conductance under a per-gene conductance fold.

The delta panels (`delta_panel.py`, `cohort_delta_panel.py`, `ag1_delta_panel.py`) all
run `measure_leak`, which grounds at OMEGA and reads glycogen's own leak current --
glycogen is never a terminal there, so those numbers are not a glucose -> glycogen
measurement. This script grounds AT glycogen, which is the probe the leak sweep named as
the sanctioned alternative for a point-to-point question.

Genes resolve to reactions through the host GEM's own `feature_name` column, so a gene
outside the eydallin cohort (pfkA/pfkB as an external control) works the same way as one
inside it.

Read the monotonicity line at the bottom before interpreting any sign. Effective
conductance between two terminals is a non-decreasing function of every edge conductance
(Rayleigh), so a fold > 1 can only push this readout up -- a positive delta is not on its
own evidence the model predicted the phenotype's direction.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/twopoint_panel.py \
        --gene glgC --gene glgA --gene glgB --fold 2.0
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                      # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"

DEFAULT_GENES = ["glgC", "glgA", "ddg", "glgB", "glgP", "nagB", "pfkA", "pfkB"]


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--gene", action="append", default=None)
    p.add_argument("--fold", type=float, action="append", default=None,
                   help="repeatable; default 2.0 plus a 0.5 monotonicity probe")
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    genes = args.gene or DEFAULT_GENES
    folds = args.fold or [2.0, 0.5]
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}

    in_universe = host[host.in_atom_universe]
    targets = {}
    for g in genes:
        rxns = sorted(set(in_universe[in_universe.feature_name.astype(str) == g]
                          .mnxr.astype(str)))
        if not rxns:
            raise SystemExit(f"[2pt] {g}: no in-universe reaction in {args.host}'s GEM")
        targets[g] = rxns
        print(f"[2pt] {g}: {rxns}  ratios="
              f"{[round(ratios.get(r, float('nan')), 3) for r in rxns]}", file=sys.stderr)

    def ieff(weights, tag):
        gr = graph_from_pairs(pairs, args.element, weights, ratios)
        src = Terminal.metabolite(gr, SOURCE_MNXM, label="glucose")
        snk = Terminal.metabolite(gr, GLYCOGEN_MNXM, label="glycogen")
        if src.missing or snk.missing:
            raise SystemExit(f"[2pt] terminal missing on {tag}")
        s = solve(gr, src, snk)
        return float(s.total)

    base = ieff(base_w, "base")
    print(f"\n[2pt] base glucose->glycogen conductance = {base:.6f}\n", file=sys.stderr)

    meas = pd.read_csv(MEASURED, sep="\t")
    pct = dict(zip(meas.condition_id, meas.pct_wt))

    rows = []
    for g, rxns in targets.items():
        for f in folds:
            v = ieff(dict(base_w, **{r: base_w[r] * f for r in rxns}), f"{g} x{f}")
            rows.append(dict(gene=g, host=args.host, rxns=",".join(rxns), fold=f,
                             in_cohort=f"eydallin:{g}" in pct,
                             pct_wt=pct.get(f"eydallin:{g}"),
                             ieff_base=base, ieff_pert=v, delta=v - base,
                             rel=v / base - 1))

    df = pd.DataFrame(rows)
    out = args.out_dir / f"twopoint_{args.host}_{args.element}.tsv"
    df.to_csv(out, sep="\t", index=False)
    print(f"[2pt] wrote {out}\n", file=sys.stderr)

    pd.set_option("display.width", 220)
    for f in folds:
        sub = df[df.fold == f].sort_values("delta", ascending=False)
        print(f"--- fold {f} ---", file=sys.stderr)
        print(sub[["gene", "in_cohort", "pct_wt", "ieff_pert", "delta", "rel"]]
              .to_string(index=False, formatters={"ieff_pert": "{:.6f}".format,
                                                  "delta": "{:+.6f}".format,
                                                  "rel": "{:+.4%}".format}),
              file=sys.stderr)
        print(file=sys.stderr)

    up = df[df.fold > 1]
    dn = df[df.fold < 1]
    print(f"[2pt] monotonicity -- fold>1 deltas all >= 0: "
          f"{bool((up.delta >= 0).all())} ({len(up)} runs); "
          f"fold<1 deltas all <= 0: {bool((dn.delta <= 0).all())} ({len(dn)} runs)",
          file=sys.stderr)

    scored = df[(df.fold == 2.0) & df.pct_wt.notna()]
    if len(scored) >= 3:
        from scipy.stats import spearmanr
        rho, pv = spearmanr(scored.delta, scored.pct_wt)
        print(f"[2pt] Spearman(delta, pct_wt) rho={rho:+.4f} p={pv:.3g} n={len(scored)} "
              f"(cohort genes only)", file=sys.stderr)


if __name__ == "__main__":
    main()
