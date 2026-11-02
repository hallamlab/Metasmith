#!/usr/bin/env python3
"""GOF delta panel on the ASKA host: ag1 vs ag1 + glgC / glgA / ddg.

The same two-solve conductance-fold model as `delta_panel.py`, moved onto the host the
screen was actually run in. `e_coli_ag1` is the strain the ASKA library lives in; the
clone GPR (`runs/eydallin_clones/gpr/gpr_gem.parquet`) is built against that host's model
(iECDH1ME8569_1439), so the two agree on reaction ids by construction. Earlier panels used
iML1515/`e_coli_k12`, which is a different reaction set.

Each of the three genes resolves to exactly one reaction the host already carries in the
atom universe, so overexpression is a fold on an existing edge -- no addition machinery.
glgC and glgA are the cohort's two strongest measured accumulators (453% and 328% of WT);
ddg is the unrelated-pathway control (150%).

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/ag1_delta_panel.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, measure_leak                               # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"

GENES = ["glgC", "glgA", "ddg"]


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--gene", action="append", default=None,
                   help=f"gene(s) to perturb; repeatable (default {'/'.join(GENES)})")
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    p.add_argument("--host", default="e_coli_ag1",
                   help="host GEM under runs/<host>/gpr/ (e_coli_k12 reproduces the "
                        "earlier panels' basis on the current bake)")
    args = p.parse_args()
    genes = args.gene or GENES
    args.out_dir.mkdir(parents=True, exist_ok=True)
    host_gem = RUNS / args.host / "gpr" / "gpr_gem.parquet"

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())

    host = pd.read_parquet(host_gem)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}
    print(f"[ag1] host {args.host} / {host.build_id.iloc[0]}: "
          f"{len(base_w):,} reactions", file=sys.stderr)

    clone = pd.read_parquet(CLONE_GPR)
    clone = clone[clone.in_atom_universe & (clone.channel == "gem_gpr")]
    targets = {}
    for g in genes:
        rows = clone[clone.condition_id == f"eydallin:{g}"]
        rxns = sorted(set(rows.mnxr.astype(str)))
        if not rxns:
            raise SystemExit(f"[ag1] no in-universe gem_gpr reaction for eydallin:{g}")
        absent = [r for r in rxns if r not in base_w]
        if absent:
            raise SystemExit(f"[ag1] {g}: {absent} not a host reaction -- an addition, "
                             f"not a fold; this script does not cover that")
        targets[g] = rxns
        print(f"[ag1] {g}: {rxns} weight 1.0 -> {args.fold}", file=sys.stderr)

    def solve_(weights, tag):
        g = graph_from_pairs(pairs, args.element, weights, ratios)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="source")
        if src.missing:
            raise SystemExit(f"[ag1] source {SOURCE_MNXM} absent from the built graph")
        r = measure_leak(g, src, [], leak=args.leak)
        print(f"[ag1] {tag}: {g.n:,} nodes / {g.m:,} edges from "
              f"{g.meta['n_reactions_used']:,} reactions (AAM gap {g.meta['n_aam_gap']:,})",
              file=sys.stderr)
        return r

    r_base = solve_(base_w, "base (ag1)")
    if GLYCOGEN_MNXM not in r_base["draw"]:
        raise SystemExit(f"[ag1] {GLYCOGEN_MNXM} (glycogen) never became a node in the base build")

    summary = []
    for g, rxns in targets.items():
        pert_w = dict(base_w)
        for r in rxns:
            pert_w[r] = base_w[r] * args.fold
        r_pert = solve_(pert_w, f"ag1 + {g}")

        shared = sorted(set(r_base["draw"]) & set(r_pert["draw"]))
        only_b = len(set(r_base["draw"]) - set(r_pert["draw"]))
        only_p = len(set(r_pert["draw"]) - set(r_base["draw"]))
        if only_b or only_p:
            print(f"[ag1] WARNING: {g} node sets differ -- {only_b} only base, "
                  f"{only_p} only pert", file=sys.stderr)

        panel = pd.DataFrame(
            [(m, r_base["draw"][m], r_pert["draw"][m], r_pert["draw"][m] - r_base["draw"][m])
             for m in shared],
            columns=["mnxm", "draw_base", "draw_pert", "delta"])
        panel.to_parquet(args.out_dir /
                         f"delta_panel_{args.host}_{g}_fold{args.fold}_{args.element}.parquet")

        gly = panel[panel.mnxm == GLYCOGEN_MNXM].iloc[0]
        rank = int((panel.delta.abs() >= abs(gly.delta)).sum())
        summary.append(dict(condition_id=f"eydallin:{g}", gene=g, host=args.host,
                            rxns=",".join(rxns),
                            fold=args.fold, n_metabolites=len(panel),
                            draw_base=gly.draw_base, draw_pert=gly.draw_pert,
                            delta=gly.delta, rank_abs_delta=rank,
                            rank_pct=rank / len(panel)))

    df = pd.DataFrame(summary)
    meas = pd.read_csv(MEASURED, sep="\t")
    df = df.merge(meas[["condition_id", "pct_wt", "nmol_glucose_per_mg_protein"]],
                  on="condition_id", how="left")
    out = args.out_dir / f"{args.host}_{'_'.join(genes)}_fold{args.fold}_{args.element}.tsv"
    df.to_csv(out, sep="\t", index=False)

    print(f"\n[ag1] wrote {out}", file=sys.stderr)
    print(df[["gene", "rxns", "pct_wt", "draw_base", "draw_pert", "delta",
              "rank_abs_delta", "n_metabolites"]].to_string(index=False), file=sys.stderr)

    ranked_meas = df.sort_values("pct_wt", ascending=False).gene.tolist()
    ranked_model = df.sort_values("delta", ascending=False).gene.tolist()
    print(f"\n[ag1] measured order (high glycogen first): {ranked_meas}", file=sys.stderr)
    print(f"[ag1] modelled order (high delta first):    {ranked_model}", file=sys.stderr)
    print(f"[ag1] orders agree: {ranked_meas == ranked_model}", file=sys.stderr)


if __name__ == "__main__":
    main()
