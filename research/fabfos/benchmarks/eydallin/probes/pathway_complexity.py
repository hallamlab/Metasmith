#!/usr/bin/env python3
"""How many levers are there between glucose and glycogen, and how much of the response is
the glycogen module?

`REPORT.md` says ECSPr finds the glg operon "tautologically" and nothing else. That is an
observation about the ranking. This asks the structural question underneath it, of the
network rather than of the phenotype: sweep EVERY reaction in the host background -- not
just the ones an Eydallin clone happens to carry -- and measure what a fold on it does to
the glucose -> glycogen conductance.

Two per-reaction numbers, both from the same probe the ASKA sweep uses (source D-glucose,
sink glycogen, element C, r7 bake):

* **elasticity** eps_r = dlnC/dln g_r, measured at a small fold. Effective conductance is
  homogeneous of degree one in the conductances, so sum_r eps_r = 1 EXACTLY on the
  symmetric network -- the elasticities are a partition of the response, and each one is
  the fraction of dissipated power that reaction carries. The diode smoothing makes the
  identity approximate rather than exact here, so the sum is reported as a check rather
  than assumed. A series chain of k equal steps gives every step 1/k; k parallel routes
  split one step's share k ways. Either way 1/sum(eps^2) is the effective number of levers
  the probe has.
* **knockout** C_eff with g_r = 0. A reaction whose removal sends it to zero is a cut of
  size one: on that step there is literally one pathway and no parallel route exists in
  this background at all.

`--channel denovo` reruns it against the de-novo background (5x the reactions), which is
the coverage control: if the extra reactions add no parallel route into glycogen, the
single-neck topology is the chemistry rather than a gap in the curated model.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/pathway_complexity.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs, reaction_currents)
from ecspr.model.graph import Terminal, solve                       # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
ASKA_GPR = ROOT / "data/fabfos/runs/aska/gpr"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"    # D-glucose
GLYCOGEN_MNXM = "MNXM738130"   # glycogen, BiGG side

_G = {}


def _init(pairs, ratios, base_w, element):
    _G.update(pairs=pairs, ratios=ratios, base_w=base_w, element=element)


def _ieff(weights) -> float:
    g = graph_from_pairs(_G["pairs"], _G["element"], weights, _G["ratios"])
    src = Terminal.metabolite(g, SOURCE_MNXM)
    snk = Terminal.metabolite(g, GLYCOGEN_MNXM)
    if src.missing or snk.missing:
        return 0.0
    return float(solve(g, src, snk).total)


def _one(job) -> dict:
    mnxr, fold = job
    base_w = _G["base_w"]
    up = _ieff(dict(base_w, **{mnxr: base_w[mnxr] * fold}))
    ko = _ieff({k: v for k, v in base_w.items() if k != mnxr})
    return dict(mnxr=mnxr, ieff_up=up, ieff_ko=ko)


def _background(channel: str, host: str) -> tuple:
    if channel == "gem":
        df = pd.read_parquet(RUNS / host / "gpr" / "gpr_gem.parquet")
        names = df.groupby(df.mnxr.astype(str)).agg(
            genes=("feature_name", lambda s: ",".join(sorted({x for x in s if x}))),
            rxn_name=("evidence_name", "first"))
    else:
        df = pd.read_parquet(ASKA_GPR / "gpr_denovo.parquet")
        names = df.groupby(df.mnxr.astype(str)).agg(
            genes=("feature_name", lambda s: ",".join(sorted({x for x in s if x}))),
            rxn_name=("evidence_name", "first"))
    return {m: 1.0 for m in df.mnxr.dropna().astype(str).unique()}, names


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--channel", default="gem", choices=("gem", "denovo"))
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--fold", type=float, default=1.01,
                   help="fold for the elasticity estimate; small is the point")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", type=int, default=0, help="debug: first N reactions only")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    base_w, names = _background(args.channel, args.host)
    print(f"[complexity] {args.channel}: {len(base_w)} background reactions", file=sys.stderr)

    _init(pairs, ratios, base_w, args.element)

    g = graph_from_pairs(pairs, args.element, base_w, ratios, with_provenance=True)
    src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
    snk = Terminal.metabolite(g, GLYCOGEN_MNXM, label="glycogen")
    if src.missing or snk.missing:
        raise SystemExit("[complexity] terminal missing from the graph")
    sol = solve(g, src, snk)
    base = float(sol.total)
    print(f"[complexity] base conductance {base:.6f}  "
          f"({g.meta['n_reactions_used']} reactions in the atom universe, "
          f"{g.n} nodes, {g.m} edges)", file=sys.stderr)
    cur = reaction_currents(g, sol)

    used = sorted(set(g.meta["edge_reactions"].mnxr))
    if args.limit:
        used = used[:args.limit]
    print(f"[complexity] sweeping {len(used)} reactions x 2 solves", file=sys.stderr)

    t0 = time.time()
    jobs = [(m, args.fold) for m in used]
    if args.workers > 1:
        import multiprocessing as mp
        with mp.get_context("fork").Pool(args.workers) as pool:
            rows = []
            for i, r in enumerate(pool.imap_unordered(_one, jobs, chunksize=8), 1):
                rows.append(r)
                if i % 200 == 0:
                    el = time.time() - t0
                    print(f"  {i}/{len(jobs)}  {el:.0f}s  eta {el/i*(len(jobs)-i):.0f}s",
                          file=sys.stderr)
    else:
        rows = [_one(j) for j in jobs]
    print(f"[complexity] swept in {time.time() - t0:.0f}s", file=sys.stderr)

    df = pd.DataFrame(rows)
    df["current"] = df.mnxr.map(cur).fillna(0.0)
    df["ieff_base"] = base
    df["elasticity"] = np.log(df.ieff_up / base) / np.log(args.fold)
    df["log2fc_up"] = np.log2(df.ieff_up / base)
    df["ko_frac"] = df.ieff_ko / base
    df["disconnects"] = df.ieff_ko <= 0.0
    df = df.join(names, on="mnxr")
    df = df.sort_values("elasticity", ascending=False).reset_index(drop=True)
    df["rank"] = np.arange(1, len(df) + 1)

    out = args.out_dir / f"pathway_complexity_{args.channel}_{args.host}_{args.element}.tsv"
    df.to_csv(out, sep="\t", index=False)
    print(f"[complexity] wrote {out}", file=sys.stderr)

    eps = df.elasticity.clip(lower=0).to_numpy()
    tot = float(eps.sum())
    share = eps / tot if tot > 0 else eps
    n_eff = float(1.0 / np.sum(share ** 2)) if tot > 0 else 0.0
    cuts = df[df.disconnects]
    summary = dict(
        channel=args.channel, host=args.host, element=args.element, fold=args.fold,
        base_conductance=base,
        n_background_reactions=len(base_w),
        n_atom_universe=int(g.meta["n_reactions_used"]),
        n_swept=len(df),
        n_carrying_current=int((df.current > 0).sum()),
        n_elasticity_gt_1e_6=int((df.elasticity > 1e-6).sum()),
        n_elasticity_gt_1e_3=int((df.elasticity > 1e-3).sum()),
        n_elasticity_gt_1e_2=int((df.elasticity > 1e-2).sum()),
        sum_elasticity=tot,
        effective_n_levers=n_eff,
        top1_share=float(share[0]) if len(share) else 0.0,
        top5_share=float(share[:5].sum()),
        top10_share=float(share[:10].sum()),
        top25_share=float(share[:25].sum()),
        n_single_reaction_cuts=int(len(cuts)),
        single_reaction_cuts=[dict(mnxr=r.mnxr, genes=r.genes, rxn_name=r.rxn_name,
                                   elasticity=float(r.elasticity))
                              for r in cuts.itertuples()],
    )
    with open(args.out_dir / f"pathway_complexity_{args.channel}_{args.host}_"
                             f"{args.element}.json", "w") as fh:
        json.dump(summary, fh, indent=2, default=str)

    pd.set_option("display.width", 220)
    print("\n=== top 25 levers between D-glucose and glycogen ===", file=sys.stderr)
    print(df.head(25)[["rank", "mnxr", "genes", "rxn_name", "current", "elasticity",
                       "ko_frac", "disconnects"]]
          .to_string(index=False, formatters={"elasticity": "{:.5f}".format,
                                              "current": "{:.4f}".format,
                                              "ko_frac": "{:.4f}".format}),
          file=sys.stderr)
    print("\n=== summary ===", file=sys.stderr)
    print(json.dumps({k: v for k, v in summary.items() if k != "single_reaction_cuts"},
                     indent=2, default=str), file=sys.stderr)
    if len(cuts):
        print(f"\n{len(cuts)} reaction(s) whose removal disconnects glucose from glycogen:",
              file=sys.stderr)
        print(cuts[["mnxr", "genes", "rxn_name", "elasticity"]].to_string(index=False),
              file=sys.stderr)


if __name__ == "__main__":
    main()
