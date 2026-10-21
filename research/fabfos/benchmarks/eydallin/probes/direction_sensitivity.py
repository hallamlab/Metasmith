#!/usr/bin/env python3
"""What does the probe look like if the direction ensemble stops abstaining on the polymer?

Most of the carbon this probe delivers to glycogen arrives by running GLYCOGEN
PHOSPHORYLASE backwards -- MetaNetX writes `MNXR145036` as G1P -> glycogen, the direction
ensemble has zero votes on it and falls to ratio 1.0, and a symmetric edge is a free
synthesis route. `glycogen_cut.py` measures the share. That abstention is the polymer gap:
MetaNetX models glycogen as a fixed-formula molecule rather than a chain increment, so the
reaction cannot be balanced and the thermo members have nothing to vote with.

This supplies the direction the ensemble does not have and re-measures under it. `tau` is
the ratio put on the phosphorylase reactions: 1.0 is the abstention the bake ships, and
larger values push the enzyme toward degradation, which is the direction E. coli runs it.
The point is the SHAPE OF THE CURVE, not any one tau -- a knob turned until a number agrees
proves nothing, so every tau is reported and the readouts are the ones that would change if
the gap were the thing limiting this benchmark: which route carries the arriving carbon,
and where the glycogen-module genes sit in the elasticity ranking.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/direction_sensitivity.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs, reaction_elasticities)
from ecspr.model.graph import Terminal, solve                       # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glycogen"))
import bake_pairs                                                   # noqa: E402
from glycogen_cut import _route_usage                                # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"

# Written G1P -> glycogen / G1P -> branched glycogen, both glgP/malP, both at an explicit
# ratio of 1.0 from zero ensemble votes.
PHOSPHORYLASE = ("MNXR145036", "MNXR145038")
WATCH = {"MNXR145036": "glgP/malP phosphorylase", "MNXR145046": "glgA synthase",
         "MNXR145050": "glgC ADP-glucose", "MNXR145021": "glgB/glgX branching",
         "MNXR198783": "agp glucose-1-phosphatase", "MNXR145632": "malP maltopentaose"}


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--taus", type=float, nargs="+",
                   default=[1.0, 3.0, 10.0, 100.0, 1e3, 1e6])
    p.add_argument("--ratio-cap", type=float, default=None,
                   help="bound |log10 direction ratio| at this many decades before the "
                        "graph is built (ecspr.model.build.cap_direction_ratios). The "
                        "shipped table spans 28.7 decades over a host's reactions and "
                        "saturates by 6; the arm is tagged so a capped run cannot be "
                        "mistaken for an uncapped one")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    base_ratios = load_direction_ratios(bake_pairs.direction_ratios(),
                                        cap=args.ratio_cap)
    host = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    gene = host.groupby(host.mnxr.astype(str)).feature_name.apply(
        lambda s: ",".join(sorted({x for x in s if isinstance(x, str) and x})))

    rows, routes = [], []
    for tau in args.taus:
        ratios = dict(base_ratios, **{r: tau for r in PHOSPHORYLASE})
        g = graph_from_pairs(pairs, args.element, base_w, ratios, with_provenance=True)
        sol = solve(g, Terminal.metabolite(g, SOURCE_MNXM),
                    Terminal.metabolite(g, GLYCOGEN_MNXM))
        eps = reaction_elasticities(g, sol)
        rank = {r: i + 1 for i, r in enumerate(eps.index)}
        use = _route_usage(g, GLYCOGEN_MNXM)
        row = dict(tau=tau, ceff=float(sol.total))
        for r, label in WATCH.items():
            row[f"eps:{label}"] = float(eps.get(r, 0.0))
            row[f"rank:{label}"] = rank.get(r, -1)
        pos = eps.clip(lower=0)
        v = np.sort((pos / pos.sum()).to_numpy())[::-1]
        row["n_eff"] = float(1.0 / np.sum(v ** 2))
        rows.append(row)
        for r, share in use.items():
            routes.append(dict(tau=tau, mnxr=r, genes=gene.get(r, ""), share=float(share)))
        print(f"[dir] tau={tau:<10g} C_eff={sol.total:8.4f}  routes: "
              + "  ".join(f"{gene.get(r, r)}={share:+.3f}" for r, share in use.items()),
              file=sys.stderr)

    df = pd.DataFrame(rows)
    rt = pd.DataFrame(routes)
    cap = f"_cap{args.ratio_cap:g}" if args.ratio_cap is not None else ""
    out = args.out_dir / f"direction_sensitivity_{args.host}_{args.element}{cap}.tsv"
    df.to_csv(out, sep="\t", index=False)
    rt.to_csv(out.with_name(out.stem + "_routes.tsv"), sep="\t", index=False)
    print(f"\n[dir] wrote {out} (+ _routes.tsv)", file=sys.stderr)

    pd.set_option("display.width", 240)
    print("\n=== elasticity of each watched reaction, against tau ===", file=sys.stderr)
    print(df[["tau", "ceff", "n_eff"] + [c for c in df.columns if c.startswith("eps:")]]
          .to_string(index=False, float_format=lambda v: f"{v:.5f}"), file=sys.stderr)
    print("\n=== its rank among all 1,553 reactions ===", file=sys.stderr)
    print(df[["tau"] + [c for c in df.columns if c.startswith("rank:")]]
          .to_string(index=False), file=sys.stderr)


if __name__ == "__main__":
    main()
