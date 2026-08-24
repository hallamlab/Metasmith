#!/usr/bin/env python3
"""Sweep universal-leak conductance to find a regime where potential-drop-from-source
tracks reaction-hop distance instead of saturating.

`attach_leak`'s uniform per-node leak conductance `g` turns the network into a grounded/
regularized Laplacian solve `(L + g*I)v = I*e_s` -- the same linear system behind
personalized PageRank / random-walk-with-restart, with `g` playing the restart-rate role.
At g=1e-6 (what we'd been using) g is ~1e6x smaller than typical internal edge
conductances, so current wanders the whole low-resistance mesh before ever paying the
leak tax, and almost every well-connected node ends up within a hair of source
potential -- the saturation this sweep exists to escape.

For each leak value: one base solve (host GEM, no perturbation, source=D-glucose,
universal ground), read V(source) and V(m) for every metabolite, compute
drop = V(source) - V(m), and correlate drop against reaction-hop minpath from glucose
(same barred-cofactor BFS basis as voltage_vs_minpath.py). Reports Spearman rho/p per
leak plus saturation diagnostics at both ends:
  * frac_near_zero -- fraction of metabolites with drop within 1% of the max drop's
    range of zero -- i.e. still pinned near source potential (the low-leak failure mode)
  * frac_near_max  -- fraction within 1% of the max observed drop -- i.e. collapsed to
    "everyone but the source's immediate neighbors is maximally far" (the high-leak
    failure mode, current short-circuits to ground before it can spread)

    mamba run -n ecspr python main/benchmarks/eydallin/leak_sweep.py
"""
from __future__ import annotations

import argparse
import collections
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, attach_leak, solve, OMEGA                  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bake_pairs  # noqa: E402

ATOM_PAIRS = bake_pairs.atom_pairs()
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
OUT_DIR = Path(__file__).resolve().parent / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
ELEMENT = "C"
LEAK_VALUES = [10.0 ** k for k in range(-8, 5)]

COFACTOR_NAMES = [
    "ATP", "ADP", "AMP", "CoA", "acetyl-CoA", "malonyl-CoA",
    "NAD(+)", "NADH", "NADP(+)", "NADPH", "FAD", "FADH2", "Flavin", "Reduced flavin",
    "CO2", "hydrogencarbonate", "UDP", "CMP", "GDP", "UMP", "UTP", "GTP", "CTP",
    "dTTP", "dTDP", "dTMP", "S-adenosyl-L-methionine", "S-adenosyl-L-homocysteine",
    "glutathione", "adenosine 3',5'-bisphosphate",
]


def hop_distances_from_source(base_w: set) -> dict:
    v = pd.read_parquet(BAKE / "vocab.parquet")
    sym = v[v.kind == "met"].set_index("code").symbol.to_dict()
    rsym = v[v.kind == "rxn"].set_index("code").symbol.to_dict()
    ap = pd.read_parquet(BAKE / "atom_pairs.parquet")
    bake_c = (ap[ap.element == 0][["rxn", "tail_met", "head_met"]]
              .assign(rxn=lambda d: d.rxn.map(rsym), tail_met=lambda d: d.tail_met.map(sym),
                      head_met=lambda d: d.head_met.map(sym)).drop_duplicates())
    cp = pd.read_csv(CHEM_PROP, sep="\t", comment="#", header=None, usecols=[0, 1],
                     names=["id", "name"], dtype=str, low_memory=False)
    name_to_id = dict(zip(cp["name"], cp["id"]))
    barred = {name_to_id[n] for n in COFACTOR_NAMES if n in name_to_id}

    sub = bake_c[bake_c.rxn.isin(base_w)]
    m2r, r2m = collections.defaultdict(set), collections.defaultdict(set)
    for rx, t, h in sub.itertuples(index=False):
        for m in (t, h):
            if m in barred or m is None:
                continue
            m2r[m].add(rx)
            r2m[rx].add(m)
    dist = {r: 0 for r in m2r.get(SOURCE_MNXM, ())}
    frontier = set(dist)
    d = 0
    while frontier:
        d += 1
        nxt = set()
        for r in frontier:
            for m in r2m[r]:
                for r2 in m2r[m]:
                    if r2 not in dist:
                        dist[r2] = d
                        nxt.add(r2)
        frontier = nxt
    met_dist = {}
    for m, rs in m2r.items():
        ds = [dist[r] for r in rs if r in dist]
        if ds:
            met_dist[m] = min(ds)
    return met_dist


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--leaks", type=float, nargs="+", default=None,
                   help="override the default log sweep with specific leak values, "
                        "e.g. --leaks 1.0")
    args = p.parse_args()
    leak_values = args.leaks if args.leaks else LEAK_VALUES

    pairs = load_pairs(ATOM_PAIRS, element=ELEMENT)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}

    met_dist = hop_distances_from_source(base_w)
    print(f"[sweep] hop distance resolved for {len(met_dist)} metabolites from "
          f"{SOURCE_MNXM}", file=sys.stderr)

    g = graph_from_pairs(pairs, ELEMENT, base_w, ratios)
    print(f"[sweep] base graph: {g.n:,} nodes / {g.m:,} edges", file=sys.stderr)

    rows = []
    per_leak_panels = {}
    for leak in leak_values:
        g2, _ = attach_leak(g, None, leak=leak)
        src = Terminal.metabolite(g2, SOURCE_MNXM, label="source")
        if src.missing:
            raise SystemExit(f"[sweep] source {SOURCE_MNXM} absent from built graph")
        omega_t = Terminal.of_nodes("OMEGA", [OMEGA])
        sol = solve(g2, src, omega_t)

        v_source = sol.voltage_metabolite(SOURCE_MNXM)["weighted_mean"]
        panel = []
        for m in g.metabolites():
            vinfo = sol.voltage_metabolite(m)
            vm = vinfo["weighted_mean"]
            if vm != vm:
                continue
            panel.append((m, vm, v_source - vm, met_dist.get(m)))
        pdf = pd.DataFrame(panel, columns=["mnxm", "voltage", "drop", "hop_dist"])
        per_leak_panels[leak] = pdf
        panel_out = OUT_DIR / f"drop_vs_minpath_leak{leak:.0e}.parquet"
        pdf.to_parquet(panel_out)

        have = pdf.dropna(subset=["hop_dist"])
        rho, pval = spearmanr(have.hop_dist, have["drop"])

        max_drop = pdf["drop"].max()
        min_drop = pdf["drop"].min()
        rng = max_drop - min_drop if max_drop > min_drop else np.nan
        frac_near_zero = float((pdf["drop"] < min_drop + 0.01 * rng).mean()) if rng == rng else np.nan
        frac_near_max = float((pdf["drop"] > max_drop - 0.01 * rng).mean()) if rng == rng else np.nan
        cv_drop = float(pdf["drop"].std() / pdf["drop"].mean()) if pdf["drop"].mean() else np.nan

        rows.append(dict(
            leak=leak, rho=rho, pval=pval, n=len(have),
            v_source=v_source, min_drop=min_drop, max_drop=max_drop,
            median_drop=float(pdf["drop"].median()), cv_drop=cv_drop,
            frac_near_zero=frac_near_zero, frac_near_max=frac_near_max,
            converged=bool(sol.converged),
        ))
        print(f"[sweep] leak={leak:.1e}  rho={rho:+.4f} (p={pval:.2g}, n={len(have)})  "
              f"drop range [{min_drop:.4g}, {max_drop:.4g}]  "
              f"near0={frac_near_zero:.2%}  near_max={frac_near_max:.2%}  "
              f"converged={sol.converged}", file=sys.stderr)

    df = pd.DataFrame(rows)
    if args.leaks is None:
        out = OUT_DIR / "leak_sweep.parquet"
        df.to_parquet(out)
        print(f"[sweep] wrote {out}", file=sys.stderr)

        best = df.loc[df.rho.idxmax()]
        print(f"[sweep] best by rho: leak={best.leak:.1e}  rho={best.rho:+.4f} "
              f"(p={best.pval:.2g})", file=sys.stderr)
    print(f"[sweep] per-leak panels written to {OUT_DIR}/drop_vs_minpath_leak*.parquet",
          file=sys.stderr)


if __name__ == "__main__":
    main()
