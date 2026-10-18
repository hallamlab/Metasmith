#!/usr/bin/env python3
"""Does reaction-hop distance from the perturbed reaction predict voltage/current shift?

Two solves (base, pert), universal ground, source D-glucose -- same setup delta_panel.py
uses, but this pulls the Solution object out directly (delta_panel.py only kept the
aggregated draw dict) so it can read per-metabolite VOLTAGE, not just current draw.

  * delta_draw[m]    = draw_pert[m] - draw_base[m]           (from measure_leak, current)
  * delta_voltage[m] = V_pert(m) - V_base(m)                 (current-weighted mean over
                       the metabolite's atoms, via Solution.voltage_metabolite)

Hop distance is REACTION distance from the perturbed reaction, not from glycogen: seed
the BFS at the perturbed MNXR itself (distance 0), then for every metabolite take the min
distance over its incident reactions. Same basis as reach_to_glycogen.py -- bake, host-
restricted, cofactor hubs barred (ATP/ADP/NAD(H)/CoA/... -- without barring them
everything is 2 hops from everything via the adenosine/nicotinamide skeleton).

Correlation is Spearman, not Pearson: the relationship (if any) is expected to be
monotonic-and-saturating (distance floors current/voltage shift at the numerical noise
level once you are topologically far enough), not linear.

    mamba run -n ecspr python main/benchmarks/eydallin/distance_vs_voltage.py \
        --gene glgC --rxn MNXR145050 --fold 2.0
"""
from __future__ import annotations

import argparse
import collections
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, attach_leak, solve, OMEGA                  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

ATOM_PAIRS = bake_pairs.atom_pairs()
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
OUT_DIR = Path(__file__).resolve().parents[1] / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"

COFACTOR_NAMES = [
    "ATP", "ADP", "AMP", "CoA", "acetyl-CoA", "malonyl-CoA",
    "NAD(+)", "NADH", "NADP(+)", "NADPH", "FAD", "FADH2", "Flavin", "Reduced flavin",
    "CO2", "hydrogencarbonate", "UDP", "CMP", "GDP", "UMP", "UTP", "GTP", "CTP",
    "dTTP", "dTDP", "dTMP", "S-adenosyl-L-methionine", "S-adenosyl-L-homocysteine",
    "glutathione", "adenosine 3',5'-bisphosphate",
]


def hop_distances(seed_rxns, element_pairs_C: pd.DataFrame, host_rxn: set, barred: set) -> dict:
    sub = element_pairs_C[element_pairs_C.rxn.isin(host_rxn)]
    m2r, r2m = collections.defaultdict(set), collections.defaultdict(set)
    for rx, t, h in sub.itertuples(index=False):
        for m in (t, h):
            if m in barred or m is None:
                continue
            m2r[m].add(rx)
            r2m[rx].add(m)
    dist = {r: 0 for r in seed_rxns if r in r2m}
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
    return dist, m2r, r2m


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--gene", required=True)
    p.add_argument("--rxn", action="append", required=True, metavar="MNXR")
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    args = p.parse_args()
    rxns = sorted(set(args.rxn))

    pairs = load_pairs(ATOM_PAIRS, element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}
    absent = [r for r in rxns if r not in base_w]
    if absent:
        raise SystemExit(f"[{args.gene}-dist] {absent} not in host GEM reaction set")
    pert_w = dict(base_w)
    for r in rxns:
        pert_w[r] = base_w[r] * args.fold

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

    dist, m2r, r2m = hop_distances(rxns, bake_c, set(base_w), barred)
    met_dist = {}
    for m, rs in m2r.items():
        ds = [dist[r] for r in rs if r in dist]
        if ds:
            met_dist[m] = min(ds)
    print(f"[{args.gene}-dist] hop distances resolved for {len(met_dist)} metabolites "
          f"(seed {rxns})", file=sys.stderr)

    def solve_(weights, tag):
        g = graph_from_pairs(pairs, args.element, weights, ratios)
        g2, leak_edges = attach_leak(g, None, leak=args.leak)
        src = Terminal.metabolite(g2, SOURCE_MNXM, label="source")
        if src.missing:
            raise SystemExit(f"[{args.gene}-dist] source {SOURCE_MNXM} absent")
        omega_t = Terminal.of_nodes("OMEGA", [OMEGA])
        sol = solve(g2, src, omega_t)
        print(f"[{args.gene}-dist] {tag}: {g.n:,} nodes / {g.m:,} edges, "
              f"converged={sol.converged}", file=sys.stderr)
        return g, sol

    g_base, sol_base = solve_(base_w, "base")
    g_pert, sol_pert = solve_(pert_w, "pert")

    mets = sorted(set(g_base.metabolites()) & set(g_pert.metabolites()))
    rows = []
    for m in mets:
        vb = sol_base.voltage_metabolite(m)["weighted_mean"]
        vp = sol_pert.voltage_metabolite(m)["weighted_mean"]
        if vb != vb or vp != vp:
            continue
        rows.append((m, vb, vp, vp - vb, met_dist.get(m)))
    df = pd.DataFrame(rows, columns=["mnxm", "v_base", "v_pert", "delta_v", "hop_dist"])
    out = OUT_DIR / f"{args.gene}_distance_vs_voltage_fold{args.fold}_{args.element}.parquet"
    df.to_parquet(out)
    print(f"[{args.gene}-dist] {len(df)} metabolites with finite voltage in both solves; "
          f"wrote {out}", file=sys.stderr)

    have_dist = df.dropna(subset=["hop_dist"])
    print(f"[{args.gene}-dist] {len(have_dist)}/{len(df)} also have a resolved hop distance "
          f"(rest: unreachable from the seed under the barred basis)", file=sys.stderr)

    for col in ("delta_v",):
        rho, pval = spearmanr(have_dist["hop_dist"], have_dist[col].abs())
        print(f"[{args.gene}-dist] Spearman(hop_dist, |{col}|) = {rho:.4f}  (p={pval:.3g}, "
              f"n={len(have_dist)})", file=sys.stderr)

    if GLYCOGEN_MNXM in set(df.mnxm):
        gly = df[df.mnxm == GLYCOGEN_MNXM].iloc[0]
        print(f"[{args.gene}-dist] glycogen: hop_dist={gly.hop_dist}, "
              f"delta_v={gly.delta_v:.6g}", file=sys.stderr)


if __name__ == "__main__":
    main()
