#!/usr/bin/env python3
"""How many independent reaction routes are there from D-glucose to glycogen?

An exact answer rather than an inference from a ranking. Model the atom graph as a
bipartite flow network -- metabolites at infinite capacity, each reaction as a node of
capacity one -- and the minimum cut is, by Menger, both the smallest set of reactions whose
deletion disconnects the two metabolites and the largest number of reaction-disjoint routes
between them. Naming the cut names every route.

Run it on both channels. The de-novo background carries five times the reactions of the
curated one, so if the cut does not grow there, the neck is the chemistry rather than a
gap in the curated model.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/glycogen_cut.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs)
from ecspr.model.graph import Terminal, solve                       # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
ASKA_GPR = ROOT / "data/fabfos/runs/aska/gpr"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
INF = float("inf")


def _triples(graph):
    nodes = graph.nodes
    e2r = graph.meta["edge_reactions"].groupby("edge").mnxr.apply(set)
    out = set()
    for e, (a, b) in enumerate(graph.edges):
        ma, mb = nodes[a][0], nodes[b][0]
        if ma == mb:
            continue
        for r in e2r.get(e, ()):
            out.add((ma, mb, r))
    return out


def _route_usage(graph, sink) -> pd.Series:
    # Net current into ``sink`` per reaction, from the base solve. Sums to the injected 1.0.
    #
    # Which of the routes the cut names the probe actually USES -- and in which direction it
    # uses them, since a degradative enzyme carrying most of the arriving carbon means the
    # model is running it backwards to synthesise.
    sol = solve(graph, Terminal.metabolite(graph, SOURCE_MNXM),
                Terminal.metabolite(graph, sink))
    if not sol.total > 0:
        return pd.Series(dtype=float)
    oe, cur = sol.edge_currents()
    nodes = graph.nodes
    shares = graph.meta["edge_reactions"].groupby("edge").apply(
        lambda d: dict(zip(d.mnxr, d.gp)), include_groups=False).to_dict()
    net = {}
    for k, e in enumerate(oe):
        a, b = graph.edges[e]
        ma, mb = nodes[a][0], nodes[b][0]
        if (ma == sink) == (mb == sink):
            continue
        i = cur[k] if mb == sink else -cur[k]
        sh = shares.get(e, {})
        tot = sum(sh.values()) or 1.0
        for r, gp in sh.items():
            net[r] = net.get(r, 0.0) + i * gp / tot
    return pd.Series(net).sort_values(ascending=False)


def _min_reaction_cut(triples, source, sink):
    import networkx as nx
    D = nx.DiGraph()
    for ma, mb, r in triples:
        for x, y in ((ma, mb), (mb, ma)):
            D.add_edge(f"m_out::{x}", f"r_in::{r}", capacity=INF)
            D.add_edge(f"r_out::{r}", f"m_in::{y}", capacity=INF)
    for r in {r for _, _, r in triples}:
        D.add_edge(f"r_in::{r}", f"r_out::{r}", capacity=1.0)
    for m in {x for a, b, _ in triples for x in (a, b)}:
        D.add_edge(f"m_in::{m}", f"m_out::{m}", capacity=INF)
    s, t = f"m_out::{source}", f"m_in::{sink}"
    if s not in D or t not in D:
        return None, []
    value, (reach, _) = nx.minimum_cut(D, s, t)
    cut = sorted({u.split("::", 1)[1] for u in reach
                  for v in D[u] if D[u][v]["capacity"] < INF and v not in reach})
    return value, cut


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--sink", default=GLYCOGEN_MNXM)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    names = {}
    if CHEM_PROP.exists():
        names = pd.read_csv(CHEM_PROP, sep="\t", comment="#", header=None,
                            usecols=[0, 1], names=["mnxm", "name"]) \
                  .drop_duplicates("mnxm").set_index("mnxm").name.to_dict()

    report = {}
    for channel in ("gem", "denovo"):
        if channel == "gem":
            gpr = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
        else:
            gpr = pd.read_parquet(ASKA_GPR / "gpr_denovo.parquet")
        gene = gpr.groupby(gpr.mnxr.astype(str)).feature_name.apply(
            lambda s: ",".join(sorted({x for x in s if isinstance(x, str) and x})[:8]))
        rname = gpr.groupby(gpr.mnxr.astype(str)).evidence_name.first() \
            if "evidence_name" in gpr.columns else {}

        w = {m: 1.0 for m in gpr.mnxr.dropna().astype(str).unique()}
        g = graph_from_pairs(pairs, args.element, w, ratios, with_provenance=True)
        tri = _triples(g)
        print(f"\n########## {channel}: {len(w)} background reactions -> "
              f"{g.meta['n_reactions_used']} in the atom universe, "
              f"{g.n} nodes, {g.m} edges", file=sys.stderr)

        inc = sorted({r for a, b, r in tri if args.sink in (a, b)})
        partners = sorted({x for a, b, r in tri for x in (a, b)
                           if args.sink in (a, b) and x != args.sink})
        print(f"reactions incident to the sink: {len(inc)}", file=sys.stderr)
        for r in inc:
            print(f"    {r}  {gene.get(r, ''):24} {rname.get(r, '') if len(rname) else ''}",
                  file=sys.stderr)
        print("partner metabolites: "
              + ", ".join(f"{m} ({names.get(m, '?')})" for m in partners), file=sys.stderr)

        value, cut = _min_reaction_cut(tri, SOURCE_MNXM, args.sink)
        print(f"MINIMUM REACTION CUT = {value}", file=sys.stderr)
        for r in cut:
            print(f"    {r}  {gene.get(r, ''):24} {rname.get(r, '') if len(rname) else ''}",
                  file=sys.stderr)

        use = _route_usage(g, args.sink)
        print("share of the arriving carbon each route carries (injected = 1.0):",
              file=sys.stderr)
        for r, v in use.items():
            print(f"    {v:+.4f}  {r}  {gene.get(r, ''):24} "
                  f"{rname.get(r, '') if len(rname) else ''}", file=sys.stderr)

        report[channel] = dict(
            n_background=len(w), n_atom_universe=int(g.meta["n_reactions_used"]),
            n_incident=len(inc),
            incident=[dict(mnxr=r, genes=gene.get(r, "")) for r in inc],
            partners=[dict(mnxm=m, name=names.get(m, "")) for m in partners],
            min_reaction_cut=value,
            cut=[dict(mnxr=r, genes=gene.get(r, "")) for r in cut],
            route_usage=[dict(mnxr=r, genes=gene.get(r, ""), share=float(v))
                         for r, v in use.items()])

    out = args.out_dir / f"glycogen_cut_{args.host}_{args.element}.json"
    with open(out, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\n[cut] wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
