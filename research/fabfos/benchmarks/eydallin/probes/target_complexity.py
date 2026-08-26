#!/usr/bin/env python3
"""Is glycogen an unusually simple target, or is every target this simple?

`pathway_complexity.py` measures the levers between D-glucose and glycogen. That number
means nothing on its own -- a probe with six levers is a lot if the median target has two
and nothing if the median has sixty. So run the same reading against EVERY metabolite the
host's atom graph can reach from D-glucose and report glycogen's percentile.

The reading is `reaction_elasticities`: one solve gives each reaction's share of the
two-point measurement, the shares sum to 1, and `1 / sum(shares^2)` is the effective number
of reactions the probe can respond to at all. It is the closed form of a perturbation
sweep, so a whole-metabolome profile costs one solve per target rather than one per
(target, reaction).

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/target_complexity.py
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs, reaction_elasticities)
from ecspr.model.graph import Terminal, solve                       # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"    # D-glucose
GLYCOGEN_MNXM = "MNXM738130"

GLG_RXNS = {"MNXR145046", "MNXR145036", "MNXR145038", "MNXR145021", "MNXR145050"}

_G = {}


def _init(graph, mask):
    _G.update(graph=graph, mask=mask)


def _profile(mnxm) -> dict:
    g = _G["graph"]
    src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
    snk = Terminal.metabolite(g, mnxm)
    if snk.missing or (src.nodes & snk.nodes):
        return dict(mnxm=mnxm, ceff=np.nan)
    sol = solve(g, src, snk)
    if not sol.total > 0:
        return dict(mnxm=mnxm, ceff=0.0)
    eps = reaction_elasticities(g, sol)
    pos = eps.clip(lower=0)
    tot = float(pos.sum())
    share = (pos / tot).sort_values(ascending=False)
    v = share.to_numpy()
    return dict(mnxm=mnxm, ceff=float(sol.total),
                n_eff=float(1.0 / np.sum(v ** 2)),
                top1_share=float(v[0]), top3_share=float(v[:3].sum()),
                top1_rxn=str(share.index[0]),
                n_eps_gt_1e3=int((v > 1e-3).sum()),
                n_eps_gt_1e2=int((v > 1e-2).sum()),
                glg_share=float(share.reindex(list(GLG_RXNS)).fillna(0).sum()))


def _reaction_degree(graph) -> dict:
    prov = graph.meta["edge_reactions"]
    nodes = graph.nodes
    e2r = prov.groupby("edge").mnxr.apply(set)
    deg = {}
    for e, (a, b) in enumerate(graph.edges):
        ma, mb = nodes[a][0], nodes[b][0]
        if ma == mb:
            continue
        rs = e2r.get(e, ())
        deg.setdefault(ma, set()).update(rs)
        deg.setdefault(mb, set()).update(rs)
    return {m: len(v) for m, v in deg.items()}


def _hops(graph) -> dict:
    adj = {}
    nodes = graph.nodes
    for a, b in graph.edges:
        ma, mb = nodes[a][0], nodes[b][0]
        if ma == mb:
            continue
        adj.setdefault(ma, set()).add(mb)
        adj.setdefault(mb, set()).add(ma)
    d = {SOURCE_MNXM: 0}
    q = deque([SOURCE_MNXM])
    while q:
        u = q.popleft()
        for v in adj.get(u, ()):
            if v not in d:
                d[v] = d[u] + 1
                q.append(v)
    return d


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    g = graph_from_pairs(pairs, args.element, base_w, ratios, with_provenance=True)
    print(f"[targets] {g.meta['n_reactions_used']} reactions, {g.n} nodes, {g.m} edges",
          file=sys.stderr)

    hops = _hops(g)
    rdeg = _reaction_degree(g)
    targets = [m for m in g.metabolites() if m != SOURCE_MNXM]
    if args.limit:
        targets = targets[:args.limit]
    print(f"[targets] profiling {len(targets)} metabolites "
          f"({len(hops)} reachable from the source)", file=sys.stderr)

    _init(g, None)
    t0 = time.time()
    if args.workers > 1:
        import multiprocessing as mp
        with mp.get_context("fork").Pool(args.workers) as pool:
            rows = []
            for i, r in enumerate(pool.imap_unordered(_profile, targets, chunksize=16), 1):
                rows.append(r)
                if i % 400 == 0:
                    el = time.time() - t0
                    print(f"  {i}/{len(targets)}  {el:.0f}s  "
                          f"eta {el/i*(len(targets)-i):.0f}s", file=sys.stderr)
    else:
        rows = [_profile(m) for m in targets]
    print(f"[targets] {time.time() - t0:.0f}s", file=sys.stderr)

    df = pd.DataFrame(rows)
    df["hops"] = df.mnxm.map(hops)
    df["rxn_degree"] = df.mnxm.map(rdeg)
    if CHEM_PROP.exists():
        nm = pd.read_csv(CHEM_PROP, sep="\t", comment="#", header=None,
                         usecols=[0, 1], names=["mnxm", "name"]).drop_duplicates("mnxm")
        df = df.merge(nm, on="mnxm", how="left")
    out = args.out_dir / f"target_complexity_{args.host}_{args.element}.tsv"
    df.to_csv(out, sep="\t", index=False)
    print(f"[targets] wrote {out}", file=sys.stderr)

    live = df[df.ceff > 0].copy()
    gly = live[live.mnxm == GLYCOGEN_MNXM]
    print(f"\n{len(live)} of {len(df)} targets are reachable from D-glucose", file=sys.stderr)
    for col in ("n_eff", "top1_share", "n_eps_gt_1e2", "hops", "rxn_degree"):
        q = live[col].describe(percentiles=[.05, .25, .5, .75, .95])
        v = float(gly[col].iloc[0]) if len(gly) else float("nan")
        pct = float((live[col] < v).mean() * 100)
        print(f"\n{col}: glycogen = {v:.4g}  ({pct:.1f}th percentile)\n"
              f"  {q.to_dict()}", file=sys.stderr)

    print("\n=== the twenty simplest reachable targets (fewest levers) ===", file=sys.stderr)
    cols = ["mnxm", "name", "hops", "ceff", "n_eff", "top1_share", "top1_rxn"]
    cols = [c for c in cols if c in live.columns]
    print(live.nsmallest(20, "n_eff")[cols].to_string(index=False), file=sys.stderr)
    print("\n=== glycogen ===", file=sys.stderr)
    print(gly[cols + ["glg_share", "n_eps_gt_1e3"]].to_string(index=False), file=sys.stderr)


if __name__ == "__main__":
    main()
