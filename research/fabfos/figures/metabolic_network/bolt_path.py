#!/usr/bin/env python3
"""Trace the emergent high-current 'bolt' from glucose to DHAP.

The figure (network_figure.py --edge-weight current) weights every drawn edge by its
share of the unit glucose->DHAP flow, so the chain of strongest corridors reads as a
lightning bolt. This prints that chain explicitly: the WIDEST path (the route that
maximizes the minimum per-edge quota, a max-bottleneck Dijkstra over the projected
met-met graph) plus a greedy deepest-descent alternative, with per-edge quotas.

Run:
    mamba run -n figure-net python research/fabfos/figures/metabolic_network/bolt_path.py
"""
from __future__ import annotations

import heapq
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.csgraph import shortest_path

ROOT = Path("/home/tony/agentic_workspace/projects/metasmith/fabfos/fig-networks")
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/figures/metabolic_network"))

import network_figure as nf                               # noqa: E402

HOST = "e_coli_epi300/gpr/gpr_gem.parquet"


def main():
    vocab = nf.load_vocab()
    met_sym, met_code = vocab["met"]
    rxn_sym, _ = vocab["rxn"]
    el_code = vocab["element"][1]["C"]

    codes = nf.host_rxn_codes(HOST, vocab)
    p = pd.read_parquet(nf.BAKE / "atom_pairs.parquet")
    p = p[(p.element == el_code) & (p.rxn.isin(codes))].reset_index(drop=True)

    # drawn set + layout, same as the figure (paths mode, absolute)
    nodes = set()
    for row in p.itertuples():
        nodes.add((int(row.tail_met), int(row.tail_rank)))
        nodes.add((int(row.head_met), int(row.head_rank)))
    nodes = sorted(nodes)
    idx = {n: i for i, n in enumerate(nodes)}
    rr, cc = [], []
    for row in p.itertuples():
        a = idx[(int(row.tail_met), int(row.tail_rank))]
        b = idx[(int(row.head_met), int(row.head_rank))]
        rr += [a, b]
        cc += [b, a]
    A = sp.coo_matrix((np.ones(len(rr)), (rr, cc)), shape=(len(nodes), len(nodes))).tocsr()

    rxn_atoms, met_atoms = {}, {}
    for row in p.itertuples():
        rxn_atoms.setdefault(int(row.rxn), set()).add(idx[(int(row.tail_met), int(row.tail_rank))])
        rxn_atoms.setdefault(int(row.rxn), set()).add(idx[(int(row.head_met), int(row.head_rank))])
        met_atoms.setdefault(int(row.tail_met), set())
        met_atoms.setdefault(int(row.head_met), set())
    for n_, i in idx.items():
        met_atoms.setdefault(n_[0], set()).add(i)

    GA = sorted(met_atoms.get(met_code[nf.GLUCOSE], ()))
    DA = sorted(met_atoms.get(met_code[nf.DHAP], ()))
    dG = shortest_path(A, method="D", directed=False, indices=GA, unweighted=True).min(axis=0)
    dD = shortest_path(A, method="D", directed=False, indices=DA, unweighted=True).min(axis=0)
    Lmin = int(dG[DA].min())

    rxn_on_path, met_list = set(), []
    for r, ats in rxn_atoms.items():
        a = np.array(sorted(ats))
        if (dG[a] < np.inf).any() and (dD[a] < np.inf).any():
            rxn_on_path.add(r)
    for m, ats in met_atoms.items():
        a = np.array(sorted(ats))
        if (dG[a] < np.inf).any() and (dD[a] < np.inf).any():
            met_list.append((m, float(dG[a].min()), float(dD[a].min())))

    g_all, sol = nf._build_solve(codes, p, rxn_sym, met_sym)
    pair_cur = nf._pair_currents(g_all, sol)
    laid = nf._mean_hop_excess(g_all, sol, met_list, met_sym, Lmin)
    laid[met_code[nf.GLUCOSE]] = (0.0, 0.0, 0.0)
    laid[met_code[nf.DHAP]] = (1.0, 0.0, 0.0)
    kept = {m: p_ for m, p_ in laid.items() if p_[1] >= 0.0}
    x_met = {m: p_[0] for m, p_ in kept.items()}

    pair_count = {}
    for row in p.itertuples():
        r = int(row.rxn)
        if r not in rxn_on_path:
            continue
        a, b = int(row.tail_met), int(row.head_met)
        if a == b or a not in x_met or b not in x_met:
            continue
        key = (a, b) if a < b else (b, a)
        pair_count[key] = pair_count.get(key, 0) + 1
    edges = sorted(pair_count)
    flows = np.array([pair_cur.get((A, B) if A < B else (B, A), 0.0)
                      for a, b in edges
                      for A, B in [(met_sym[a], met_sym[b])]])

    names = pd.read_parquet(nf.NAME_TABLE).drop_duplicates("mnxm")
    nm = dict(zip(names.mnxm, names.name))

    def label(code):
        sym = met_sym[code]
        return f"{sym} {nm.get(sym, sym)}"

    g_code, d_code = met_code[nf.GLUCOSE], met_code[nf.DHAP]

    adj = defaultdict(list)
    for (a, b), c in zip(edges, flows):
        if c <= 0.0:
            continue
        adj[a].append((b, c))
        adj[b].append((a, c))

    # widest path: maximize the minimum quota along the route
    best = {g_code: float("inf")}
    prev = {}
    pq = [(-float("inf"), g_code)]
    while pq:
        negb, u = heapq.heappop(pq)
        if -negb < best.get(u, 0.0):
            continue
        if u == d_code:
            break
        for v, c in adj[u]:
            nb = min(best[u], c)
            if nb > best.get(v, 0.0):
                best[v] = nb
                prev[v] = u
                heapq.heappush(pq, (-nb, v))

    def chain(prev_map, end):
        out = []
        while end in prev_map:
            out.append(end)
            end = prev_map[end]
        out.append(g_code)
        return out[::-1]

    path = chain(prev, d_code)
    print(f"WIDEST path (max bottleneck quota), bottleneck = {best.get(d_code):.3f}:")
    tot = 0.0
    for u, v in zip(path[:-1], path[1:]):
        c = next(c for (x, c) in adj[u] if x == v)
        tot += c
        print(f"  {c:7.4f}  {label(u):<28} -> {label(v)}")
    print(f"  (sum of quotas along the chain: {tot:.3f}; not a cut -- parallel routes "
          f"also carry flow)")
    print(f"bottleneck edge: min quota on the chain = {best.get(d_code):.3f}")

    # greedy deepest descent: always take the highest-current corridor forward
    gpath, cur = [g_code], g_code
    seen = {g_code}
    while cur != d_code:
        nxt = max((v for v, c in adj[cur] if v not in seen), key=lambda v: next(c for (x, c) in adj[cur] if x == v), default=None)
        if nxt is None:
            break
        gpath.append(nxt)
        seen.add(nxt)
        cur = nxt
    if gpath[-1] == d_code and gpath != path:
        print("GREEDY deepest-descent path:")
        for u, v in zip(gpath[:-1], gpath[1:]):
            c = next(c for (x, c) in adj[u] if x == v)
            print(f"  {c:7.4f}  {label(u):<28} -> {label(v)}")
    print(f"\ncounts: {len(adj)} current-carrying drawn metabolites; "
          f"{sum(1 for c in flows if c > 0)} corridors")

    # what the bolt is: glucose's own outflow (the source-side concentration)
    print(f"\nglucose out-quota {flows[[met_sym[a] == met_sym[g_code] or met_sym[b] == met_sym[g_code] for a, b in edges]].sum():.3f} over "
          f"{(met_sym[a] == met_sym[g_code] or met_sym[b] == met_sym[g_code] for a, b in edges).__len__() if False else sum(1 for a, b in edges if met_sym[a] == met_sym[g_code] or met_sym[b] == met_sym[g_code])} edges:")
    for (a, b), c in sorted(((e, c) for e, c in zip(edges, flows) if met_sym[e[0]] == met_sym[g_code] or met_sym[e[1]] == met_sym[g_code]), key=lambda t: -t[1]):
        other = b if met_sym[a] == met_sym[g_code] else a
        print(f"  {c:.4f}  -> {label(other)}")
    print(f"\nDHAP in-quota over the terminal edges:")
    for (a, b), c in sorted(((e, c) for e, c in zip(edges, flows) if met_sym[e[0]] == met_sym[d_code] or met_sym[e[1]] == met_sym[d_code]), key=lambda t: -t[1]):
        other = b if met_sym[a] == met_sym[d_code] else a
        print(f"  {c:.4f}  {label(other)} ->")


if __name__ == "__main__":
    main()
