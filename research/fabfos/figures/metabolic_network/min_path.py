#!/usr/bin/env python3
"""Glucose -> DHAP (minimum path) in a host carbon-atom metabolic network.

Builds the carbon-atom transit graph from the metabolism bake, restricted to the
reactions a host GPR table names, then computes the fewest-transition path from
any one of glucose's carbon atoms to any of DHAP's. The atom spine is collapsed to
the metabolite/reaction sequence the figure will show.

Run inside the fig-networks worktree:
    mamba run -n msm python research/fabfos/figures/metabolic_network/min_path.py \
        --host e_coli_epi300/gpr/gpr_gem.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.csgraph import shortest_path, connected_components

ROOT = Path(__file__).resolve().parents[4]           # -> fig-networks
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
NAME_TABLE = ROOT / "data/fabfos/nostoc/ecspr/metabolite_names.parquet"
REAC_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/reac_prop.tsv"

GLUCOSE = "MNXM1364061"      # D-glucose (stereo-resolved; plain glucose 7381 is not used by the bake)
DHAP = "MNXM77"           # dihydroxyacetone phosphate


def load_reac_equations():
    eq = {}
    try:
        with open(REAC_PROP) as fh:
            for line in fh:
                if line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 2:
                    eq[parts[0]] = parts[1]
    except FileNotFoundError:
        pass
    return eq


def load_vocab():
    v = pd.read_parquet(BAKE / "vocab.parquet")
    out = {}
    for kind, sub in v.groupby("kind"):
        out[kind] = (dict(zip(sub.code, sub.symbol)), dict(zip(sub.symbol, sub.code)))
    return out


def host_rxn_codes(host_table, vocab):
    df = pd.read_parquet(ROOT / "data/fabfos/runs" / host_table)
    mnxr = pd.unique(df["mnxr"].dropna())
    rxn_code = vocab["rxn"][1]
    return {rxn_code[s] for s in mnxr if s in rxn_code}


def reconstruct(parent, start, to):
    """Walk parent pointers from `start` back to a node in set `to`."""
    path = [start]
    cur = start
    while True:
        if cur in to:
            return path
        if parent[cur] == -9999 or parent[cur] == cur:
            return None
        cur = parent[cur]
        path.append(cur)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True,
                    help="host GPR table path under data/fabfos/runs/, e.g. e_coli_epi300/gpr/gpr_gem.parquet")
    ap.add_argument("--src", default=GLUCOSE)
    ap.add_argument("--dst", default=DHAP)
    args = ap.parse_args()

    import pandas as pd  # noqa
    vocab = load_vocab()
    met_sym, met_code = vocab["met"]
    rxn_sym, rxn_code = vocab["rxn"]
    el_code = vocab["element"][1]["C"]

    host = args.host
    rxn_codes = host_rxn_codes(host, vocab)
    print(f"host {host}: {len(rxn_codes)} recognized reactions")

    p = pd.read_parquet(BAKE / "atom_pairs.parquet")
    p = p[(p.element == el_code) & (p.rxn.isin(rxn_codes))].reset_index(drop=True)

    tails = list(zip(p.tail_met.map(met_sym), p.tail_rank))
    heads = list(zip(p.head_met.map(met_sym), p.head_rank))
    rxn_of = p["rxn"].to_numpy()
    rxn_mnxr = {r: rxn_sym[r] for r in np.unique(rxn_of)}
    edge_rxn = {}
    for i in range(len(p)):
        edge_rxn.setdefault((tails[i], heads[i]), []).append(rxn_mnxr[rxn_of[i]])
        edge_rxn.setdefault((heads[i], tails[i]), []).append(rxn_mnxr[rxn_of[i]])

    # unique atom nodes
    all_nodes = sorted(set(tails) | set(heads))
    idx = {nd: i for i, nd in enumerate(all_nodes)}
    n = len(all_nodes)

    rows = [idx[t] for t in tails] + [idx[h] for h in heads]
    cols = [idx[h] for h in heads] + [idx[t] for t in tails]
    data = np.ones(len(rows))
    A = sp.coo_matrix((data, (rows, cols)), shape=(n, n)).tocsr()

    _nc, lab = connected_components(A, directed=False)
    # restrict to giant to cut tiny self-loops/duplicates
    giant = np.bincount(lab).argmax()
    keep = np.flatnonzero(lab == giant)
    idxr = {i: j for j, i in enumerate(keep)}
    A = A[keep][:, keep]
    all_nodes = [all_nodes[i] for i in keep]
    n = len(all_nodes)

    def atom_nodes(mn):
        return [all_nodes[i] for i in range(n) if all_nodes[i][0] == mn]

    src_nodes = atom_nodes(args.src)
    dst_nodes = atom_nodes(args.dst)
    dst_set = {idxr[idx[nd]] for nd in dst_nodes}
    print(f"{args.src} atoms present: {len(src_nodes)}; {args.dst} atoms present: {len(dst_nodes)}")

    # unweighted shortest paths from every src atom, find candidate to any dst atom
    dist, pred = sp.csgraph.shortest_path(A, method="D", directed=False,
                                          indices=[idxr[idx[s]] for s in src_nodes],
                                          unweighted=True, return_predecessors=True)
    best_len = np.inf
    best = None
    for si, s in enumerate(src_nodes):
        drow = dist[si]
        js = [j for j in range(n) if all_nodes[j][0] in {args.dst}]  # dst atom indices
        if not js:
            continue
        j = min(js, key=lambda j: drow[j])
        L = drow[j]
        if L < best_len:
            best_len = L
            # backtrack from j
            path = []
            cur = int(j)
            while cur != int(si) and cur != -9999 and cur != -1:
                path.append(all_nodes[cur])
                cur = int(pred[si, cur])
            path.append(all_nodes[idxr[idx[s]]])
            path = path[::-1]
            best = path
    if best is None or np.isinf(best_len):
        print("NO path found; glucose/DHAP not connected in this host reaction set.")
        return

    print(f"\nminimum path: {int(best_len)} transitions, {len(best)} atoms:")
    print(f"  {best[0][0]} r{best[0][1]}  (start)")
    for a in best[1:]:
        print(f"  {a[0]} r{a[1]}")

    names = pd.read_parquet(NAME_TABLE).drop_duplicates("mnxm")
    nm = dict(zip(names.mnxm, names.name))
    spine = []
    for a in best:
        m = a[0]
        if spine and spine[-1][0] == m:
            spine[-1][1] += 1
        else:
            spine.append([m, 1])
    eq = load_reac_equations()
    print("\nreaction steps along the path:")
    for (u, v) in zip(best, best[1:]):
        rx = edge_rxn.get((u, v)) or edge_rxn.get((v, u))
        for r in (rx or []):
            print(f"  {r}  {eq.get(r, '')}")
    print("\nmetabolite spine (MNXM : name : atoms crossed):")
    for m, c in spine:
        print(f"  {m}  {nm.get(m, '?')}  [{c}]")


if __name__ == "__main__":
    main()