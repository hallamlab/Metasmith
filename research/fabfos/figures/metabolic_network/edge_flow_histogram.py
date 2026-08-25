#!/usr/bin/env python3
"""Histogram of per-edge flow (current) values for the glucose->DHAP absolute figure.

Reconstructs the exact edge set drawn by network_figure.py in absolute (non-delta)
paths mode -- host e_coli_epi300/gpr/gpr_gem.parquet, default flags -- and pairs each
collapsed met-met edge with its current from the two-terminal solve (the corridor's
share of the injected carbon flow). Outputs one PNG with a linear and a log-x
histogram of the per-edge flow plus the edge-width distribution.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.csgraph import shortest_path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path("/home/tony/agentic_workspace/projects/metasmith/fabfos/fig-networks")
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/figures/metabolic_network"))

import network_figure as nf                               # noqa: E402

HOST = "e_coli_epi300/gpr/gpr_gem.parquet"
OUT = ROOT / "research/fabfos/figures/metabolic_network/cache/edge_flow_histogram.png"

# ---- same construction as network_figure.main(), absolute paths mode ----
vocab = nf.load_vocab()
met_sym, met_code = vocab["met"]
rxn_sym, _ = vocab["rxn"]
el_code = vocab["element"][1]["C"]

codes = nf.host_rxn_codes(HOST, vocab)
p = pd.read_parquet(nf.BAKE / "atom_pairs.parquet")
p = p[(p.element == el_code) & (p.rxn.isin(codes))].reset_index(drop=True)

# atom-transit graph for on-path determination (as in main())
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

rxn_atoms = {}
for row in p.itertuples():
    rxn_atoms.setdefault(int(row.rxn), set()).add(idx[(int(row.tail_met), int(row.tail_rank))])
    rxn_atoms.setdefault(int(row.rxn), set()).add(idx[(int(row.head_met), int(row.head_rank))])
met_atoms = {}
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

# ---- solve + per-pair flow + layout (the drawn set in paths mode) ----
g_all, sol = nf._build_solve(codes, p, rxn_sym, met_sym)
pair_cur = nf._pair_currents(g_all, sol)
laid = nf._mean_hop_excess(g_all, sol, met_list, met_sym, Lmin)
laid[met_code[nf.GLUCOSE]] = (0.0, 0.0, 0.0)
laid[met_code[nf.DHAP]] = (1.0, 0.0, 0.0)
kept = {m: p_ for m, p_ in laid.items() if p_[1] >= 0.0}
x_met = {m: p_[0] for m, p_ in kept.items()}

# ---- collapsed edges: exactly main()'s pair_count block ----
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
cnts = np.array([pair_count[e] for e in edges])

# per-edge flow: the pair's current from the solve, keyed the same way as the
# delta block (string-sorted MNXM pair) so it matches _pair_currents' keys.
flows = np.zeros(len(edges))
for i, (a, b) in enumerate(edges):
    A, B = met_sym[a], met_sym[b]
    key = (A, B) if A < B else (B, A)
    flows[i] = pair_cur.get(key, 0.0)

# ---- verification against the figure's own stdout ----
print(f"kept metabolites: {len(kept)} (figure: 686)")
print(f"collapsed edges: {len(edges)} (figure: 1371)")
print(f"atom transits: {int(cnts.sum())} (figure: 35866)")
print(f"total injected current: {flows.sum():.6f} (unit current, expect ~1)")
print(f"edges carrying zero flow: {(flows == 0.0).sum()} of {len(edges)}")
nz = flows[flows > 0.0]
print(f"nonzero flow: n={len(nz)} min={nz.min():.2e} median={np.median(nz):.2e} "
      f"mean={nz.mean():.2e} p90={np.percentile(nz, 90):.2e} max={nz.max():.2e}")
print(f"transit counts: min={cnts.min()} median={np.median(cnts):.1f} "
      f"p90={np.percentile(cnts, 90):.0f} max={cnts.max()}")

# ---- histogram ----
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
ax = axes[0]
ax.hist(flows, bins=60, color="#3a7bd5", alpha=0.85)
ax.set_title(f"per-edge current flow, linear (n={len(flows)})")
ax.set_xlabel("flow (share of unit glucose\u2192DHAP current)")
ax.set_ylabel("edges")
ax.text(0.98, 0.95, f"median {np.median(nz):.2e}\nmax {nz.max():.2e}",
        transform=ax.transAxes, ha="right", va="top", fontsize=9,
        bbox=dict(fc="white", ec="0.8"))

ax = axes[1]
bins = np.logspace(np.log10(max(nz.min(), 1e-8)), np.log10(nz.max()), 50)
ax.hist(flows, bins=bins, color="#3a7bd5", alpha=0.85)
ax.set_xscale("log")
ax.set_title("same, log-x (heavy tail)")
ax.set_xlabel("flow")
ax.set_ylabel("edges")
fig.suptitle("Glucose\u2192DHAP absolute figure (epi300 GEM): "
             f"flow on {len(edges)} drawn edges, "
             f"{(flows == 0.0).sum()} zero-flow corridors", fontsize=12)
fig.tight_layout(rect=(0, 0, 1, 0.94))
OUT.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(OUT, dpi=150)
print(f"wrote {OUT}")
