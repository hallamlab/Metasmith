#!/usr/bin/env python3
"""Interactive HTML render of the glucose->DHAP network (plotly, hover = names).

Same construction as network_figure.py (paths mode, absolute y, host e_coli_epi300/gpr/
gpr_gem.parquet), but as a self-contained plotly figure:

  - metabolite nodes: markers with a tooltip (common name + MNXM id + handled current
    = the max of inflow/outflow across the metabolite's atom boundary) and marker size
    proportional to that current, so aggregators read at a glance.
  - edges: drawn for every pair with nonzero current; line width and darkness encode the
    pair's share of the unit glucose->DHAP flow (the mass-flow quota), matching the PNG
    render. Hovering an edge gives the pair currents and atom-transit count.

Run:
    mamba run -n figure-net python research/fabfos/figures/metabolic_network/network_figure_html.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.csgraph import shortest_path

import plotly.graph_objects as go

ROOT = Path("/home/tony/agentic_workspace/projects/metasmith/fabfos/fig-networks")
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/figures/metabolic_network"))

import network_figure as nf                               # noqa: E402

HOST = "e_coli_epi300/gpr/gpr_gem.parquet"
OUT = ROOT / "research/fabfos/figures/metabolic_network/cache/network_glucose_dhap_paths_absolute.html"


def main():
    vocab = nf.load_vocab()
    met_sym, met_code = vocab["met"]
    rxn_sym, _ = vocab["rxn"]
    el_code = vocab["element"][1]["C"]

    codes = nf.host_rxn_codes(HOST, vocab)
    p = pd.read_parquet(nf.BAKE / "atom_pairs.parquet")
    p = p[(p.element == el_code) & (p.rxn.isin(codes))].reset_index(drop=True)

    # drawn set + layout (paths mode, absolute), same as network_figure.main()
    nodes = sorted({(int(r.tail_met), int(r.tail_rank)) for r in p.itertuples()}
                   | {(int(r.head_met), int(r.head_rank)) for r in p.itertuples()})
    idx = {n: i for i, n in enumerate(nodes)}
    rr, cc = [], []
    for r in p.itertuples():
        a = idx[(int(r.tail_met), int(r.tail_rank))]
        b = idx[(int(r.head_met), int(r.head_rank))]
        rr += [a, b]
        cc += [b, a]
    A = sp.coo_matrix((np.ones(len(rr)), (rr, cc)), shape=(len(nodes), len(nodes))).tocsr()

    rxn_atoms, met_atoms = {}, {}
    for r in p.itertuples():
        rxn_atoms.setdefault(int(r.rxn), set()).add(idx[(int(r.tail_met), int(r.tail_rank))])
        rxn_atoms.setdefault(int(r.rxn), set()).add(idx[(int(r.head_met), int(r.head_rank))])
        met_atoms.setdefault(int(r.tail_met), set())
        met_atoms.setdefault(int(r.head_met), set())
    for n_, i in idx.items():
        met_atoms.setdefault(n_[0], set()).add(i)

    GA = sorted(met_atoms.get(met_code[nf.GLUCOSE], ()))
    DA = sorted(met_atoms.get(met_code[nf.DHAP], ()))
    dG = shortest_path(A, method="D", directed=False, indices=GA, unweighted=True).min(axis=0)
    dD = shortest_path(A, method="D", directed=False, indices=DA, unweighted=True).min(axis=0)
    Lmin = int(dG[DA].min())

    rxn_on_path = {r for r, ats in rxn_atoms.items()
                   if (dG[np.array(sorted(ats))] < np.inf).any()
                   and (dD[np.array(sorted(ats))] < np.inf).any()}
    met_list = [(m, float(dG[a].min()), float(dD[a].min()))
                for m, ats in met_atoms.items()
                if (a := np.array(sorted(ats))).size
                and (dG[a] < np.inf).any() and (dD[a] < np.inf).any()]

    g_all, sol = nf._build_solve(codes, p, rxn_sym, met_sym)
    pair_cur = nf._pair_currents(g_all, sol)
    laid = nf._mean_hop_excess(g_all, sol, met_list, met_sym, Lmin)
    laid[met_code[nf.GLUCOSE]] = (0.0, 0.0, 0.0)
    laid[met_code[nf.DHAP]] = (1.0, 0.0, 0.0)
    kept = {m: pp for m, pp in laid.items() if pp[1] >= 0.0}
    x_met = {m: pp[0] for m, pp in kept.items()}
    y_met = {m: pp[1] for m, pp in kept.items()}

    pair_count = {}
    for r in p.itertuples():
        rk = int(r.rxn)
        if rk not in rxn_on_path:
            continue
        a, b = int(r.tail_met), int(r.head_met)
        if a == b or a not in x_met or b not in x_met:
            continue
        key = (a, b) if a < b else (b, a)
        pair_count[key] = pair_count.get(key, 0) + 1
    edges = sorted(pair_count)
    cnts = np.array([pair_count[e] for e in edges])
    flows = np.array([pair_cur.get((A, B) if A < B else (B, A), 0.0)
                      for a, b in edges for A, B in [(met_sym[a], met_sym[b])]])

    names = pd.read_parquet(nf.NAME_TABLE).drop_duplicates("mnxm")
    nm = dict(zip(names.mnxm, names.name))

    def label(code, sym):
        return f"{sym} · {nm.get(sym, sym)}"

    # per-metabolite handled current for node sizing + hover
    thr = {}
    for code, sym in met_sym.items():
        t = sol.throughput_metabolite(sym)
        if t > 0:
            thr[code] = t

    # edge traces: split into a few groups by quota so the huge dynamic range reads
    scale = np.sqrt(flows / flows.max()) if flows.max() > 0 else np.zeros_like(flows)
    groups = np.zeros(len(flows), dtype=int)
    groups[scale >= 0.4] = 0
    groups[(scale >= 0.15) & (scale < 0.4)] = 1
    groups[scale < 0.15] = 2

    fig = go.Figure()
    gc, dc = met_code[nf.GLUCOSE], met_code[nf.DHAP]
    for gi in (0, 1, 2):
        sel = np.flatnonzero(groups == gi)
        if not len(sel):
            continue
        ex, ey = [], []
        hover = []
        for i in sel:
            a, b = edges[i]
            ex += [x_met[a], x_met[b], None]
            ey += [y_met[a], y_met[b], None]
            ca, cb = met_sym[a], met_sym[b]
            hover.append(f"<b>{label(a, ca)}</b> → <b>{label(b, cb)}</b>"
                         f"<br>flow quota: {flows[i]:.4f} of unit injected"
                         f"<br>atom transits: {cnts[i]}"
                         f"<br>(width group {gi})")
        fig.add_trace(go.Scatter(
            x=ex, y=ey, mode="lines", name=f"edges · quota group {gi}",
            line=dict(width=0.25 + 6.0 * (scale[sel].max() if len(sel) else 0),
                      color="rgba(60,70,90,%.2f)" % (0.10 + 0.85 * (scale[sel].max() if len(sel) else 0))),
            hoverinfo="none", showlegend=False))

    # nodes
    codes_drawn = list(kept)
    nx = [x_met[m] for m in codes_drawn]
    ny = [y_met[m] for m in codes_drawn]
    nmx = np.array([thr.get(m, 0.0) for m in codes_drawn])
    sizes = 3 + 22 * np.sqrt(nmx / nmx.max())
    hover = [f"<b>{label(m, met_sym[m])}</b>"
             f"<br>x = {x_met[m]:.3f} (voltage progress)"
             f"<br>y = {y_met[m]:.2f} (mean detour above minpath, atom steps)"
             f"<br>current handled = {thr.get(m, 0.0):.4f}" for m in codes_drawn]
    fig.add_trace(go.Scatter(
        x=nx, y=ny, mode="markers+text", name="metabolites",
        text=[met_sym[m] for m in codes_drawn],
        textposition="top center", textfont=dict(size=7, color="#3a7bd5"),
        marker=dict(size=sizes, color="#3a7bd5", line=dict(width=0.4, color="#1b3a5c")),
        hovertemplate="%{customdata}<extra></extra>", customdata=hover))

    # endpoints
    for m, lbl in ((gc, "D-glucose (source)"), (dc, "DHAP (sink)")):
        fig.add_trace(go.Scatter(
            x=[x_met[m]], y=[0.0], mode="markers+text",
            name=lbl, text=[lbl], textposition="top center",
            textfont=dict(size=12, color="#c0392b"),
            marker=dict(size=16, color="#c0392b", line=dict(width=1, color="black")),
            hovertemplate="%{customdata}<extra></extra>",
            customdata=[f"<b>{lbl}</b><br>x = {x_met[m]:.3f}<br>current handled = 1.0000"]))

    fig.update_layout(
        title=f"Glucose → DHAP · {HOST} · drawn {len(kept)} metabolites, {len(edges)} edges"
              f"<br>edge width/darkness = mass-flow quota (unit injected current; cut sums to 1)",
        xaxis_title="voltage progress (glucose → DHAP)",
        yaxis_title="mean detour above minpath baseline (atom steps)",
        hovermode="closest", template="plotly_white", height=820,
        font=dict(size=11))
    fig.write_html(OUT, include_plotlyjs="cdn")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()