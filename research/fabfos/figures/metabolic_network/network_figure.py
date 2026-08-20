#!/usr/bin/env python3
"""Glucose->DHAP host network figure, metabolite projection (PNG only).

Layout: every metabolite on a carbon-atom path from glucose to DHAP is placed
by --dist. 'min' is the fewest carbon-to-carbon hops: x = dG/(dG+dD), y
proportional to distance to the nearer endpoint. 'paths' (default) replaces
both with one two-terminal solve (unit current glucose -> DHAP): x is the
voltage progress 1 - v/v_source -- a conductance-weighted superposition over
every route -- and y is the current-weighted mean hop-excess above the
shortest chain, in absolute atom steps (mS + mT - Lmin), so a metabolite the
flow passes through by a detour rises off the x-axis while a shortest-chain
one sits at y=0. Pendant branches carry no current and are pruned. The
reaction nodes are collapsed OUT:
every carbon atom-to-atom transit between two metabolites becomes one undirected
metabolite-metabolite edge, drawn when both endpoints are on a glucose->DHAP
path. Line width encodes how many atom transits a pair carries, so a collapsed
edge still reads as a real reaction corridor rather than a summary line. The
glycolysis spine carrying the minimum path is highlighted.

Run:
    mamba run -n figure-net python research/fabfos/figures/metabolic_network/network_figure.py \
        --host e_coli_epi300/gpr/gpr_gem.parquet --out network_glucose_dhap.png
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.csgraph import shortest_path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[4]
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
NAME_TABLE = ROOT / "data/fabfos/nostoc/ecspr/metabolite_names.parquet"

sys.path.insert(0, str(ROOT / "src"))
import ecspr                                                    # noqa: E402,F401
from ecspr.model.build import graph_from_pairs, load_direction_ratios       # noqa: E402
from ecspr.model.graph import Terminal, solve                                # noqa: E402

GLUCOSE = "MNXM1364061"   # D-glucose (stereo-resolved)
DHAP = "MNXM77"           # dihydroxyacetone phosphate

MET_C = "#3a7bd5"
END_C = "#c0392b"
SPINE_C = "#111111"
EDGE_C = "#9aa0a6"


def load_vocab():
    v = pd.read_parquet(BAKE / "vocab.parquet")
    out = {}
    for kind, sub in v.groupby("kind"):
        out[kind] = (dict(zip(sub.code, sub.symbol)), dict(zip(sub.symbol, sub.code)))
    return out


def host_rxn_codes(host, vocab):
    df = pd.read_parquet(ROOT / "data/fabfos/runs" / host)
    mnxr = pd.unique(df["mnxr"].dropna())
    rcode = vocab["rxn"][1]
    return {rcode[s] for s in mnxr if s in rcode}


def _pair_currents(g, sol):
    # Total |current| flowing along each unordered (met, met) pair, summed over the atom
    # transits that build the pair's corridor. Two-terminal unit injection, so a pair's
    # current is its share of the injected carbon flow -- the corridor's conductance as
    # measured by the solve. Keys are MNXM strings (graph node keys).
    oe, cur = sol.edge_currents()
    out = {}
    if oe.size == 0:
        return out
    nodes = g.nodes
    for e, c in zip(oe, cur):
        a, b = g.edges[int(e)]
        ma, mb = nodes[a][0], nodes[b][0]
        if ma == mb:
            continue
        key = (ma, mb) if ma < mb else (mb, ma)
        out[key] = out.get(key, 0.0) + abs(float(c))
    return out


def _build_solve(codes, p, rxn_sym, met_sym):
    # Graph + solve for one reaction set (element C), the same construction main() uses.
    ap = p[p.rxn.isin(codes)]
    pairs_dec = pd.DataFrame({
        "mnxr": ap.rxn.map(rxn_sym),
        "element": "C",
        "substrate": ap.tail_met.map(met_sym),
        "product": ap.head_met.map(met_sym),
        "sub_idx": ap.tail_rank,
        "prod_idx": ap.head_rank,
        "pair_w": ap.pair_w,
    })
    host_mnxr = set(ap.rxn.map(rxn_sym))
    weights = {mnxr: 1.0 for mnxr in host_mnxr}
    d = pd.read_parquet(BAKE / "direction.parquet")
    ratios = (d.assign(mnxr=d.rxn.map(rxn_sym), ratio=d.ratio)
              .dropna(subset=["mnxr"])
              .set_index("mnxr").ratio.to_dict())
    g = graph_from_pairs(pairs_dec, "C", weights, ratios)
    src = Terminal.metabolite(g, GLUCOSE, label="source")
    snk = Terminal.merge(g, [DHAP], label="ground")
    return g, solve(g, src, snk)


def _mean_hop_excess(g, sol, met_list, met_sym, Lmin, *, thr=1e-8):
    # Per-metabolite (x, y) from ONE two-terminal solve.
    #
    # x is the voltage progress: 1 - v/v_source, so glucose -> 0 and DHAP -> 1. Because the
    # potential is harmonic (current flows strictly downhill), x is a true all-paths,
    # conductance-weighted progress coordinate, but it is the ONLY scalar the solve
    # carries -- y must come from elsewhere or the layout collapses onto the curve
    # y ~ min(x, 1-x).
    #
    # y_abs is the current-weighted mean hop-excess, in absolute atom steps:
    #
    #     y_abs(m) = mS(m) + mT(m) - Lmin
    #
    # mS(m) is the current-weighted mean number of atom transits the current has taken to
    # reach m from the source, mT(m) the mean it still has to go to reach the sink -- both
    # accumulated by recursing over edges in potential order, current only ever flows
    # downhill, so the recursion is triangular: O(E) total, no extra solves. y_dir is the
    # same mean path length minus the SHORTEST chain through the molecule itself
    # (dG+dD hops), i.e. the detour above the direct route rather than above the global
    # 4-step spine. Pendant branches (throughput 0) carry no current and are pruned.
    oe, cur = sol.edge_currents()
    if oe.size == 0:
        return {}
    et = np.array([g.edges[e][0] for e in oe], dtype=np.int64)
    eh = np.array([g.edges[e][1] for e in oe], dtype=np.int64)
    fwd = cur > 0                       # cur > 0 flows tail -> head
    t_from = np.where(fwd, et, eh)
    t_to = np.where(fwd, eh, et)
    jabs = np.abs(cur)
    n = g.n
    through = np.maximum(np.bincount(t_to, weights=jabs, minlength=n),
                         np.bincount(t_from, weights=jabs, minlength=n))

    v = np.array([sol.voltage(nd) for nd in g.nodes], dtype=float)
    finite = np.flatnonzero(np.isfinite(v))
    order = finite[np.argsort(-v[finite])]      # decreasing potential: source first
    src_idx = frozenset(g.idx[nd] for nd in sol.source.nodes)
    snk_idx = frozenset(g.idx[nd] for nd in sol.sink.nodes)

    # edges grouped by head (in-edges) and by tail (out-edges), once, O(E)
    by_head, by_tail = {}, {}
    for (key, arr, idxs) in ((by_head, t_to, t_from), (by_tail, t_from, t_to)):
        sa = np.argsort(arr, kind="stable")
        a_s, b_s = arr[sa], idxs[sa]
        w_s = jabs[sa]
        starts = np.flatnonzero(np.concatenate([[True], a_s[1:] != a_s[:-1]]))
        ends = np.append(starts[1:], a_s.size)
        for i0, i1 in zip(starts, ends):
            key[int(a_s[i0])] = (b_s[i0:i1], w_s[i0:i1])

    mS = np.zeros(n)
    mT = np.zeros(n)
    for u in order:                     # source -> sink
        if u in src_idx:
            continue
        grp = by_head.get(u)
        if grp is None:
            continue
        tails, w = grp
        wsum = w.sum()
        if wsum <= 0.0:
            continue
        mS[u] = float(np.sum((mS[tails] + 1.0) * w) / wsum)
    for u in order[::-1]:               # sink -> source
        if u in snk_idx:
            continue
        grp = by_tail.get(u)
        if grp is None:
            continue
        heads, w = grp
        wsum = w.sum()
        if wsum <= 0.0:
            continue
        mT[u] = float(np.sum((mT[heads] + 1.0) * w) / wsum)
    ms = mS + mT                                  # per-atom mean S->T path length
    y_abs = ms - Lmin                              # SIGNED: negative = sub-global single-carbon chains

    v_src = float(v[list(src_idx)].max()) if src_idx else float("nan")
    out = {}
    for m, dg, dd in met_list:
        atoms = g.atoms_of(met_sym[m])
        if not atoms:
            continue
        ai = np.array([g.idx[a] for a in atoms], dtype=np.int64)
        w = through[ai]
        nz = w > 0.0
        if w[nz].sum() <= thr:
            continue                    # pendant branch: no current flows through it
        vm = float(np.sum(v[ai[nz]] * w[nz]) / w[nz].sum())
        y_mean_abs = float(np.sum(y_abs[ai[nz]] * w[nz]) / w[nz].sum())
        # direct: excess above the shortest chain through THIS molecule (dG + dD hops);
        # per-molecule baseline so the floor is 0 by construction (rarely negative).
        y_dir = y_mean_abs - (dg + dd - Lmin)
        out[m] = (1.0 - vm / v_src if v_src > 0 else 0.5, y_mean_abs, y_dir)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="e_coli_epi300/gpr/gpr_gem.parquet")
    ap.add_argument("--out", type=Path,
                    default=Path(__file__).resolve().parent / "cache" / "network_glucose_dhap.png")
    ap.add_argument("--dpi", type=int, default=150)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dist", choices=("min", "paths"), default="paths",
                    help="layout distance: 'min' = fewest carbon-to-carbon hops; "
                         "'paths' = all-paths (voltage-drop) distance")
    ap.add_argument("--y-mode", choices=("absolute", "direct"), default="absolute",
                    help="paths-mode y: 'absolute' = mean hop-excess above the global "
                         "shortest chain (mS+mT-Lmin); 'direct' = excess above the "
                         "shortest chain through each molecule (mS+mT-(dG+dD))")
    ap.add_argument("--delta", type=str, default=None,
                    help="second host parquet (e.g. e_coli_epi300/gpr/gpr_epi300_clone2."
                         "parquet): diff the two-terminal solve against the baseline host "
                         "and weight edge darkness by the per-pair conductance change "
                         "(positive = clone adds conductance, negative = clone reroutes "
                         "flow away). Layout and drawn set come from the union of both "
                         "reaction sets.")
    args = ap.parse_args()
    rng = np.random.default_rng(args.seed)

    vocab = load_vocab()
    met_sym, met_code = vocab["met"]
    rxn_sym, rxn_code = vocab["rxn"]
    el_code = vocab["element"][1]["C"]

    codes = host_rxn_codes(args.host, vocab)
    print(f"host {args.host}: {len(codes)} recognized reactions")
    base_codes = codes
    if args.delta:
        dcodes = host_rxn_codes(args.delta, vocab)
        codes = codes | dcodes
        print(f"delta host {args.delta}: {len(dcodes)} recognized reactions; "
              f"union = {len(codes)}")

    p = pd.read_parquet(BAKE / "atom_pairs.parquet")
    p = p[(p.element == el_code) & (p.rxn.isin(codes))].reset_index(drop=True)
    print(f"carbon atom-pairs in host: {len(p)}")

    # ---- atom-transit graph, for shortest-path distances only ----
    nodes = set()
    for row in p.itertuples():
        nodes.add((int(row.tail_met), int(row.tail_rank)))
        nodes.add((int(row.head_met), int(row.head_rank)))
    nodes = sorted(nodes)
    idx = {n: i for i, n in enumerate(nodes)}
    nn = len(nodes)
    rr, cc = [], []
    for row in p.itertuples():
        a = idx[(int(row.tail_met), int(row.tail_rank))]
        b = idx[(int(row.head_met), int(row.head_rank))]
        rr += [a, b]
        cc += [b, a]
    A = sp.coo_matrix((np.ones(len(rr)), (rr, cc)), shape=(nn, nn)).tocsr()

    rxn_atoms = {}
    for row in p.itertuples():
        rxn_atoms.setdefault(int(row.rxn), set()).add(idx[(int(row.tail_met), int(row.tail_rank))])
        rxn_atoms.setdefault(int(row.rxn), set()).add(idx[(int(row.head_met), int(row.head_rank))])
    met_atoms = {}
    for n_, i in idx.items():
        met_atoms.setdefault(n_[0], set()).add(i)

    def atom_ids(mnxm):
        return sorted(met_atoms.get(met_code[mnxm], ()))

    GA = atom_ids(GLUCOSE)
    DA = atom_ids(DHAP)

    dG = shortest_path(A, method="D", directed=False, indices=GA, unweighted=True).min(axis=0)
    dD = shortest_path(A, method="D", directed=False, indices=DA, unweighted=True).min(axis=0)
    Lmin = int(dG[DA].min())
    print(f"min atom-transit glucose->DHAP: {Lmin}")

    # ---- drawn set: metabolites/reactions on a carbon path glucose->DHAP ----
    rxn_on_path, met_list = set(), []
    for r, ats in rxn_atoms.items():
        a = np.array(sorted(ats))
        if (dG[a] < np.inf).any() and (dD[a] < np.inf).any():
            rxn_on_path.add(r)
    for m, ats in met_atoms.items():
        a = np.array(sorted(ats))
        if (dG[a] < np.inf).any() and (dD[a] < np.inf).any():
            met_list.append((m, float(dG[a].min()), float(dD[a].min())))
    print(f"drawn metabolites (on a carbon path): {len(met_list)}; on-path reactions: {len(rxn_on_path)}")

    # ---- metabolite positions: x = dG/(dG+dD), y = density spread ----
    def x_of(dg, dd):
        return dg / (dg + dd) if (dg + dd) > 0 else 0.5

    x_met = {m: x_of(dg, dd) for m, dg, dd in met_list}
    # y proportional to distance to the nearer endpoint (glucose or DHAP):
    # endpoints sit at y=0, mid-network metabolites rise toward a peak. A tiny
    # jitter keeps neighbours that agree on x and min-distance from stacking.
    dmin = {m: min(dg, dd) for m, dg, dd in met_list}
    dmax = max(dmin.values()) or 1.0
    y_met = {m: 0.5 * (dmin[m] / dmax) for m in dmin}

    # ---- optional: all-paths (voltage-drop) layout ---------------------
    # ``min`` counts only the fewest-hop route -- the thinnest chain that still connects,
    # badly under-counting metabolites reachable by many routes. The all-paths layout
    # replaces hop count with one two-terminal solve: x = voltage progress (a
    # conductance-weighted superposition over every carbon route), y = current-weighted
    # mean hop-excess above the shortest chain -- see :func:`_mean_hop_excess`. Pendant
    # branches carry no current and are pruned.
    ytop = 0.55
    ylabel = ""
    pair_cur_union = pair_cur_base = None
    if args.delta and args.dist != "paths":
        print("[delta] warning: --delta weights edges by a two-terminal solve; "
              "forcing --dist paths", file=sys.stderr)
        args.dist = "paths"
    if args.dist == "paths":
        g_all, sol = _build_solve(codes, p, rxn_sym, met_sym)
        if args.delta:
            g_base, sol_base = _build_solve(base_codes, p, rxn_sym, met_sym)
            pair_cur_union = _pair_currents(g_all, sol)
            pair_cur_base = _pair_currents(g_base, sol_base)
            print(f"[delta] current-carrying met-met pairs: "
                  f"union={len(pair_cur_union)} base={len(pair_cur_base)}")
        laid = _mean_hop_excess(g_all, sol, met_list, met_sym, Lmin)
        if not laid:
            print("[paths] solve produced no current-carrying metabolites; "
                  "falling back to min-hop layout", file=sys.stderr)
        else:
            laid[met_code[GLUCOSE]] = (0.0, 0.0, 0.0)   # endpoints pinned to the poles
            laid[met_code[DHAP]] = (1.0, 0.0, 0.0)
            n_all = len(met_list)
            # prune side-chains whose current path is SHORTER than the minpath baseline
            # (sub-global single-carbon routes): their signed detour is negative and they
            # are not meaningful detour, so drop them rather than floor them onto y=0.
            y_idx_prune = 2 if args.y_mode == "direct" else 1
            kept = {m: p_ for m, p_ in laid.items() if p_[y_idx_prune] >= 0.0}
            met_list = [(m, dg, dd) for m, dg, dd in met_list if m in kept]
            x_met = {m: p_[0] for m, p_ in kept.items()}
            # pick the y variant by --y-mode
            y_idx = 2 if args.y_mode == "direct" else 1
            y_met = {m: p_[y_idx] for m, p_ in kept.items()}
            ylabel = ("mean detour above shortest chain THROUGH the molecule (atom steps)"
                      if args.y_mode == "direct"
                      else "mean detour above minpath baseline (atom steps)")
            ytop = max(0.55, 1.05 * max(y_met.values()))
            xs = np.array(list(x_met.values()))
            ys = np.array(list(y_met.values()))
            r = float(np.corrcoef(xs, ys)[0, 1]) if len(ys) > 1 else float("nan")
            pct = np.percentile(ys, [10, 25, 50, 75, 90])
            print(f"[paths] y-mode={args.y_mode}: kept {len(kept)} carrying "
                  f"(pruned {n_all - len(met_list)} pendant+negative); "
                  f"y range [{ys.min():.3g}, {ys.max():.3g}] "
                  f"(pct 10/25/50/75/90: {', '.join(f'{p:.3g}' for p in pct)}); "
                  f"corr(x, y) = {r:.3f}")
            for mnxm in (GLUCOSE, "MNXM1364111", "MNXM1107898", "MNXM1372044", DHAP):
                c = met_code[mnxm]
                print(f"[paths]  {mnxm}: y = {y_met.get(c)}")

    # ---- debug: check the endpoint/spine assumptions ----

    def _dbg(mnxm):
        c = met_code[mnxm]
        ats = atom_ids(mnxm)
        return (f"{mnxm}: code={c} drawn={c in x_met} "
                f"x={x_met.get(c)} y={y_met.get(c)} "
                f"(hop dG={float(dG[ats].min()) if ats else None}, "
                f"dD={float(dD[ats].min()) if ats else None})")

    for mnxm in (GLUCOSE, DHAP, "MNXM1364111", "MNXM1107898", "MNXM1372044"):
        print("DBG", _dbg(mnxm))

    # ---- collapse atom transits into metabolite-metabolite edges ----
    # An undirected edge (A,B) exists when some on-path reaction moves a carbon
    # atom between A and B. count = how many atom transits the pair carries, summed
    # over every reaction between them.
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
    print(f"metabolite-metabolite edges: {len(edges)} (from {int(cnts.sum())} atom transits)")

    # per-edge delta conductance: union (host+clone2) current minus baseline host current,
    # per met-met pair. A pair the clone adds gets a positive delta (its whole current is
    # new); a host corridor the clone reroutes flow away from goes negative.
    delta_vals = None
    if pair_cur_union is not None:
        delta_vals = []
        for a, b in edges:
            A, B = met_sym[a], met_sym[b]
            key = (A, B) if A < B else (B, A)
            d = pair_cur_union.get(key, 0.0) - pair_cur_base.get(key, 0.0)
            delta_vals.append(d)
        delta_vals = np.array(delta_vals)
        nz = delta_vals[delta_vals != 0.0]
        if nz.size:
            print(f"[delta] per-pair current delta: range "
                  f"[{delta_vals.min():.3g}, {delta_vals.max():.3g}]; "
                  f"|delta| median {np.median(np.abs(nz)):.3g}, "
                  f"max {np.abs(nz).max():.3g}; "
                  f"n up {int((delta_vals > 0).sum())}, "
                  f"n down {int((delta_vals < 0).sum())}, "
                  f"n unchanged {int((delta_vals == 0).sum())}")

    names = pd.read_parquet(NAME_TABLE).drop_duplicates("mnxm")
    nm = dict(zip(names.mnxm, names.name))

    # ---- draw ----
    fig, ax = plt.subplots(figsize=(26, 16))
    # edges: width encodes the strength of the collapsed atom transits; with --delta the
    # darkness (alpha) weights |delta conductance| so corridors the clone strengthens go
    # dark and corridors it reroutes flow away from go faint, with a diverging colour for
    # the sign (red = more conductive with the clone, blue = less).
    wmax = cnts.max() if len(cnts) else 1
    lw = 0.15 + 2.2 * np.sqrt(cnts / wmax)
    if delta_vals is None:
        for (a, b), w in zip(edges, lw):
            ax.plot([x_met[a], x_met[b]], [y_met[a], y_met[b]],
                    color=EDGE_C, lw=w, alpha=0.22, solid_capstyle="round", zorder=1)
    else:
        dmax = float(np.abs(delta_vals).max()) or 1.0
        cmap_d = plt.get_cmap("RdBu_r")
        for (a, b), w, dv in zip(edges, lw, delta_vals):
            nd = float(np.clip(dv / dmax, -1.0, 1.0))
            ax.plot([x_met[a], x_met[b]], [y_met[a], y_met[b]],
                    color=cmap_d(0.5 + 0.5 * nd), lw=w,
                    alpha=0.08 + 0.75 * abs(nd), solid_capstyle="round", zorder=1)

    # metabolite nodes
    mx = [x_met[m] for m, _, _ in met_list]
    my = [y_met[m] for m, _, _ in met_list]
    ax.scatter(mx, my, s=4, c=MET_C, zorder=2, label=f"metabolites ({len(met_list)})")

    # endpoints
    for m, lbl, xx in ((met_code[GLUCOSE], "D-glucose", 0.0), (met_code[DHAP], "DHAP", 1.0)):
        ax.scatter([xx], [0.0], s=90, c=END_C, edgecolors="black", zorder=5)
        ax.annotate(lbl, (xx, 0.0), textcoords="offset points", xytext=(0, 14),
                    ha="center", fontsize=13, fontweight="bold", color=END_C)

    # minpath spine: the baseline itself, drawn flat along y=0 so it reads as the
    # reference the detour is measured against (the five spine metabolites' own detour
    # values are plotted as data with the other nodes, not as the line).
    spine_mets = [m for m in (met_code[GLUCOSE], met_code["MNXM1364111"],
                              met_code["MNXM1107898"], met_code["MNXM1372044"],
                              met_code[DHAP]) if m in x_met]
    spine_x = [x_met[m] for m in spine_mets]
    ax.plot(spine_x, [0.0] * len(spine_x), color=SPINE_C, lw=2.2, zorder=4)
    for m, xx in zip(spine_mets, spine_x):
        if m in (met_code[GLUCOSE], met_code[DHAP]):
            continue
        sym = {v: k for k, v in met_code.items()}[m]
        lbl = nm.get(sym, sym)
        ax.annotate(lbl, (xx, 0.0), textcoords="offset points", xytext=(0, -14),
                    ha="center", fontsize=8, color=SPINE_C)

    ax.set_xlim(-0.03, 1.03)
    ax.set_ylim(-0.03, ytop)
    ax.set_aspect("auto")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=9)
    title_host = args.host
    if args.delta:
        title_host = f"{args.host} vs {args.delta} (line darkness = |delta conductance|)"
    ax.set_title(f"Glucose -> DHAP in {title_host}  ({'all-paths voltage' if args.dist == 'paths'
                 else 'min-hop'} layout; min atom shortest-path = {Lmin})\n"
                 f"{len(met_list)} metabolites, {len(edges)} collapsed met-met edges "
                 f"from {int(cnts.sum())} atom transits",
                 fontsize=12)
    if delta_vals is not None:
        sm = plt.cm.ScalarMappable(cmap="RdBu_r", norm=plt.Normalize(-1.0, 1.0))
        sm.set_array([])
        cb = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.01)
        cb.set_label("delta conductance (host+clone2 - host), per met-met pair", fontsize=9)
    ax.legend(loc="upper right", fontsize=9)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
    ax.set_xticklabels(["glucose\nx=0", "", "mid", "", "DHAP\nx=1"], fontsize=9)
    ax.set_yticks([])
    ax.spines[["top", "right", "left"]].set_visible(False)
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    # separate output per y-mode / delta: when --out is the default cache path, branch it
    out = args.out
    if out == Path(__file__).resolve().parent / "cache" / "network_glucose_dhap.png":
        if args.delta:
            out = out.with_name(f"network_glucose_dhap_paths_{args.y_mode}_delta.png")
        elif args.dist == "paths":
            out = out.with_name(f"network_glucose_dhap_paths_{args.y_mode}.png")
    fig.savefig(out, dpi=args.dpi)
    print(f"wrote {out} ({out.stat().st_size/1e6:.1f} MB, dpi {args.dpi})")
    plt.close(fig)


if __name__ == "__main__":
    main()