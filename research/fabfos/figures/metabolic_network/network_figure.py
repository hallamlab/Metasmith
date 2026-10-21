#!/usr/bin/env python3
"""Glucose->DHAP host network figure, metabolite projection (PNG + SVG + HTML).

Layout: every metabolite on a carbon-atom path from glucose to DHAP is placed
by --dist. 'min' is the fewest carbon-to-carbon hops: x = dG/(dG+dD), y
proportional to distance to the nearer endpoint. 'paths' (default) replaces
both with one two-terminal solve (unit current glucose -> DHAP): x is the
voltage progress 1 - v/v_source -- a conductance-weighted superposition over
every route -- and y is the log2 fold change of the current-weighted mean
source->sink path length through the metabolite against the flow-weighted
average distance an atom actually travels glucose->DHAP (the flow mean, ~16
atom steps, not the 4-step minpath, because most carbon loops through
pentose-phosphate branches). y=0 is the typical route the current actually
takes; negative is a shortcut, positive a detour. Pendant branches carry no
current and are pruned. The
reaction nodes are collapsed OUT:
every carbon atom-to-atom transit between two metabolites becomes one undirected
metabolite-metabolite edge, drawn when both endpoints are on a glucose->DHAP
path. By default (--edge-weight current) the line's darkness and width both encode
the pair's share of the unit injected flow -- its mass-flow quota. Kirchhoff
makes the cross-sectional current (any cut separating glucose from DHAP) sum
to 1, so the figure reads as conservative mass flow: dark, thick at the two
terminals where the current is concentrated, faint in the diffused middle, and
the chain of strongest corridors emerges by itself as a connected "bolt".
--edge-weight transits restores the older atom-transit-count width.
The rendered figure is self-contained: no title, axis titles, legend, ticks, or
axis lines (only the delta colourbar survives under --delta); each metabolite is an
opaque #212121 dot whose area scales with the current flow through it, so the two
terminals that carry the whole injected unit are the largest.

One plotly figure is built and written three ways -- PNG, SVG, and a self-contained
HTML -- so the static exports and the interactive render cannot drift. The on-hover
annotation (metabolite name + MNXM id + position + handled current) is the sole
difference between them: it is trace-level hovertext, which plotly drops when it
rasterises or vectorises the same figure. Edges carry no hover and no persistent
label; nothing is annotated in the static copies.

Run:
    mamba run -n figure-net python research/fabfos/figures/metabolic_network/network_figure.py \
        --host e_coli_epi300/gpr/gpr_gem.parquet

Default output names branch per mode: paths mode writes
network_glucose_dhap_paths_<y_mode>.{png,svg,html}, --delta appends _delta.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp
from scipy.sparse.csgraph import shortest_path

import plotly.graph_objects as go
from plotly.colors import sample_colorscale

ROOT = Path(__file__).resolve().parents[4]
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
NAME_TABLE = ROOT / "data/fabfos/nostoc/ecspr/metabolite_names.parquet"

sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src/metasmith_libraries/resources/lib"))
import ecspr                                                    # noqa: E402,F401
from ecspr.model.build import graph_from_pairs, load_direction_ratios       # noqa: E402
from ecspr.model.graph import Terminal, solve                                # noqa: E402
import ecspr.model.evidence as EV                                            # noqa: E402
import fabfos_evidence as FE                                                 # noqa: E402

GLUCOSE = "MNXM1364061"   # D-glucose (stereo-resolved)
DHAP = "MNXM77"           # dihydroxyacetone phosphate

MET_C = "#3a7bd5"
END_C = "#c0392b"
SPINE_C = "#111111"
EDGE_C = "#9aa0a6"

FIG_W, FIG_H = 26 * 72, 16 * 72     # the 26in x 16in canvas, in points: 1 plotly px == 1pt
PAD = 11                            # matplotlib tight_layout's pad, 1.08 * the 10pt font


def load_vocab():
    v = pd.read_parquet(BAKE / "vocab.parquet")
    out = {}
    for kind, sub in v.groupby("kind"):
        out[kind] = (dict(zip(sub.code, sub.symbol)), dict(zip(sub.symbol, sub.code)))
    return out


def _resolve_host(host: str) -> Path:
    # relative to data/fabfos/runs, matching every other reader of a run's GPR table --
    # except an absolute path, which lets --weighting belief point at a table reparsed
    # with a real calibrated llr (e.g. by reparse_run.py --graft-onto) without publishing
    # it over the DVC-pinned run.
    p = Path(host)
    return p if p.is_absolute() else ROOT / "data/fabfos/runs" / host


def host_rxn_codes(host, vocab):
    df = pd.read_parquet(_resolve_host(host))
    mnxr = pd.unique(df["mnxr"].dropna())
    rcode = vocab["rxn"][1]
    return {rcode[s] for s in mnxr if s in rcode}


def belief_weights(hosts) -> dict:
    # Per-reaction E_r pooled from the calibrated per-lane llr (ref::lane_calibration),
    # the same call `run_arms.py::base_weights` and `pooling_sanity.py` make -- see
    # `ecspr.model.evidence.compute_E`. `FE.read_gpr` fills a placeholder llr=2.0 for a
    # table that was never reparsed with the calibration, so this is silently a flat
    # weighting again on any host that hasn't been through
    # `annotation_lanes/calibration/reparse_run.py`.
    frames = [FE.read_gpr(_resolve_host(h)) for h in hosts]
    d = pd.concat(frames, ignore_index=True, sort=False)
    d["llr"] = d["llr"].astype("float64")
    e = EV.compute_E(d[["orf", "channel", "intermediate_id", "mnxr", "llr"]])
    return {str(k): float(v) for k, v in e.items()}


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


def _build_solve(codes, p, rxn_sym, met_sym, belief=None):
    # Graph + solve for one reaction set (element C), the same construction main() uses.
    # `belief`, when given, is the {mnxr: E_r} dict from `belief_weights()` -- the base
    # per-reaction conductance is the calibrated pooled belief rather than a flat 1.0 for
    # every present reaction (the GPR-union scheme).
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
    if belief is None:
        weights = {mnxr: 1.0 for mnxr in host_mnxr}
    else:
        missing = host_mnxr - belief.keys()
        if missing:
            raise SystemExit(
                f"[belief] {len(missing)} of {len(host_mnxr)} drawn reactions have no "
                f"calibrated weight (e.g. {sorted(missing)[:5]}) -- the belief host table "
                f"does not cover the same reaction set host_rxn_codes read")
        weights = {mnxr: belief[mnxr] for mnxr in host_mnxr}
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
    # y_fc is the log2 fold change of the current-weighted mean source->sink path
    # length through the metabolite against the flow-weighted mean path length of the
    # whole solve -- the baseline is the distance an average carbon token actually
    # travels glucose->DHAP (~16 atom steps, not the 4-step minpath):
    #
    #     y_fc(m) = log2( ms(m) / mean_path )
    #     mean_path = sum( ms * through ) / sum(through)
    #
    # mS(m) is the current-weighted mean number of atom transits the current has taken to
    # reach m from the source, mT(m) the mean it still has to go to reach the sink, so
    # ms = mS + mT is the mean source->sink steps through the molecule -- both accumulated
    # by recursing over edges in potential order, current only ever flows downhill, so the
    # recursion is triangular: O(E) total, no extra solves. The tuple also carries the
    # signed absolute excess (ms - Lmin) at index 4 so the caller can prune sub-global
    # single-carbon side-chains without touching the log2 value, and y_dir, the excess
    # above the SHORTEST chain through the molecule itself (dG+dD hops) for the 'direct'
    # y-mode. Pendant branches (throughput 0) carry no current and are pruned.
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

    # flow-mean baseline: the current-weighted mean source->sink distance over the whole
    # solve -- the distance an average carbon token actually travels (~16 atom steps, not
    # the 4-step minpath). Every metabolite's y is its path length as a log2 fold change
    # against this baseline, so 0 = the typical distance the current actually takes.
    thr_t = float(through.sum())
    mean_path = float(np.sum(ms * through) / thr_t) if thr_t > 0.0 else float(Lmin)

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
        ms_met = float(np.sum(ms[ai[nz]] * w[nz]) / w[nz].sum())
        y_mean_abs = ms_met - Lmin                # signed excess above minpath (for pruning)
        y_fc = float(np.log2(max(ms_met, 1e-12) / max(mean_path, 1e-12)))
        # direct: excess above the shortest chain through THIS molecule (dG + dD hops);
        # per-molecule baseline so the floor is 0 by construction (rarely negative).
        y_dir = y_mean_abs - (dg + dd - Lmin)
        out[m] = (1.0 - vm / v_src if v_src > 0 else 0.5, y_fc, y_dir,
                  float(np.sum(w[nz])), y_mean_abs)
    return out


def _kcl_cut_sums(g, sol):
    # Kirchhoff cross-sections on the atom graph: any cut separating the source set from
    # the sink set carries exactly the injected unit current, because current flows only
    # downhill in a harmonic potential -- so |i_e| summed over the cut IS the
    # cross-sectional mass flow. Reports the source boundary, the sink boundary, and
    # three mid cuts at 25/50/75% of the potential drop.
    oe, cur = sol.edge_currents()
    out = {}
    if oe.size == 0:
        return out
    nodes = g.nodes
    src = set(g.idx[nd] for nd in sol.source.nodes)
    snk = set(g.idx[nd] for nd in sol.sink.nodes)
    vmax = max(sol.voltage(nd) for nd in sol.source.nodes)
    vmin = min(sol.voltage(nd) for nd in sol.sink.nodes)
    out["source boundary"] = 0.0
    out["sink boundary"] = 0.0
    cuts = {f"cut {int(k)}%": vmax - (vmax - vmin) * k / 100.0 for k in (25, 50, 75)}
    out.update({k: 0.0 for k in cuts})
    for e, c in zip(oe, cur):
        a, b = g.edges[int(e)]
        j = abs(float(c))
        if a in src or b in src:
            out["source boundary"] += j
        if a in snk or b in snk:
            out["sink boundary"] += j
        va, vb = sol.voltage(nodes[a]), sol.voltage(nodes[b])
        lo, hi = (va, vb) if va < vb else (vb, va)
        for k, thr in cuts.items():
            if lo < thr < hi:
                out[k] += j
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="e_coli_epi300/gpr/gpr_gem.parquet")
    ap.add_argument("--out", type=Path,
                    default=Path(__file__).resolve().parent / "cache" / "network_glucose_dhap.png")
    ap.add_argument("--dpi", type=int, default=150,
                    help="raster resolution of the PNG export; the SVG and the HTML are "
                         "resolution-independent. 150 reproduces the 3900x2400 raster.")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dist", choices=("min", "paths"), default="paths",
                    help="layout distance: 'min' = fewest carbon-to-carbon hops; "
                         "'paths' = all-paths (voltage-drop) distance")
    ap.add_argument("--y-mode", choices=("absolute", "direct"), default="absolute",
                    help="paths-mode y: 'absolute' = log2 fold change of the metabolite's "
                         "current-weighted mean source->sink path length against the "
                         "flow-weighted current distance glucose->DHAP (y=0 = typical "
                         "route); 'direct' = excess above the shortest chain through "
                         "each molecule (mS+mT-(dG+dD) atom steps)")
    ap.add_argument("--weighting", choices=("uniform", "belief"), default="uniform",
                    help="per-reaction base conductance before the atom-transit split: "
                         "'uniform' = every present reaction counts as 1.0, the GPR-union "
                         "scheme this figure always used; 'belief' = the calibrated pooled "
                         "log-odds weight E_r (ecspr.model.evidence.compute_E) read off "
                         "--host's (and --delta's) own llr column -- a table that was never "
                         "reparsed against ref::lane_calibration carries a flat placeholder "
                         "llr, so this is silently 'uniform' again on such a host")
    ap.add_argument("--edge-weight", choices=("transits", "current"),
                    default="current",
                    help="edge visual weight: 'transits' = number of atom transits "
                         "between the pair (corridor strength); 'current' = the pair's "
                         "share of the unit glucose->DHAP injected flow, the mass-flow "
                         "quota -- a cut across the network sums to 1 (Kirchhoff), so "
                         "current concentrates at the terminals and diffuses in the middle")
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
    # conductance-weighted superposition over every carbon route), y = log2 fold change
    # against the flow-weighted current distance -- see :func:`_mean_hop_excess`. Pendant
    # branches carry no current and are pruned.
    ytop = 0.55
    ymin = 0.0
    met_flow = {}
    pair_cur_union = pair_cur_base = None
    if args.delta and args.dist != "paths":
        print("[delta] warning: --delta weights edges by a two-terminal solve; "
              "forcing --dist paths", file=sys.stderr)
        args.dist = "paths"
    if args.dist == "min" and args.edge_weight == "current":
        print("[edge-weight] --dist min has no solve; falling back to 'transits'",
              file=sys.stderr)
        args.edge_weight = "transits"
    if args.delta and args.edge_weight == "current":
        print("[edge-weight] --delta darkness is |delta conductance|; "
              "ignoring --edge-weight current", file=sys.stderr)
        args.edge_weight = "transits"
    if args.dist == "paths":
        belief_all = belief_base = None
        if args.weighting == "belief":
            belief_base = belief_weights([args.host])
            belief_all = (belief_base if not args.delta else
                          belief_weights([args.host, args.delta]))
            print(f"[belief] {len(belief_all)} reactions weighted; "
                  f"E median {np.median(list(belief_all.values())):.4g}, "
                  f"max {max(belief_all.values()):.4g}")
        g_all, sol = _build_solve(codes, p, rxn_sym, met_sym, belief=belief_all)
        pair_cur = _pair_currents(g_all, sol)
        if args.delta:
            g_base, sol_base = _build_solve(base_codes, p, rxn_sym, met_sym, belief=belief_base)
            pair_cur_union = pair_cur
            pair_cur_base = _pair_currents(g_base, sol_base)
            print(f"[delta] current-carrying met-met pairs: "
                  f"union={len(pair_cur_union)} base={len(pair_cur_base)}")
        laid = _mean_hop_excess(g_all, sol, met_list, met_sym, Lmin)
        if not laid:
            print("[paths] solve produced no current-carrying metabolites; "
                  "falling back to min-hop layout", file=sys.stderr)
        else:
            met_flow = {m: p_[3] for m, p_ in laid.items()}
            laid[met_code[GLUCOSE]] = (0.0, 0.0, 0.0, 0.0, 0.0)   # endpoints pinned to poles
            laid[met_code[DHAP]] = (1.0, 0.0, 0.0, 0.0, 0.0)
            n_all = len(met_list)
            # prune side-chains whose current path is SHORTER than the minpath baseline
            # (sub-global single-carbon routes): their signed excess over the minimum is
            # negative and they are not meaningful routes; drop them rather than floor
            # them. In Log2FC mode this must use the ABSOLUTE excess (index 4), not the
            # log2 fold change -- nearly everything is below the flow mean (~16 steps),
            # so a log2<0 prune would wipe the figure.
            y_idx_prune = 2 if args.y_mode == "direct" else 4
            kept = {m: p_ for m, p_ in laid.items() if p_[y_idx_prune] >= 0.0}
            met_list = [(m, dg, dd) for m, dg, dd in met_list if m in kept]
            x_met = {m: p_[0] for m, p_ in kept.items()}
            # pick the y variant by --y-mode
            y_idx = 2 if args.y_mode == "direct" else 1
            y_met = {m: p_[y_idx] for m, p_ in kept.items()}
            met_flow = {m: f for m, f in met_flow.items() if m in kept}
            ymin = min(0.0, 1.05 * min(y_met.values())) if y_met else 0.0
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

    # per-edge weight: 'current' = the pair's share of the unit glucose->DHAP solve
    # (the mass-flow quota); 'transits' = pooled atom-transit count. With current we
    # verify Kirchhoff -- every cut separating glucose from DHAP carries exactly the
    # injected unit current -- so the drawn weights are a conserved mass flow that
    # concentrates at the terminals and diffuses in between.
    ew, ew_label = cnts.astype(float), "transits"
    if not args.delta and args.edge_weight == "current" and pair_cur is not None:
        ew = np.array([pair_cur.get((A, B) if A < B else (B, A), 0.0)
                       for a, b in edges
                       for A, B in [(met_sym[a], met_sym[b])]])
        ew_label = "current (mass-flow quota)"
        kcl = _kcl_cut_sums(g_all, sol)
        print("[kcl] cross-sectional current (injected = 1; every cut should sum to 1):")
        for k, s in kcl.items():
            print(f"      {k:>14s}: {s:.4f}")
        g_sym = met_sym[met_code[GLUCOSE]]
        d_sym = met_sym[met_code[DHAP]]
        p_src = ew[[met_sym[a] == g_sym or met_sym[b] == g_sym for a, b in edges]].sum()
        p_snk = ew[[met_sym[a] == d_sym or met_sym[b] == d_sym for a, b in edges]].sum()
        print(f"      projected glucose boundary {p_src:.4f}; "
              f"DHAP boundary {p_snk:.4f}")

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

    # ---- draw: one plotly figure, written as PNG, SVG and interactive HTML ----
    # The canvas is sized in POINTS (26in x 16in at 72pt/in) because plotly measures
    # line.width and marker.size in pixels of the base canvas: at 1px == 1pt the
    # matplotlib numbers below transfer unchanged, and exporting at scale = dpi/72
    # reproduces matplotlib's 3900x2400 raster and its 1872x1152pt vector exactly.
    # marker.size is a DIAMETER where scatter's s is an AREA -- hence the sqrt.
    fig = go.Figure()

    # edges: width encodes the strength of the collapsed atom transits; with --delta the
    # darkness (alpha) weights |delta conductance| so corridors the clone strengthens go
    # dark and corridors it reroutes flow away from go faint, with a diverging colour for
    # the sign (red = more conductive with the clone, blue = less).
    # visual weight: darkness and width both encode the per-edge measure. Current is
    # square-root-scaled (the quota is heavy-tailed: hundreds of thin middle corridors
    # against a handful of thick terminal ones), so the two-terminal concentration and
    # the mid-network diffusion are both readable.
    wmax = cnts.max() if len(cnts) else 1
    lw = 0.45 + 6.6 * np.sqrt(cnts / wmax)   # 3x the old width

    alpha = np.full(len(edges), 0.22)
    if delta_vals is None and ew_label == "current (mass-flow quota)":
        cmax = ew.max() if len(ew) else 1.0
        scale = np.sqrt(ew / cmax) if cmax > 0 else np.zeros_like(ew)
        alpha = 0.20 + 0.80 * scale   # opacity: 0.2 (thinnest current) ... 1.00 (max quota)
        lw = 0.30 + 7.2 * scale   # 3x the old width

        nz = ew > 0.0
        print(f"[edge-weight] current: {int(nz.sum())}/{len(ew)} edges carry current "
              f"(skipping {(~nz).sum()} zero-flow); max quota {cmax:.3f}")

    # Plotly cannot vary width or colour along a single line trace, so edges are pooled
    # by their QUANTISED visual weight rather than by population quantile -- a quantile
    # bucket would have to be drawn at one member's width, which inks every edge in it
    # as heavily as the bucket's heaviest. The steps here (0.05pt of width, 0.01 of
    # alpha) are an order of magnitude below one rendered pixel, so the pooling is
    # invisible while the trace count stays in the low hundreds. Groups are emitted
    # faintest-first so the heavy corridors composite on top.
    def _rgba(css, a):
        if css.startswith("#"):
            r, g, b = (int(css[i:i + 2], 16) for i in (1, 3, 5))
        else:
            r, g, b = (int(float(v)) for v in css[css.index("(") + 1:css.index(")")].split(","))
        return f"rgba({r},{g},{b},{a:.3f})"

    def _edge_trace(sel, width, color):
        xs, ys = [], []
        for k in sel:
            a, b = edges[k]
            xs += [x_met[a], x_met[b], None]
            ys += [y_met[a], y_met[b], None]
        fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", hoverinfo="skip",
                                 showlegend=False, line=dict(width=width, color=color)))

    groups = {}
    if delta_vals is not None:
        dmax = float(np.abs(delta_vals).max()) or 1.0
        for k in range(len(edges)):
            nd = float(np.clip(delta_vals[k] / dmax, -1.0, 1.0))
            groups.setdefault((round(float(lw[k]), 1), round(nd, 2)), []).append(k)
        order = sorted(groups, key=lambda key: (abs(key[1]), key[0]))
        for w, nd in order:
            rgb = sample_colorscale("RdBu_r", [0.5 + 0.5 * nd])[0]
            _edge_trace(groups[(w, nd)], w, _rgba(rgb, 0.08 + 0.75 * abs(nd)))
    else:
        for k in range(len(edges)):
            if ew_label == "current (mass-flow quota)" and ew[k] <= 0.0:
                continue
            groups.setdefault((round(float(lw[k]), 2), round(float(alpha[k]), 2)),
                              []).append(k)
        for w, al in sorted(groups, key=lambda key: (key[1], key[0])):
            _edge_trace(groups[(w, al)], w, _rgba(EDGE_C, al))
    print(f"[draw] {len(edges)} edges pooled into {len(groups)} line traces")

    # metabolite nodes: fully opaque constant #212121 (no outline); ONLY the area scales
    # with current flow -- min stays 3.0, max 10x (103 -> 1030).
    mx = [x_met[m] for m, _, _ in met_list]
    my = [y_met[m] for m, _, _ in met_list]
    if met_flow:
        fmax = max(met_flow.values()) or 1.0
        qs = np.array([met_flow[m] / fmax for m, _, _ in met_list])
        node_sizes = 3.0 + 1027.0 * qs      # min stays 3.0; max 10x (103 -> 1030)
    else:
        node_sizes = np.full(len(met_list), 4.0)
    # hover -- the only annotation layer, and the only reason the HTML copy differs from
    # the static ones: plotly drops it when it rasterises or vectorises the same figure.
    y_hover = ("distance to the nearer endpoint" if args.dist == "min" else
               "detour above the shortest chain through the molecule, atom steps"
               if args.y_mode == "direct" else "log2FC vs the flow-weighted distance")
    node_hover = [f"<b>{met_sym[m]} · {nm.get(met_sym[m], met_sym[m])}</b>"
                  f"<br>x = {x_met[m]:.3f} (voltage progress)"
                  f"<br>y = {y_met[m]:.3f} ({y_hover})"
                  + (f"<br>current handled = {met_flow[m]:.4f}" if m in met_flow else "")
                  for m, _, _ in met_list]
    fig.add_trace(go.Scatter(
        x=mx, y=my, mode="markers", showlegend=False,
        # opacity is explicit: plotly auto-dims markers to 0.7 on a busy scatter, which
        # would let the corridors show through the dots.
        marker=dict(size=np.sqrt(node_sizes), color="#212121", opacity=1.0,
                    line=dict(width=0)),
        customdata=node_hover, hovertemplate="%{customdata}<extra></extra>"))

    margin = dict(l=PAD, r=PAD, t=PAD, b=PAD)
    if delta_vals is not None:
        # the colourbar is the one piece of furniture that survives; it rides a hidden
        # zero-size marker trace because plotly attaches colourbars to data, not to axes.
        # 307pt is what matplotlib's fig.colorbar(shrink=0.6, pad=0.01) leaves the axes,
        # measured off the reference render; thickness follows its aspect=20 default.
        margin["r"] = 307
        fig.add_trace(go.Scatter(
            x=[0] * 2, y=[0] * 2, mode="markers", hoverinfo="skip", showlegend=False,
            marker=dict(size=0, color=[-1.0, 1.0], colorscale="RdBu_r",
                        cmin=-1.0, cmax=1.0, showscale=True,
                        colorbar=dict(len=0.6, thickness=35, tickfont=dict(size=9),
                                      title=dict(text="delta conductance (host+clone2 - host),"
                                                      "<br>per met-met pair",
                                                 font=dict(size=9), side="right")))))

    # no title, no axis titles, no legend, no ticks or axis lines -- the figure is
    # self-contained; only the delta colourbar (when present) survives.
    fig.update_layout(
        width=FIG_W, height=FIG_H, margin=margin, hovermode="closest",
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(range=(-0.03, 1.03), showgrid=False, zeroline=False,
                   showticklabels=False, ticks="", visible=False),
        yaxis=dict(range=(ymin - 0.03, ytop), showgrid=False, zeroline=False,
                   showticklabels=False, ticks="", visible=False))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    # separate output per y-mode / delta: when --out is the default cache path, branch it
    default = Path(__file__).resolve().parent / "cache" / "network_glucose_dhap"
    stem = args.out.with_suffix("")
    if stem == default:
        if args.delta:
            stem = default.parent / f"network_glucose_dhap_paths_{args.y_mode}_delta"
        elif args.dist == "paths":
            stem = default.parent / f"network_glucose_dhap_paths_{args.y_mode}"
    png, svg, html = stem.with_suffix(".png"), stem.with_suffix(".svg"), stem.with_suffix(".html")
    fig.write_image(png, scale=args.dpi / 72.0)
    fig.write_image(svg)
    fig.write_html(html, include_plotlyjs="cdn", config={"displayModeBar": False})
    for f_ in (png, svg, html):
        print(f"wrote {f_} ({f_.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
