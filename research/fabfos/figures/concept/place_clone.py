import argparse
import json
import sys
from pathlib import Path

import numpy as np
import matplotlib
import matplotlib.colors
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                      # noqa: E402
from matplotlib.collections import LineCollection                    # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from ieff_layout import (draw_network, incidence, load_sparse,       # noqa: E402
                         metabolite_positions, BARE_NODE, MET_SIZE,
                         depth_scale, EDGE_BUCKETS, EDGE_ALPHA, EDGE_WIDTH)

FLOOR = 1e-16
FOSMID = "#EF553B"
CONNECTOR_ALPHA = (0.10, 0.95)


def symmetric_current(z):
    import scipy.sparse as sp
    st = load_sparse(z)
    src = [str(s) for s in st["meta"]["src"]]
    rxn = [str(r) for r in st["meta"]["rxn"]]
    n = len(src)
    spos = {r: i for i, r in enumerate(src)}
    col2row = np.array([spos.get(r, -1) for r in rxn], dtype=np.int64)
    rows = np.repeat(np.arange(n, dtype=np.int64), st["K"])
    cols = col2row[st["idx"].ravel().astype(np.int64)]
    vals = st["val"].ravel().astype(np.float64)
    ok = (vals > 0) & (cols >= 0) & (cols != rows)
    A = sp.csr_matrix((vals[ok], (rows[ok], cols[ok])), shape=(n, n))
    return A.maximum(A.T), src, np.asarray(st["rowsum"], float)


def memberships(d, k, iters=64):
    target = np.log2(k)
    rho = d[0]
    lo, hi, sigma = 0.0, np.inf, 1.0
    for _ in range(iters):
        psum = np.exp(-(np.maximum(d - rho, 0.0)) / sigma).sum()
        if abs(psum - target) < 1e-5:
            break
        if psum > target:
            hi = sigma
            sigma = (lo + hi) / 2.0
        else:
            lo = sigma
            sigma = sigma * 2 if hi == np.inf else (lo + hi) / 2.0
    return np.exp(-(np.maximum(d - rho, 0.0)) / sigma)


def place(A, names, anchor_ix, target_ix, xy_anchor, k, imax, exclude_self=False):
    out = np.zeros((len(target_ix), 2))
    used = np.zeros(len(target_ix), int)
    for t, i in enumerate(target_ix):
        row = A.getrow(i)
        cur = np.zeros(A.shape[0])
        cur[row.indices] = row.data
        v = cur[anchor_ix]
        if exclude_self:
            v = v.copy()
            v[anchor_ix == i] = 0.0
        live = np.flatnonzero(v > 0)
        if len(live) == 0:
            out[t] = np.nan
            continue
        kk = min(k, len(live))
        sel = live[np.argsort(v[live])[::-1][:kk]]
        d = np.log10(imax / np.maximum(v[sel], FLOOR))
        w = memberships(d, max(kk, 2))
        out[t] = (w[:, None] * xy_anchor[sel]).sum(0) / w.sum()
        used[t] = kk
    return out, used


def joint_knn(A, order, k, imax):
    n = len(order)
    ki = np.zeros((n, k), np.int32)
    kd = np.zeros((n, k), np.float32)
    far = np.log10(imax / FLOOR)
    for r, i in enumerate(order):
        row = A.getrow(i)
        v = np.zeros(A.shape[0])
        v[row.indices] = row.data
        v = v[order]
        v[r] = 0.0
        live = np.flatnonzero(v > 0)
        c = min(k, len(live))
        if c:
            sel = live[np.argsort(v[live])[::-1][:c]]
            ki[r, :c] = sel
            kd[r, :c] = np.log10(imax / v[sel])
        if c < k:
            ki[r, c:] = r
            kd[r, c:] = far
    return ki, kd


def optimise_pinned(xy_all, ki, kd, free, n_epochs, min_dist, spread, seed):
    from umap.umap_ import fuzzy_simplicial_set, find_ab_params, make_epochs_per_sample
    from umap.layouts import optimize_layout_euclidean
    import scipy.sparse as sp

    n = len(xy_all)
    G, _, _ = fuzzy_simplicial_set(
        X=sp.csr_matrix((n, 1)), n_neighbors=ki.shape[1],
        random_state=np.random.RandomState(seed), metric="precomputed",
        knn_indices=ki, knn_dists=kd)
    G = G.tocoo()
    keep = (G.data > 0) & (G.row != G.col) & free[G.row]
    head, tail, w = G.row[keep], G.col[keep], G.data[keep]
    print(f"joint graph: {G.nnz} fuzzy edges, {len(head)} with a free head "
          f"({int(free[tail][free[head]].sum()) if len(head) else 0} of them free-free)",
          flush=True)
    w = w / w.max()
    eps = make_epochs_per_sample(w, n_epochs)
    a, b = find_ab_params(spread, min_dist)
    rng = np.random.RandomState(seed).randint(np.iinfo(np.int64).min,
                                              np.iinfo(np.int64).max, 3).astype(np.int64)
    return optimize_layout_euclidean(
        xy_all, xy_all, head.astype(np.int32), tail.astype(np.int32),
        n_epochs, n, eps, a, b, rng, gamma=1.0, initial_alpha=1.0,
        negative_sample_rate=5, parallel=False, verbose=False, move_other=False)


def fosmid_reactions(clone_gpr_table):
    import pandas as pd
    d = pd.read_parquet(clone_gpr_table)
    if "origin" not in d.columns:
        raise SystemExit(f"{clone_gpr_table} has no 'origin' column")
    return set(d.loc[d["origin"] != "host", "mnxr"].astype(str))


def bucket_by_degree(pairs, sel, n_met):
    deg_all = np.bincount(pairs[:, 1], minlength=n_met)[pairs[:, 1]]
    cuts = np.quantile(1.0 / deg_all, np.linspace(0, 1, EDGE_BUCKETS + 1)[1:-1])
    w = 1.0 / np.bincount(pairs[:, 1], minlength=n_met)[sel[:, 1]]
    return np.searchsorted(cuts, w, side="right")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sparse", type=Path, required=True,
                    help="the clone's own packed pairwise table")
    ap.add_argument("--layout-store", type=Path, required=True,
                    help="the host store, for the reaction order the layout was drawn in")
    ap.add_argument("--xy", type=Path, required=True, help="the frozen host layout")
    ap.add_argument("--gpr-table", type=Path, default=None,
                    help="host GPR table; draws the metabolite network under the points")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("-k", type=int, default=15, help="anchors per placed reaction")
    ap.add_argument("--clone-gpr-table", type=Path, required=True,
                    help="clone GPR table; its 'origin' column is what names the fosmid layer")
    ap.add_argument("--epochs", type=int, default=200,
                    help="pinned UMAP epochs; 0 stops at the interpolated initialisation")
    ap.add_argument("--min-dist", type=float, default=1.2)
    ap.add_argument("--spread", type=float, default=2.5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--clip", type=float, default=1.5)
    ap.add_argument("--y-scale", type=float, default=0.5,
                    help="drawing-only anisotropic squash, applied after placement")
    ap.add_argument("--offset", type=float, default=0.35,
                    help="gap to the offset fosmid copy, in drawn host y-spans")
    ap.add_argument("--validate", action="store_true",
                    help="leave-one-out over the anchors, to size the placement error")
    ap.add_argument("--metrics-out", type=Path, default=None)
    args = ap.parse_args()

    xy = np.load(args.xy)
    host = [str(s) for s in json.load(open(args.layout_store / "meta.json"))["src"]]
    if len(host) != len(xy):
        raise SystemExit(f"layout has {len(xy)} points, store names {len(host)}")

    A, names, rowsum = symmetric_current(args.sparse)
    pos = {r: i for i, r in enumerate(names)}
    missing = [r for r in host if r not in pos]
    if missing:
        raise SystemExit(f"{len(missing)} host reactions absent from the clone solve")
    anchor_ix = np.array([pos[r] for r in host])
    new = [r for r in names if r not in set(host)]
    target_ix = np.array([pos[r] for r in new])
    imax = float(A.data.max())
    print(f"{len(names)} reactions in the clone solve: {len(anchor_ix)} anchors "
          f"(the frozen layout), {len(target_ix)} new", flush=True)

    metrics = {"anchors": len(anchor_ix), "new": len(target_ix), "k": args.k}

    if args.validate:
        vxy, vused = place(A, names, anchor_ix, anchor_ix, xy, args.k, imax,
                           exclude_self=True)
        err = np.linalg.norm(vxy - xy, axis=1)
        ok = np.isfinite(err)
        from scipy.spatial import cKDTree
        nn = cKDTree(xy).query(xy, k=2)[0][:, 1]
        rng = np.random.default_rng(0)
        rand = np.linalg.norm(xy[rng.integers(0, len(xy), 20000)]
                              - xy[rng.integers(0, len(xy), 20000)], axis=1)
        metrics["validate"] = {
            "n": int(ok.sum()),
            "err_median": float(np.median(err[ok])),
            "err_p90": float(np.percentile(err[ok], 90)),
            "nn_spacing_median": float(np.median(nn)),
            "random_pair_median": float(np.median(rand)),
            "err_in_nn_units": float(np.median(err[ok]) / np.median(nn)),
            "err_vs_random": float(np.median(err[ok]) / np.median(rand)),
        }
        v = metrics["validate"]
        print(f"leave-one-out over {v['n']} anchors: median error {v['err_median']:.3f} "
              f"(p90 {v['err_p90']:.3f})", flush=True)
        print(f"  layout scale: nearest-neighbour spacing {v['nn_spacing_median']:.3f}, "
              f"random pair {v['random_pair_median']:.3f}", flush=True)
        print(f"  => {v['err_in_nn_units']:.1f} neighbour spacings, "
              f"{100 * v['err_vs_random']:.0f}% of a random pair", flush=True)

    nxy, used = place(A, names, anchor_ix, target_ix, xy, args.k, imax)
    live = np.isfinite(nxy).all(1)
    print(f"initialised {int(live.sum())}/{len(new)} new reactions "
          f"(median {int(np.median(used[live]))} anchors each)", flush=True)
    metrics["placed"] = int(live.sum())
    nxy[~live] = xy.mean(0)

    if args.epochs:
        order = np.concatenate([anchor_ix, target_ix])
        xy_all = np.ascontiguousarray(np.vstack([xy, nxy]), dtype=np.float32)
        free = np.zeros(len(order), bool)
        free[len(anchor_ix):] = True
        ki, kd = joint_knn(A, order, args.k, imax)
        before = xy_all[len(anchor_ix):].copy()
        xy_all = optimise_pinned(xy_all, ki, kd, free, args.epochs,
                                 args.min_dist, args.spread, args.seed)
        moved = np.linalg.norm(xy_all[len(anchor_ix):] - before, axis=1)
        host_drift = float(np.abs(xy_all[:len(anchor_ix)] - xy).max())
        print(f"pinned UMAP, {args.epochs} epochs: clone points moved "
              f"median {np.median(moved):.2f}, p90 {np.percentile(moved, 90):.2f}; "
              f"host drift {host_drift:.2e}", flush=True)
        metrics["optimise"] = {"epochs": args.epochs,
                               "moved_median": float(np.median(moved)),
                               "moved_p90": float(np.percentile(moved, 90)),
                               "host_drift_max": host_drift}
        nxy = np.asarray(xy_all[len(anchor_ix):], float)

    S = np.array([1.0, args.y_scale])
    dxy, dnxy = xy * S, nxy * S
    lo, hi = np.percentile(dxy, [args.clip, 100 - args.clip], axis=0) if args.clip else (
        dxy.min(0), dxy.max(0))
    off = np.array([0.0, (hi[1] - lo[1]) * (1.0 + args.offset)])
    dep = (lo[1], hi[1])

    span = (hi - lo) + np.array([0.0, off[1]])
    fig, ax = plt.subplots(figsize=(11, 11 * span[1] / span[0]))
    ax.set_aspect("equal")

    if args.gpr_table:
        met, pairs, miss = incidence(args.gpr_table, host)
        print(f"incidence: {len(host)} reactions, {len(met)} metabolites, "
              f"{len(pairs)} edges; {miss} with no row", flush=True)
        draw_network(ax, dxy, metabolite_positions(dxy, pairs, len(met)), pairs, bare=True,
                     depth=dep)

    ins = fosmid_reactions(args.clone_gpr_table)
    all_names = host + new
    all_xy = np.vstack([dxy, dnxy])
    fos = np.array([i for i, r in enumerate(all_names) if r in ins])
    dup = fos[fos < len(host)]
    print(f"fosmid: {len(ins)} insert reactions, {len(fos)} of them in the clone solve "
          f"({len(dup)} duplicating a host reaction, {len(fos) - len(dup)} new)", flush=True)
    metrics["fosmid"] = {"in_table": len(ins), "drawn": int(len(fos)),
                         "duplicating_host": int(len(dup))}

    cmet, cpairs, cmiss = incidence(args.clone_gpr_table, all_names)
    hostside = cpairs[cpairs[:, 0] < len(host)]
    cnt = np.bincount(hostside[:, 1], minlength=len(cmet))
    mpos = np.full((len(cmet), 2), np.nan)
    okm = cnt > 0
    for d in (0, 1):
        s = np.bincount(hostside[:, 1], dxy[hostside[:, 0], d], minlength=len(cmet))
        mpos[okm, d] = s[okm] / cnt[okm]
    isfos = np.zeros(len(all_names), bool)
    isfos[fos] = True
    ce = cpairs[isfos[cpairs[:, 0]] & okm[cpairs[:, 1]]]
    b = bucket_by_degree(cpairs, ce, len(cmet))
    seg = np.stack([all_xy[ce[:, 0]] + off, mpos[ce[:, 1]] + off], axis=1)
    for k in range(1, EDGE_BUCKETS):
        m = b == k
        if m.any():
            ax.add_collection(LineCollection(
                seg[m], colors=FOSMID, linewidths=EDGE_WIDTH,
                alpha=EDGE_ALPHA[EDGE_BUCKETS - 1 - k], zorder=3))
    drawn = ce[b > 0]
    print(f"fosmid layer: {len(cmet)} metabolites ({int(okm.sum())} host-placed), "
          f"{int(isfos[cpairs[:, 0]].sum())} incident edges of which {len(drawn)} drawn; "
          f"{len(np.unique(drawn[:, 0]))}/{len(fos)} reactions have one", flush=True)
    metrics["fosmid"]["edges_drawn"] = int(len(drawn))
    metrics["fosmid"]["reactions_with_edge"] = int(len(np.unique(drawn[:, 0])))

    ax.scatter(dxy[:, 0], dxy[:, 1], s=3 * MET_SIZE * depth_scale(dxy[:, 1], *dep),
               c=BARE_NODE, alpha=1.0, linewidths=0, zorder=2)
    if len(dup):
        g = rowsum[np.array([pos[all_names[i]] for i in dup])]
        t = (g - g.min()) / max(g.max() - g.min(), 1e-12)
        rgba = np.tile(matplotlib.colors.to_rgba(FOSMID), (len(dup), 1))
        rgba[:, 3] = CONNECTOR_ALPHA[0] + t * (CONNECTOR_ALPHA[1] - CONNECTOR_ALPHA[0])
        print(f"connector conductance: rowsum {g.min():.2f}..{g.max():.2f} over {len(dup)} "
              f"duplicated reactions (all 1,615 span {rowsum.min():.2f}..{rowsum.max():.2f})",
              flush=True)
        metrics["connector_conductance"] = {"min": float(g.min()), "max": float(g.max()),
                                            "global_min": float(rowsum.min()),
                                            "global_max": float(rowsum.max())}
        ax.add_collection(LineCollection(
            np.stack([dxy[dup], dxy[dup] + off], axis=1),
            colors=rgba, linewidths=0.6, zorder=5))
        d = depth_scale(dxy[dup, 1], *dep)
        ax.scatter(dxy[dup, 0], dxy[dup, 1], s=30 * d, facecolors="none",
                   edgecolors=FOSMID, linewidths=0.9 * np.sqrt(d), zorder=6)
    ax.scatter(all_xy[fos, 0], all_xy[fos, 1] + off[1],
               s=22 * depth_scale(all_xy[fos, 1], *dep), c=FOSMID,
               alpha=0.95, linewidths=0, zorder=6)

    pad = 0.03 * (hi - lo)
    ax.set_xlim(lo[0] - pad[0], hi[0] + pad[0])
    ax.set_ylim(lo[1] - pad[1], hi[1] + off[1] + pad[1])
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    fig.tight_layout()
    png, svg = (args.out.with_name(args.out.name + e) for e in (".png", ".svg"))
    fig.savefig(png, dpi=250); fig.savefig(svg)
    plt.close(fig)
    print(f"wrote {png} and .svg", flush=True)

    np.save(args.out.with_name(args.out.name + ".clone_xy.npy"), nxy)
    if args.metrics_out:
        metrics["argv"] = sys.argv
        json.dump(metrics, open(args.metrics_out, "w"), indent=1)
        print(f"wrote {args.metrics_out}", flush=True)


if __name__ == "__main__":
    main()
