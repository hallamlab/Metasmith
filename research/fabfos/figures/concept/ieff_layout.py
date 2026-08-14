"""Lay the reaction network out from the true pairwise I_eff distance.

Reads an :mod:`ieff_sweep` store, symmetrises it, turns current into distance, builds a
k-nearest-neighbour graph and hands that to UMAP. The KEGG overlay is the same one the
landmark figure uses, so the two are directly comparable.

**Distance.** ``I`` is asymmetric -- injecting at A and reading B is not injecting at B and
reading A -- and the pairwise convention is the larger of the two currents (the shorter
resistance): ``R = 1/max(I, I^T)``. Currents span fourteen orders of magnitude, so the
distance handed to UMAP is ``log10(I_max / I)``. A linear ``1/I`` is numerically dominated
by the handful of near-adjacent pairs and flattens everything else into one blob.

**Metabolites are drawn, not placed.** ``--gpr-table`` rebuilds the host's carbon incidence
and draws each metabolite at the mean position of the reactions it moves an atom through,
with its edges. Nothing about a metabolite enters the layout, which stays reaction-to-reaction.

**Neighbours, not a matrix.** UMAP only ever reads a kNN graph, so the store's top-K rows
are enough and no N x N array is built. Symmetrisation with top-K rows is a transposing
pass: reaction b may appear in a's row without a appearing in b's, and that entry is a real
measurement that would be lost by intersecting instead of merging.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                      # noqa: E402
from matplotlib.collections import LineCollection                    # noqa: E402
import umap                                                          # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

XREF = Path("/home/tony/agentic_workspace/projects/fabfos/figure/data/originals/"
            "metanetx/4.5/reac_xref.tsv")
# ModelSEED's copy of the KEGG pathway -> reaction table (Source ID / Name / Reactions),
# vendored here because the promoted scripts must not depend on gitignored scratch.
PATHWAYS = HERE / "KEGG.pathways"

# The panel the landmark figure overlays: central carbon, then four peripheral pathways.
PANEL = ["map00010", "map00020", "map00030", "map00230",
         "map00061", "map00260", "map00860", "map00280"]
PALETTE = ["#d62728", "#2ca02c", "#9467bd", "#17becf",
           "#e377c2", "#bcbd22", "#ff7f0e", "#1f77b4"]
# Origin colouring: the clone's new reactions are the figure's subject and take the loud
# colour; everything the host already had recedes.
# Greys for the two classes that are context rather than subject, and the other inserts get
# the paler one: at community scale they are 82% of the points, so if they are the darker grey
# the figure is a grey mass with the host lost inside it.
ORIGIN_PALETTE = ["#d62728", "#ff9d3f", "#909090", "#e3e3e3"]

# The reaction-metabolite drawing. Reactions are the layout's own points and metabolites are
# derived from them, so reactions are drawn three times the size. Edge weights are bucketed
# by quantile and the lightest bucket is dropped, which leaves EDGE_BUCKETS - 1 collections.
MET_SIZE = 4
EDGE_BUCKETS = 5
# Bucket k gets width and alpha from (k/(EDGE_BUCKETS-1)) ** EDGE_GAMMA. Above 1 the ramp is
# convex, so the light buckets drop away sharply and the heavy ones carry the drawing --
# 1/degree already spans two orders of magnitude and a linear ramp flattens that back out.
# Width is constant across buckets, so weight reads as tone alone and a heavy edge cannot be
# mistaken for a near one. Alphas are given per bucket, heaviest first, rather than as a ramp:
# the useful separation here is not smooth and a formula only obscured where it was put.
EDGE_ALPHA = [0.85, 0.2, 0.05, 0.01]        # heaviest bucket first; len == EDGE_BUCKETS - 1
EDGE_WIDTH = 0.45
EDGE_COLOR = "#8a8a8a"
BARE_NODE = "#555555"
# Pseudo-depth. A point's drawn *area* is scaled by where it sits on y, so the bottom of a
# layer reads as near and the top as far. This is redundant encoding, not a second channel:
# y already carries the coordinate and the size only restates it, which is what makes it safe
# -- nothing is legible from size that was not already legible from position.
DEPTH = (2.2, 0.35)          # area multiplier at the near (low y) and far (high y) edge


def depth_scale(y, lo, hi):
    """Area multiplier for a point at height ``y`` within a layer spanning ``lo``..``hi``."""
    t = np.clip((y - lo) / max(hi - lo, 1e-12), 0.0, 1.0)
    return DEPTH[0] + t * (DEPTH[1] - DEPTH[0])


def pathway_members(pids=PANEL):
    """MNXR -> KEGG pathway id, via MetaNetX's own ``kegg.reaction:`` cross-reference."""
    pr, pn = {}, {}
    for line in open(PATHWAYS).readlines()[1:]:
        p = line.rstrip("\n").split("\t")
        if len(p) >= 5 and p[1] in pids:
            pr[p[1]], pn[p[1]] = set(p[4].split("|")), p[2]
    m2k = {}
    for line in open(XREF):
        if line.startswith("#"):
            continue
        p = line.rstrip("\n").split("\t")
        if len(p) >= 2 and p[0].startswith("kegg.reaction:") and p[1] != "EMPTY":
            m2k.setdefault(p[1], set()).add(p[0][len("kegg.reaction:"):])
    members = {pid: {m for m, ks in m2k.items() if ks & pr[pid]} for pid in pr}
    return members, pn


def origin_labels(table, src, highlight):
    """Label each laid-out reaction by where it came from, using a :mod:`gpr_union` table.

    An alternative to the KEGG overlay, for the figures whose subject is the clone rather
    than the pathway: the two cannot share the palette. A reaction is claimed by the
    highlighted insert first, then by the host, then by the remaining inserts -- and the
    highlighted insert's reactions are split by whether the host already had them, which is
    the whole question the figure is asked to answer.
    """
    import pandas as pd
    d = pd.read_parquet(table, columns=["mnxr", "origin"])
    host = set(d.loc[d.origin == "host", "mnxr"])
    hi = set(d.loc[d.origin.isin(highlight), "mnxr"]) if highlight else set()
    other = set(d.loc[(d.origin != "host") & (~d.origin.isin(highlight)), "mnxr"])
    names = {"clone_new": "clone reactions the host lacks",
             "clone_shared": "clone reactions the host already has",
             "host": "host only (EPI300 GEM lane)",
             "other": "other inserts"}
    lab = []
    for r in src:
        if r in hi:
            lab.append("clone_shared" if r in host else "clone_new")
        elif r in host:
            lab.append("host")
        elif r in other:
            lab.append("other")
        else:
            lab.append("")
    return np.array(lab), names, ["clone_new", "clone_shared", "host", "other"]


def load_store(path):
    path = Path(path)
    meta = json.load(open(path / "meta.json"))
    n, K = len(meta["src"]), meta["K"]
    st = dict(
        meta=meta, n=n, K=K, path=path,
        idx=np.memmap(path / "idx.i32", np.int32, "r", shape=(n, K)),
        val=np.memmap(path / "val.f32", np.float32, "r", shape=(n, K)),
        done=np.memmap(path / "done.u8", np.uint8, "r", shape=(n,)),
        rowsum=np.memmap(path / "rowsum.f64", np.float64, "r", shape=(n,)),
    )
    st["ch"] = {c: np.memmap(path / f"{c}.f32", np.float32, "r", shape=(n, K))
                for c in ("power", "dv") if (path / f"{c}.f32").exists()}
    return st


def load_sparse(path):
    """A packed :mod:`gpr_ieff` table, presented as :func:`load_store` presents a store.

    Rows are ragged after the current-cover truncation, so they are padded out to the widest
    row. The padding is zero-valued and :func:`knn_from_store` already drops non-positive
    entries, so nothing downstream needs to know the difference -- which is the point:
    laying out from the delivered table rather than from the store is what demonstrates the
    table is sufficient on its own.
    """
    z = np.load(path, allow_pickle=False)
    indptr, indices, data = z["indptr"], z["indices"], z["data"]
    n = len(indptr) - 1
    K = int(np.diff(indptr).max())
    idx = np.zeros((n, K), np.int32)
    val = np.zeros((n, K), np.float32)
    for i in range(n):
        a, b = indptr[i], indptr[i + 1]
        # Descending, so a row truncated to fewer than k neighbours loses only its weakest.
        o = np.argsort(data[a:b])[::-1]
        idx[i, :b - a] = indices[a:b][o]
        val[i, :b - a] = data[a:b][o]
    meta = dict(src=[str(s) for s in z["src"]], rxn=[str(r) for r in z["rxn"]], K=K)
    return dict(meta=meta, n=n, K=K, idx=idx, val=val,
                done=np.ones(n, np.uint8), rowsum=z["rowsum"])


def knn_from_store(st, k=30):
    """Merge the stored rows into a symmetric top-k neighbour graph over the source set.

    Only reactions that were themselves sources can be laid out (a reaction that appears as
    a target but never as a source has no row of its own, so its neighbourhood is only
    half-measured). At full-sweep scale that set is everything.
    """
    src = st["meta"]["src"]
    rxn = st["meta"]["rxn"]
    spos = {r: i for i, r in enumerate(src)}
    # target column -> row position, -1 for targets that are not sources
    col2row = np.array([spos.get(r, -1) for r in rxn], dtype=np.int64)

    import scipy.sparse as sp
    n = len(src)
    idx, val = np.asarray(st["idx"]), np.asarray(st["val"])
    rows = np.repeat(np.arange(n, dtype=np.int64), st["K"])
    cols = col2row[idx.ravel()]
    vals = val.ravel().astype(np.float64)
    live = np.repeat(np.asarray(st["done"]).astype(bool), st["K"])
    ok = live & (cols >= 0) & (cols != rows) & (vals > 0)
    A = sp.csr_matrix((vals[ok], (rows[ok], cols[ok])), shape=(n, n))
    A = A.maximum(A.T)                 # max(I, I^T), the pairwise convention
    del rows, cols, vals, live, ok

    imax = float(A.data.max()) if A.nnz else 1.0
    ki = np.zeros((n, k), np.int32)
    kd = np.zeros((n, k), np.float32)
    far = np.log10(imax / 1e-16)
    indptr, indices, data = A.indptr, A.indices, A.data
    rng = np.random.default_rng(0)
    short = 0
    for i in range(n):
        a, b = indptr[i], indptr[i + 1]
        deg = b - a
        c = 0
        if deg:
            d = data[a:b]
            sel = np.argpartition(d, -k)[-k:] if deg > k else np.arange(deg)
            sel = sel[np.argsort(d[sel])[::-1]]
            c = len(sel)
            ki[i, :c] = indices[a:b][sel]
            kd[i, :c] = np.log10(imax / d[sel])
        if c < k:
            # A row with fewer than k measured partners only happens on a partial sweep.
            # Pad with RANDOM vertices at the far distance, never with self: a self-loop is
            # dropped as a non-edge, which leaves the vertex disconnected and UMAP returns
            # NaN coordinates for it. Random far edges keep it in the graph while
            # contributing essentially no membership at short range.
            short += 1
            ki[i, c:] = rng.integers(0, n, k - c)
            kd[i, c:] = far
    return ki, kd, short


def _scatter_dense(st, values):
    """Store rows -> a dense (n_src, n_src) matrix over the source set, NaN where unmeasured.

    Only honest when K covers every target, which is the case for an organism-scale sweep
    (1,489 reactions, K=1,489) and is asserted by the caller. NaN, not zero: for the
    similarity channels zero is "no current", but for ``dv`` zero means *coincident*, which
    is the one wrong answer an unmeasured entry could give.
    """
    src, rxn = st["meta"]["src"], st["meta"]["rxn"]
    spos = {r: i for i, r in enumerate(src)}
    col2row = np.array([spos.get(r, -1) for r in rxn], dtype=np.int64)
    n = len(src)
    M = np.full((n, n), np.nan, np.float64)
    idx = np.asarray(st["idx"])
    v = np.asarray(values, np.float64)
    done = np.asarray(st["done"]).astype(bool)
    rows = np.repeat(np.arange(n, dtype=np.int64), st["K"])
    cols = col2row[idx.ravel()]
    ok = np.repeat(done, st["K"]) & (cols >= 0) & np.isfinite(v.ravel())
    M[rows[ok], cols[ok]] = v.ravel()[ok]
    np.fill_diagonal(M, np.nan)
    return M


def channel_distance(st, channel, beta=1.0):
    """Dense pairwise distance for one channel.

    ``current`` and ``power`` are *similarities*: the pairwise convention is the larger of
    the two directions (the stronger connection wins, as ``max(I, I^T)`` does today), then
    ``log10(max/x)``. ``dv`` is already a distance, so it symmetrises by the *smaller* of the
    two directions and takes no log. ``combo`` adds the dv and power distances after
    normalising each to unit median, so ``beta`` is a dimensionless mixing weight -- the raw
    scales differ by about three orders and a literal sum would be whichever leg is bigger.
    """
    def sim(vals):
        M = _scatter_dense(st, vals)
        M = np.fmax(M, M.T)
        pos = M[np.isfinite(M) & (M > 0)]
        imax = float(pos.max()) if pos.size else 1.0
        with np.errstate(divide="ignore", invalid="ignore"):
            D = np.log10(imax / np.where(M > 0, M, np.nan))
        return D

    def drop():
        M = _scatter_dense(st, st["ch"]["dv"])
        M = np.maximum(M, 0.0)          # a solver-noise negative is "no separation", not -d
        return np.fmin(M, M.T)

    if channel == "current":
        return sim(st["val"])
    if channel == "power":
        return sim(st["ch"]["power"])
    if channel == "dv":
        return drop()
    if channel == "combo":
        a, b = drop(), sim(st["ch"]["power"])
        return a / np.nanmedian(a) + beta * (b / np.nanmedian(b))
    raise SystemExit(f"unknown channel {channel}")


def knn_from_dense(D, k):
    """Top-k nearest per row of a dense distance matrix, NaN pushed to the farthest measured
    distance -- never dropped, because a dropped row is a disconnected vertex and UMAP
    returns NaN coordinates for it."""
    far = float(np.nanmax(D))
    D = np.where(np.isfinite(D), D, far)
    np.fill_diagonal(D, np.inf)
    sel = np.argpartition(D, k, axis=1)[:, :k]
    ord_ = np.take_along_axis(D, sel, 1).argsort(1)
    ki = np.take_along_axis(sel, ord_, 1).astype(np.int32)
    kd = np.take_along_axis(D, ki.astype(np.int64), 1).astype(np.float32)
    return ki, kd


def incidence(gpr_table, src, element="C"):
    """Reaction -> the metabolites it moves an atom of ``element`` through.

    Rebuilt with the two calls the sweep itself made, so the drawn network is the one that
    was solved rather than a re-derivation that could disagree with it. Returns the
    metabolite names, the ``(row, metabolite)`` pairs, and how many reactions of the graph
    have no row in the table -- which should be none.
    """
    from atom_graph import build_atom_graph, restrict_to_giant
    from ieff_sweep import gpr_medium

    g, term, met_sym = build_atom_graph(element, gpr_medium(gpr_table))
    g, term = restrict_to_giant(g, term)
    row = {r: i for i, r in enumerate(src)}
    mets, pairs, missing = {}, [], 0
    for r, (subs, prods) in term.items():
        i = row.get(r)
        if i is None:
            missing += 1
            continue
        for a in subs | prods:
            m = met_sym[g.nodes[a][0]]
            pairs.append((i, mets.setdefault(m, len(mets))))
    # A reaction touches a metabolite through several atoms; the drawn edge is the incidence,
    # not the atom, so collapse them.
    pairs = np.unique(np.asarray(pairs, np.int64), axis=0)
    return list(mets), pairs, missing


def metabolite_positions(xy, pairs, n_met):
    """The mean of the positions of the reactions a metabolite is incident to.

    Derived from the layout and contributing nothing back to it: a metabolite carries no
    information its reactions did not already carry.
    """
    cnt = np.bincount(pairs[:, 1], minlength=n_met).astype(float)
    s = np.stack([np.bincount(pairs[:, 1], xy[pairs[:, 0], d], minlength=n_met)
                  for d in (0, 1)], axis=1)
    return s / cnt[:, None]


def scatter_index(xy, ii, rng, reps=40):
    """Mean pairwise 2D distance within a group / among random equal-size sets.
    1.0 = randomly placed; below 1.0 = the group holds together."""
    def mpd(jj):
        p = xy[jj]
        d = np.linalg.norm(p[:, None] - p[None, :], axis=-1)
        return d[np.triu_indices(len(jj), 1)].mean()
    base = np.mean([mpd(rng.choice(len(xy), len(ii), replace=False)) for _ in range(reps)])
    return mpd(ii) / base


def draw_network(ax, xy, mxy, pairs, bare=False, depth=None):
    """The reaction-metabolite drawing, laid over an existing layout.

    An edge's weight is 1/(its metabolite's degree). Currency metabolites are incident to
    hundreds of reactions and would otherwise draw hundreds of equally dark lines each, which
    is the whole of what makes this drawing a hairball; 1/degree cancels exactly that. Weight
    sets tone only -- it is a drawing decision, and nothing here feeds back into the layout.
    """
    # Quantile buckets, so the split adapts to the degree distribution instead of to an
    # absolute weight that means something different at every scale. Bucket 0 is the hub
    # edges and is not drawn at all: it is the densest bucket and the least informative, so
    # dropping it is most of the speed-up and most of the legibility.
    deg = np.bincount(pairs[:, 1], minlength=len(mxy))[pairs[:, 1]]
    w = 1.0 / deg
    cuts = np.quantile(w, np.linspace(0, 1, EDGE_BUCKETS + 1)[1:-1])
    b = np.searchsorted(cuts, w, side="right")
    seg = np.stack([xy[pairs[:, 0]], mxy[pairs[:, 1]]], axis=1)
    nb = np.bincount(b, minlength=EDGE_BUCKETS)
    print(f"edges: {len(pairs)} in {EDGE_BUCKETS} weight buckets {list(nb)}; "
          f"drew {int(nb[1:].sum())} in {EDGE_BUCKETS - 1} collections, "
          f"dropped {int(nb[0])} hub edges (degree >= {int(np.ceil(1.0 / cuts[0]))})",
          flush=True)
    for k in range(1, EDGE_BUCKETS):
        m = b == k
        if not m.any():
            continue
        ax.add_collection(LineCollection(
            seg[m], colors=EDGE_COLOR, linewidths=EDGE_WIDTH,
            alpha=EDGE_ALPHA[EDGE_BUCKETS - 1 - k], zorder=0))
    ax.scatter(mxy[:, 0], mxy[:, 1],
               s=MET_SIZE if depth is None else MET_SIZE * depth_scale(mxy[:, 1], *depth),
               c=BARE_NODE if bare else "#3a7bd5", alpha=1.0 if bare else 0.65,
               linewidths=0, zorder=1,
               label=f"metabolite (mean of its reactions, n={len(mxy)})")


def render(xy, labels, names, title, out, size=14, clip=1.5, net=None, palette=PALETTE,
           base_label="reaction (unassigned, n={n})", base_size=None, base_alpha=None,
           bare=False):
    fig, ax = plt.subplots(figsize=(9, 9))
    if bare:
        # The figure as pure structure: no title, no legend, and every reaction the same
        # grey. The pathway overlay is an annotation of the layout, not part of it, so
        # dropping it removes nothing the positions encode.
        labels = np.full(len(xy), "", dtype=object)
        names = {}
    if net is not None:
        draw_network(ax, xy, *net, bare=bare)
    base = labels == ""
    if base.any():
        # The defaults are sized for the 57k-point universe figure, where the unassigned
        # mass is background. On an organism-scale panel it is 89% of the points and has to
        # be legible, so both are overridable rather than tuned to one scale.
        ax.scatter(xy[base, 0], xy[base, 1],
                   s=(2 if net is None else 3 * MET_SIZE) if base_size is None else base_size,
                   c=BARE_NODE if bare else ("#cccccc" if net is None else "#9a9a9a"),
                   alpha=(1.0 if bare else (0.45 if net is None else 0.8))
                         if base_alpha is None else base_alpha,
                   linewidths=0, zorder=2,
                   label=base_label.format(n=int(base.sum())))
    for i, pid in enumerate(names):
        m = labels == pid
        if not m.any():
            continue
        ax.scatter(xy[m, 0], xy[m, 1], s=size, c=palette[i % len(palette)],
                   edgecolors="black", linewidths=0.25, zorder=3,
                   label=f"{names[pid][:34]} ({pid}, n={int(m.sum())})")
    if clip:
        # UMAP flings a handful of weakly-connected reactions far out, and on a square
        # canvas those few points cost most of the frame. Clip the view (not the data) to a
        # symmetric quantile band and say how many points fall outside it.
        lo, hi = np.percentile(xy, [clip, 100 - clip], axis=0)
        pad = 0.03 * (hi - lo)
        out_of_view = int((~((xy >= lo) & (xy <= hi)).all(1)).sum())
        ax.set_xlim(lo[0] - pad[0], hi[0] + pad[0])
        ax.set_ylim(lo[1] - pad[1], hi[1] + pad[1])
        print(f"view clipped at the {clip}/{100 - clip} percentile: "
              f"{out_of_view} of {len(xy)} points off-frame", flush=True)
    if not bare:
        ax.set_title(title, fontsize=11)
        ax.legend(loc="upper right", fontsize=7, markerscale=1.4, framealpha=0.9)
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    # Append, do not with_suffix: an --out whose name contains a dot (a beta, a leak) has
    # everything from that dot read as an extension and replaced, and two runs silently
    # render to the same file.
    png, svg = out.with_name(out.name + ".png"), out.with_name(out.name + ".svg")
    fig.savefig(png, dpi=250)
    fig.savefig(svg)
    plt.close(fig)
    print(f"wrote {png} and .svg", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, default=None)
    ap.add_argument("--sparse", type=Path, default=None,
                    help="a packed gpr_ieff table instead of a sweep store")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--n-neighbors", type=int, default=30)
    ap.add_argument("--min-dist", type=float, default=0.05)
    ap.add_argument("--spread", type=float, default=1.0,
                    help="UMAP's scale for the embedded distance; must be >= --min-dist")
    ap.add_argument("--radial-gamma", type=float, default=1.0,
                    help="post-hoc radial compression exponent about the layout centroid; "
                         "1.0 is a no-op, below 1.0 pulls outliers toward the median radius")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--title", default="MetaNetX reaction network, exact pairwise ECSPr "
                                       "I_eff distance")
    ap.add_argument("--knn-out", type=Path, default=None)
    ap.add_argument("--clip", type=float, default=1.5,
                    help="view percentile band; 0 disables")
    ap.add_argument("--base-size", type=float, default=None,
                    help="marker size for unassigned reactions; default suits the universe")
    ap.add_argument("--base-alpha", type=float, default=None,
                    help="marker alpha for unassigned reactions")
    ap.add_argument("--bare", action="store_true",
                    help="structure only: no title, no legend, no pathway colouring")
    ap.add_argument("--reuse-xy", action="store_true",
                    help="re-render from the saved coordinates instead of running UMAP")
    ap.add_argument("--xy", type=Path, default=None,
                    help="coordinate file to read/write (default: alongside --out). Point it "
                         "at another figure's coordinates to draw the same layout twice.")
    ap.add_argument("--origin-table", type=Path, default=None,
                    help="a gpr_union table; colour reactions by origin instead of by KEGG "
                         "pathway (the two cannot share the palette)")
    ap.add_argument("--highlight", action="append", default=[],
                    help="an origin value to draw as the highlighted clone; repeat")
    ap.add_argument("--gpr-table", type=Path, default=None,
                    help="draw metabolites and carbon-incidence edges from this host GPR "
                         "table; positions stay reaction-only")
    ap.add_argument("--channel", choices=["current", "power", "dv", "combo"], default=None,
                    help="lay out from a stored channel via the dense path instead of the "
                         "sparse top-K current graph; needs --store")
    ap.add_argument("--beta", type=float, default=1.0,
                    help="--channel combo: weight of the power leg against the dv leg, both "
                         "median-normalised first")
    ap.add_argument("--metrics-out", type=Path, default=None,
                    help="write the scatter indices to this JSON")
    args = ap.parse_args()

    if (args.store is None) == (args.sparse is None):
        raise SystemExit("give exactly one of --store or --sparse")
    if args.channel and args.store is None:
        raise SystemExit("--channel needs --store")
    if args.min_dist > args.spread:
        raise SystemExit("UMAP requires --min-dist <= --spread")
    st = load_sparse(args.sparse) if args.sparse else load_store(args.store)
    print(f"{'sparse table' if args.sparse else 'store'}: "
          f"{int(st['done'].sum())}/{st['n']} rows, K={st['K']}, "
          f"{len(st['meta']['rxn'])} target reactions", flush=True)

    xypath = args.xy or args.out.with_name(args.out.name + ".xy.npy")
    if args.reuse_xy:
        xy = np.load(xypath)
        print(f"reusing {xypath}", flush=True)
    else:
        if args.channel:
            # The dense path is only honest when every target has a column.
            if st["K"] < len(st["meta"]["rxn"]):
                raise SystemExit(f"--channel needs a dense store: K={st['K']} < "
                                 f"{len(st['meta']['rxn'])} targets")
            if args.channel in ("power", "dv") and args.channel not in st["ch"]:
                raise SystemExit(f"store has no {args.channel} channel")
            if args.channel == "combo" and {"power", "dv"} - set(st["ch"]):
                raise SystemExit("--channel combo needs both power and dv")
            D = channel_distance(st, args.channel, args.beta)
            ki, kd = knn_from_dense(D, args.n_neighbors)
            short = 0
            print(f"channel {args.channel}: measured {np.isfinite(D).mean():.4f} of pairs, "
                  f"distance p1/p50/p99 = "
                  f"{np.nanpercentile(D, [1, 50, 99]).round(4).tolist()}", flush=True)
        else:
            ki, kd, short = knn_from_store(st, args.n_neighbors)
        print(f"knn: {ki.shape}, {short} rows padded (fewer than k measured partners); "
              f"distance range {kd.min():.3f}..{kd.max():.3f}", flush=True)
        if args.knn_out:
            np.savez_compressed(args.knn_out, idx=ki, dist=kd,
                                src=np.array(st["meta"]["src"]))

        xy = umap.UMAP(n_components=2, n_neighbors=args.n_neighbors,
                       min_dist=args.min_dist, spread=args.spread,
                       init="random", random_state=args.seed,
                       precomputed_knn=(ki, kd, None)
                       ).fit_transform(np.zeros((st["n"], 1)))
        bad = ~np.isfinite(xy).all(1)
        if bad.any():
            raise RuntimeError(f"{int(bad.sum())} points have non-finite coordinates -- "
                               "UMAP disconnected them; the neighbour graph is incomplete")
        if args.radial_gamma < 1.0:
            # The retired network_concept.py is the definition of what a gamma means, so
            # import it rather than restating it here.
            from network_concept import compress_radial_outliers
            xy = compress_radial_outliers(xy, args.radial_gamma)
            print(f"radial compression at gamma={args.radial_gamma}", flush=True)

    src = st["meta"]["src"]
    rng = np.random.default_rng(7)
    kw, metrics = {}, {}
    if args.origin_table:
        labels, names, order = origin_labels(args.origin_table, src, args.highlight)
        names = {k: names[k] for k in order}
        kw = dict(palette=ORIGIN_PALETTE, size=6 if st["n"] < 4000 else 3,
                  base_label="reaction with no row in the origin table (n={n})")
        print("scatter index (1.0 = randomly placed, lower = the group holds together):")
        for k in names:
            ii = np.where(labels == k)[0]
            if len(ii) > 2:
                metrics[k] = float(scatter_index(xy, ii, rng))
            print(f"  {k:<14} n={len(ii):<6} {metrics[k]:.3f}" if len(ii) > 2 else
                  f"  {k:<14} n={len(ii)}", flush=True)
    else:
        members, names = pathway_members()
        lab = {}
        for pid in PANEL:
            for r in members.get(pid, ()):
                lab.setdefault(r, pid)
        labels = np.array([lab.get(r, "") for r in src])
        print("scatter index (1.0 = randomly placed, lower = pathway holds together):")
        for pid in PANEL:
            ii = np.where(labels == pid)[0]
            if len(ii) > 2:
                metrics[pid] = float(scatter_index(xy, ii, rng))
                print(f"  {pid} {names[pid][:34]:<36} n={len(ii):<4} "
                      f"{metrics[pid]:.3f}", flush=True)
    net, sub = None, ("reactions only; metabolites are the medium, not points")
    if args.gpr_table:
        met, pairs, missing = incidence(args.gpr_table, src)
        print(f"incidence: {len(src)} reactions, {len(met)} metabolites, {len(pairs)} edges; "
              f"{missing} graph reactions with no row in the table", flush=True)
        net = (metabolite_positions(xy, pairs, len(met)), pairs)
        sub = "metabolites drawn at the mean of their reactions, and do not enter the layout"
    render(xy, labels, names, f"{args.title}\n({sub}. Position = UMAP force projection)",
           args.out, clip=args.clip, net=net,
           base_size=args.base_size, base_alpha=args.base_alpha, bare=args.bare, **kw)
    if not args.reuse_xy:
        np.save(xypath, xy)
    if args.metrics_out:
        args.metrics_out.parent.mkdir(parents=True, exist_ok=True)
        json.dump(dict(channel=args.channel or "current(sparse)", beta=args.beta,
                       store=str(args.store or args.sparse), n=int(st["n"]),
                       scatter=metrics,
                       mean_scatter=float(np.mean(list(metrics.values()))) if metrics else None,
                       argv=sys.argv), open(args.metrics_out, "w"), indent=1)
        print(f"wrote {args.metrics_out}", flush=True)


if __name__ == "__main__":
    main()
