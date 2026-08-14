"""Lay the reaction network out on a circle instead of the plane, host outside, clone inside.

The plane figure was drawn to show a fosmid insert *expanding* its host, and it answers the
wrong question: an insert whose reactions are threaded through the host network has no island
to point at, so "expansion" reads as scatter. On a circle the reading changes. The angle is
the whole layout -- one periodic coordinate fitted to the same exact pairwise I_eff distance --
and the radius is free to carry provenance instead. Host reactions occupy an outer band, the
insert's an inner one, and the question becomes visible as an arc: an insert that plugs into
one region of host metabolism is one inner segment, an insert that is spread through the host
is inner points all the way round.

**The circle is fitted, not projected from a 2D layout.** The embedding is one-dimensional
with a periodic output metric -- ``d(x,y) = |x-y|`` wrapped to the circumference -- handed to
UMAP's own generic optimiser, so the coordinate is what UMAP would choose *if the manifold
were a circle*. Taking the angle of a finished 2D layout would instead be a projection, and
would inherit whichever radial structure that layout happened to have. Initialisation is the
graph's own 2D spectral embedding read as an angle, which is the right start for a circle:
on a ring-like graph the first two nontrivial Laplacian eigenvectors *are* cos and sin of the
circular coordinate.

**Radius is provenance plus anti-overlap, and carries no measurement.** Within its band a
point is nudged outward only to stop it landing on a neighbour it is already at the same angle
as -- a circular beeswarm. Nothing is legible from radius except which layer a point is in.

Metabolites keep the rule the plane figures use: the mean of the positions of the reactions
they move an atom through. On a ring that rule earns something extra for free -- a metabolite
shared right around the circle averages to near the centre, so the promiscuous cofactors fall
into the middle and the local ones sit on their own arc.

    python ring_layout.py --sparse cache/pairwise_Ieff_epi300_clone2.npz \
        --origin-table .../gpr_epi300_clone2.parquet --highlight pool33_... \
        --gpr-table .../gpr_epi300_clone2.parquet --out cache/ring
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import numba
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                      # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from ieff_layout import (draw_network, incidence, knn_from_store,    # noqa: E402
                         load_sparse, load_store, metabolite_positions,
                         origin_labels, pathway_members, ORIGIN_PALETTE,
                         PALETTE, PANEL)

TAU = 2.0 * np.pi

# Radial bands, outermost first. A band is (inner, outer) radius; the beeswarm packs within
# it. The gap between host and clone is what makes the two layers read as separate rings
# without either being drawn.
BAND = {"host": (0.88, 1.00),
        "other": (0.88, 1.00),
        "clone_shared": (0.52, 0.64),
        "clone_new": (0.52, 0.64),
        "": (0.88, 1.00)}


@numba.njit(fastmath=True)
def circular_grad(x, y):
    """Distance on a circle of circumference TAU, and its gradient.

    The 1-D euclidean metric with the difference wrapped into [-TAU/2, TAU/2), which is what
    makes the coordinate periodic: a point near 0 and a point near TAU are neighbours, and the
    optimiser can carry a cluster across the seam instead of tearing it.
    """
    d = x[0] - y[0]
    d = d - 6.283185307179586 * np.floor(d / 6.283185307179586 + 0.5)
    grad = np.empty(1, dtype=x.dtype)
    if d > 0.0:
        grad[0] = 1.0
    elif d < 0.0:
        grad[0] = -1.0
    else:
        grad[0] = 0.0
    return abs(d), grad


def circ_dist(a, b):
    """Pairwise angular distance, wrapped. Broadcasts."""
    d = np.abs(a - b) % TAU
    return np.minimum(d, TAU - d)


def fit_circle(ki, kd, min_dist, spread, seed, n_epochs):
    """UMAP on a circle: the same fuzzy graph and the same optimiser, one periodic dimension."""
    from umap.umap_ import fuzzy_simplicial_set, find_ab_params, make_epochs_per_sample
    from umap.layouts import optimize_layout_generic
    from umap.spectral import spectral_layout
    import scipy.sparse as sp

    n = len(ki)
    G, _, _ = fuzzy_simplicial_set(
        X=sp.csr_matrix((n, 1)), n_neighbors=ki.shape[1],
        random_state=np.random.RandomState(seed), metric="precomputed",
        knn_indices=ki, knn_dists=kd)
    G = G.tocoo()
    keep = (G.data > 0) & (G.row != G.col)
    head, tail, w = G.row[keep], G.col[keep], G.data[keep]

    # Spectral init read as an angle. The two eigenvectors are only defined up to rotation and
    # sign, which is exactly the symmetry a circular coordinate has, so nothing is lost.
    init2 = np.asarray(spectral_layout(None, G.tocsr(), 2,
                                       np.random.RandomState(seed)), dtype=np.float64)
    theta = np.arctan2(init2[:, 1], init2[:, 0])
    bad = ~np.isfinite(theta)
    if bad.any():
        theta[bad] = np.random.RandomState(seed).uniform(0, TAU, int(bad.sum()))
    emb = np.ascontiguousarray(theta.reshape(-1, 1).astype(np.float32))

    w = w / w.max()
    eps = make_epochs_per_sample(w, n_epochs)
    a, b = find_ab_params(spread, min_dist)
    rng = np.random.RandomState(seed).randint(np.iinfo(np.int64).min,
                                              np.iinfo(np.int64).max, 3).astype(np.int64)
    print(f"circle fit: {len(head)} fuzzy edges, {n_epochs} epochs, a={a:.3f} b={b:.3f} "
          f"(min_dist={min_dist} spread={spread} radians on a {TAU:.3f}-circumference circle)",
          flush=True)
    emb = optimize_layout_generic(
        emb, emb, head.astype(np.int32), tail.astype(np.int32), n_epochs, n, eps, a, b, rng,
        gamma=1.0, initial_alpha=1.0, negative_sample_rate=5,
        output_metric=circular_grad, move_other=True)
    return np.asarray(emb[:, 0], dtype=np.float64) % TAU


def equalise(theta):
    """Respace the angles uniformly, keeping the cyclic order.

    UMAP clumps, so the fitted circle is dense arcs separated by empty ones and the host reads
    as a broken ring rather than a ring. This is the monotone reparameterisation that closes
    the gaps: the *only* thing a 1-D circular embedding carries beyond distances is the cyclic
    order of the points, and that survives exactly. What does not survive is the gaps -- after
    this, two adjacent clusters and two adjacent reactions look alike. All reported statistics
    are computed on the fitted angles for that reason; this affects presentation only.
    """
    order = np.argsort(theta)
    out = np.empty_like(theta)
    out[order] = np.arange(len(theta)) * (TAU / len(theta))
    return out


def beeswarm(theta, band, arc):
    """Radii within ``band``: the innermost slot free of a point already within ``arc``.

    Purely anti-overlap. Points are visited in angle order and each takes the lowest radial
    slot whose last occupant is more than ``arc`` radians behind it, so a sparse stretch of the
    circle stays a clean single ring and only a crowded one thickens.
    """
    lo, hi = band
    slots = max(int(np.ceil((hi - lo) / 0.010)), 1)
    radii = np.linspace(lo, hi, slots)
    last = np.full(slots, -np.inf)
    out = np.empty(len(theta))
    order = np.argsort(theta)
    for i in order:
        t = theta[i]
        s = next((s for s in range(slots) if t - last[s] > arc), int(np.argmin(last)))
        out[i] = radii[s]
        last[s] = t
    return out


def circular_scatter(theta, ii, rng, reps=60):
    """Mean pairwise angular distance within a group / among random equal-size sets.

    1.0 = the group is spread like a random selection of the same size; below 1.0 = it holds
    together on an arc. The same quantity ``ieff_layout.scatter_index`` reports in the plane,
    in the only coordinate this layout has.
    """
    def mpd(jj):
        t = theta[jj]
        d = circ_dist(t[:, None], t[None, :])
        return d[np.triu_indices(len(jj), 1)].mean()
    base = np.mean([mpd(rng.choice(len(theta), len(ii), replace=False)) for _ in range(reps)])
    return float(mpd(np.asarray(ii)) / base)


def arc_structure(theta, ii, all_theta):
    """How the group sits on the circle: resultant length, arc count, angular coverage.

    An arc break is a gap wider than 20x the mean spacing of *all* reactions, so "one segment"
    means the group is contiguous relative to the density of the layout it sits in rather than
    to an absolute angle.
    """
    t = np.sort(theta[ii])
    gap_cut = 20.0 * (TAU / len(all_theta))
    gaps = np.diff(np.concatenate([t, [t[0] + TAU]]))
    cut = gaps > gap_cut
    breaks = int(cut.sum())
    R = float(np.hypot(np.cos(t).mean(), np.sin(t).mean()))
    covered = float(gaps[~cut].sum() / TAU)
    # Members per arc, walking the circle from the first break so the wrap-around arc is one
    # arc rather than two.
    if breaks:
        start = int(np.flatnonzero(cut)[0]) + 1
        sizes, run = [], 0
        for j in range(len(t)):
            run += 1
            if cut[(start + j) % len(t)]:
                sizes.append(run)
                run = 0
        if run:
            sizes.append(run)
    else:
        sizes = [len(t)]
    return dict(n=int(len(ii)), resultant_length=R, arcs=max(breaks, 1),
                arc_gap_threshold_rad=float(gap_cut), circle_covered=covered,
                widest_gap_rad=float(gaps.max()),
                largest_arc=int(max(sizes)),
                largest_arc_frac=float(max(sizes) / len(t)),
                arc_sizes=sorted(sizes, reverse=True)[:8])


def render(theta, radius, labels, names, palette, title, out, net=None, size=9, base_size=6):
    fig, ax = plt.subplots(figsize=(10, 10))
    xy = np.stack([radius * np.cos(theta), radius * np.sin(theta)], axis=1)
    if net is not None:
        draw_network(ax, xy, *net)
    base = labels == ""
    if base.any():
        ax.scatter(xy[base, 0], xy[base, 1], s=base_size, c="#9a9a9a", alpha=0.8,
                   linewidths=0, zorder=2, label=f"reaction, no origin row (n={int(base.sum())})")
    for i, pid in enumerate(names):
        m = labels == pid
        if not m.any():
            continue
        ax.scatter(xy[m, 0], xy[m, 1], s=size, c=palette[i % len(palette)],
                   edgecolors="black", linewidths=0.2, zorder=3,
                   label=f"{names[pid][:44]} (n={int(m.sum())})")
    ax.set_aspect("equal")
    # Wrapped, not shrunk: matplotlib clips a long title at the axes edge without warning, so
    # a caption that reads fine in the shell silently loses its ends in the figure.
    import textwrap
    ax.set_title("\n".join(textwrap.fill(ln, 96) for ln in title.split("\n")), fontsize=10)
    ax.legend(loc="upper right", fontsize=7.5, markerscale=1.6, framealpha=0.92)
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    png, svg = out.with_name(out.name + ".png"), out.with_name(out.name + ".svg")
    fig.savefig(png, dpi=250)
    fig.savefig(svg)
    plt.close(fig)
    print(f"wrote {png} and .svg", flush=True)
    return xy


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", type=Path, default=None)
    ap.add_argument("--sparse", type=Path, default=None)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--n-neighbors", type=int, default=30)
    ap.add_argument("--min-dist", type=float, default=0.02,
                    help="radians; the circle's circumference is 2*pi")
    ap.add_argument("--spread", type=float, default=0.20, help="radians")
    ap.add_argument("--epochs", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--pack-arc", type=float, default=TAU / 700,
                    help="radians a point occupies for the anti-overlap beeswarm; roughly a "
                         "marker's own angular width at radius 1 on the default canvas")
    ap.add_argument("--origin-table", type=Path, default=None)
    ap.add_argument("--highlight", action="append", default=[])
    ap.add_argument("--gpr-table", type=Path, default=None,
                    help="draw the metabolites at the mean of their reactions")
    ap.add_argument("--equalise", action="store_true",
                    help="respace the fitted angles uniformly for drawing (order kept, gaps "
                         "closed); statistics stay on the fitted angles either way")
    ap.add_argument("--theta", type=Path, default=None)
    ap.add_argument("--reuse-theta", action="store_true")
    ap.add_argument("--title", default="Reaction network on a circle, exact pairwise ECSPr I_eff")
    ap.add_argument("--metrics-out", type=Path, default=None)
    args = ap.parse_args()

    if (args.store is None) == (args.sparse is None):
        raise SystemExit("give exactly one of --store or --sparse")
    st = load_sparse(args.sparse) if args.sparse else load_store(args.store)
    src = st["meta"]["src"]
    print(f"{'sparse table' if args.sparse else 'store'}: {int(st['done'].sum())}/{st['n']} "
          f"rows, K={st['K']}, {len(st['meta']['rxn'])} target reactions", flush=True)

    tpath = args.theta or args.out.with_name(args.out.name + ".theta.npy")
    if args.reuse_theta:
        theta = np.load(tpath)
        print(f"reusing {tpath}", flush=True)
    else:
        ki, kd, short = knn_from_store(st, args.n_neighbors)
        print(f"knn: {ki.shape}, {short} rows padded; distance range "
              f"{kd.min():.3f}..{kd.max():.3f}", flush=True)
        theta = fit_circle(ki, kd, args.min_dist, args.spread, args.seed, args.epochs)

    rng = np.random.default_rng(7)
    metrics = {}
    if args.origin_table:
        labels, names, order = origin_labels(args.origin_table, src, args.highlight)
        names = {k: names[k] for k in order if (labels == k).any()}
        palette = [ORIGIN_PALETTE[["clone_new", "clone_shared", "host", "other"].index(k)]
                   for k in names]
    else:
        members, pn = pathway_members()
        lab = {}
        for pid in PANEL:
            for r in members.get(pid, ()):
                lab.setdefault(r, pid)
        labels = np.array([lab.get(r, "") for r in src])
        names = {pid: pn[pid] for pid in PANEL if (labels == pid).any()}
        palette = PALETTE

    print("circular scatter index (1.0 = spread like a random set of the same size):")
    for k in names:
        ii = np.where(labels == k)[0]
        if len(ii) > 2:
            metrics[k] = dict(scatter=circular_scatter(theta, ii, rng),
                              **arc_structure(theta, ii, theta))
            m = metrics[k]
            print(f"  {k:<14} n={m['n']:<5} scatter={m['scatter']:.3f} "
                  f"R={m['resultant_length']:.3f} arcs={m['arcs']} "
                  f"(largest holds {m['largest_arc']}, {m['largest_arc_frac']:.0%}) "
                  f"covers {m['circle_covered']:.1%} of the circle", flush=True)

    # The KEGG panel is always measured, whatever the figure is coloured by: it is the check
    # that the circle kept the biology the plane layout holds, not an overlay.
    members, pn = pathway_members()
    kegg = {}
    for pid in PANEL:
        ii = np.array([i for i, r in enumerate(src) if r in members.get(pid, ())])
        if len(ii) > 2:
            kegg[pid] = circular_scatter(theta, ii, rng)
    if kegg:
        print(f"KEGG panel on the circle: mean scatter {np.mean(list(kegg.values())):.3f} "
              f"over {len(kegg)} pathways "
              f"({', '.join(f'{p}={v:.2f}' for p, v in kegg.items())})", flush=True)

    draw = equalise(theta) if args.equalise else theta
    radius = np.empty(len(draw))
    for k in set(labels):
        m = labels == k
        radius[m] = beeswarm(draw[m], BAND.get(k, BAND[""]), args.pack_arc)

    net = None
    sub = "reactions only; metabolites are the medium, not points"
    if args.gpr_table:
        met, pairs, missing = incidence(args.gpr_table, src)
        print(f"incidence: {len(src)} reactions, {len(met)} metabolites, {len(pairs)} edges; "
              f"{missing} graph reactions with no row in the table", flush=True)
        xy = np.stack([radius * np.cos(draw), radius * np.sin(draw)], axis=1)
        net = (metabolite_positions(xy, pairs, len(met)), pairs)
        sub = ("metabolites at the mean of their reactions -- a cofactor shared round the "
               "circle averages to the centre")
    angle = ("angle = UMAP fitted on a circle, respaced uniformly (order kept, gaps closed)"
             if args.equalise else "angle = UMAP fitted on a circle")
    render(draw, radius, labels, names, palette,
           f"{args.title}\n({angle}; radius = layer + anti-overlap only. {sub})",
           args.out, net=net)

    if not args.reuse_theta:
        np.save(tpath, theta)
    if args.metrics_out:
        args.metrics_out.parent.mkdir(parents=True, exist_ok=True)
        json.dump(dict(store=str(args.store or args.sparse), n=int(st["n"]),
                       groups=metrics, kegg_panel=kegg,
                       kegg_mean=float(np.mean(list(kegg.values()))) if kegg else None,
                       argv=sys.argv), open(args.metrics_out, "w"), indent=1)
        print(f"wrote {args.metrics_out}", flush=True)


if __name__ == "__main__":
    main()
