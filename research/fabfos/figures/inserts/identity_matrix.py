"""The identity matrix the inserts were cut out of, beside how long each one is.

SPEC
  Left: pairwise identity over all 669 clustering-input pieces, ordered so the
  170 inserts read as contiguous diagonal blocks. Order is a 1D UMAP of the
  identity distance, then each cluster's members are forced contiguous by their
  cluster-mean coordinate.

  Right: one length lollipop per insert, sitting at the vertical centre of that
  insert's block, so the two panels share a y axis. Length is the insert's own --
  the representative the pipeline shipped -- coloured blue inside the 29-44 kb
  lambda packaging window and black outside it. A zebra ruler at length 0 marks
  every other block, so cluster boundaries and sizes read straight off the base:
  singletons are fine stripes, big clusters are tall bars.

  Clustering is to the 170 INSERTS, not to the 272 pre-absorb clusters: 170 is
  the set every other table in this run is keyed on, and a lollipop panel keyed
  to a set nothing else names would be unreadable against them.

  The heatmap is MAX-pooled, never mean-pooled, when it is reduced to fit the
  raster. A thin high-identity band is one pixel wide and mean-pooling averages
  it into the background -- the reduction would erase exactly the signal the
  figure is about.

INPUT   identity.py -> pieces.py, and membership.csv / inserts.csv (./data)
ENV     mamba run -n figure-net python main/figures/inserts/identity_matrix.py
OUT     cache/identity_matrix.{png,svg}
        cache/identity_colorbar.{png,svg}
        cache/identity_length_legend.{png,svg}
        cache/identity_order.npy -- the row order, so anything else drawn against
        this matrix can reuse it instead of re-embedding into a different one
"""
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import numpy as np                   # noqa: E402
import pandas as pd                  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.transforms import blended_transform_factory, offset_copy  # noqa: E402

import identity
import pieces
from _common import C_A, CACHE, INK, INSERT_META, INSERTS, save

MEMBERSHIP = INSERT_META / "membership.csv"
INSERT_TABLE = INSERT_META / "inserts.csv"

WIN_LO, WIN_HI = 29.0, 44.0     # lambda packaging window (kb)
C_IN, C_OUT = C_A, INK          # lollipop head inside / outside the window
C_STEM = "#999999"
HEAD_ALPHA = 0.7
UMAP_SEED = 0                   # the layout is fixed; a re-run must not re-order


def load():
    """-> (labels, symmetric identity, cluster per piece, {cluster: length kb})."""
    meta, _seqs = pieces.load()
    labels, sym, _cont = identity.matrices()

    mem = pd.read_csv(MEMBERSHIP).set_index("contig")["centroid"]
    ins = pd.read_csv(INSERT_TABLE).set_index("centroid")["length"]
    clust = np.array([mem[meta[k]["piece"]] for k in labels], dtype=object)
    rep_kb = {c: L / 1000.0 for c, L in ins.items()}
    return labels, sym, clust, rep_kb


def order_rows(sym, clust, force=False):
    """1D UMAP of the identity distance, then cluster-contiguous. -> row order."""
    cache = CACHE / "identity_order.npy"
    if cache.exists() and not force:
        order = np.load(cache)
        if len(order) == sym.shape[0]:
            return order
    import umap

    dist = 1.0 - sym
    np.fill_diagonal(dist, 0.0)
    coord = umap.UMAP(n_components=1, metric="precomputed", n_neighbors=15,
                      random_state=UMAP_SEED).fit_transform(dist)[:, 0]
    cmean = {c: coord[clust == c].mean() for c in set(clust)}
    order = np.array(sorted(range(len(coord)),
                            key=lambda i: (cmean[clust[i]], coord[i])))
    CACHE.mkdir(parents=True, exist_ok=True)
    np.save(cache, order)
    return order


def maxpool(M, cap=600):
    """Reduce to at most `cap` cells a side by MAX pooling. No-op below `cap`."""
    n = M.shape[0]
    if n <= cap:
        return M
    r = int(np.ceil(n / cap))
    m = int(np.ceil(n / r))
    P = np.pad(M, [(0, m * r - n), (0, m * r - n)], constant_values=0.0)
    return P.reshape(m, r, m, r).max(axis=(1, 3))


def generate(force_order=False):
    labels, sym, clust, rep_kb = load()
    n = len(labels)
    order = order_rows(sym, clust, force=force_order)
    M = sym[np.ix_(order, order)]
    clust_ord = clust[order]

    centers, lens, spans = [], [], []
    for c in dict.fromkeys(clust_ord):          # blocks in display order
        pos = np.where(clust_ord == c)[0]
        centers.append(pos.mean())
        lens.append(rep_kb[c])
        spans.append((pos.min() - 0.5, pos.max() + 0.5))
    centers, lens = np.array(centers), np.array(lens)
    inwin = (lens >= WIN_LO) & (lens <= WIN_HI)
    colors = np.where(inwin, C_IN, C_OUT)

    fig = plt.figure(figsize=(7.4, 5.4), dpi=300)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 0.5], wspace=0.05)
    axm = fig.add_subplot(gs[0, 0])
    axb = fig.add_subplot(gs[0, 1], sharey=axm)

    axm.imshow(maxpool(M), cmap="gray_r", vmin=0, vmax=1, interpolation="nearest",
               aspect="auto", extent=(-0.5, n - 0.5, n - 0.5, -0.5))
    axm.set_box_aspect(1)
    axm.set_xticks([]); axm.set_yticks([])
    axm.set_xlabel("Pieces", fontsize=11)
    pad = n * 0.01
    axm.set_xlim(n - 0.5 + pad, -0.5 - pad)     # diagonal top-right -> bottom-left
    axm.set_ylim(n - 0.5 + pad, -0.5 - pad)

    for xv in (WIN_LO, WIN_HI):
        axb.axvline(xv, color=C_IN, ls="--", lw=0.8, zorder=0)
    axb.hlines(centers, 0, lens, color=C_STEM, lw=0.7, zorder=1)
    axb.scatter(lens, centers, s=9, c=colors, edgecolors="none",
                alpha=HEAD_ALPHA, zorder=2)

    ev = spans[::2]
    zlw, znudge = 3.5, 2.0
    zebra = axb.vlines([0] * len(ev), [lo for lo, _hi in ev], [hi for _lo, hi in ev],
                       color=C_OUT, lw=zlw, zorder=3, clip_on=False)
    # shifted left by half its own stroke plus a hair, so the ruler sits clear of
    # the stems rather than overprinting the zero end of every one
    zebra.set_transform(offset_copy(axb.transData, fig=fig, x=-zlw / 2 - znudge,
                                    y=0, units="points"))
    axb.set_ylim(n - 0.5 + pad, -0.5 - pad)
    axb.set_box_aspect(2)
    axb.set_xlim(0, 80)
    axb.set_xticks([0, 20, 40, 60, 80])
    axb.set_xticklabels(["", "20", "40", "60", "80"])   # x=0 belongs to the ruler
    axb.tick_params(labelleft=False, labelsize=9)
    axb.set_xlabel("Length (kb)", fontsize=11)
    for sp in ("top", "right", "left"):
        axb.spines[sp].set_visible(False)

    fig.canvas.draw()
    inv = axb.transAxes.inverted()
    bb = axb.xaxis.label.get_window_extent()
    x_c = inv.transform(((bb.x0 + bb.x1) / 2, 0))[0]
    y_top = inv.transform((0, bb.y1))[1]
    y_bottom = inv.transform((0, bb.y0))[1]
    axb.xaxis.set_label_coords(x_c - 0.03, y_top)

    tr = offset_copy(blended_transform_factory(axb.transData, axb.transAxes),
                     fig=fig, x=-zlw / 2 - znudge, y=0, units="points")
    axb.annotate("Every\nother\ninsert", xy=(0, 0.0), xycoords=tr,
                 xytext=(0, y_bottom), textcoords=tr,
                 ha="center", va="bottom", ma="center", fontsize=6, color=C_OUT,
                 arrowprops=dict(arrowstyle="-|>", color=C_OUT, lw=0.7,
                                 mutation_scale=6, shrinkA=1, shrinkB=1,
                                 relpos=(0.5, 1.0)),
                 annotation_clip=False)
    axb.text(WIN_HI + 2, 0.43, "λ packaging\n29–44 kb",
             transform=blended_transform_factory(axb.transData, axb.transAxes),
             ha="left", va="center", ma="center", fontsize=8, color=C_IN)

    fig.align_xlabels([axm, axb])
    save(fig, "identity_matrix")
    plt.close(fig)

    # colorbar and lollipop legend as their own figures, so the main panel keeps
    # its full width in a layout
    figc, axc = plt.subplots(figsize=(0.585, 4.4), dpi=300)
    cb = figc.colorbar(plt.cm.ScalarMappable(cmap="gray_r",
                                             norm=plt.Normalize(vmin=0, vmax=1)),
                       cax=axc)
    cb.set_label("Identity (nident / max(qlen, slen))", fontsize=9)
    cb.ax.tick_params(labelsize=9)
    save(figc, "identity_colorbar")
    plt.close(figc)

    figl, axl = plt.subplots(figsize=(1.7, 0.7), dpi=300)
    axl.axis("off")
    axl.legend(handles=[
        Line2D([0], [0], marker="o", ls="none", markerfacecolor=C_IN,
               markeredgecolor="none", markersize=8, alpha=HEAD_ALPHA,
               label="29–44 kb"),
        Line2D([0], [0], marker="o", ls="none", markerfacecolor=C_OUT,
               markeredgecolor="none", markersize=8, alpha=HEAD_ALPHA,
               label="< 29 or > 44 kb"),
    ], loc="center", frameon=False, fontsize=11, labelspacing=0.9,
        handletextpad=0.5)
    save(figl, "identity_length_legend")
    plt.close(figl)

    print(f"{n} pieces in {len(centers)} inserts; "
          f"{int(inwin.sum())} inserts in the {WIN_LO:g}-{WIN_HI:g} kb window, "
          f"{int((~inwin).sum())} outside "
          f"(median {np.median(lens):.1f} kb, max {lens.max():.1f} kb)")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reorder", action="store_true",
                    help="re-embed instead of reusing the cached row order")
    generate(force_order=ap.parse_args().reorder)


if __name__ == "__main__":
    main()
