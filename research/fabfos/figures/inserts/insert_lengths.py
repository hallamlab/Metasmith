"""How long the resolved inserts are, what was done to each, and how far each one took over.

SPEC  (three stacked panels sharing the length x-axis)
  All three panels key on ONE set -- the 170 inserts, at the length the insert
  table ships -- so a rug tick, the density under it and the abundance dot above
  it are the same insert at the same x.

  1. KDE CLOUD: a one-sided Gaussian KDE over all 170 lengths, drawn as a curve
     across the whole range but FILLED only between the 29-44 kb lambda
     packaging cutoffs, so the fill is the claim and the curve is the context.
     The window is grey, with dashed cutoffs, and is drawn on THIS panel only --
     no hue, so the only colour on the figure is insert identity.
  2. JUNCTION BARCODE: one tick per insert at its length. What the tick's SHAPE
     encodes is how many vector junctions the insert closed on -- 2 (both ends)
     draws the full-height line, 1 is truncated at the bottom, 0 is truncated at
     both ends and is the shortest. Three lengths, nothing else. A custom legend
     to the left of the barcode shows the three cases.
  3. MAX RELATIVE ABUNDANCE: per insert, the largest share of any single pool it
     reaches, on a log y axis floored at 1%. Inserts that never clear it are
     parked as down triangles underneath -- their exact value is not the point.
     The resolved winners tower toward 100%: a pool has gone clonal on them.
     A decade is a third of this panel, so the top four points sit within a
     marker of each other and are told apart by COLOUR, not height: NRC 001 and
     002 are the two at 100%, 003 is at 90% and 004 at 54%. Read them off the
     pie tree's legend.

  Share is base-weighted -- depth x length over the pool's total -- because that
  is what "this clone is most of this pool" names. `main/figures/concept/
  dominant_clone.py` ranks inserts on the same quantity for a different question.

ONE COLOUR KEY, SHARED WITH THE POOL TREE
  Both the barcode tick and the abundance dot carry the insert's colour from
  `pool_pie_tree`, so `cache/pool_pie_tree_legend.svg` is the key for both
  figures and an insert is the same colour wherever it appears. That palette is
  IMPORTED, not restated: `load_composition` and `build_palette` are called
  here, which also settles what the share is. The pie tree merges pool 01's
  three barcodes into one node before taking the share, and this panel used to
  take its max over the 35 libraries instead -- a 1.6-point difference on eight
  pool-01 inserts, and two figures claiming to plot the same quantity. The
  merged share is now the only one computed.

  Only the 23 inserts the pie tree gives a wedge to (peak >= 3%) are coloured.
  The other 147 stay in ink rather than taking the tree's two greys, because
  135 of them are its `max<1%` light grey and a barcode drawn in it is not a
  barcode.

JUNCTIONS ARE READ, EDIT STATUS IS RECOMPUTED
  `inserts.csv`'s `ends` column is the junction count and is taken verbatim --
  closure is not recomputed here (see `pieces.py`, NO BACKBONE BLAST). The cut's
  action still comes from `pieces.py` re-running the pipeline's own cut, because
  this run ships no action column and the obvious substitute -- does the id's
  coordinate range span the whole contig -- calls untouched graph-circular
  contigs trimmed. It is reported in the printout, not drawn.

INPUT   pieces.py (actions), pool_pie_tree.py (shares + palette), and
        inserts.csv / insert_coverage_matrix.tsv (./data)
ENV     mamba run -n figure-net python main/figures/inserts/insert_lengths.py
OUT     cache/insert_lengths.{png,svg}
        cache/insert_lengths_legend.{png,svg}
"""
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import numpy as np                   # noqa: E402
import pandas as pd                  # noqa: E402
from matplotlib.ticker import LogLocator, NullFormatter   # noqa: E402

import pieces
import pool_pie_tree
from _common import INK, INSERT_META, save

INSERT_TABLE = INSERT_META / "inserts.csv"

WIN = (29.0, 44.0)
WIN_GREY = "#8a8a8a"
KEYED = pool_pie_tree.MID_PCT / 100.0
KDE_BW_KB = 1.00
GRID_STEP_KB = 0.20
KDE_HEIGHT = 0.275
FLOOR = 0.01
TRI_Y = 0.0068
BAR_LW = 0.3
STEM_LW = 0.6
RUG_ALPHA = 0.85

ENDS = [2, 1, 0]
SPAN = {2: (0.00, 1.00), 1: (0.30, 1.00), 0: (0.30, 0.70)}


def load():
    ins = pd.read_csv(INSERT_TABLE)
    _seqs, actions = pieces.by_piece_id()
    missing = set(ins["centroid"]) - set(actions)
    if missing:
        raise SystemExit(f"{len(missing)} inserts have no rebuilt piece, "
                         f"e.g. {sorted(missing)[:3]}")

    comp, peak_of = pool_pie_tree.load_composition()
    _order, colours, _labels = pool_pie_tree.build_palette(peak_of)
    peak = pd.Series(peak_of)

    ins["length_kb"] = ins["length"] / 1000.0
    ins["action"] = ins["centroid"].map(actions)
    ins["ends"] = ins["ends"].astype(int)
    if not set(ins["ends"]) <= set(SPAN):
        raise SystemExit(f"unexpected junction counts: {sorted(set(ins['ends']))}")
    ins["max_share"] = ins["centroid"].map(peak)
    if ins["max_share"].isna().any():
        raise SystemExit("an insert is missing from the coverage matrix")
    ins["colour"] = [colours[c] if s >= KEYED else INK
                     for c, s in zip(ins["centroid"], ins["max_share"])]
    return ins, len(comp)


def kde(data, grid, h):
    d = np.asarray(data, float)
    u = (grid[:, None] - d[None, :]) / h
    return np.exp(-0.5 * u * u).sum(1) / (len(d) * h * np.sqrt(2 * np.pi))


def junction_key(ax_rug):
    kax = ax_rug.inset_axes([-0.100, 0.0, 0.085, 2.6], zorder=5)
    kax.set_xlim(0, 3)
    kax.set_ylim(0, 2.6)
    kax.set_xticks([])
    kax.set_yticks([])
    kax.patch.set_visible(False)
    for sp in kax.spines.values():
        sp.set_visible(False)

    xs = {0: 0.6, 1: 1.5, 2: 2.4}
    for e in ENDS:
        lo_y, hi_y = SPAN[e]
        kax.vlines(xs[e], lo_y, hi_y, color=INK, lw=BAR_LW, alpha=RUG_ALPHA)
        kax.text(xs[e], 1.10, str(e), ha="center", va="bottom", fontsize=8,
                 color=INK)
    kax.text(1.5, 1.62, "N junctions", ha="center", va="bottom", fontsize=8,
             color=INK)


def generate():
    ins, n_pools = load()
    lengths = ins["length_kb"].to_numpy()
    counts = ins["ends"].value_counts().to_dict()

    fig, (ax, ax_rug, ax2) = plt.subplots(
        3, 1, figsize=(6.6, 3.2), dpi=300, sharex=True,
        gridspec_kw=dict(height_ratios=[1.6, 0.5, 1.15], hspace=0.06))

    ax.axvspan(*WIN, color=WIN_GREY, alpha=0.16, zorder=0)
    for x in WIN:
        ax.axvline(x, color=WIN_GREY, lw=0.8, ls="--", zorder=1)

    grid = np.arange(0, lengths.max() + 4, GRID_STEP_KB)
    dens = kde(lengths, grid, KDE_BW_KB)
    dens = dens / dens.max() * KDE_HEIGHT
    ax.fill_between(grid, 0, dens, where=(grid >= WIN[0]) & (grid <= WIN[1]),
                    color="#dcdcdc", alpha=0.8, lw=0, zorder=2)
    ax.plot(grid, dens, color="#9e9e9e", lw=1.0, zorder=3)
    ax.axhline(0, color="#555", lw=0.8, zorder=4)
    ax.text(sum(WIN) / 2, KDE_HEIGHT + 0.03, "λ packaging\n29–44 kb", ha="center",
            va="bottom", fontsize=8, color=WIN_GREY)
    ax.set_ylim(-0.02, KDE_HEIGHT + 0.22)
    ax.set_yticks([])
    ax.tick_params(bottom=False, labelbottom=False)
    for sp in ("top", "right", "left", "bottom"):
        ax.spines[sp].set_visible(False)

    bar = ins.sort_values("max_share")
    ax_rug.vlines(bar["length_kb"],
                  [SPAN[e][0] for e in bar["ends"]],
                  [SPAN[e][1] for e in bar["ends"]],
                  colors=bar["colour"].tolist(), lw=BAR_LW, alpha=RUG_ALPHA,
                  zorder=4)
    ax_rug.set_ylim(0.0, 1.0)
    ax_rug.set_yticks([])
    ax_rug.tick_params(bottom=False, labelbottom=False)
    for sp in ("top", "right", "left", "bottom"):
        ax_rug.spines[sp].set_visible(False)
    junction_key(ax_rug)

    ax2.axhline(FLOOR, color="#bbb", lw=0.6, ls=":", zorder=1)
    for keyed, z in ((False, 4), (True, 6)):
        sub = ins[(ins["max_share"] >= KEYED) == keyed]
        edge = INK if keyed else "white"
        hi = sub[sub["max_share"] >= FLOOR]
        lo = sub[sub["max_share"] < FLOOR]
        if len(hi):
            ax2.vlines(hi["length_kb"], FLOOR, hi["max_share"],
                       colors=hi["colour"].tolist(), lw=STEM_LW, alpha=RUG_ALPHA,
                       zorder=z)
            ax2.scatter(hi["length_kb"], hi["max_share"], s=13,
                        c=hi["colour"].tolist(), alpha=RUG_ALPHA,
                        edgecolors=edge, linewidths=0.4, zorder=z + 1)
        if len(lo):
            ax2.scatter(lo["length_kb"], [TRI_Y] * len(lo), s=15, marker="v",
                        c=lo["colour"].tolist(), alpha=RUG_ALPHA,
                        edgecolors=edge, linewidths=0.4, zorder=z + 1)

    ax2.set_xlim(0, lengths.max() + 4)
    ax2.set_yscale("log")
    ax2.set_ylim(0.005, 1.5)
    ax2.set_xticks(list(range(10, int(lengths.max()) + 1, 10)))
    ax2.set_yticks([0.01, 0.1, 1.0])
    ax2.set_yticklabels(["1", "10", "100"])
    ax2.yaxis.set_minor_locator(
        LogLocator(base=10.0, subs=(2, 3, 4, 5, 6, 7, 8, 9), numticks=100))
    ax2.yaxis.set_minor_formatter(NullFormatter())
    ax2.tick_params(axis="y", which="minor", length=2.5, color="#888")
    ax2.set_ylabel("Max relative\nabundance (%)", fontsize=9)
    ax2.set_xlabel("Resolved insert length (kb)", fontsize=11)
    for sp in ("top", "right"):
        ax2.spines[sp].set_visible(False)

    save(fig, "insert_lengths")
    plt.close(fig)

    lfig, lax = plt.subplots(figsize=(2.4, 0.9), dpi=300)
    lax.axis("off")
    handles = [plt.Line2D([0], [0], marker="v", color=INK, linestyle="None",
                          markersize=7, markeredgecolor="white",
                          markeredgewidth=0.3, label="max < 1%")]
    lax.legend(handles=handles, loc="center", frameon=False, fontsize=9,
               handlelength=1.6, labelspacing=0.7)
    lfig.tight_layout()
    save(lfig, "insert_lengths_legend")
    plt.close(lfig)

    inwin = ((lengths >= WIN[0]) & (lengths <= WIN[1])).sum()
    junc = "  ".join(f"{e} junctions {counts.get(e, 0)}" for e in ENDS)
    print(f"{len(ins)} inserts over {n_pools} pools; {junc}; "
          f"{inwin} in the {WIN[0]:g}-{WIN[1]:g} kb window; "
          f"median {np.median(lengths):.1f} kb")
    print("action  " + "  ".join(f"{k} {v}" for k, v in
                                 sorted(ins['action'].value_counts().items())))
    print(f"max share {ins['max_share'].max():.3f}; "
          f"{int((ins['max_share'] >= 0.5).sum())} inserts take over half a pool; "
          f"{int((ins['max_share'] < FLOOR).sum())} never clear 1%")


def main():
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    generate()


if __name__ == "__main__":
    main()
