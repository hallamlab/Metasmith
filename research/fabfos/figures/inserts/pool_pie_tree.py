"""The enrichment lineage, with each pool's insert composition drawn on it.

SPEC
  The pool lineage as a tree, sampling day down the y axis, one donut per pool
  showing that pool's composition over the 170 resolved inserts. What it captures
  is collapse: the diverse founder narrows, generation by generation, until a
  single insert is most of a pool.

  A slice is an insert's share of its pool -- depth x length over the pool's
  total, so the quantity is DNA and not depth, the same share the length figure's
  abundance panel plots. The coloured slices are a within-insert normalisation,
  which is what the collapse claim rides on; they are then scaled down by the
  pool's mapped fraction so that a BLACK wedge can carry the unmapped reads.

  The black wedge is 1 - `fraction_reads_mapped` from `pool_summary.tsv`, the
  reads of that pool that hit neither an insert nor the pCC1fos backbone. It runs
  0.9-3.2% per pool, so it is a thin sliver by construction -- that is the
  measurement, not a drawing choice. (Over the 35 LIBRARIES the same quantity
  runs 1.2-3.8%; pool 01's three fold into one here.)

  Slices are bucketed GLOBALLY, by an insert's peak share across all pools, so a
  given insert is the same colour in every pool it appears in and never flips
  between coloured and grey:

      peak >= 3%        its own colour and NRC number
      1% <= peak < 3%   the darker grey aggregate
      peak < 1%         the light grey aggregate

  Inserts are numbered NRC 001.. by peak share. The colour key is a separate
  figure.

POOL 01 IS ONE NODE
  33 pools, 35 sequencing libraries: the founder was sequenced under three
  barcodes. The lineage has one pool 01, so its three libraries are summed into
  one donut. Drawing them separately would draw the common ancestor three times.

INPUT   data/fabfos/runs/scadc_fosmids/pools/pool_lineage.csv
        data/fabfos/runs/scadc_fosmids/sequences/insert_coverage/insert_coverage_matrix.tsv
        data/fabfos/runs/scadc_fosmids/sequences/insert_coverage/pool_summary.tsv
ENV     mamba run -n figure-net python main/figures/inserts/pool_pie_tree.py
OUT     cache/pool_pie_tree.{png,svg}
        cache/pool_pie_tree_legend.{png,svg}
"""
import argparse
import colorsys
import csv
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt              # noqa: E402
import numpy as np                           # noqa: E402
import pandas as pd                          # noqa: E402
from matplotlib.lines import Line2D          # noqa: E402
from matplotlib.patches import Circle, Wedge  # noqa: E402

from _common import COVERAGE, PLOTLY, POOLS, read_coverage_matrix, save

LINEAGE = POOLS / "pool_lineage.csv"
SUMMARY = COVERAGE / "pool_summary.tsv"

MIN_PCT, MID_PCT = 1.0, 3.0
COL_OTH = "#cdcdcd"
COL_MID = "#8f9294"
COL_UNMAPPED = "#000000"
COL_OUTLINE = "#333333"
COL_EDGE = "#d1d3d4"
COL_TEXT = "#212121"


def load_tree():
    raw = {}
    if not LINEAGE.exists():
        raise SystemExit(
            f"no lineage table at {LINEAGE}.\nThe day, parentage and vitamin arm "
            f"are experimental facts -- nothing in this repo derives them, and a "
            f"lineage-free grid of pies is not this figure.")
    with open(LINEAGE) as fh:
        for r in csv.DictReader(fh):
            raw[int(r["pool"])] = (
                int(r["day"]),
                r["with_vitamins"].strip().lower() == "true",
                int(r["parent"]) if r["parent"].strip() else None)
    pool_map = {p: (p, d, v) for p, (d, v, _par) in raw.items()}
    nodes = {pool_map[p]: (pool_map[par] if par is not None else None)
             for p, (_d, _v, par) in raw.items()}
    return nodes, pool_map


def load_composition():
    m = read_coverage_matrix()
    libs = [c for c in m.columns if c not in ("insert_id", "length")]
    bases = m[libs].to_numpy(float) * m["length"].to_numpy(float)[:, None]

    pool_of = np.array([int(c.split("_")[0].removeprefix("pool")) for c in libs])
    pools = sorted(set(pool_of.tolist()))
    merged = np.stack([bases[:, pool_of == p].sum(1) for p in pools], axis=1)
    tot = merged.sum(0)
    share = merged / np.where(tot > 0, tot, 1.0)

    comp = {p: list(zip(m["insert_id"], share[:, i])) for i, p in enumerate(pools)}
    peak = dict(zip(m["insert_id"], share.max(1)))
    return comp, peak


def load_unmapped():
    s = pd.read_csv(SUMMARY, sep="\t")
    s["pool_no"] = [int(p.split("_")[0].removeprefix("pool")) for p in s["pool"]]
    g = s.groupby("pool_no")[["reads", "mapped"]].sum()
    return {int(p): 1.0 - m / r for p, (r, m) in
            zip(g.index, g[["reads", "mapped"]].to_numpy(float))}


def build_palette(peak):
    order = sorted(peak, key=lambda c: peak[c], reverse=True)
    colours, labels = {}, {}
    for i, c in enumerate(order):
        loop = i // len(PLOTLY)
        base = PLOTLY[i % len(PLOTLY)]
        r, g, b = (int(base[j:j + 2], 16) / 255 for j in (1, 3, 5))
        h, s, v = colorsys.rgb_to_hsv(r, g, b)
        for _ in range(loop):
            s += (0.0 - s) * 0.4
            v += (1.0 - v) * 0.6
        h = (h - 0.02 * loop) % 1.0
        colours[c] = "#%02x%02x%02x" % tuple(
            int(x * 255) for x in colorsys.hsv_to_rgb(h, s, v))
        labels[c] = f"{i + 1:03d}"
    return order, colours, labels


def layout(nodes):
    children = defaultdict(list)
    for k, par in nodes.items():
        if par is not None:
            children[par].append(k)
    for par in children:
        children[par].sort(key=lambda k: k[0])

    days = sorted({d for _p, d, _v in nodes})
    ymap = {d: i for i, d in enumerate(days)}

    wmap = {}

    def widths(k):
        w = sum(widths(ch) for ch in children.get(k, [])) or 1
        wmap[k] = w
        return w

    pos = {}

    def place(k, left):
        pos[k] = (left + wmap[k] / 2.0, -ymap[k[1]])
        cl = left
        for ch in children.get(k, []):
            cl = place(ch, cl)
        return left + wmap[k]

    root = next(k for k, par in nodes.items() if par is None)
    total = widths(root)
    for k in list(wmap):
        wmap[k] /= total
    place(root, 0.0)

    def _find(pool):
        return next((k for k in pos if k[0] == pool), None)
    r, a, b = _find(1), _find(4), _find(17)
    if r and a and b:
        pos[r] = ((pos[a][0] + pos[b][0]) / 2.0, pos[r][1])
    p2, p9 = _find(2), _find(9)
    if p2 and p9:
        pos[p2] = (pos[p9][0], pos[p2][1])
    return pos, days, ymap


def draw(nodes, pos, days, ymap, comp, pool_map, colours, peak, unmapped):
    XW = 13.0
    pos = {k: (x * XW, y) for k, (x, y) in pos.items()}
    xs = [x for x, _ in pos.values()]
    R, INNER = 0.34, 0.42
    rpool = {v: k for k, v in pool_map.items()}

    fig, ax = plt.subplots(figsize=(15.5, 7.2))
    ax.set_aspect("equal")
    ax.axis("off")

    for k, par in nodes.items():
        if par is None or k not in pos or par not in pos:
            continue
        (x0, y0), (x1, y1) = pos[par], pos[k]
        ax.plot([x0, x1], [y0, y1], color=COL_EDGE, lw=1.4, zorder=1,
                solid_capstyle="round")

    for k, (x, y) in pos.items():
        slices = comp.get(rpool[k], [])
        big = sorted([(c, f) for c, f in slices if peak[c] >= MID_PCT / 100.0],
                     key=lambda t: t[1], reverse=True)
        mid = sum(f for c, f in slices
                  if MIN_PCT / 100.0 <= peak[c] < MID_PCT / 100.0)
        small = sum(f for c, f in slices if peak[c] < MIN_PCT / 100.0)

        ax.add_patch(Circle((x, y), R, facecolor="white",
                            edgecolor=COL_OUTLINE, lw=1.1, zorder=2))
        lost = unmapped.get(rpool[k], 0.0)
        keep = 1.0 - lost
        wedges = [(f * keep, colours[c]) for c, f in big]
        if mid > 0:
            wedges.append((mid * keep, COL_MID))
        if small > 0:
            wedges.append((small * keep, COL_OTH))
        if lost > 0:
            wedges.append((lost, COL_UNMAPPED))
        ang = 90.0
        for frac, col in wedges:
            if frac <= 0:
                continue
            sweep = 360.0 * frac
            ax.add_patch(Wedge((x, y), R, ang - sweep, ang, facecolor=col,
                               edgecolor="none", zorder=3))
            ang -= sweep
        ax.add_patch(Circle((x, y), R * INNER, facecolor="white", edgecolor="none",
                            zorder=4))
        ax.text(x, y, f"{rpool[k]:02d}", ha="center", va="center", fontsize=6.5,
                color=COL_TEXT, zorder=5)

    x_axis = min(xs) - 0.9
    y_top, y_bot = -min(ymap.values()), -max(ymap.values())
    ax.plot([x_axis, x_axis], [y_bot - 0.5, y_top + 0.5], color="black", lw=0.8,
            zorder=1)
    for d in days:
        y = -ymap[d]
        ax.plot([x_axis - 0.12, x_axis], [y, y], color="black", lw=0.8)
        ax.text(x_axis - 0.22, y, f"{d}", ha="right", va="center", fontsize=11)
    ax.text(x_axis - 0.62, (y_top + y_bot) / 2.0, "Day", ha="center", va="center",
            fontsize=12, rotation=90)

    fig.patch.set_facecolor("white")
    save(fig, "pool_pie_tree", dpi=200)
    plt.close(fig)


def legend(used, colours, labels):
    entries = ([(labels[c], colours[c]) for c in used]
               + [("1≤max<3%", COL_MID), ("max<1%", COL_OTH),
                  ("unmapped reads", COL_UNMAPPED)])
    handles = [Line2D([0], [0], marker="o", linestyle="", markersize=9,
                      markerfacecolor=col, markeredgecolor="none", label=lab)
               for lab, col in entries]
    ncol = 2
    rows = -(-len(entries) // ncol)
    fig = plt.figure(figsize=(2.6, 0.34 * (rows + 1.5)))
    ax = fig.add_subplot(111)
    ax.axis("off")
    leg = ax.legend(handles=handles, loc="center", frameon=False, fontsize=9,
                    ncol=ncol, title="NRC", handletextpad=0.35,
                    columnspacing=1.1, labelspacing=0.55)
    leg.get_title().set_fontsize(12)
    fig.patch.set_facecolor("white")
    save(fig, "pool_pie_tree_legend", dpi=200)
    plt.close(fig)


def generate():
    nodes, pool_map = load_tree()
    comp, peak = load_composition()
    unmapped = load_unmapped()
    order, colours, labels = build_palette(peak)
    pos, days, ymap = layout(nodes)
    draw(nodes, pos, days, ymap, comp, pool_map, colours, peak, unmapped)

    used = [c for c in order if peak[c] >= MID_PCT / 100.0]
    legend(used, colours, labels)
    dominant = [(rp, max(comp[rp], key=lambda t: t[1])) for rp in sorted(comp)]
    clonal = [(p, f) for p, (_c, f) in dominant if f >= 0.5]
    print(f"{len(nodes)} pools, {len(peak)} inserts; {len(used)} reach {MID_PCT:g}% "
          f"in some pool and get their own wedge")
    print(f"{len(clonal)} pools are more than half one insert: "
          + ", ".join(f"{p:02d} ({f:.0%})" for p, f in clonal))
    u = sorted(unmapped.values())
    print(f"the black wedge is {u[0]:.1%}-{u[-1]:.1%} of a pool "
          f"(median {u[len(u) // 2]:.1%})")


def main():
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    generate()


if __name__ == "__main__":
    main()
