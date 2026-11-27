#!/usr/bin/env python3
"""Which JS4038 accessory regions each genome of a PPanGGOLiN tile plot carries, beside its tree.

    python accessory_figure.py --paf-dir WORKDIR/rep --tree tile_plot.html --out accessory_regions.png

Each PAF is the region set (regions.fna, one record per region, named r<start>_<end>) aligned to
one genome with minimap2 -c -x asm20 --secondary=no, named <accession>.paf.
"""

import argparse
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import ppanggolin_tree as tree

INK, INK2 = "#0b0b0b", "#52514e"
REGIONS = [
    ("r475000_491000", "glycosyl-\ntransferase\ncluster\n0.48 Mb, 16 kb"),
    ("r790000_804000", "type I R-M\nsystem\n0.79 Mb, 14 kb"),
    ("r904800_910000", "repBAC insert\n(engineered)\n0.90 Mb, 5 kb"),
    ("r1525296_1645430", "metal-resistance\nICE\n1.53 Mb, 120 kb"),
    ("r2513000_2559000", "integrase/ParB\nelement\n2.51 Mb, 46 kb"),
    ("r3941500_3986000", "integrase/ParB\nelement\n3.94 Mb, 44 kb"),
]

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8, "figure.dpi": 200, "savefig.bbox": "tight"})


def coverage(paf):
    iv, length = defaultdict(list), {}
    for line in open(paf):
        f = line.split("\t")
        length[f[0]] = int(f[1])
        if int(f[11]) >= 5:
            iv[f[0]].append((int(f[2]), int(f[3])))
    out = {}
    for q, n in length.items():
        total, end = 0, -1
        for s, e in sorted(iv[q]):
            if s > end:
                total += e - s
            elif e > end:
                total += e - end
            end = max(end, e)
        out[q] = total / n
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--paf-dir", required=True)
    ap.add_argument("--tree", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    leaves, links, families = tree.load(a.tree)
    names = ["CB2A JS4038 (not in the tree)", ""]
    m = np.full((len(leaves) + 2, len(REGIONS)), np.nan)
    m[0] = 1
    for i, leaf in enumerate(leaves, start=2):
        acc, label = tree.GENOMES[leaf]
        c = coverage(Path(a.paf_dir) / f"{acc}.paf")
        m[i] = [c.get(r, 0) for r, _ in REGIONS]
        names.append(label.split(" — ")[0].replace("C. vibrioides ", "") + (" †" if not families[leaf] else ""))

    fig = plt.figure(figsize=(10, 5.4))
    gs = fig.add_gridspec(1, 3, width_ratios=[1.1, 1.8, 7.5], wspace=0)
    ax = fig.add_subplot(gs[0, 2])
    ad = fig.add_subplot(gs[0, 0])
    ax.imshow(np.ma.masked_invalid(m), cmap="Greys", vmin=0, vmax=1.15, aspect="auto")
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            if not np.isnan(m[i, j]):
                ax.text(j, i, f"{m[i, j]:.0%}", ha="center", va="center", fontsize=7,
                        color="white" if m[i, j] > 0.5 else INK2)
    ax.set_xticks(range(len(REGIONS)), [t for _, t in REGIONS], fontsize=7)
    ax.xaxis.tick_top()
    ax.set_yticks(range(len(names)), names)
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ylim = ax.get_ylim()
    # Leaf k is image row 2 + k, counted downward; draw() puts it at y = -k on an upward axis.
    tree.draw(ad, links, 0)
    ad.set_ylim(2 - ylim[0], 2 - ylim[1])
    ax.set_title("JS4038 accessory regions across the genomes of the PPanGGOLiN plot (share of each region aligned)",
                 loc="left", fontsize=9, pad=62)
    fig.text(0.1, 0.02, "† No gene families for this genome in the PPanGGOLiN matrix, so its place in the tree is "
             "not informative.", fontsize=6.5, color=INK2)
    fig.savefig(a.out)


if __name__ == "__main__":
    main()
