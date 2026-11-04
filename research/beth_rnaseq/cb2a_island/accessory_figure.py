#!/usr/bin/env python3
"""Which JS4038 accessory regions each C. vibrioides genome in the PPanGGOLiN figure carries.

    python accessory_figure.py --paf-dir WORKDIR/rep --out accessory_regions.png

Each PAF is the region set (regions.fna, one record per region, named r<start>_<end>) aligned to
one genome with minimap2 -c -x asm20 --secondary=no, named <accession>.paf. Rows follow the
dendrogram order of the JS4038 genome announcement (doi 10.1128/mra.01001-25, Fig. 1).
"""

import argparse
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

INK, INK2 = "#0b0b0b", "#52514e"
REGIONS = [
    ("r475000_491000", "glycosyl-\ntransferase\ncluster\n0.48 Mb, 16 kb"),
    ("r790000_804000", "type I R-M\nsystem\n0.79 Mb, 14 kb"),
    ("r904800_910000", "repBAC insert\n(engineered)\n0.90 Mb, 5 kb"),
    ("r1525296_1645430", "metal-resistance\nICE\n1.53 Mb, 120 kb"),
    ("r2513000_2559000", "integrase/ParB\nelement\n2.51 Mb, 46 kb"),
    ("r3941500_3986000", "integrase/ParB\nelement\n3.94 Mb, 44 kb"),
]
GENOMES = [
    ("GCF_002310375.3", "CB13b1a"),
    (None, "CB2A JS4038"),
    ("GCA_002310295.2", "CB2"),
    ("GCF_002858865.1", "DSM 9893 (draft)"),
    ("GCF_000006905.1", "CB15"),
    ("GCA_003856435.1", "CB2A reference"),
    ("GCF_002310455.2", "CB1"),
    ("GCF_000022005.1", "NA1000"),
    ("GCF_019356235.1", "CB S2B lysogen"),
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
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    m = np.ones((len(GENOMES), len(REGIONS)))
    for i, (acc, _) in enumerate(GENOMES):
        if acc:
            c = coverage(Path(a.paf_dir) / f"{acc}.paf")
            m[i] = [c.get(r, 0) for r, _ in REGIONS]

    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    ax.imshow(m, cmap="Greys", vmin=0, vmax=1.15, aspect="auto")
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            ax.text(j, i, f"{m[i, j]:.0%}", ha="center", va="center", fontsize=7,
                    color="white" if m[i, j] > 0.5 else INK2)
    ax.set_xticks(range(len(REGIONS)), [t for _, t in REGIONS], fontsize=7)
    ax.xaxis.tick_top()
    ax.set_yticks(range(len(GENOMES)), [n for _, n in GENOMES])
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    for y in (0.5, 3.5):
        ax.axhline(y, color="white", lw=3)
    brackets = [(0, 0, "outgroup"), (1, 3, "cluster with JS4038"), (4, 8, "cluster with NA1000")]
    for y0, y1, text in brackets:
        ax.annotate("", xy=(5.62, y0 - 0.35), xytext=(5.62, y1 + 0.35), annotation_clip=False,
                    arrowprops=dict(arrowstyle="-", color=INK2, lw=1))
        ax.text(5.72, (y0 + y1) / 2, text, va="center", fontsize=7.5, color=INK2)
    ax.set_title("JS4038 accessory regions in the genomes of the PPanGGOLiN figure (share of each region aligned)",
                 loc="left", fontsize=9, pad=62)
    fig.savefig(a.out)


if __name__ == "__main__":
    main()
