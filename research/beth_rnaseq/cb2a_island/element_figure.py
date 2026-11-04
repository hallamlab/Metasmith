#!/usr/bin/env python3
"""Map of the CB2A JS4038 integrative element and which other genomes carry it.

    python element_figure.py --gff genomic.gff --bacmet bacmet_best_hit_per_gene_with_TPM.csv \
        --paf-dir WORKDIR --ani ani_edges.tsv --out element_map.png

Each PAF in --paf-dir is the element (JS4038 1,525,296-1,645,430) aligned to one genome with
minimap2 -c -x asm20 --secondary=no, named paf_<accession>.paf. --ani is skani triangle -E output
over the same genomes. Genomes are grouped by the PPanGGOLiN clusters of the JS4038 genome
announcement (doi 10.1128/mra.01001-25, Fig. 1).
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyArrow, Rectangle

E0, E1 = 1525296, 1645430
TRNA = (1525219, 1525295)
BLUE, ORANGE, AQUA, GREY, INK, INK2, LIGHT = "#2a78d6", "#eb6834", "#1baf7a", "#b4b3ad", "#0b0b0b", "#52514e", "#e4e3de"
MOBILITY = r"(?i:recombinase|integrase|partition|relaxase|conjugal|conjugative|type IV secretion|VirB|Trb[A-Z]|entry exclusion)"

JS4038 = "GCF_052446975.1"
GROUPS = [
    ("PPanGGOLiN cluster with JS4038", [
        (JS4038, "C. vibrioides CB2A JS4038 (this genome)"),
        ("GCA_002310295.2", "C. vibrioides CB2 — tap water, California"),
        ("GCF_002858865.1", "C. vibrioides DSM 9893 (draft)"),
    ]),
    ("PPanGGOLiN cluster with NA1000", [
        ("GCA_003856435.1", "C. vibrioides CB2A reference"),
        ("GCF_000006905.1", "C. vibrioides CB15"),
        ("GCF_002310455.2", "C. vibrioides CB1"),
        ("GCF_000022005.1", "C. vibrioides NA1000"),
        ("GCF_019356235.1", "C. vibrioides CB S2B lysogen"),
    ]),
    ("PPanGGOLiN outgroup", [
        ("GCF_002310375.3", "C. vibrioides CB13b1a"),
    ]),
    ("Other Caulobacter (not in the PPanGGOLiN figure)", [
        ("GCF_044056025.1", "Caulobacter sp. UC70_42 — protist culture, USA"),
        ("GCF_005502685.1", "Caulobacter sp. 3R27C2-B — alpine spring, Italy"),
        ("GCF_002742635.1", "Caulobacter sp. X — freshwater, New York"),
        ("GCF_001556585.1", "Caulobacter sp. CCH5-E12 — shower-hose biofilm, Ohio"),
    ]),
]

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8, "figure.dpi": 200, "savefig.bbox": "tight",
                     "axes.spines.top": False, "axes.spines.right": False})


def load_paf(p):
    rows = []
    for line in open(p):
        f = line.split("\t")
        if int(f[11]) >= 5:
            rows.append((int(f[2]), int(f[3]), int(f[9]) / int(f[10])))
    return rows


def ani_to_js4038(path):
    d = pd.read_csv(path, sep="\t")
    acc = lambda p: Path(p).name[:15]
    out = {}
    for r in d.itertuples():
        a, b = acc(r.Ref_file), acc(r.Query_file)
        if JS4038 in (a, b):
            out[b if a == JS4038 else a] = r.ANI
    return out


def merged_length(rows):
    total, end = 0, -1
    for s, e, _ in sorted(rows):
        if s > end:
            total += e - s
        elif e > end:
            total += e - end
        end = max(end, e)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gff", required=True)
    ap.add_argument("--bacmet", required=True)
    ap.add_argument("--paf-dir", required=True)
    ap.add_argument("--ani", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    gff = pd.read_csv(a.gff, sep="\t", comment="#", header=None,
                      names="seq src type start end score strand phase attr".split())
    cds = gff[(gff.type == "CDS") & (gff.start >= E0 - 2000) & (gff.end <= E1 + 2000)].copy()
    cds["gene_id"] = cds.attr.str.extract(r"locus_tag=([^;]+)")
    cds["product"] = cds.attr.str.extract(r"product=([^;]+)")
    cds = cds.drop_duplicates("gene_id")
    bac = pd.read_csv(a.bacmet)
    strong = set(bac[(bac.pident >= 40) & (bac.qcov_pct >= 70)].gene_id)
    cds["module"] = "other"
    cds.loc[cds["product"].str.contains(MOBILITY, na=False), "module"] = "mobility"
    cds.loc[cds.gene_id.isin(strong), "module"] = "metal"
    colour = {"mobility": AQUA, "metal": ORANGE, "other": GREY}

    ani = ani_to_js4038(a.ani)
    n_rows = sum(len(g) for _, g in GROUPS) + len(GROUPS)
    fig, (am, at) = plt.subplots(2, 1, figsize=(11, 3 + 0.27 * n_rows), sharex=True,
                                 gridspec_kw={"height_ratios": [1.3, 0.45 * n_rows], "hspace": 0.06})
    kb = lambda x: (x - E0) / 1e3
    for r in cds.itertuples():
        x0, x1 = kb(r.start), kb(r.end)
        y = 0.65 if r.strand == "+" else 0.35
        dx = (x1 - x0) if r.strand == "+" else -(x1 - x0)
        am.add_patch(FancyArrow(x1 - (x1 - x0) if r.strand == "+" else x1, y, dx, 0, width=0.18, head_width=0.26,
                                head_length=min(0.6, abs(dx) * 0.4), length_includes_head=True,
                                color=colour[r.module], lw=0))
    am.add_patch(Rectangle((kb(TRNA[0]), 0.15), kb(TRNA[1]) - kb(TRNA[0]) + 0.3, 0.7, color=BLUE, lw=0))
    notes = [(1525254, "tRNA-Arg site, recombinase", 1.02), (1535771, "ParB", 1.22), (1556817, "relaxase, TraG", 1.02),
             (1574521, "Trb/VirB type IV secretion", 1.22), (1586582, "metal-resistance island (RS07640–RS07925) →", 1.02)]
    for pos, text, y in notes:
        am.text(kb(pos), y, text, fontsize=7, color=INK, ha="left", va="bottom")
    am.set_ylim(0, 1.45)
    am.set_yticks([0.35, 0.65], ["− strand", "+ strand"])
    am.spines["left"].set_visible(False)
    am.set_title("CB2A JS4038: a ~120 kb integrative conjugative element at tRNA-Arg carries the metal-resistance island",
                 loc="left", fontsize=10)
    handles = [Rectangle((0, 0), 1, 1, color=c) for c in (AQUA, ORANGE, GREY, BLUE)]
    fig.legend(handles, ["integration / conjugation", "BacMet metal-resistance hit (strong)", "other / hypothetical",
                         "tRNA-Arg"], fontsize=7, frameon=False, ncol=4, loc="lower left", bbox_to_anchor=(0.08, -0.025))

    cmap = plt.get_cmap("Greys")
    y = n_rows - 1
    for title, genomes in GROUPS:
        at.text(-1.5, y, title, va="center", ha="right", fontsize=7.5, color=INK, fontweight="bold")
        y -= 1
        for acc, label in genomes:
            at.add_patch(Rectangle((0, y - 0.3), kb(E1), 0.6, color=LIGHT, lw=0))
            p = Path(a.paf_dir) / f"paf_{acc}.paf"
            rows = load_paf(p) if p.exists() else []
            for s, e, ident in rows:
                at.add_patch(Rectangle((s / 1e3, y - 0.3), (e - s) / 1e3, 0.6,
                                       color=cmap(0.35 + 0.65 * max(0, (ident - 0.7) / 0.3)), lw=0))
            frac = merged_length(rows) / (E1 - E0 + 1)
            at.text(kb(E1) + 1.5, y, f"{frac:.0%}", va="center", fontsize=7, color=INK2)
            at.text(kb(E1) + 10, y, "—" if acc == JS4038 else f"{ani[acc]:.2f}%", va="center", fontsize=7,
                    color=INK2)
            at.text(-1.5, y, label, va="center", ha="right", fontsize=7.5, color=INK)
            y -= 1
    at.set_ylim(-0.7, n_rows - 0.3)
    at.set_yticks([])
    at.spines["left"].set_visible(False)
    at.set_xlim(-0.5, kb(E1) + 19)
    at.set_xlabel("kb along the element (JS4038 chromosome 1.525–1.645 Mb)")
    at.text(kb(E1) + 1.5, n_rows - 0.35, "element\ncovered", fontsize=7, color=INK2, va="bottom")
    at.text(kb(E1) + 10, n_rows - 0.35, "genome ANI\nto JS4038", fontsize=7, color=INK2, va="bottom")
    fig.text(0.08, -0.04, "Light background = no alignment (minimap2 -c, MAPQ ≥5). Bar shade = alignment identity, "
             "grey 70% to black 100%. Genome ANI from skani.",
             fontsize=6.5, color=INK2, va="top")
    fig.savefig(a.out)


if __name__ == "__main__":
    main()
