#!/usr/bin/env python3
"""Figures for the single CB2A library (cb2a_cu3, 10 ppm copper) from the B2 pipeline outputs.

    python cb2a_figures.py --tpm tpm_table.tsv --raw raw_count_table_all_biotypes.tsv \
        --gff cb2a.gff --bacmet bacmet_best_hit_per_gene_with_TPM.csv \
        --epi300-tpm epi300_tpm_table.tsv --epi300-classes epi300_assigned_fragments_by_RNA_class.csv \
        --out figures/

One library and no control, so every figure is expression within the genome: TPM, or TPM as a
multiple of the genome median.
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import FancyArrow

BLUE, ORANGE, AQUA, VIOLET = "#2a78d6", "#eb6834", "#1baf7a", "#6250d6"
GREY, LIGHT, INK, INK2 = "#b4b3ad", "#e4e3de", "#0b0b0b", "#52514e"
TPM = "TPM_cb2a_cu3"
COPPER = r"^(?:cop|cus|cue|pco|sil|cut|hmrR|cin|csoR)"
ISLAND = ("ACP82L_RS07640", "ACP82L_RS07925")
ISLAND_IDS = {f"ACP82L_RS{n:05d}" for n in range(7640, 7930, 5)}

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": INK2, "axes.labelcolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": LIGHT, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "figure.dpi": 200, "savefig.bbox": "tight",
})


def load(args):
    tpm = pd.read_csv(args.tpm, sep="\t")
    gff = pd.read_csv(args.gff, sep="\t", comment="#", header=None,
                      names=["seq", "src", "type", "start", "end", "score", "strand", "phase", "attr"])
    gff = gff[gff.type == "gene"].copy()
    gff["gene_id"] = gff.attr.str.extract(r"locus_tag=([^;]+)")
    gff["name"] = gff.attr.str.extract(r"(?:^|;)gene=([^;]+)")
    bac = pd.read_csv(args.bacmet)
    bac["strong"] = (bac.pident >= 40) & (bac.qcov_pct >= 70)
    bac["copper"] = bac.bacmet_gene.str.contains(COPPER, na=False)
    # A row in the count tables can join identical copies with "+"; place it at its first copy.
    tpm["first_id"] = tpm.gene_id.str.split("+").str[0]
    g = tpm.merge(gff[["gene_id", "start", "end", "strand", "name"]], left_on="first_id",
                  right_on="gene_id", how="left", suffixes=("", "_gff"))
    g = g.merge(bac[["gene_id", "bacmet_gene", "pident", "qcov_pct", "strong", "copper"]],
                on="gene_id", how="left")
    g["strong"] = g.strong.astype("boolean").fillna(False).astype(bool)
    g["copper"] = g.copper.astype("boolean").fillna(False).astype(bool)
    g["label"] = g.name.fillna(g.bacmet_gene).fillna(g.gene_id.str.replace("ACP82L_", ""))
    return g, gff, bac


def label_of(row):
    short = row.gene_id.replace("ACP82L_", "")
    if isinstance(row.bacmet_gene, str) and row.strong:
        return f"{short} ({row.bacmet_gene})"
    return short


def place_labels(ax, points):
    fig = ax.figure
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    placed = []
    offsets = [(5, 3), (5, -9), (-5, 3), (-5, -9), (5, 11), (-5, 11), (5, -17), (-5, -17)]
    for x, y, text in sorted(points, key=lambda p: -p[1]):
        for dx, dy in offsets:
            a = ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", fontsize=7, color=INK,
                            ha="left" if dx > 0 else "right",
                            arrowprops=dict(arrowstyle="-", color=INK2, lw=0.4, shrinkA=0, shrinkB=2))
            box = a.get_window_extent(renderer)
            if not any(box.overlaps(p) for p in placed):
                placed.append(box)
                break
            a.remove()


def fig_genome(g, out):
    med = g[TPM].median()
    g = g.dropna(subset=["start"]).sort_values("start")
    x = g.start / 1e6
    y = np.log10(g[TPM] + 0.1)
    fig, ax = plt.subplots(figsize=(10, 3.6))
    i0, i1 = (g.loc[g.gene_id == t, "start"].iloc[0] / 1e6 for t in ISLAND)
    ax.axvspan(i0, i1, color=ORANGE, alpha=0.12, lw=0)
    ax.scatter(x, y, s=4, color=GREY, lw=0, label="protein-coding gene")
    hit = g.strong & ~g.copper
    ax.scatter(x[hit], y[hit], s=14, color=BLUE, lw=0.6, edgecolor="white", label="BacMet hit (≥40% id, ≥70% cov)")
    cu = g.strong & g.copper
    ax.scatter(x[cu], y[cu], s=18, color=ORANGE, lw=0.6, edgecolor="white", label="BacMet copper gene")
    roll = y.rolling(41, center=True, min_periods=10).median()
    ax.plot(x, roll, color=INK, lw=1.2, label="running median (41 genes)")
    ax.axhline(np.log10(med), color=INK2, lw=0.8, ls=(0, (3, 3)))
    ax.text(4.13, np.log10(med), f" genome median\n {med:.0f} TPM", va="center", fontsize=7.5, color=INK2)
    ax.text(i1 + 0.03, 4.6, "metal-resistance island\nRS07640–RS07925", ha="left", va="top", fontsize=8,
            color=INK, bbox=dict(facecolor="white", edgecolor="none", pad=1))
    ax.set_xlim(0, 4.13)
    ax.set_ylim(-1.2, 4.9)
    ax.set_yticks(range(-1, 5), ["0.1", "1", "10", "100", "1k", "10k"])
    ax.set_xlabel("Chromosome position (Mb), NZ_CP189825.1")
    ax.set_ylabel("TPM (log scale)")
    ax.set_title("CB2A at 10 ppm copper: expression along the chromosome", loc="left", fontsize=10.5)
    ax.legend(loc="lower left", fontsize=7.5, frameon=False, ncol=4, bbox_to_anchor=(0, -0.27))
    fig.savefig(out / "1_expression_along_chromosome.png")
    plt.close(fig)


def fig_island(g, out):
    g = g.dropna(subset=["start"]).sort_values("start")
    lo = g.index[g.gene_id == ISLAND[0]][0]
    hi = g.index[g.gene_id == ISLAND[1]][0]
    s = g.loc[lo:hi] if lo < hi else g.loc[hi:lo]
    s = g[(g.start >= s.start.min()) & (g.start <= s.start.max())]
    med = g[TPM].median()
    fig, (ax, am) = plt.subplots(2, 1, figsize=(11, 5.2), sharex=True,
                                 gridspec_kw={"height_ratios": [3, 1.1], "hspace": 0.05})
    kb0 = s.start.min()
    mid = ((s.start + s.end) / 2 - kb0) / 1e3
    width = (s.end - s.start) / 1e3 * 0.9
    col = np.where(s.copper & s.strong, ORANGE, np.where(s.copper, "#f3a27f",
                   np.where(s.strong, BLUE, GREY)))
    ax.bar(mid, s[TPM], width=width, color=col, lw=0)
    ax.set_yscale("log")
    ax.axhline(med, color=INK2, lw=0.8, ls=(0, (3, 3)))
    ax.set_ylabel("TPM (log scale)")
    ax.set_title("The metal-resistance island (ACP82L_RS07640–RS07925) at 10 ppm copper", loc="left", fontsize=10.5)
    for m, row in zip(mid, s.itertuples()):
        if isinstance(row.bacmet_gene, str) and (row.strong or row.copper):
            tag = row.bacmet_gene
        elif getattr(row, TPM) >= 1000:
            tag = row.gene_id.replace("ACP82L_", "")
        else:
            continue
        ax.text(m, getattr(row, TPM) * 1.15, tag, rotation=90, ha="center", va="bottom", fontsize=7, color=INK)
    ax.set_ylim(top=s[TPM].max() * 40)
    for row in s.itertuples():
        a, b = (row.start - kb0) / 1e3, (row.end - kb0) / 1e3
        y = 0.62 if row.strand == "+" else 0.38
        dx = (b - a) if row.strand == "+" else -(b - a)
        x0 = a if row.strand == "+" else b
        c = ORANGE if (row.copper and row.strong) else ("#f3a27f" if row.copper else (BLUE if row.strong else GREY))
        am.add_patch(FancyArrow(x0, y, dx, 0, width=0.16, head_width=0.24,
                                head_length=min(0.25, abs(dx) * 0.4), length_includes_head=True,
                                color=c, lw=0))
    am.set_ylim(0.1, 0.9)
    am.set_yticks([0.38, 0.62], ["− strand", "+ strand"])
    am.grid(False)
    am.spines["left"].set_visible(False)
    am.set_xlabel(f"kb from {s.gene_id.iloc[0]} (chromosome {kb0/1e6:.3f} Mb)")
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in (ORANGE, "#f3a27f", BLUE, GREY)]
    handles.append(plt.Line2D([], [], color=INK2, lw=0.8, ls=(0, (3, 3))))
    ax.legend(handles, ["BacMet copper gene, strong hit", "BacMet copper gene, weak hit",
                        "other BacMet gene, strong hit", "no strong BacMet hit", f"genome median {med:.0f} TPM"],
              fontsize=7.5, frameon=False, loc="upper left", ncol=3)
    fig.savefig(out / "2_copper_island_locus.png")
    plt.close(fig)
    s[["gene_id", "start", "end", "strand", "product", "bacmet_gene", "pident", "qcov_pct", TPM,
       "count_cb2a_cu3"]].to_csv(out / "2_copper_island_genes.csv", index=False)


def fig_top(g, out, n=30):
    t = g.nlargest(n, TPM).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 7.5))
    col = np.where(t.copper & t.strong, ORANGE, np.where(t.copper, "#f3a27f", np.where(t.strong, BLUE, GREY)))
    ax.barh(range(len(t)), t[TPM], color=col, height=0.72, lw=0)
    names = [f"{r.gene_id.replace('ACP82L_', '')}  {str(r.product)[:48]}" for r in t.itertuples()]
    ax.set_yticks(range(len(t)), names, fontsize=7.5)
    for i, r in enumerate(t.itertuples()):
        tag = f"  {r.bacmet_gene}" if isinstance(r.bacmet_gene, str) and (r.strong or r.copper) else ""
        tag += "  ◆ island" if r.gene_id in ISLAND_IDS else ""
        ax.text(getattr(r, TPM), i, f" {getattr(r, TPM):,.0f}{tag}", va="center", fontsize=7, color=INK2)
    ax.set_xlim(0, t[TPM].max() * 1.3)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("TPM")
    ax.set_title(f"CB2A at 10 ppm: the {n} most expressed protein-coding genes", loc="left", fontsize=10.5)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in (ORANGE, "#f3a27f", BLUE, GREY)]
    ax.legend(handles, ["BacMet copper gene, strong hit", "BacMet copper gene, weak hit",
                        "other BacMet gene, strong hit", "no strong BacMet hit"],
              title="◆ island = in the metal-resistance island RS07640–RS07925", title_fontsize=7.5,
              fontsize=7.5, frameon=False, loc="lower right")
    fig.savefig(out / "3_top_expressed_genes.png")
    plt.close(fig)


def fig_bacmet(g, out):
    med = g[TPM].median()
    b = g.dropna(subset=["pident"])
    fig, ax = plt.subplots(figsize=(8, 5))
    for mask, c, lab in ((~b.copper, BLUE, "other BacMet gene"), (b.copper, ORANGE, "BacMet copper gene")):
        s = b[mask]
        full = s.qcov_pct >= 70
        ax.scatter(s.pident[full], s[TPM][full], s=22, color=c, lw=0.6, edgecolor="white",
                   label=f"{lab}, ≥70% coverage")
        ax.scatter(s.pident[~full], s[TPM][~full], s=22, facecolor="none", edgecolor=c, lw=0.9,
                   label=f"{lab}, <70% coverage")
    ax.set_yscale("log")
    ax.axhline(med, color=INK2, lw=0.8, ls=(0, (3, 3)))
    ax.axvline(40, color=INK2, lw=0.8, ls=(0, (1, 2)))
    ax.text(40.5, b[TPM].min(), "40% identity", fontsize=7.5, color=INK2, va="bottom")
    ax.text(b.pident.max(), med * 1.08, f"genome median {med:.0f} TPM", fontsize=7.5, color=INK2, ha="right", va="bottom")
    top = b[(b.pident >= 40) & (b.qcov_pct >= 70)].nlargest(16, TPM)
    place_labels(ax, [(r.pident, getattr(r, TPM), r.bacmet_gene) for r in top.itertuples()])
    ax.set_xlabel("% identity of the best BacMet hit")
    ax.set_ylabel("TPM (log scale)")
    ax.set_title(f"CB2A at 10 ppm: {len(b)} genes with a BacMet hit, by identity and expression",
                 loc="left", fontsize=10.5)
    ax.legend(fontsize=7.5, frameon=False, loc="lower right")
    fig.savefig(out / "4_bacmet_identity_vs_expression.png")
    plt.close(fig)


def fig_classes(raw, epi_classes, out):
    names = {"rRNA": "rRNA", "protein_coding": "protein coding", "RNase_P_RNA": "RNase P RNA",
             "tmRNA": "tmRNA", "ncRNA": "6S RNA", "tRNA": "tRNA", "SRP_RNA": "SRP RNA"}
    cb = raw.groupby("gene_biotype")["cb2a_cu3"].sum()
    cb = 100 * cb / cb.sum()
    cb.index = [names[i] for i in cb.index]
    e = pd.read_csv(epi_classes, index_col=0)
    e.index = [i.split(" (")[0] for i in e.index]
    pct = {"CB2A 10 ppm": cb}
    for d in (0, 10, 300):
        pct[f"EPI300 {d} ppm"] = e[f"percent_of_assigned_{d}ppm"].drop("total assigned", errors="ignore")
    df = pd.DataFrame(pct).fillna(0)
    df = df.drop([i for i in df.index if i.startswith("total")], errors="ignore")
    order = ["protein coding", "rRNA", "tmRNA", "RNase P RNA", "6S RNA", "tRNA", "SRP RNA"]
    df = df.reindex(order).fillna(0)
    colors = [BLUE, ORANGE, AQUA, VIOLET, "#e87ba4", GREY, LIGHT]
    fig, ax = plt.subplots(figsize=(8, 3.2))
    left = np.zeros(len(df.columns))
    for cls, c in zip(df.index, colors):
        v = df.loc[cls].values
        ax.barh(range(len(df.columns)), v, left=left, color=c, height=0.62, lw=1.5, edgecolor="white", label=cls)
        for i, (l, w) in enumerate(zip(left, v)):
            if w >= 5:
                ax.text(l + w / 2, i, f"{w:.0f}%", ha="center", va="center", fontsize=7.5,
                        color="white" if c in (BLUE, VIOLET) else INK)
        left += v
    ax.set_yticks(range(len(df.columns)), df.columns)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("% of fragments assigned to genes (MAPQ 0)")
    ax.set_title("Where the reads go: CB2A against EPI300", loc="left", fontsize=10.5)
    ax.legend(fontsize=7.5, frameon=False, ncol=7, loc="upper left", bbox_to_anchor=(0, -0.22),
              handlelength=1, columnspacing=1)
    fig.savefig(out / "5_rna_classes_vs_epi300.png")
    plt.close(fig)
    df.round(2).to_csv(out / "5_rna_classes_vs_epi300.csv")


def fig_vs_epi300(g, epi_tpm, out):
    e = pd.read_csv(epi_tpm, sep="\t")
    col = "TPM_epi300_cu3"
    emed = e[col].median()
    cmed = g[TPM].median()
    epi = e[e.gene_name.str.match(r"^(copA|cueO|cueR|cus[RSCFBA]|cutC|copD|comR|cpxP|zraP|spy)$", na=False)]
    epi = pd.DataFrame({"gene": epi.gene_name, "x_median": epi[col] / emed, "org": "EPI300 (10 ppm)"})
    cb = g[g.copper & g.strong]
    cb = pd.DataFrame({"gene": cb.bacmet_gene + " " + cb.gene_id.str.replace("ACP82L_RS", ""),
                       "x_median": cb[TPM] / cmed, "org": "CB2A (10 ppm)"})
    fig, axes = plt.subplots(1, 2, figsize=(10, 5.2), sharex=True)
    for ax, df, c in zip(axes, (cb, epi), (ORANGE, BLUE)):
        df = df.sort_values("x_median")
        ax.barh(range(len(df)), df.x_median, color=c, height=0.7, lw=0)
        ax.set_yticks(range(len(df)), df.gene, fontsize=7.5)
        ax.set_xscale("log")
        ax.axvline(1, color=INK2, lw=0.8, ls=(0, (3, 3)))
        ax.grid(axis="y", visible=False)
        ax.set_title(df.org.iloc[0], loc="left", fontsize=9.5)
        ax.set_xlabel("TPM ÷ genome median TPM (log scale)")
    fig.suptitle("Copper-system genes at 10 ppm, each against its own genome's median", x=0.02, ha="left",
                 fontsize=10.5)
    fig.text(0.02, -0.03, "CB2A: genes whose best BacMet hit is a copper gene at ≥40% identity over ≥70% of the "
             "protein. EPI300: named E. coli copper genes (cus counted as both copies). Not orthologue pairs.",
             fontsize=7, color=INK2)
    fig.savefig(out / "6_copper_genes_vs_epi300.png")
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for a in ("tpm", "raw", "gff", "bacmet", "epi300-tpm", "epi300-classes", "out"):
        ap.add_argument(f"--{a}", required=True)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    g, gff, bac = load(args)
    fig_genome(g, out)
    fig_island(g, out)
    fig_top(g, out)
    fig_bacmet(g, out)
    fig_classes(pd.read_csv(args.raw, sep="\t"), args.epi300_classes, out)
    fig_vs_epi300(g, args.epi300_tpm, out)


if __name__ == "__main__":
    main()
