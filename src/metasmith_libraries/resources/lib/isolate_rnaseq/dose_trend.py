"""Copper dose trend over a series with one library per dose.

    dose_trend.py <gene_count_table.csv> <raw_count_table.tsv> <sample_sheet.tsv> <count_qc.tsv> <trend.csv> <figures_dir>

One library per dose leaves pairwise contrasts nothing to estimate dispersion from, so pydeseq2
fits one slope per gene on log10(ppm + 1) across every dose. The fit is repeated without the
highest dose, and a gene that keeps its significance and sign there is flagged robust. Each
dose against 0 ppm is reported beside the trend as a descriptive log2 ratio, without a p-value.
"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pydeseq2.dds import DeseqDataSet
from pydeseq2.ds import DeseqStats
from scipy.cluster.hierarchy import fcluster, linkage

from sheet import read_sheet

ALPHA = 0.05
N_CLUSTER_GENES = 500
N_CLUSTERS = 6


def fit_trend(counts: pd.DataFrame, design: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dds = DeseqDataSet(counts=counts.T, metadata=design[["log_dose"]], design="~log_dose",
                       refit_cooks=True, quiet=True)
    dds.deseq2()
    stat = DeseqStats(dds, contrast=np.array([0.0, 1.0]), alpha=ALPHA, quiet=True)
    stat.summary()
    normed = pd.DataFrame(dds.layers["normed_counts"], index=counts.columns, columns=counts.index).T
    return stat.results_df, normed


def main(counts_path, raw_path, sheet_path, qc_path, out_csv, figures) -> None:
    figures = Path(figures)
    figures.mkdir(parents=True, exist_ok=True)
    sheet = read_sheet(sheet_path)
    design = sheet.set_index("label")
    labels = list(design.index)
    ann = pd.read_csv(raw_path, sep="\t", index_col="gene_id")
    all_counts = pd.read_csv(counts_path, index_col=0)[labels]

    coding = ann.index[ann.gene_biotype == "protein_coding"]
    counts = all_counts.loc[coding].round().astype(int)
    counts = counts[counts.sum(axis=1) >= 10]
    print(f"{len(all_counts)} features, {len(coding)} protein-coding, {len(counts)} with >= 10 reads", flush=True)

    res, normed = fit_trend(counts, design)
    top_dose = design.ppm.max()
    reduced = design[design.ppm < top_dose]
    res_reduced, _ = fit_trend(counts[reduced.index], reduced)
    without = f"without_{top_dose:g}ppm"

    rate = counts.div(ann.loc[counts.index, "Length"] / 1e3, axis=0)
    tpm = rate.div(rate.sum()) * 1e6
    base = design.index[design.ppm == 0][0]
    ratio = np.log2(normed.add(1).div(normed[base] + 1, axis=0))

    table = ann.loc[counts.index, ["gene_name", "product", "copies", "multimap_fraction"]].copy()
    table["log2FC_per_10x_copper"] = res.log2FoldChange
    table["lfcSE"] = res.lfcSE
    table["pvalue"] = res.pvalue
    table["padj"] = res.padj
    table[f"log2FC_per_10x_copper_{without}"] = res_reduced.log2FoldChange
    table[f"padj_{without}"] = res_reduced.padj
    sig = table.padj < ALPHA
    table[f"robust_{without}"] = sig & (table[f"padj_{without}"] < ALPHA) & (
        np.sign(table.log2FC_per_10x_copper) == np.sign(table[f"log2FC_per_10x_copper_{without}"]))
    for label in labels:
        if label != base:
            table[f"log2_ratio_{label}_vs_{base}"] = ratio[label]
    for label in labels:
        table[f"TPM_{label}"] = tpm[label]
    for label in labels:
        table[f"normalised_count_{label}"] = normed[label]

    logn = np.log2(normed + 1)
    expressed = logn[normed.mean(axis=1) >= 20]
    top = expressed.loc[expressed.var(axis=1).sort_values(ascending=False).index[:N_CLUSTER_GENES]]
    z = top.sub(top.mean(axis=1), axis=0).div(top.std(axis=1), axis=0)
    clusters = fcluster(linkage(z.values, method="average", metric="correlation"), N_CLUSTERS, criterion="maxclust")
    table["dose_cluster"] = pd.Series(clusters, index=z.index).reindex(table.index)

    table = table.sort_values("padj")
    table.index.name = "gene_id"
    table.to_csv(out_csv)
    sig = table.padj < ALPHA
    up = table[sig & (table.log2FC_per_10x_copper > 0)]
    down = table[sig & (table.log2FC_per_10x_copper < 0)]

    fig, ax = plt.subplots(figsize=(6.5, 5))
    p = -np.log10(table.padj.fillna(1).clip(lower=1e-300))
    colour = np.where(~sig, "#c4c8d0", np.where(table.log2FC_per_10x_copper > 0, "#c0392b", "#2e5fb0"))
    ax.scatter(table.log2FC_per_10x_copper, p, s=6, c=colour, linewidths=0)
    for g, row in table[sig].head(15).iterrows():
        ax.annotate(row.gene_name if isinstance(row.gene_name, str) else g,
                    (row.log2FC_per_10x_copper, -np.log10(max(row.padj, 1e-300))), fontsize=7)
    ax.axhline(-np.log10(ALPHA), ls="--", lw=0.8, c="#888")
    ax.set_xlabel("log2 fold change per 10x more copper")
    ax.set_ylabel("-log10 adjusted p")
    ax.set_title(f"Copper dose trend: {len(up)} up, {len(down)} down (padj < {ALPHA})")
    fig.tight_layout()
    fig.savefig(figures / "dose_trend_volcano.png", dpi=200)
    fig.savefig(figures / "dose_trend_volcano.svg")
    plt.close(fig)

    xpos = np.arange(len(labels))
    fig, axes = plt.subplots(2, 3, figsize=(11, 6), sharex=True, sharey=True)
    for k, ax in zip(range(1, N_CLUSTERS + 1), axes.flat):
        members = z[clusters == k]
        ax.plot(xpos, members.T.values, c="#9aa3b4", lw=0.4, alpha=0.5)
        ax.plot(xpos, members.mean().values, c="#c0392b", lw=2)
        ax.set_title(f"cluster {k}: {len(members)} genes", fontsize=10)
        ax.set_xticks(xpos, [f"{v:g}" for v in design.ppm])
    for ax in axes[-1]:
        ax.set_xlabel("CuSO4 (ppm)")
    for ax in axes[:, 0]:
        ax.set_ylabel("z-score")
    fig.suptitle(f"Dose-response shapes of the {N_CLUSTER_GENES} most variable genes")
    fig.tight_layout()
    fig.savefig(figures / "dose_clusters.png", dpi=200)
    plt.close(fig)

    qc = pd.read_csv(qc_path, sep="\t", index_col="library").loc[list(sheet.index)]
    qc.index = labels
    qc["protein_coding_tested"] = counts.sum()
    qc["size_factor"] = counts.sum() / np.exp(np.log(counts.sum()).mean())
    pcs = logn.loc[logn.var(axis=1).sort_values(ascending=False).index[:N_CLUSTER_GENES]]
    centred = pcs.sub(pcs.mean(axis=1), axis=0).T.values
    u, sv, _ = np.linalg.svd(centred, full_matrices=False)
    explained = sv ** 2 / (sv ** 2).sum()
    qc["PC1"] = u[:, 0] * sv[0]
    qc["PC2"] = u[:, 1] * sv[1]
    qc.index.name = "dose"
    qc.to_csv(figures / "library_qc.csv")

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    a1.bar(xpos, qc.percent_rRNA, color="#2e5fb0")
    a1.set_xticks(xpos, labels)
    a1.set_ylabel("% fragments on rRNA")
    a1.set_title("rRNA left after depletion")
    a2.scatter(qc.PC1, qc.PC2, c=design.log_dose, cmap="copper_r", s=60, edgecolors="#333")
    for label, (px, py) in zip(labels, qc[["PC1", "PC2"]].values):
        a2.annotate(label, (px, py), xytext=(5, 4), textcoords="offset points", fontsize=8)
    a2.set_xlabel(f"PC1 ({explained[0]:.0%})")
    a2.set_ylabel(f"PC2 ({explained[1]:.0%})")
    a2.set_title(f"Libraries on the top-{N_CLUSTER_GENES} variable genes")
    fig.tight_layout()
    fig.savefig(figures / "library_qc.png", dpi=200)
    plt.close(fig)

    merged = table[table.copies > 1]
    summary = [
        f"genes tested: {len(table)}",
        f"identical-copy groups summed: {len(merged)}; genes losing over half their reads at MAPQ 10: "
        f"{int((table.multimap_fraction > 0.5).sum())}",
        f"trend padj < {ALPHA}: {sig.sum()} ({len(up)} up, {len(down)} down)",
        f"also significant, same direction, {without.replace('_', ' ')}: {int(table[f'robust_{without}'].sum())}",
        f"PC1 {explained[0]:.0%}, PC2 {explained[1]:.0%}",
        "top up: " + ", ".join(str(n) for n in up.head(15).gene_name),
        "top down: " + ", ".join(str(n) for n in down.head(15).gene_name),
    ]
    (figures / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))


if __name__ == "__main__":
    main(*sys.argv[1:7])
