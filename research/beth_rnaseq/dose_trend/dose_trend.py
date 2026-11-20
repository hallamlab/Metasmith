#!/usr/bin/env python3
"""Copper dose-trend analysis of Beth's EPI300 RNA-seq (TCAG run DAV36692), from her count table.

    python dose_trend.py <input_dir> <output_dir>

<input_dir> holds copies of her epi300_counts_annotated.tsv (featureCounts -s 2 -Q 10),
epi300_gene_counts_Q0.txt (the same at -Q 0), sample_metadata.csv,
epi300_gene_counts.txt.summary and epi300_rrna_summary.tsv.
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

ALPHA = 0.05
N_CLUSTER_GENES = 500
N_CLUSTERS = 6

# E. coli copper homeostasis: the cue and cus systems, CopA, the Cpx envelope-stress response
# that copper triggers, and the smaller copper-tolerance genes. Names as RefSeq annotates K-12.
COPPER_GENES = ["copA", "cueO", "cueR", "cusS", "cusR", "cusC", "cusF", "cusB", "cusA",
                "cpxP", "cpxR", "cpxA", "spy", "cutC", "cutA", "yobA", "copD", "comR",
                "bhsA", "zntA", "zraP", "mntH", "soxS", "sodA"]


def load(inp: Path):
    ann = pd.read_csv(inp / "epi300_counts_annotated.tsv", sep="\t", index_col="locus_tag")
    meta = pd.read_csv(inp / "sample_metadata.csv", encoding="utf-8-sig")
    meta = meta[meta.strain == "epi300"].set_index("sample_id")
    meta["ppm"] = meta.copper_value.astype(float)
    meta = meta.sort_values("ppm")
    meta["label"] = [f"{p:g}ppm" for p in meta.ppm]
    meta["log_dose"] = np.log10(meta.ppm + 1)
    return ann, meta


# The EPI300 assembly carries some genes twice, cusRSCFBA among them. Reads on a duplicated
# gene align equally well to both copies, so bowtie2 gives them MAPQ 0 or 1, and the -Q 10
# table counts the gene as silent. At -Q 0 each read lands on one copy at random, so only the
# sum over the copies means anything: same-name, same-length copies are summed into one row.
def q0_counts(inp: Path, ann: pd.DataFrame, samples: list[str]) -> pd.DataFrame:
    q0 = pd.read_csv(inp / "epi300_gene_counts_Q0.txt", sep="\t", comment="#", index_col="Geneid")
    q0.columns = [Path(c).name.split(".")[0] for c in q0.columns]
    q0 = q0[samples].reindex(ann.index).fillna(0)
    lost = q0.sum(axis=1) - ann[samples].sum(axis=1)
    ann = ann.assign(multi_mapped=(q0.sum(axis=1) >= 50) & (lost > 0.5 * q0.sum(axis=1)))
    key = ann.gene_name.fillna(ann["product"]) + "|" + ann.Length.astype(str)
    rows = []
    for _, members in ann[ann.multi_mapped].groupby(key[ann.multi_mapped]):
        rows.append((list(members.index), members.iloc[0]))
    merged_ids = {i for ids, _ in rows for i in ids}
    keep = ann.drop(index=list(merged_ids))
    out = pd.concat([keep, pd.DataFrame([r for _, r in rows], index=["+".join(ids) for ids, _ in rows])])
    counts = pd.concat([q0.loc[keep.index], pd.DataFrame(
        [q0.loc[ids].sum() for ids, _ in rows], index=["+".join(ids) for ids, _ in rows])])
    out["copies"] = [len(i.split("+")) for i in out.index]
    out[samples] = counts[samples]
    return out


def fit_trend(counts: pd.DataFrame, meta: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dds = DeseqDataSet(counts=counts.T, metadata=meta[["log_dose"]], design="~log_dose",
                       refit_cooks=True, quiet=True)
    dds.deseq2()
    stat = DeseqStats(dds, contrast=np.array([0.0, 1.0]), alpha=ALPHA, quiet=True)
    stat.summary()
    normed = pd.DataFrame(dds.layers["normed_counts"], index=counts.columns, columns=counts.index).T
    return stat.results_df, normed


def main(inp: Path, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    ann, meta = load(inp)
    samples = list(meta.index)
    labels = dict(zip(meta.index, meta.label))

    ann = q0_counts(inp, ann, samples)
    coding = ann[ann.gene_biotype == "protein_coding"]
    counts = coding[samples].round().astype(int)
    counts = counts[counts.sum(axis=1) >= 10]
    print(f"{len(ann)} features, {len(coding)} protein-coding, {len(counts)} with >= 10 reads", flush=True)

    res, normed = fit_trend(counts, meta)
    no300 = meta[meta.ppm < 300]
    res_no300, _ = fit_trend(counts[no300.index], no300)

    tpm_rate = coding.loc[counts.index, samples].div(coding.loc[counts.index, "Length"] / 1e3, axis=0)
    tpm = tpm_rate.div(tpm_rate.sum()) * 1e6

    base = meta.index[meta.ppm == 0][0]
    ratio = np.log2((normed[samples].add(1)).div(normed[base] + 1, axis=0))

    table = coding.loc[counts.index, ["gene_name", "product", "copies", "multi_mapped"]].copy()
    table["log2FC_per_10x_copper"] = res.log2FoldChange
    table["lfcSE"] = res.lfcSE
    table["pvalue"] = res.pvalue
    table["padj"] = res.padj
    table["log2FC_per_10x_copper_without_300ppm"] = res_no300.log2FoldChange
    table["padj_without_300ppm"] = res_no300.padj
    sig = table.padj < ALPHA
    table["robust_without_300ppm"] = sig & (table.padj_without_300ppm < ALPHA) & (
        np.sign(table.log2FC_per_10x_copper) == np.sign(table.log2FC_per_10x_copper_without_300ppm))
    for s in samples:
        if s != base:
            table[f"log2_ratio_{labels[s]}_vs_0ppm"] = ratio[s]
    for s in samples:
        table[f"TPM_{labels[s]}"] = tpm[s]
    for s in samples:
        table[f"normalised_count_{labels[s]}"] = normed[s]

    # Clusters: the most variable genes by log normalised expression, z-scored across doses.
    logn = np.log2(normed[samples] + 1)
    expressed = logn[normed[samples].mean(axis=1) >= 20]
    top = expressed.loc[expressed.var(axis=1).sort_values(ascending=False).index[:N_CLUSTER_GENES]]
    z = top.sub(top.mean(axis=1), axis=0).div(top.std(axis=1), axis=0)
    clusters = fcluster(linkage(z.values, method="average", metric="correlation"), N_CLUSTERS, criterion="maxclust")
    table["dose_cluster"] = pd.Series(clusters, index=z.index).reindex(table.index)

    table = table.sort_values("padj")
    table.to_csv(out / "epi300_copper_dose_trend.csv")
    sig = table.padj < ALPHA

    up = table[sig & (table.log2FC_per_10x_copper > 0)]
    down = table[sig & (table.log2FC_per_10x_copper < 0)]
    robust = table[table.robust_without_300ppm]

    # Volcano of the trend
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
    ax.set_title(f"EPI300 copper dose trend: {len(up)} up, {len(down)} down (padj < {ALPHA})")
    fig.tight_layout()
    fig.savefig(out / "epi300_dose_trend_volcano.png", dpi=200)
    fig.savefig(out / "epi300_dose_trend_volcano.svg")
    plt.close(fig)

    # Copper gene panel
    names = table.gene_name.astype(str)
    panel = table[names.isin(COPPER_GENES)].copy()
    panel["order"] = [COPPER_GENES.index(n) for n in panel.gene_name]
    panel = panel.sort_values("order")
    x = meta.ppm.values
    xpos = np.arange(len(x))
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 0.32 * len(panel) + 2), gridspec_kw={"width_ratios": [3, 1]})
    tcols = [f"TPM_{labels[s]}" for s in samples]
    heat = np.log10(panel[tcols].values + 1)
    im = a1.imshow(heat, aspect="auto", cmap="viridis")
    a1.set_xticks(xpos, [f"{v:g}" for v in x])
    a1.set_yticks(range(len(panel)), panel.gene_name)
    a1.set_xlabel("CuSO4 (ppm)")
    a1.set_title("Expression, log10(TPM + 1)")
    fig.colorbar(im, ax=a1, fraction=0.03)
    bars = panel.log2FC_per_10x_copper.values
    a2.barh(range(len(panel)), bars, color=np.where(panel.padj < ALPHA, "#c0392b", "#c4c8d0"))
    a2.set_yticks(range(len(panel)), panel.gene_name)
    a2.set_ylim(len(panel) - 0.5, -0.5)
    a2.axvline(0, c="#555", lw=0.8)
    a2.set_xlabel("log2FC per 10x copper")
    a2.set_title(f"Trend (red: padj < {ALPHA})")
    fig.tight_layout()
    fig.savefig(out / "epi300_copper_gene_panel.png", dpi=200)
    fig.savefig(out / "epi300_copper_gene_panel.svg")
    plt.close(fig)
    panel.drop(columns="order").to_csv(out / "epi300_copper_gene_panel.csv")

    # Cluster profiles
    fig, axes = plt.subplots(2, 3, figsize=(11, 6), sharex=True, sharey=True)
    for k, ax in zip(range(1, N_CLUSTERS + 1), axes.flat):
        members = z[clusters == k]
        ax.plot(xpos, members.T.values, c="#9aa3b4", lw=0.4, alpha=0.5)
        ax.plot(xpos, members.mean().values, c="#c0392b", lw=2)
        ax.set_title(f"cluster {k}: {len(members)} genes", fontsize=10)
        ax.set_xticks(xpos, [f"{v:g}" for v in x])
    for ax in axes[-1]:
        ax.set_xlabel("CuSO4 (ppm)")
    for ax in axes[:, 0]:
        ax.set_ylabel("z-score")
    fig.suptitle(f"Dose-response shapes of the {N_CLUSTER_GENES} most variable genes")
    fig.tight_layout()
    fig.savefig(out / "epi300_dose_clusters.png", dpi=200)
    plt.close(fig)

    # QC: per library depth, rRNA share, low-MAPQ loss, size factor, and PCA position.
    summ = pd.read_csv(inp / "epi300_gene_counts.txt.summary", sep="\t", index_col=0)
    summ.columns = [Path(c).name.split(".")[0] for c in summ.columns]
    rrna = pd.read_csv(inp / "epi300_rrna_summary.tsv", sep="\t", index_col="sample")
    qc = pd.DataFrame({
        "copper_ppm": meta.ppm,
        "total_fragments": rrna.total_fragments,
        "percent_rRNA": rrna.percent_rrna,
        "assigned_fragments": summ.loc["Assigned"],
        "unassigned_low_MAPQ": summ.loc["Unassigned_MappingQuality"],
        "protein_coding_counted": counts.sum(),
        "size_factor": counts.sum() / np.exp(np.log(counts.sum()).mean()),
    }).loc[samples]
    qc.index = [f"{s} ({labels[s]})" for s in samples]
    pcs = logn.loc[logn.var(axis=1).sort_values(ascending=False).index[:N_CLUSTER_GENES]]
    centred = pcs.sub(pcs.mean(axis=1), axis=0).T.values
    u, sv, _ = np.linalg.svd(centred, full_matrices=False)
    explained = sv ** 2 / (sv ** 2).sum()
    qc["PC1"] = u[:, 0] * sv[0]
    qc["PC2"] = u[:, 1] * sv[1]
    qc.to_csv(out / "epi300_library_qc.csv")

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    a1.bar(range(len(qc)), qc.percent_rRNA, color="#2e5fb0")
    a1.set_xticks(range(len(qc)), [labels[s] for s in samples])
    a1.set_ylabel("% fragments on rRNA")
    a1.set_title("rRNA left after depletion")
    a2.scatter(qc.PC1, qc.PC2, c=np.log10(meta.ppm + 1), cmap="copper_r", s=60, edgecolors="#333")
    for s, (px, py) in zip(samples, qc[["PC1", "PC2"]].values):
        a2.annotate(labels[s], (px, py), xytext=(5, 4), textcoords="offset points", fontsize=8)
    a2.set_xlabel(f"PC1 ({explained[0]:.0%})")
    a2.set_ylabel(f"PC2 ({explained[1]:.0%})")
    a2.set_title("Libraries on the top-500 variable genes")
    fig.tight_layout()
    fig.savefig(out / "epi300_library_qc.png", dpi=200)
    plt.close(fig)

    cpanel = ", ".join(f"{r.gene_name} {r.log2FC_per_10x_copper:+.2f}{'*' if r.padj < ALPHA else ''}"
                       for _, r in panel.iterrows())
    summary = [
        f"genes tested: {len(table)}",
        f"multi-mapped genes rescued at -Q 0: {int(table.multi_mapped.sum())}, "
        f"of them duplicate copies merged: {int((table.copies > 1).sum())}",
        "  " + ", ".join(f"{r.gene_name}x{r.copies}" for _, r in table[table.copies > 1].iterrows()),
        f"trend padj < {ALPHA}: {sig.sum()} ({len(up)} up, {len(down)} down)",
        f"also significant, same direction, without 300 ppm: {len(robust)}",
        f"PC1 {explained[0]:.0%}, PC2 {explained[1]:.0%}",
        "top up: " + ", ".join(str(n) for n in up.head(15).gene_name),
        "top down: " + ", ".join(str(n) for n in down.head(15).gene_name),
        "copper genes (log2FC per 10x, * padj<0.05): " + cpanel,
    ]
    (out / "summary.txt").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(qc.round(3).to_string())


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
