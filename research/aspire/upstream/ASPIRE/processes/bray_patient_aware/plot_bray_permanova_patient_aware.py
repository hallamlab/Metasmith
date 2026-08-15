#!/usr/bin/env python3
"""
Plot PERMANOVA results from run_bray_permanova_patient_aware.R.

Expected input files under --indir:
- bray_permanova_sample_type_global.tsv
- bray_permanova_sample_type_pairwise.tsv
- bray_permanova_case_patient_pooled.tsv
- bray_permanova_case_by_sample_type.tsv
- bray_permdisp_diagnostics.tsv

Outputs (SVG + PDF) under --outdir:
- Beta_Heatmap_permanova_sample_type_pairwise.{svg,pdf}
- PERMANOVA_R2_summary.{svg,pdf}
- PERMDISP_diagnostics.{svg,pdf}
- Bray_Boxplot_sample_type_within_patient.{svg,pdf}
- Bray_Boxplot_case_patient_pooled.{svg,pdf}
- Bray_Boxplot_case_by_sample_type.{svg,pdf}
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns


# Match ASPIRE plotting defaults
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
mpl.rcParams["savefig.dpi"] = 600
plt.rcParams.update({"font.size": 12})
sns.set_theme()
sns.set_style("white")

PALETTE_TYPES = {
    "Lung Brush": "#009E73",
    "BAL": "#0072B2",
    "Oral Rinse": "#6A3D9A",
}


def bh_adjust(pvals: pd.Series) -> pd.Series:
    p = pvals.astype(float).values
    n = len(p)
    order = np.argsort(p)
    ranked = p[order]
    adj = ranked * n / (np.arange(n) + 1)
    adj = np.minimum.accumulate(adj[::-1])[::-1]
    adj = np.clip(adj, 0, 1)
    out = np.empty_like(adj)
    out[order] = adj
    return pd.Series(out, index=pvals.index)


def read_tsv(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[warn] Missing input: {path}")
        return pd.DataFrame()
    return pd.read_csv(path, sep="\t")


def save_fig(fig: plt.Figure, out_base: Path) -> None:
    out_base.parent.mkdir(parents=True, exist_ok=True)
    for ext in ("svg", "pdf"):
        fig.savefig(out_base.with_suffix(f".{ext}"), bbox_inches="tight")
    plt.close(fig)


def canonical_pair_matrix(pair_df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if pair_df.empty or value_col not in pair_df.columns:
        return pd.DataFrame()

    m = pair_df[["group1", "group2", value_col]].copy()
    mat = m.pivot(index="group1", columns="group2", values=value_col)
    mat = mat.combine_first(mat.T)

    order = [x for x in ["Oral Rinse", "BAL", "Lung Brush"] if x in mat.index or x in mat.columns]
    if order:
        mat = mat.reindex(index=order, columns=order)
    return mat


def plot_pairwise_heatmap(pair_df: pd.DataFrame, outdir: Path) -> None:
    if pair_df.empty:
        return

    value_col = "q_value" if "q_value" in pair_df.columns else "p_value"
    mat = canonical_pair_matrix(pair_df, value_col)
    if mat.empty:
        return

    fig, ax = plt.subplots(figsize=(7, 6))
    sns.heatmap(
        mat,
        annot=True,
        fmt=".3f",
        cmap="coolwarm_r",
        vmin=0,
        vmax=0.1,
        linewidths=0.5,
        linecolor="lightgray",
        cbar_kws={"label": value_col.replace("_", " ")},
        ax=ax,
    )
    ax.set_title("Pairwise PERMANOVA: Sample Types")
    save_fig(fig, outdir / "Beta_Heatmap_permanova_sample_type_pairwise")


def build_r2_summary(global_type: pd.DataFrame, case_pooled: pd.DataFrame, case_by_type: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if not global_type.empty and "term" in global_type.columns:
        gt = global_type.loc[global_type["term"] == "sample_type"]
        if not gt.empty:
            row = gt.iloc[0]
            rows.append(
                {
                    "model": "Sample type (within-patient)",
                    "R2": row.get("R2", np.nan),
                    "p_value": row.get("Pr(>F)", np.nan),
                    "group": "Sample Type",
                }
            )

    if not case_pooled.empty:
        row = case_pooled.iloc[0]
        rows.append(
            {
                "model": "Cancer vs Control (pooled patient)",
                "R2": row.get("R2", np.nan),
                "p_value": row.get("p_value", np.nan),
                "group": "Cancer/Control",
            }
        )

    if not case_by_type.empty:
        tmp = case_by_type.copy()
        if "q_value" not in tmp.columns and "p_value" in tmp.columns:
            tmp["q_value"] = bh_adjust(tmp["p_value"])
        for _, r in tmp.iterrows():
            stype = r.get("sample_type", "Unknown")
            rows.append(
                {
                    "model": f"Cancer vs Control ({stype})",
                    "R2": r.get("R2", np.nan),
                    "p_value": r.get("p_value", np.nan),
                    "q_value": r.get("q_value", np.nan),
                    "group": stype,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["significant"] = out["p_value"] < 0.05
    return out


def plot_r2_summary(summary_df: pd.DataFrame, outdir: Path) -> None:
    if summary_df.empty:
        return

    plot_df = summary_df.sort_values("R2", ascending=True).copy()
    colors = []
    for _, r in plot_df.iterrows():
        g = r.get("group", "")
        if g in PALETTE_TYPES:
            colors.append(PALETTE_TYPES[g])
        elif g == "Sample Type":
            colors.append("#444444")
        else:
            colors.append("#A50026")

    fig, ax = plt.subplots(figsize=(10, max(4, 0.55 * len(plot_df))))
    y = np.arange(len(plot_df))
    ax.barh(y, plot_df["R2"], color=colors, alpha=0.9)
    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["model"])
    ax.set_xlabel("PERMANOVA R²")
    ax.set_title("Bray-Curtis PERMANOVA Effect Sizes")

    for i, (_, r) in enumerate(plot_df.iterrows()):
        p = r.get("p_value", np.nan)
        if pd.notna(p):
            ax.text(float(r["R2"]) + 0.002, i, f"p={p:.3g}", va="center", ha="left", fontsize=10)

    ax.grid(axis="x", linestyle=":", alpha=0.3)
    sns.despine(ax=ax, left=False, bottom=False)
    save_fig(fig, outdir / "PERMANOVA_R2_summary")


def plot_dispersion(disp_df: pd.DataFrame, outdir: Path) -> None:
    if disp_df.empty or "p_value" not in disp_df.columns:
        return

    d = disp_df.copy()
    d = d[d["p_value"].notna()]
    if d.empty:
        return

    d["neglog10_p"] = -np.log10(d["p_value"].clip(lower=1e-300))
    d = d.sort_values("neglog10_p", ascending=True)

    fig, ax = plt.subplots(figsize=(10, max(3.5, 0.5 * len(d))))
    ax.barh(d["model"], d["neglog10_p"], color="#8C8C8C")
    ax.axvline(-np.log10(0.05), color="#A50026", linestyle="--", linewidth=1, label="p=0.05")
    ax.set_xlabel("-log10(p)")
    ax.set_title("PERMDISP Diagnostics")
    ax.legend(frameon=False, loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax.grid(axis="x", linestyle=":", alpha=0.3)
    sns.despine(ax=ax, left=False, bottom=False)
    save_fig(fig, outdir / "PERMDISP_diagnostics")


def plot_box_sample_type(sample_type_dist: pd.DataFrame, outdir: Path) -> None:
    if sample_type_dist.empty:
        print("[warn] Skipping sample-type boxplot (no distance table rows).")
        return
    d = sample_type_dist.copy()
    if "comparison" not in d.columns or "bray" not in d.columns:
        return
    order = sorted(d["comparison"].dropna().unique())
    fig, ax = plt.subplots(figsize=(9, 6))
    sns.boxplot(data=d, x="comparison", y="bray", order=order, color="#bdbdbd", fliersize=0, ax=ax)
    sns.stripplot(data=d, x="comparison", y="bray", order=order, color="#222222", alpha=0.45, size=3, ax=ax)
    ax.set_xlabel("Sample Type Pair (within patient)")
    ax.set_ylabel("Bray-Curtis distance")
    ax.set_title("Within-Patient Bray-Curtis by Sample-Type Pair")
    ax.tick_params(axis="x", rotation=20)
    sns.despine(ax=ax)
    save_fig(fig, outdir / "Bray_Boxplot_sample_type_within_patient")


def plot_box_case_pooled(case_pooled_dist: pd.DataFrame, outdir: Path) -> None:
    if case_pooled_dist.empty:
        print("[warn] Skipping pooled case/control boxplot (no distance table rows).")
        return
    d = case_pooled_dist.copy()
    if "group_pair" not in d.columns or "bray" not in d.columns:
        return
    order = [x for x in ["Control-Control", "Cancer-Control", "Cancer-Cancer"] if x in d["group_pair"].unique()]
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.boxplot(data=d, x="group_pair", y="bray", order=order, color="#bdbdbd", fliersize=0, ax=ax)
    sns.stripplot(data=d, x="group_pair", y="bray", order=order, color="#222222", alpha=0.35, size=2.7, ax=ax)
    ax.set_xlabel("Pair Type")
    ax.set_ylabel("Bray-Curtis distance")
    ax.set_title("Patient-Level Bray-Curtis by Cancer/Control Pair")
    sns.despine(ax=ax)
    save_fig(fig, outdir / "Bray_Boxplot_case_patient_pooled")


def plot_box_case_by_type(case_by_type_dist: pd.DataFrame, outdir: Path) -> None:
    if case_by_type_dist.empty:
        print("[warn] Skipping stratified case/control boxplot (no distance table rows).")
        return
    d = case_by_type_dist.copy()
    needed = {"sample_type", "group_pair", "bray"}
    if not needed.issubset(set(d.columns)):
        return
    order = [x for x in ["Control-Control", "Cancer-Control", "Cancer-Cancer"] if x in d["group_pair"].unique()]
    col_order = [x for x in ["Oral Rinse", "BAL", "Lung Brush"] if x in d["sample_type"].unique()]
    if not col_order:
        col_order = sorted(d["sample_type"].dropna().unique())
    g = sns.catplot(
        data=d,
        kind="box",
        x="group_pair",
        y="bray",
        col="sample_type",
        col_order=col_order,
        order=order,
        color="#bdbdbd",
        sharey=True,
        fliersize=0,
        height=4.2,
        aspect=1.0,
    )
    for ax, stype in zip(g.axes.flat, col_order):
        s = d[d["sample_type"] == stype]
        sns.stripplot(
            data=s,
            x="group_pair",
            y="bray",
            order=order,
            color="#222222",
            alpha=0.3,
            size=2.2,
            ax=ax,
        )
        ax.set_xlabel("Pair Type")
        ax.set_ylabel("Bray-Curtis distance")
        ax.tick_params(axis="x", rotation=20)
    g.fig.suptitle("Bray-Curtis by Cancer/Control Pair Within Sample Type", y=1.04)
    save_fig(g.fig, outdir / "Bray_Boxplot_case_by_sample_type")


def main() -> None:
    p = argparse.ArgumentParser(description="Plot PERMANOVA outputs from patient-aware Bray-Curtis analysis.")
    p.add_argument("--indir", type=Path, required=True, help="Directory with TSV outputs from run_bray_permanova_patient_aware.R")
    p.add_argument("--outdir", type=Path, default=None, help="Output figure directory (default: <indir>/figures)")
    args = p.parse_args()

    outdir = args.outdir if args.outdir is not None else args.indir / "figures"
    outdir.mkdir(parents=True, exist_ok=True)

    global_type = read_tsv(args.indir / "bray_permanova_sample_type_global.tsv")
    pairwise_type = read_tsv(args.indir / "bray_permanova_sample_type_pairwise.tsv")
    case_pooled = read_tsv(args.indir / "bray_permanova_case_patient_pooled.tsv")
    case_by_type = read_tsv(args.indir / "bray_permanova_case_by_sample_type.tsv")
    permdisp = read_tsv(args.indir / "bray_permdisp_diagnostics.tsv")
    sample_type_dist = read_tsv(args.indir / "bray_sample_type_within_patient_pairdist.tsv")
    case_pooled_dist = read_tsv(args.indir / "bray_case_patient_pairdist.tsv")
    case_by_type_dist = read_tsv(args.indir / "bray_case_by_sample_type_pairdist.tsv")

    plot_pairwise_heatmap(pairwise_type, outdir)

    summary = build_r2_summary(global_type, case_pooled, case_by_type)
    if not summary.empty:
        summary.to_csv(outdir / "PERMANOVA_R2_summary.tsv", sep="\t", index=False)
    plot_r2_summary(summary, outdir)

    plot_dispersion(permdisp, outdir)
    plot_box_sample_type(sample_type_dist, outdir)
    plot_box_case_pooled(case_pooled_dist, outdir)
    plot_box_case_by_type(case_by_type_dist, outdir)

    print(f"Saved figures to: {outdir}")


if __name__ == "__main__":
    main()
