#!/usr/bin/env python3
"""
Plot observed taxonomic analysis results.

Inputs:
- data_long: original long ASV table (for abundance boxplots)
- cancer_results: output from run_taxonomic_abundance_analysis.py
- sampletype_results: output from run_taxonomic_sample_type_analysis.py (pairwise)

Outputs (SVG + PDF):
- cancer_vs_control_boxplots_*.{svg,pdf}
- sample_type_three_group_boxplots_*.{svg,pdf}
- effect_heatmap_cancer_*.{svg,pdf}
- effect_heatmap_sample_type_*.{svg,pdf}
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns


# Match prior plot style
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
mpl.rcParams["savefig.dpi"] = 600
plt.rcParams.update({"font.size": 12})
sns.set_theme()
sns.set_style("white")

PALETTE_STATUS = {"Control": "#bdbdbd", "Cancer": "#A50026"}
PALETTE_TYPES = {"BAL": "#0072B2", "Lung Brush": "#009E73", "Oral Rinse": "#6A3D9A"}
BOXPLOT_HEIGHT = 6.0
AXIS_LABEL_SIZE = 10
AXIS_TICK_SIZE = 9
TITLE_SIZE = 11
BOXPLOT_SUBPLOT_ADJUST = dict(left=0.10, right=0.98, bottom=0.42, top=0.90)
AXIS_LINEWIDTH = 0.5
TICK_LINEWIDTH = 0.5
BOXPLOT_LINEWIDTH = 0.5
SPLIT_THRESHOLD_REL_ABUND = 0.03


def style_axes(ax: plt.Axes) -> None:
    for side in ("left", "bottom", "right", "top"):
        ax.spines[side].set_linewidth(AXIS_LINEWIDTH)
    ax.tick_params(axis="both", width=TICK_LINEWIDTH)


def save_fig(fig: plt.Figure, out_base: Path) -> None:
    out_base.parent.mkdir(parents=True, exist_ok=True)
    for ext in ("svg", "pdf"):
        fig.savefig(out_base.with_suffix(f".{ext}"), bbox_inches="tight")
    plt.close(fig)


def canonical_case(x: str) -> str:
    return "Control" if str(x) in {"Control", "Non-Cancer"} else "Cancer"


def canonicalize_sample_type(x: str) -> str:
    x_clean = str(x).strip().lower()
    if x_clean in {"oral", "oral rinse", "oral_rinse"}:
        return "Oral Rinse"
    if x_clean in {"bal", "bronchoalveolar lavage"}:
        return "BAL"
    if x_clean in {"lung brush", "bronchial brush", "brochial brush", "brush"}:
        return "Lung Brush"
    return str(x)


def parse_palette(text: str) -> dict[str, str]:
    palette: dict[str, str] = {}
    for token in str(text or "").split(","):
        if not token.strip():
            continue
        if "=" not in token:
            raise ValueError(f"Invalid palette entry '{token}'; expected label=#RRGGBB")
        label, color = (part.strip() for part in token.split("=", 1))
        palette[canonicalize_sample_type(label)] = color
    return palette


def observed_type_order(values: Iterable[str]) -> list[str]:
    observed = list(dict.fromkeys(str(value) for value in values if pd.notna(value)))
    preferred = ["BAL", "Lung Brush", "Oral Rinse"]
    return [value for value in preferred if value in observed] + [value for value in observed if value not in preferred]


def patient_type_taxon_rel(
    long_df: pd.DataFrame,
    tax_level: str,
    sample_col: str,
    patient_col: str,
    type_col: str,
    case_col: str,
    count_col: str,
) -> pd.DataFrame:
    agg = long_df.groupby([sample_col, tax_level], as_index=False)[count_col].sum()
    sample_tax = agg.pivot(index=sample_col, columns=tax_level, values=count_col).fillna(0)

    meta = long_df[[sample_col, patient_col, type_col, case_col]].drop_duplicates().set_index(sample_col)
    common = sample_tax.index.intersection(meta.index)
    sample_tax = sample_tax.loc[common]
    meta = meta.loc[common]

    tmp = sample_tax.copy()
    tmp[patient_col] = meta[patient_col].values
    tmp[type_col] = meta[type_col].values
    tmp[case_col] = meta[case_col].map(canonical_case).values

    taxa_cols = [c for c in tmp.columns if c not in {patient_col, type_col, case_col}]
    pt = tmp.groupby([patient_col, type_col, case_col], as_index=False)[taxa_cols].sum()

    totals = pt[taxa_cols].sum(axis=1).replace(0, 1)
    pt[taxa_cols] = pt[taxa_cols].div(totals, axis=0)

    out = pt.melt(
        id_vars=[patient_col, type_col, case_col],
        value_vars=taxa_cols,
        var_name="taxon",
        value_name="rel_abundance",
    )
    return out


def top_taxa(df: pd.DataFrame, n: int, q_col: str = "q_value") -> list[str]:
    d = df.copy()
    if q_col in d.columns:
        d = d.sort_values(q_col)
    return d["taxon"].dropna().astype(str).head(n).tolist()


def q_to_stars(q: float) -> str:
    if pd.isna(q):
        return ""
    if q <= 0.001:
        return "***"
    if q <= 0.01:
        return "**"
    if q <= 0.05:
        return "*"
    return ""


def order_taxa_by_desc_median(d: pd.DataFrame, taxa: list[str]) -> list[str]:
    present = [t for t in taxa if t in set(d["taxon"].dropna().astype(str))]
    if not present:
        return taxa
    med = (
        d[d["taxon"].isin(present)]
        .groupby("taxon", as_index=False)["rel_abundance"]
        .median()
        .sort_values("rel_abundance", ascending=False)
    )
    ordered = med["taxon"].tolist()
    missing = [t for t in taxa if t not in ordered]
    return ordered + missing


def plot_sample_type_split_boxplots(
    d: pd.DataFrame,
    tax_level: str,
    outdir: Path,
    type_col: str,
    hue_order: list[str],
    base_name: str,
    title_prefix: str,
) -> None:
    if d.empty:
        return
    threshold = SPLIT_THRESHOLD_REL_ABUND
    stats = d.groupby("taxon", as_index=False)["rel_abundance"].agg(median="median", max="max")
    # Keep the "low" panel compact: if a taxon has any value above threshold,
    # force it into the high panel even if its median is low.
    high_taxa = stats.loc[(stats["median"] > threshold) | (stats["max"] > threshold), "taxon"].tolist()
    low_taxa = stats.loc[~stats["taxon"].isin(high_taxa), "taxon"].tolist()

    d_low = d[d["taxon"].isin(low_taxa)].copy()
    low_max = float(d_low["rel_abundance"].max()) if not d_low.empty else np.nan

    def _draw(sub_df: pd.DataFrame, taxa: list[str], out_suffix: str, title_suffix: str, add_line: bool) -> None:
        if sub_df.empty or not taxa:
            return
        order = order_taxa_by_desc_median(sub_df, taxa)
        fig, ax = plt.subplots(figsize=(max(3.0, len(order) * 0.9), BOXPLOT_HEIGHT))
        sns.boxplot(
            data=sub_df,
            x="taxon",
            y="rel_abundance",
            hue=type_col,
            order=order,
            hue_order=hue_order,
            palette=PALETTE_TYPES,
            linewidth=BOXPLOT_LINEWIDTH,
            fliersize=0,
            ax=ax,
        )
        sns.stripplot(
            data=sub_df,
            x="taxon",
            y="rel_abundance",
            hue=type_col,
            order=order,
            hue_order=hue_order,
            dodge=True,
            color="black",
            size=2.2,
            alpha=0.25,
            ax=ax,
        )
        handles, labels = ax.get_legend_handles_labels()
        n = len(hue_order)
        ax.legend(handles[:n], labels[:n], frameon=False, title="Sample type", loc="upper left", bbox_to_anchor=(1.02, 1.0))
        ax.set_xlabel(f"{tax_level}", fontsize=AXIS_LABEL_SIZE)
        ax.set_ylabel("Relative abundance", fontsize=AXIS_LABEL_SIZE)
        ax.set_title(f"{title_prefix} - {tax_level} ({title_suffix})", fontsize=TITLE_SIZE)
        ax.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
        ax.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
        if add_line and np.isfinite(low_max):
            ax.axhline(low_max, ls="--", lw=0.8, color="black", alpha=0.8, zorder=0)
        style_axes(ax)
        sns.despine(ax=ax)
        fig.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
        save_fig(fig, outdir / f"{base_name}_{out_suffix}_{tax_level}")

    _draw(d[d["taxon"].isin(high_taxa)].copy(), high_taxa, "median_gt3", "median/max > 3%", add_line=True)
    _draw(d_low, low_taxa, "median_le3", "median/max <= 3%", add_line=False)


def apply_log_y_axis(ax: plt.Axes, d: pd.DataFrame) -> bool:
    vals = pd.to_numeric(d["rel_abundance"], errors="coerce")
    pos = vals[vals > 0]
    if pos.empty:
        return False
    ymin = float(pos.min()) * 0.8
    ymax = float(pos.max()) * 1.1
    if ymax <= ymin:
        ymax = ymin * 10.0
    ax.set_yscale("log")
    ax.set_ylim(ymin, ymax)
    return True


def plot_cancer_boxplots(
    rel_df: pd.DataFrame,
    cancer_res: pd.DataFrame,
    tax_level: str,
    outdir: Path,
    type_col: str,
    case_col: str,
    alpha: float,
    top_n: int,
) -> None:
    sub = cancer_res[(cancer_res["tax_level"] == tax_level) & (cancer_res["q_value"].notna())]
    if sub.empty:
        return

    # Use one fixed taxa set per taxonomic level so all sample types display the same count.
    ranked = sub.groupby("taxon", as_index=False)["q_value"].min().sort_values("q_value")
    chosen = ranked["taxon"].head(top_n).tolist()
    if not chosen:
        return

    for st in sorted(sub["sample_type"].unique()):
        s = sub[sub["sample_type"] == st]
        q_map = s.set_index("taxon")["q_value"].to_dict()

        d = rel_df[(rel_df[type_col] == st) & (rel_df["taxon"].isin(chosen))].copy()
        if d.empty:
            continue
        order = order_taxa_by_desc_median(d, chosen)

        fig, ax = plt.subplots(figsize=(max(8.0, len(order) * 0.72), BOXPLOT_HEIGHT))
        sns.boxplot(
            data=d,
            x="taxon",
            y="rel_abundance",
            hue=case_col,
            order=order,
            hue_order=["Control", "Cancer"],
            palette=PALETTE_STATUS,
            width=0.5,
            linewidth=BOXPLOT_LINEWIDTH,
            fliersize=0,
            ax=ax,
        )
        sns.stripplot(
            data=d,
            x="taxon",
            y="rel_abundance",
            hue=case_col,
            order=order,
            hue_order=["Control", "Cancer"],
            dodge=True,
            color="black",
            size=2.2,
            alpha=0.30,
            ax=ax,
        )

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[:2], labels[:2], frameon=False, title="Status", loc="upper left", bbox_to_anchor=(1.02, 1.0))
        ax.set_xlabel(f"{tax_level}", fontsize=AXIS_LABEL_SIZE)
        ax.set_ylabel("Relative abundance", fontsize=AXIS_LABEL_SIZE)
        ax.set_title(f"Cancer vs Control ({st}) - {tax_level}", fontsize=TITLE_SIZE)
        ax.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
        ax.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
        style_axes(ax)

        # Add significance markers from corrected q-values for this sample type.
        y_max = float(d["rel_abundance"].max()) if not d["rel_abundance"].empty else 0.0
        pad = max(0.002, y_max * 0.06)
        y_ann = y_max + pad
        ax.set_ylim(top=y_ann + pad * 1.5)
        for i, taxon in enumerate(order):
            stars = q_to_stars(q_map.get(taxon, np.nan))
            if stars:
                ax.text(i, y_ann, stars, ha="center", va="bottom", fontsize=11, color="black")

        sns.despine(ax=ax)
        fig.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
        save_fig(fig, outdir / f"cancer_vs_control_boxplots_{tax_level}_{st.replace(' ', '_')}")


def plot_sample_type_three_group_boxplots(
    rel_df: pd.DataFrame,
    pair_res: pd.DataFrame,
    tax_level: str,
    outdir: Path,
    patient_col: str,
    type_col: str,
    alpha: float,
    top_n: int,
) -> None:
    sub = pair_res[(pair_res["tax_level"] == tax_level) & (pair_res["q_value"].notna())].copy()
    if sub.empty:
        return

    # Choose top taxa by best q across all pairwise contrasts at this level
    ranked = sub.groupby("taxon", as_index=False)["q_value"].min().sort_values("q_value")
    sig_ranked = ranked[ranked["q_value"] <= alpha]
    chosen = sig_ranked["taxon"].head(top_n).tolist()
    if not chosen:
        chosen = ranked["taxon"].head(top_n).tolist()
    if not chosen:
        return

    d = rel_df[rel_df["taxon"].isin(chosen)].copy()
    if d.empty:
        return

    # Keep only patients with all observed sample types for paired comparability.
    needed_types = set(rel_df[type_col].dropna().astype(str).unique())
    patient_types = d.groupby(patient_col)[type_col].apply(lambda x: set(x.dropna().unique()))
    keep_patients = patient_types[patient_types.apply(lambda s: needed_types.issubset(s))].index
    d = d[d[patient_col].isin(keep_patients)]
    if d.empty:
        return
    order = order_taxa_by_desc_median(d, chosen)

    hue_order = observed_type_order(d[type_col].unique())
    fig, ax = plt.subplots(figsize=(max(9, len(order) * 0.85), BOXPLOT_HEIGHT))
    sns.boxplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        palette=PALETTE_TYPES,
        linewidth=BOXPLOT_LINEWIDTH,
        fliersize=0,
        ax=ax,
    )
    sns.stripplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        dodge=True,
        color="black",
        size=2.2,
        alpha=0.25,
        ax=ax,
    )

    handles, labels = ax.get_legend_handles_labels()
    n = len(hue_order)
    ax.legend(handles[:n], labels[:n], frameon=False, title="Sample type", loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax.set_xlabel(f"{tax_level}", fontsize=AXIS_LABEL_SIZE)
    ax.set_ylabel("Relative abundance", fontsize=AXIS_LABEL_SIZE)
    ax.set_title(f"Sample Type Comparison (All Observed Types) - {tax_level}", fontsize=TITLE_SIZE)
    ax.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
    ax.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
    style_axes(ax)
    sns.despine(ax=ax)
    fig.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
    save_fig(fig, outdir / f"sample_type_three_group_boxplots_{tax_level}")
    # Additional log-scale variant for wide dynamic-range taxa.
    fig_log, ax_log = plt.subplots(figsize=(max(9, len(order) * 0.85), BOXPLOT_HEIGHT))
    sns.boxplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        palette=PALETTE_TYPES,
        linewidth=BOXPLOT_LINEWIDTH,
        fliersize=0,
        ax=ax_log,
    )
    sns.stripplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        dodge=True,
        color="black",
        size=2.2,
        alpha=0.25,
        ax=ax_log,
    )
    handles, labels = ax_log.get_legend_handles_labels()
    n = len(hue_order)
    ax_log.legend(handles[:n], labels[:n], frameon=False, title="Sample type", loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax_log.set_xlabel(f"{tax_level}", fontsize=AXIS_LABEL_SIZE)
    ax_log.set_ylabel("Relative abundance (log scale)", fontsize=AXIS_LABEL_SIZE)
    ax_log.set_title(f"Sample Type Comparison (All Observed Types, log y) - {tax_level}", fontsize=TITLE_SIZE)
    ax_log.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
    ax_log.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
    apply_log_y_axis(ax_log, d)
    style_axes(ax_log)
    sns.despine(ax=ax_log)
    fig_log.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
    save_fig(fig_log, outdir / f"sample_type_three_group_boxplots_logy_{tax_level}")
    plot_sample_type_split_boxplots(
        d=d,
        tax_level=tax_level,
        outdir=outdir,
        type_col=type_col,
        hue_order=hue_order,
        base_name="sample_type_three_group_boxplots_split",
        title_prefix="Sample Type Comparison (All Observed Types)",
    )


def plot_sample_type_three_group_boxplots_significant(
    rel_df: pd.DataFrame,
    pair_res: pd.DataFrame,
    tax_level: str,
    outdir: Path,
    patient_col: str,
    type_col: str,
    alpha: float,
    top_n: int,
) -> None:
    sub = pair_res[
        (pair_res["tax_level"] == tax_level)
        & (pair_res["q_value"].notna())
        & (pair_res["q_value"] <= alpha)
    ].copy()
    if sub.empty:
        return

    ranked = sub.groupby("taxon", as_index=False)["q_value"].min().sort_values("q_value")
    chosen = ranked["taxon"].head(top_n).tolist()
    if not chosen:
        return

    d = rel_df[rel_df["taxon"].isin(chosen)].copy()
    if d.empty:
        return

    needed_types = set(rel_df[type_col].dropna().astype(str).unique())
    patient_types = d.groupby(patient_col)[type_col].apply(lambda x: set(x.dropna().unique()))
    keep_patients = patient_types[patient_types.apply(lambda s: needed_types.issubset(s))].index
    d = d[d[patient_col].isin(keep_patients)]
    if d.empty:
        return
    order = order_taxa_by_desc_median(d, chosen)

    hue_order = observed_type_order(d[type_col].unique())
    fig, ax = plt.subplots(figsize=(max(9, len(order) * 0.85), BOXPLOT_HEIGHT))
    sns.boxplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        palette=PALETTE_TYPES,
        linewidth=BOXPLOT_LINEWIDTH,
        fliersize=0,
        ax=ax,
    )
    sns.stripplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        dodge=True,
        color="black",
        size=2.2,
        alpha=0.25,
        ax=ax,
    )

    handles, labels = ax.get_legend_handles_labels()
    n = len(hue_order)
    ax.legend(handles[:n], labels[:n], frameon=False, title="Sample type", loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax.set_xlabel(f"{tax_level} (significant only)", fontsize=AXIS_LABEL_SIZE)
    ax.set_ylabel("Relative abundance", fontsize=AXIS_LABEL_SIZE)
    ax.set_title(f"Sample Type Comparison (Significant, All Observed Types) - {tax_level}", fontsize=TITLE_SIZE)
    ax.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
    ax.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
    style_axes(ax)
    sns.despine(ax=ax)
    fig.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
    save_fig(fig, outdir / f"sample_type_three_group_boxplots_significant_{tax_level}")
    # Additional log-scale variant for wide dynamic-range taxa.
    fig_log, ax_log = plt.subplots(figsize=(max(9, len(order) * 0.85), BOXPLOT_HEIGHT))
    sns.boxplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        palette=PALETTE_TYPES,
        linewidth=BOXPLOT_LINEWIDTH,
        fliersize=0,
        ax=ax_log,
    )
    sns.stripplot(
        data=d,
        x="taxon",
        y="rel_abundance",
        hue=type_col,
        order=order,
        hue_order=hue_order,
        dodge=True,
        color="black",
        size=2.2,
        alpha=0.25,
        ax=ax_log,
    )
    handles, labels = ax_log.get_legend_handles_labels()
    n = len(hue_order)
    ax_log.legend(handles[:n], labels[:n], frameon=False, title="Sample type", loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax_log.set_xlabel(f"{tax_level} (significant only)", fontsize=AXIS_LABEL_SIZE)
    ax_log.set_ylabel("Relative abundance (log scale)", fontsize=AXIS_LABEL_SIZE)
    ax_log.set_title(f"Sample Type Comparison (Significant, log y) - {tax_level}", fontsize=TITLE_SIZE)
    ax_log.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
    ax_log.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
    apply_log_y_axis(ax_log, d)
    style_axes(ax_log)
    sns.despine(ax=ax_log)
    fig_log.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
    save_fig(fig_log, outdir / f"sample_type_three_group_boxplots_significant_logy_{tax_level}")
    plot_sample_type_split_boxplots(
        d=d,
        tax_level=tax_level,
        outdir=outdir,
        type_col=type_col,
        hue_order=hue_order,
        base_name="sample_type_three_group_boxplots_significant_split",
        title_prefix="Sample Type Comparison (Significant, All Observed Types)",
    )


def plot_cancer_boxplots_significant_panels(
    rel_df: pd.DataFrame,
    cancer_res: pd.DataFrame,
    tax_level: str,
    outdir: Path,
    type_col: str,
    case_col: str,
    alpha: float,
    top_n: int,
) -> None:
    sub = cancer_res[
        (cancer_res["tax_level"] == tax_level)
        & (cancer_res["q_value"].notna())
        & (cancer_res["q_value"] <= alpha)
    ].copy()
    if sub.empty:
        return

    ranked = sub.groupby("taxon", as_index=False)["q_value"].min().sort_values("q_value")
    chosen = ranked["taxon"].head(top_n).tolist()
    if not chosen:
        return

    sample_types = [x for x in ["BAL", "Lung Brush", "Oral Rinse"] if x in sub["sample_type"].dropna().unique()]
    if not sample_types:
        return

    for st in sample_types:
        s = sub[sub["sample_type"] == st]
        q_map = s.set_index("taxon")["q_value"].to_dict()

        d = rel_df[(rel_df[type_col] == st) & (rel_df["taxon"].isin(chosen))].copy()
        if d.empty:
            continue
        order = order_taxa_by_desc_median(d, chosen)
        fig, ax = plt.subplots(figsize=(max(8.0, len(order) * 0.72), BOXPLOT_HEIGHT))

        sns.boxplot(
            data=d,
            x="taxon",
            y="rel_abundance",
            hue=case_col,
            order=order,
            hue_order=["Control", "Cancer"],
            palette=PALETTE_STATUS,
            width=0.5,
            linewidth=BOXPLOT_LINEWIDTH,
            fliersize=0,
            ax=ax,
        )
        sns.stripplot(
            data=d,
            x="taxon",
            y="rel_abundance",
            hue=case_col,
            order=order,
            hue_order=["Control", "Cancer"],
            dodge=True,
            color="black",
            size=2.2,
            alpha=0.30,
            ax=ax,
        )

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[:2], labels[:2], frameon=False, title="Status", loc="upper left", bbox_to_anchor=(1.02, 1.0))
        ax.set_xlabel(f"{tax_level} (significant)", fontsize=AXIS_LABEL_SIZE)
        ax.set_ylabel("Relative abundance", fontsize=AXIS_LABEL_SIZE)
        ax.set_title(st, fontsize=TITLE_SIZE)
        ax.tick_params(axis="x", rotation=45, labelsize=AXIS_TICK_SIZE)
        ax.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
        style_axes(ax)

        y_max = float(d["rel_abundance"].max()) if not d["rel_abundance"].empty else 0.0
        pad = max(0.002, y_max * 0.06)
        y_ann = y_max + pad
        ax.set_ylim(top=y_ann + pad * 1.5)
        for i, taxon in enumerate(order):
            stars = q_to_stars(q_map.get(taxon, np.nan))
            if stars:
                ax.text(i, y_ann, stars, ha="center", va="bottom", fontsize=11, color="black")
        sns.despine(ax=ax)
        fig.subplots_adjust(**BOXPLOT_SUBPLOT_ADJUST)
        save_fig(fig, outdir / f"cancer_vs_control_boxplots_significant_{tax_level}_{st.replace(' ', '_')}")


def effect_heatmap(df: pd.DataFrame, tax_level: str, col_name: str, value_name: str, out_base: Path, top_n: int) -> None:
    sub = df[df["tax_level"] == tax_level].copy()
    if sub.empty:
        return

    # pick taxa with best min q across comparisons
    ranked = (
        sub.groupby("taxon", as_index=False)["q_value"].min().sort_values("q_value").head(top_n)["taxon"].tolist()
    )
    m = sub[sub["taxon"].isin(ranked)].pivot(index="taxon", columns=col_name, values=value_name)
    m = m.reindex(ranked)
    if m.empty:
        return

    vmax = np.nanmax(np.abs(m.values))
    vmax = 0.01 if (not np.isfinite(vmax) or vmax == 0) else vmax

    fig, ax = plt.subplots(figsize=(max(7, 0.8 * m.shape[1] + 2), max(5, 0.35 * m.shape[0] + 2)))
    sns.heatmap(
        m,
        cmap="coolwarm",
        center=0,
        vmin=-vmax,
        vmax=vmax,
        linewidths=0.4,
        linecolor="white",
        cbar_kws={"label": value_name},
        ax=ax,
    )
    ax.set_title(f"Effect Heatmap ({tax_level})", fontsize=TITLE_SIZE)
    ax.set_xlabel(col_name, fontsize=AXIS_LABEL_SIZE)
    ax.set_ylabel(tax_level, fontsize=AXIS_LABEL_SIZE)
    ax.tick_params(axis="x", labelsize=AXIS_TICK_SIZE)
    ax.tick_params(axis="y", labelsize=AXIS_TICK_SIZE)
    style_axes(ax)
    save_fig(fig, out_base)


def main() -> None:
    p = argparse.ArgumentParser(description="Plot observed taxonomic analysis results")
    p.add_argument("--data-long", required=True)
    p.add_argument("--cancer-results", required=True)
    p.add_argument("--sampletype-results", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--alpha", type=float, default=0.05)
    p.add_argument("--top-n", type=int, default=12)
    p.add_argument("--sample-col", default="sample")
    p.add_argument("--patient-col", default="Participant_ID")
    p.add_argument("--type-col", default="type_group")
    p.add_argument("--case-col", default="Case")
    p.add_argument("--count-col", default="count")
    p.add_argument("--type-palette", default="", help="Sample-type label=#hex palette.")
    p.add_argument("--case-palette", default="", help="Case label=#hex palette.")
    args = p.parse_args()

    PALETTE_TYPES.update(parse_palette(args.type_palette))
    PALETTE_STATUS.update({
        ("Control" if label in {"Control", "Non-Cancer"} else label): color
        for label, color in parse_palette(args.case_palette).items()
    })

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    long_df = pd.read_csv(args.data_long, sep="\t", low_memory=False)
    long_df[args.type_col] = long_df[args.type_col].map(canonicalize_sample_type)
    cancer_res = pd.read_csv(args.cancer_results, sep="\t")
    pair_res = pd.read_csv(args.sampletype_results, sep="\t")
    if "sample_type" in cancer_res.columns:
        cancer_res["sample_type"] = cancer_res["sample_type"].map(canonicalize_sample_type)
    if "group1" in pair_res.columns:
        pair_res["group1"] = pair_res["group1"].map(canonicalize_sample_type)
    if "group2" in pair_res.columns:
        pair_res["group2"] = pair_res["group2"].map(canonicalize_sample_type)
    if {"group1", "group2"}.issubset(pair_res.columns):
        pair_res["contrast"] = pair_res["group1"].astype(str) + "_vs_" + pair_res["group2"].astype(str)

    for tax_level in ["Phylum", "Family"]:
        rel_df = patient_type_taxon_rel(
            long_df,
            tax_level=tax_level,
            sample_col=args.sample_col,
            patient_col=args.patient_col,
            type_col=args.type_col,
            case_col=args.case_col,
            count_col=args.count_col,
        )
        plot_cancer_boxplots(
            rel_df,
            cancer_res,
            tax_level=tax_level,
            outdir=outdir,
            type_col=args.type_col,
            case_col=args.case_col,
            alpha=args.alpha,
            top_n=args.top_n,
        )

        plot_sample_type_three_group_boxplots(
            rel_df,
            pair_res,
            tax_level=tax_level,
            outdir=outdir,
            patient_col=args.patient_col,
            type_col=args.type_col,
            alpha=args.alpha,
            top_n=args.top_n,
        )
        plot_sample_type_three_group_boxplots_significant(
            rel_df,
            pair_res,
            tax_level=tax_level,
            outdir=outdir,
            patient_col=args.patient_col,
            type_col=args.type_col,
            alpha=args.alpha,
            top_n=args.top_n,
        )
        plot_cancer_boxplots_significant_panels(
            rel_df,
            cancer_res,
            tax_level=tax_level,
            outdir=outdir,
            type_col=args.type_col,
            case_col=args.case_col,
            alpha=args.alpha,
            top_n=args.top_n,
        )

        effect_heatmap(
            cancer_res,
            tax_level=tax_level,
            col_name="sample_type",
            value_name="delta_median",
            out_base=outdir / f"effect_heatmap_cancer_{tax_level}",
            top_n=args.top_n,
        )
        effect_heatmap(
            pair_res,
            tax_level=tax_level,
            col_name="contrast",
            value_name="median_delta_g1_minus_g2",
            out_base=outdir / f"effect_heatmap_sample_type_{tax_level}",
            top_n=args.top_n,
        )

    print(f"Saved plots to: {outdir}")


if __name__ == "__main__":
    main()
