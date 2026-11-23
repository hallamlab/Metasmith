#!/usr/bin/env python3
"""
Render downstream plots for ASV-to-genome barrnap linkage outputs.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def read_tsv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, sep="\t", low_memory=False)


def save_png_and_svg(path: Path) -> None:
    """Save the current matplotlib figure as PNG plus matching SVG."""
    plt.savefig(path, dpi=300)
    plt.savefig(path.with_suffix(".svg"))


def add_display_label(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "genome_id" in df.columns:
        base_col = "genome_id"
    else:
        return df

    species_col = "mag_species" if "mag_species" in df.columns else None
    genus_col = "mag_genus" if "mag_genus" in df.columns else None
    tier_col = "mag_mimag_tier" if "mag_mimag_tier" in df.columns else None

    labels = []
    for _, row in df.iterrows():
        genome_id = row.get(base_col)
        species = row.get(species_col) if species_col else None
        genus = row.get(genus_col) if genus_col else None
        tier = row.get(tier_col) if tier_col else None
        label = str(genome_id)
        if pd.notna(species) and str(species).strip():
            label = f"{genome_id} | {species}"
        elif pd.notna(genus) and str(genus).strip():
            label = f"{genome_id} | {genus}"
        if pd.notna(tier) and str(tier).strip():
            label = f"{label} ({tier})"
        labels.append(label)
    df["display_label"] = labels
    return df


def plot_top_genome_status(pairing: pd.DataFrame, plots_dir: Path, top_n: int) -> None:
    plot_df = pairing.loc[pairing["pairing_status"] != "unpaired"].copy()
    plot_df = plot_df.dropna(subset=["genome_id"])
    if plot_df.empty:
        return
    plot_df = add_display_label(plot_df)
    top = plot_df["genome_id"].value_counts().head(top_n).index
    plot_df = plot_df.loc[plot_df["genome_id"].isin(top)].copy()
    grouped = (
        plot_df.groupby(["genome_id", "display_label", "pairing_status"])["ASV_ID"]
        .nunique()
        .reset_index(name="n_asvs")
    )
    order = (
        plot_df["genome_id"]
        .value_counts()
        .head(top_n)
        .sort_values(ascending=True)
        .index
    )
    label_map = (
        plot_df.drop_duplicates(subset=["genome_id"])[["genome_id", "display_label"]]
        .set_index("genome_id")["display_label"]
        .to_dict()
    )
    plt.figure(figsize=(9, max(4, 0.4 * len(order))))
    ax = sns.barplot(
        data=grouped,
        y="display_label",
        x="n_asvs",
        hue="pairing_status",
        order=[label_map.get(x, x) for x in order],
        palette={"paired_unique": "#4C78A8", "paired_ambiguous": "#F58518"},
    )
    ax.set_xlabel("ASVs assigned")
    ax.set_ylabel("Genome/MAG")
    ax.set_title("Top genome/MAG pairings by assignment status")
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), title="Pairing status")
    plt.tight_layout()
    save_png_and_svg(plots_dir / "asv2mag_top_genomes_stacked_status.png")
    plt.close()


def plot_quality_by_status(pairing: pd.DataFrame, plots_dir: Path) -> None:
    if pairing.empty:
        return
    plot_df = pairing.dropna(subset=["link_pident", "link_qcov"]).copy()
    if plot_df.empty:
        return
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=False)
    sns.boxplot(
        data=plot_df,
        x="pairing_status",
        y="link_pident",
        order=["paired_unique", "paired_ambiguous"],
        palette=["#4C78A8", "#F58518"],
        ax=axes[0],
    )
    axes[0].set_title("Best-hit identity by pairing status")
    axes[0].set_xlabel("")
    axes[0].set_ylabel("Percent identity")
    sns.boxplot(
        data=plot_df,
        x="pairing_status",
        y="link_qcov",
        order=["paired_unique", "paired_ambiguous"],
        palette=["#4C78A8", "#F58518"],
        ax=axes[1],
    )
    axes[1].set_title("Best-hit query coverage by pairing status")
    axes[1].set_xlabel("")
    axes[1].set_ylabel("Query coverage (%)")
    for ax in axes:
        ax.tick_params(axis="x", rotation=20)
    plt.tight_layout()
    save_png_and_svg(plots_dir / "asv2mag_hit_quality_by_status.png")
    plt.close()


def plot_top_genome_metrics(genome_summary: pd.DataFrame, plots_dir: Path, top_n: int) -> None:
    if genome_summary.empty:
        return
    plot_df = add_display_label(genome_summary.head(top_n).copy())
    plot_df = plot_df.set_index("display_label")[
        ["n_best_asvs", "n_unique_asvs", "n_ambiguous_asvs", "median_pident", "median_qcov"]
    ]
    if plot_df.empty:
        return
    plt.figure(figsize=(8, max(4, 0.45 * len(plot_df))))
    ax = sns.heatmap(plot_df, cmap="viridis", annot=True, fmt=".1f", cbar_kws={"label": "Value"})
    ax.set_title("Top genome/MAG linkage summary")
    ax.set_xlabel("")
    ax.set_ylabel("Genome/MAG")
    plt.tight_layout()
    save_png_and_svg(plots_dir / "asv2mag_top_genome_metrics_heatmap.png")
    plt.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot ASV-to-genome barrnap linkage summaries.")
    parser.add_argument("--input-dir", required=True, type=Path, help="ASV-MAG linkage output directory.")
    parser.add_argument("--top-n", type=int, default=20, help="Top genomes/MAGs to display in summary plots.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    sns.set_theme(style="whitegrid")
    tables_dir = args.input_dir / "tables"
    plots_dir = args.input_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    pairing = read_tsv(tables_dir / "asv2mag_pairing.tsv")
    genome_summary = read_tsv(tables_dir / "asv2mag_genome_summary.tsv")

    plot_top_genome_status(pairing, plots_dir, args.top_n)
    plot_quality_by_status(pairing, plots_dir)
    plot_top_genome_metrics(genome_summary, plots_dir, args.top_n)


if __name__ == "__main__":
    main()
