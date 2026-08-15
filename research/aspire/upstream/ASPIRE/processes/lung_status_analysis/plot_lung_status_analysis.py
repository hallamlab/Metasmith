#!/usr/bin/env python3
"""
Visualize lung status analysis results (TumorSide vs Contralateral vs Healthy).

Creates:
1. PCoA plot (proper eigendecomposition) with paired connection lines
2. Alpha diversity boxplots by lung_status (sample-level and patient-level)
3. Bray-Curtis distance boxplots for all three contrasts (A/B/C)
4. R² summary bar plot for all contrasts with FDR-corrected q-values

Dependencies:
- scikit-bio (for proper PCoA)
- pandas, numpy, matplotlib, seaborn
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.spatial.distance import squareform
from skbio.stats.ordination import pcoa

# Match ASPIRE plotting defaults
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
mpl.rcParams["savefig.dpi"] = 600
plt.rcParams.update({"font.size": 12})
sns.set_theme()
sns.set_style("white")

PALETTE_LUNG_STATUS = {
    "TumorSide": "#E31A1C",  # Red
    "Contralateral": "#FF7F00",  # Orange
    "Healthy": "#33A02C",  # Green
}


def save_fig(fig: plt.Figure, out_base: Path) -> None:
    out_base.parent.mkdir(parents=True, exist_ok=True)
    for ext in ("svg", "pdf"):
        fig.savefig(out_base.with_suffix(f".{ext}"), bbox_inches="tight")
    plt.close(fig)


def shannon_from_counts(counts: np.ndarray) -> float:
    counts = np.asarray(counts, dtype=float)
    total = counts.sum()
    if total <= 0:
        return 0.0
    p = counts / total
    p = p[p > 0]
    return float(-np.sum(p * np.log(p)))


def attach_sample_level_shannon(metadata: pd.DataFrame, asv_table: Path | None) -> pd.DataFrame:
    out = metadata.copy()
    if "shannon" in out.columns:
        return out
    if asv_table is None or not asv_table.exists():
        return out

    asv = pd.read_csv(asv_table, sep="\t", index_col=0)
    shannon = asv.apply(lambda r: shannon_from_counts(r.values), axis=1)
    out["shannon"] = out["sample"].map(shannon.to_dict())
    return out


def plot_pcoa(
    metadata: pd.DataFrame,
    dist_mat: pd.DataFrame,
    outdir: Path,
    title: str = "PCoA: Lung Status",
) -> None:
    """Create PCoA plot with paired connections for cancer patients."""
    # Run proper PCoA (eigendecomposition of distance matrix)
    from skbio import DistanceMatrix
    dm = DistanceMatrix(dist_mat.values, ids=dist_mat.index.astype(str))
    pcoa_result = pcoa(dm)

    # Extract coordinates and variance explained
    coords = pcoa_result.samples.values[:, :2]
    pc1_var = pcoa_result.proportion_explained.iloc[0] * 100
    pc2_var = pcoa_result.proportion_explained.iloc[1] * 100

    pcoa_df = pd.DataFrame(coords, columns=["PC1", "PC2"])
    pcoa_df.index = dist_mat.index

    # Merge with metadata
    patient_col = "Participant_ID" if "Participant_ID" in metadata.columns else "patient_code"

    # Handle patient_level metadata (rownames are "PatientID_lung_status")
    # Check if index looks like patient-level (contains underscore in string format)
    first_index = str(pcoa_df.index[0])
    if "_" in first_index and patient_col in metadata.columns:
        # Patient-level data
        metadata_use = metadata.copy()
        metadata_use["pcoa_key"] = metadata_use[patient_col].astype(str) + "_" + metadata_use["lung_status"]
        pcoa_df = pcoa_df.merge(
            metadata_use[[patient_col, "lung_status", "pcoa_key"]],
            left_index=True,
            right_on="pcoa_key",
            how="left",
        )
    else:
        # Sample-level metadata
        pcoa_df = pcoa_df.merge(
            metadata[["sample", patient_col, "lung_status"]],
            left_index=True,
            right_on="sample",
            how="left",
        )

    fig, ax = plt.subplots(figsize=(10, 8))

    # Draw lines connecting paired samples from same cancer patient
    cancer_patients = pcoa_df[
        pcoa_df["lung_status"].isin(["TumorSide", "Contralateral"])
    ][patient_col].unique()

    for pat in cancer_patients:
        pat_data = pcoa_df[pcoa_df[patient_col] == pat]
        if len(pat_data) == 2:
            ax.plot(
                pat_data["PC1"],
                pat_data["PC2"],
                color="gray",
                linewidth=1.5,
                alpha=0.5,
                zorder=1,
            )

    # Plot points
    for status, color in PALETTE_LUNG_STATUS.items():
        subset = pcoa_df[pcoa_df["lung_status"] == status]
        if not subset.empty:
            ax.scatter(
                subset["PC1"],
                subset["PC2"],
                c=color,
                label=status,
                s=150,
                alpha=0.8,
                edgecolors="black",
                linewidth=1,
                zorder=2,
            )

    ax.set_xlabel(f"PC1 ({pc1_var:.1f}%)")
    ax.set_ylabel(f"PC2 ({pc2_var:.1f}%)")
    ax.set_title(title)
    ax.legend(frameon=True, loc="upper left", bbox_to_anchor=(1.02, 1.0))
    ax.axhline(0, color="black", linewidth=0.5, linestyle="--", alpha=0.3)
    ax.axvline(0, color="black", linewidth=0.5, linestyle="--", alpha=0.3)
    sns.despine(ax=ax)

    save_fig(fig, outdir / "PCoA_lung_status")


def plot_alpha_boxplot(
    metadata: pd.DataFrame,
    patient_level: pd.DataFrame,
    outdir: Path,
) -> None:
    """Alpha diversity boxplots with paired connections."""
    has_sample_alpha = "shannon" in metadata.columns and "lung_status" in metadata.columns
    has_patient_alpha = "shannon_mean" in patient_level.columns and "lung_status" in patient_level.columns
    if not has_sample_alpha and not has_patient_alpha:
        return

    order = ["TumorSide", "Contralateral", "Healthy"]
    present_order = [x for x in order if x in pd.concat([metadata["lung_status"], patient_level["lung_status"]]).dropna().unique()]

    # Plot one or two panels depending on available alpha fields
    if has_sample_alpha and has_patient_alpha:
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        ax_sample, ax_patient = axes[0], axes[1]
    else:
        fig, ax = plt.subplots(1, 1, figsize=(7, 6))
        ax_sample = ax if has_sample_alpha else None
        ax_patient = ax if has_patient_alpha else None

    if has_sample_alpha and ax_sample is not None:
        sns.boxplot(
            data=metadata,
            x="lung_status",
            y="shannon",
            hue="lung_status",
            order=present_order,
            hue_order=present_order,
            palette=PALETTE_LUNG_STATUS,
            legend=False,
            fliersize=0,
            ax=ax_sample,
        )
        sns.stripplot(
            data=metadata,
            x="lung_status",
            y="shannon",
            order=present_order,
            color="black",
            alpha=0.4,
            size=5,
            ax=ax_sample,
        )
        ax_sample.set_ylabel("Shannon Diversity")
        ax_sample.set_xlabel("Lung Status")
        ax_sample.set_title("Sample-Level Shannon Diversity")
        sns.despine(ax=ax_sample)

    if has_patient_alpha and ax_patient is not None:
        sns.boxplot(
            data=patient_level,
            x="lung_status",
            y="shannon_mean",
            hue="lung_status",
            order=present_order,
            hue_order=present_order,
            palette=PALETTE_LUNG_STATUS,
            legend=False,
            fliersize=0,
            ax=ax_patient,
        )
        sns.stripplot(
            data=patient_level,
            x="lung_status",
            y="shannon_mean",
            order=present_order,
            color="black",
            alpha=0.4,
            size=5,
            ax=ax_patient,
        )
        ax_patient.set_ylabel("Shannon Diversity (Patient Mean)")
        ax_patient.set_xlabel("Lung Status")
        ax_patient.set_title("Patient-Level Shannon Diversity")
        sns.despine(ax=ax_patient)

    plt.tight_layout()
    save_fig(fig, outdir / "Alpha_diversity_lung_status")


def plot_contrast_distances(
    summary: pd.DataFrame,
    pairdist_a: pd.DataFrame,
    patient_dist_mat: pd.DataFrame,
    patient_metadata: pd.DataFrame,
    outdir: Path,
) -> None:
    """Single combined boxplot of Bray-Curtis distances for contrasts A/B/C."""
    combined = []

    # A
    if not pairdist_a.empty and "bray" in pairdist_a.columns:
        tmp = pairdist_a[["bray"]].copy()
        tmp["contrast"] = "A: TumorSide vs Contralateral"
        combined.append(tmp)

    # B/C from patient-level matrix
    if not patient_dist_mat.empty and not patient_metadata.empty:
        dist_mat_arr = patient_dist_mat.values
        ids = patient_dist_mat.index.astype(str)
        rows = []
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                id_i_parts = ids[i].rsplit("_", 1)
                id_j_parts = ids[j].rsplit("_", 1)
                if len(id_i_parts) != 2 or len(id_j_parts) != 2:
                    continue
                pat_i, status_i = id_i_parts
                pat_j, status_j = id_j_parts
                if pat_i == pat_j:
                    continue
                pair = tuple(sorted([status_i, status_j]))
                if pair == ("Contralateral", "Healthy"):
                    label = "B: Contralateral vs Healthy"
                elif pair == ("Healthy", "TumorSide"):
                    label = "C: TumorSide vs Healthy"
                else:
                    continue
                rows.append({"bray": dist_mat_arr[i, j], "contrast": label})
        if rows:
            combined.append(pd.DataFrame(rows))

    if not combined:
        return

    dist_df = pd.concat(combined, ignore_index=True)
    order = [
        "A: TumorSide vs Contralateral",
        "B: Contralateral vs Healthy",
        "C: TumorSide vs Healthy",
    ]
    order = [x for x in order if x in dist_df["contrast"].unique()]
    palette = {
        "A: TumorSide vs Contralateral": PALETTE_LUNG_STATUS["TumorSide"],
        "B: Contralateral vs Healthy": PALETTE_LUNG_STATUS["Contralateral"],
        "C: TumorSide vs Healthy": "#6A3D9A",
    }

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.boxplot(
        data=dist_df,
        x="contrast",
        y="bray",
        hue="contrast",
        order=order,
        hue_order=order,
        palette=palette,
        legend=False,
        fliersize=0,
        ax=ax,
    )
    sns.stripplot(
        data=dist_df,
        x="contrast",
        y="bray",
        order=order,
        color="black",
        alpha=0.4,
        size=4,
        ax=ax,
    )
    ax.set_xlabel("Planned Contrast")
    ax.set_ylabel("Bray-Curtis Distance")
    ax.set_title("Bray-Curtis Distances Across Planned Contrasts (A/B/C)")
    ax.tick_params(axis="x", rotation=12)
    sns.despine(ax=ax, left=False, bottom=False)
    save_fig(fig, outdir / "Distance_contrasts_ABC")


def plot_r2_summary(summary: pd.DataFrame, outdir: Path) -> None:
    """Bar plot of PERMANOVA R² values for all contrasts."""
    if summary.empty:
        return

    plot_df = summary[pd.to_numeric(summary["permanova_R2"], errors="coerce").notna()].copy()
    if plot_df.empty:
        return
    plot_df = plot_df.sort_values("permanova_R2", ascending=True)

    # Create color mapping
    colors = []
    for contrast in plot_df["contrast"]:
        if "TumorSide_vs_Contralateral" in contrast:
            colors.append(PALETTE_LUNG_STATUS["TumorSide"])
        elif "Contralateral_vs_Healthy" in contrast:
            colors.append(PALETTE_LUNG_STATUS["Contralateral"])
        elif "TumorSide_vs_Healthy" in contrast:
            colors.append("#6A3D9A")  # Purple for T vs H
        else:
            colors.append("#888888")

    # Clean labels
    plot_df["contrast_label"] = plot_df["contrast"].str.replace("_", " ").str.replace(
        "A ", "A: "
    ).str.replace("B ", "B: ").str.replace("C ", "C: ")

    fig, ax = plt.subplots(figsize=(10, 6))
    y = np.arange(len(plot_df))
    bars = ax.barh(y, plot_df["permanova_R2"], color=colors, alpha=0.85)

    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["contrast_label"])
    ax.set_xlabel("PERMANOVA R²")
    ax.set_title("Effect Sizes: Lung Status Contrasts")

    # Add p-values and sample sizes
    for i, (_, r) in enumerate(plot_df.iterrows()):
        p = r.get("permanova_p", np.nan)
        q = r.get("permanova_q", np.nan)
        n = r.get("n_patients", "?")

        if pd.notna(q):
            sig = "***" if q < 0.001 else "**" if q < 0.01 else "*" if q < 0.05 else "ns"
            label = f"q={q:.3g} {sig}\n(n={n})"
        elif pd.notna(p):
            label = f"p={p:.3g}\n(n={n})"
        else:
            label = f"(n={n})"

        ax.text(
            float(r["permanova_R2"]) + 0.005,
            i,
            label,
            va="center",
            ha="left",
            fontsize=9,
        )

    ax.grid(axis="x", linestyle=":", alpha=0.3)
    ax.set_xlim(0, max(plot_df["permanova_R2"]) * 1.25)
    sns.despine(ax=ax, left=False, bottom=False)

    save_fig(fig, outdir / "PERMANOVA_R2_contrasts")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Visualize lung status analysis (TumorSide vs Contralateral vs Healthy)"
    )
    p.add_argument(
        "--metadata",
        type=Path,
        required=True,
        help="Sample-level metadata with lung_status",
    )
    p.add_argument(
        "--patient-level",
        type=Path,
        required=True,
        help="Patient-level metadata (from R analysis)",
    )
    p.add_argument(
        "--distances",
        type=Path,
        required=True,
        help="Patient-level Bray-Curtis distance matrix",
    )
    p.add_argument(
        "--summary",
        type=Path,
        required=True,
        help="Contrast summary table (from R analysis)",
    )
    p.add_argument(
        "--pairdist-a",
        type=Path,
        default=None,
        help="Contrast A pairwise distances (optional)",
    )
    p.add_argument(
        "--asv-table",
        type=Path,
        default=None,
        help="ASV table (rows=samples, cols=ASVs) for computing sample-level Shannon if missing",
    )
    p.add_argument("--outdir", type=Path, required=True, help="Output directory")

    args = p.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    # Load data
    print("Loading data...")
    metadata = pd.read_csv(args.metadata, sep="\t")
    patient_level = pd.read_csv(args.patient_level, sep="\t")
    dist_mat = pd.read_csv(args.distances, sep="\t", index_col=0)
    summary = pd.read_csv(args.summary, sep="\t")
    metadata = attach_sample_level_shannon(metadata, args.asv_table)

    pairdist_a = pd.DataFrame()
    if args.pairdist_a and args.pairdist_a.exists():
        pairdist_a = pd.read_csv(args.pairdist_a, sep="\t")

    print(f"  {len(metadata)} samples")
    print(f"  {len(patient_level)} patient-level profiles")
    print(f"  {len(summary)} contrasts")

    # Create plots
    print("\nGenerating PCoA plot...")
    plot_pcoa(patient_level, dist_mat, args.outdir)

    print("Generating alpha diversity plots...")
    plot_alpha_boxplot(metadata, patient_level, args.outdir)

    print("Generating contrast distance plots...")
    plot_contrast_distances(summary, pairdist_a, dist_mat, patient_level, args.outdir)

    print("Generating R² summary plot...")
    plot_r2_summary(summary, args.outdir)

    print(f"\nPlots saved to: {args.outdir}")


if __name__ == "__main__":
    main()
