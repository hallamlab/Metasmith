#!/usr/bin/env python3
"""
Clustering analysis of metabolomics differential expression data.

Combines all 3 nutrient conditions (Selenium/8, Phosphorus/P, Sulfur/S) into a
single 18-feature vector per metabolite (log2FC + -log10p at 3 timepoints × 3
conditions), applies GMM clustering (BIC-selected, k=2..9), then visualizes
with a 4-panel UMAP (clusters + nutrient overlays) and a cluster-colored
volcano plot grid.

Merges cluster assignments into compound_metadata.csv and slims
differential_results.csv by removing redundant columns.

Usage:
    python clustering_analysis.py <output_dir>
"""

import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from sklearn.preprocessing import StandardScaler
from sklearn.mixture import GaussianMixture
import umap
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

# ── Paths ──────────────────────────────────────────────────────────────
if len(sys.argv) < 2:
    print("Usage: python clustering_analysis.py <output_dir>", file=sys.stderr)
    sys.exit(1)

OUT_DIR = Path(sys.argv[1])
DIFF_CSV = OUT_DIR / "differential_results.csv"
COMPOUND_CSV = OUT_DIR / "compound_metadata.csv"
GENE_MAP_CSV = OUT_DIR / "metabolite_gene_mapping.csv"

CONDITIONS = ["8", "P", "S"]
CONDITION_LABELS = {
    "8": "Se",
    "P": "P",
    "S": "S",
}
CONDITION_FULL = {
    "8": "Selenium (8 µM)",
    "P": "Phosphorus deprivation",
    "S": "Sulfur deprivation",
}
TIMEPOINTS = [24, 72, 144]

# Plotly default qualitative palette (D3 / category10)
PLOTLY_COLORS = [
    "#636EFA",  # muted blue
    "#EF553B",  # red
    "#00CC96",  # green
    "#AB63FA",  # purple
    "#FFA15A",  # orange
    "#19D3F3",  # cyan
    "#FF6692",  # pink
    "#B6E880",  # lime
    "#FF97FF",  # magenta
    "#FECB52",  # yellow
]

CLUSTER_COLORS = {
    "upregulated": "tomato",
    "downregulated": "steelblue",
    "insignificant": "gray",
}
CLUSTER_ALPHAS = {
    "upregulated": 0.8,
    "downregulated": 0.8,
    "insignificant": 0.5,
}


# ── 1. Data Loading & Combined Feature Construction ────────────────────
def build_combined_feature_matrix(df_diff):
    """Build 18-feature matrix: 6 features (log2FC + -log10p x 3 timepoints) x 3 conditions."""
    df_diff = df_diff.copy()
    df_diff["neg_log10p"] = -np.log10(df_diff["pval"].clip(lower=1e-300))

    all_compounds = df_diff[["compound_id"]].drop_duplicates().reset_index(drop=True)

    feature_cols = []
    for cond in CONDITIONS:
        for tp in TIMEPOINTS:
            feature_cols.append(f"log2FC_{CONDITION_LABELS[cond]}_{tp}h")
            feature_cols.append(f"neg_log10p_{CONDITION_LABELS[cond]}_{tp}h")

    feat_df = all_compounds.copy()
    for cond in CONDITIONS:
        cond_label = CONDITION_LABELS[cond]
        sub = df_diff[df_diff["condition"] == cond]
        for tp in TIMEPOINTS:
            tp_sub = sub[sub["timepoint_h"] == tp][["compound_id", "log2FC", "neg_log10p"]]
            tp_sub = tp_sub.rename(columns={
                "log2FC": f"log2FC_{cond_label}_{tp}h",
                "neg_log10p": f"neg_log10p_{cond_label}_{tp}h",
            })
            tp_sub = tp_sub.drop_duplicates(subset="compound_id", keep="first")
            feat_df = feat_df.merge(tp_sub, on="compound_id", how="left")

    feat_df[feature_cols] = feat_df[feature_cols].fillna(0.0)
    return feat_df, feature_cols


# ── 2. GMM Clustering with BIC Selection ──────────────────────────────
def fit_gmm(X_scaled, k_range=range(2, 10), random_state=42):
    """Fit GMM for k range, select best k via BIC elbow (first k where improvement < 5%)."""
    bics = {}
    models = {}
    for k in k_range:
        gmm = GaussianMixture(
            n_components=k, covariance_type="full",
            random_state=random_state, n_init=5,
        )
        gmm.fit(X_scaled)
        bics[k] = gmm.bic(X_scaled)
        models[k] = gmm

    print(f"  BIC: {', '.join(f'k={k}:{v:.0f}' for k, v in sorted(bics.items()))}")

    # Elbow: pick first k where marginal BIC improvement drops below 5%
    ks = sorted(bics.keys())
    best_k = ks[0]
    for i in range(1, len(ks)):
        prev_bic = bics[ks[i - 1]]
        curr_bic = bics[ks[i]]
        improvement = (prev_bic - curr_bic) / abs(prev_bic)
        if improvement < 0.05:
            break
        best_k = ks[i]

    print(f"  Selected k={best_k} (BIC={bics[best_k]:.0f}, elbow criterion: <5% marginal improvement)")
    return models[best_k], best_k


def label_gmm_clusters(gmm, X_scaled, scaler, feature_cols):
    """Assign semantic labels based on mean log2FC of each GMM component centroid."""
    cluster_ids = gmm.predict(X_scaled)
    centroids_orig = scaler.inverse_transform(gmm.means_)
    fc_idx = [i for i, c in enumerate(feature_cols) if c.startswith("log2FC")]

    semantic_map = {}
    for ci in range(gmm.n_components):
        mean_fc = centroids_orig[ci, fc_idx].mean()
        if mean_fc > 0.3:
            semantic_map[ci] = "upregulated"
        elif mean_fc < -0.3:
            semantic_map[ci] = "downregulated"
        else:
            semantic_map[ci] = "insignificant"

    named_labels = [semantic_map[c] for c in cluster_ids]
    return named_labels, cluster_ids, semantic_map


# ── 3. UMAP 4-panel (clusters + 3 nutrient overlays) ─────────────────
def _fc_to_rgba(fc_mean, neg_log10p_mean, fc_max, p_max):
    """Map fold-change -> red/blue hue+saturation, -log10p -> alpha."""
    sat = min(abs(fc_mean) / max(fc_max, 1e-6), 1.0)
    alpha = 0.08 + 0.82 * min(neg_log10p_mean / max(p_max, 1e-6), 1.0)
    if fc_mean >= 0:
        r, g, b = 1.0, 1.0 - sat, 1.0 - sat
    else:
        r, g, b = 1.0 - sat, 1.0 - sat, 1.0
    return (r, g, b, alpha)


def plot_umap_panels(X_scaled, cluster_ids, n_clusters, feat_df, out_dir):
    """4-panel UMAP: supervised by GMM cluster labels."""
    reducer = umap.UMAP(n_neighbors=10, min_dist=0.8, spread=2.0,
                        repulsion_strength=1.5, random_state=42,
                        target_metric="categorical")
    X_umap = reducer.fit_transform(X_scaled, y=cluster_ids)

    fig, axes = plt.subplots(2, 2, figsize=(14, 12))

    # ── Panel 0: GMM clusters (top-left) ──
    ax = axes[0, 0]
    for ci in range(n_clusters):
        mask = cluster_ids == ci
        color = PLOTLY_COLORS[ci % len(PLOTLY_COLORS)]
        ax.scatter(X_umap[mask, 0], X_umap[mask, 1],
                   c=color, s=14, alpha=0.7,
                   label=f"C{ci} (n={mask.sum()})", edgecolors="none")
    ax.set_title(f"GMM clusters (k={n_clusters})", fontsize=11, fontweight="bold")
    ax.legend(loc="best", fontsize=6, ncol=2, markerscale=1.5)
    # ── Panels 1-3: per-nutrient FC/significance overlays ──
    for panel_i, cond in enumerate(CONDITIONS):
        row, col = divmod(panel_i + 1, 2)
        ax = axes[row, col]
        cond_label = CONDITION_LABELS[cond]

        fc_cols = [f"log2FC_{cond_label}_{tp}h" for tp in TIMEPOINTS]
        p_cols = [f"neg_log10p_{cond_label}_{tp}h" for tp in TIMEPOINTS]
        fc_mean = feat_df[fc_cols].mean(axis=1).values
        p_mean = feat_df[p_cols].mean(axis=1).values

        fc_max = np.percentile(np.abs(fc_mean), 95)
        p_max = np.percentile(p_mean, 95)

        colors = np.array([_fc_to_rgba(fc_mean[i], p_mean[i], fc_max, p_max)
                           for i in range(len(fc_mean))])

        order = np.argsort(colors[:, 3])
        ax.scatter(X_umap[order, 0], X_umap[order, 1],
                   c=colors[order], s=14, edgecolors="none")

        ax.set_title(f"{CONDITION_FULL[cond]}", fontsize=11, fontweight="bold")

        legend_elements = [
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='tomato',
                       markersize=7, label='Up (high FC)'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='steelblue',
                       markersize=7, label='Down (low FC)'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='lightgray',
                       markersize=7, alpha=0.3, label='Low significance'),
        ]
        ax.legend(handles=legend_elements, loc="best", fontsize=6)

    # ── Remove all axes (UMAP coordinates are meaningless) ──
    for ax_row in axes:
        for ax in ax_row:
            ax.set_xticks([])
            ax.set_yticks([])
            ax.set_xlabel("")
            ax.set_ylabel("")
            for spine in ax.spines.values():
                spine.set_visible(False)

    # Share axis limits
    xlim = [X_umap[:, 0].min() - 1, X_umap[:, 0].max() + 1]
    ylim = [X_umap[:, 1].min() - 1, X_umap[:, 1].max() + 1]
    for ax_row in axes:
        for ax in ax_row:
            ax.set_xlim(xlim)
            ax.set_ylim(ylim)

    plt.tight_layout()
    path = out_dir / "umap_combined.png"
    fig.savefig(path, dpi=170)
    plt.close(fig)
    print(f"  Saved {path.name}")
    return X_umap


# ── 4. Volcano plot grid colored by cluster ───────────────────────────
def plot_volcano_clusters(df_diff, feat_df, cluster_ids, n_clusters, out_dir):
    """3x3 volcano grid (conditions x timepoints) with points colored by GMM cluster."""
    cid_to_cluster = dict(zip(feat_df["compound_id"], cluster_ids))

    nrows, ncols = len(CONDITIONS), len(TIMEPOINTS)
    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 12))

    for ri, cond in enumerate(CONDITIONS):
        for ci, tp in enumerate(TIMEPOINTS):
            ax = axes[ri, ci]
            sub = df_diff[(df_diff["condition"] == cond) & (df_diff["timepoint_h"] == tp)].copy()
            sub["neg_log10p"] = -np.log10(sub["pval"].clip(lower=1e-300))
            sub["cluster"] = sub["compound_id"].map(cid_to_cluster)

            for cl in range(n_clusters):
                mask = sub["cluster"] == cl
                if mask.sum() == 0:
                    continue
                color = PLOTLY_COLORS[cl % len(PLOTLY_COLORS)]
                ax.scatter(
                    sub.loc[mask, "log2FC"],
                    sub.loc[mask, "neg_log10p"],
                    c=color, s=12, alpha=0.7, edgecolors="none",
                    label=f"C{cl}" if ri == 0 and ci == 0 else None,
                )

            ax.axhline(-np.log10(0.05), color="gray", ls="--", lw=0.7)
            ax.axvline(-1, color="lightgray", ls="--", lw=0.7)
            ax.axvline(1, color="lightgray", ls="--", lw=0.7)

            if ri == 0:
                ax.set_title(f"{tp}h", fontsize=11, fontweight="bold")

            if ci == ncols - 1:
                ax.annotate(
                    CONDITION_FULL[cond], xy=(1.02, 0.5),
                    xycoords="axes fraction", fontsize=10, fontweight="bold",
                    rotation=-90, va="center", ha="left",
                )

    # Hide inner spines and ticks (matching generate_report.py volcano style)
    for i in range(nrows):
        for j in range(ncols):
            ax = axes[i][j]
            ax.spines["right"].set_visible(False)
            ax.spines["top"].set_visible(False)
            if j > 0:
                ax.spines["left"].set_visible(False)
                ax.tick_params(left=False, labelleft=False)
                ax.set_ylabel("")
            else:
                ax.set_ylabel("-log₁₀(p-value)")
            if i < nrows - 1:
                ax.spines["bottom"].set_visible(False)
                ax.tick_params(bottom=False, labelbottom=False)
                ax.set_xlabel("")
            else:
                ax.set_xlabel("log₂FC")

    handles = [plt.Line2D([0], [0], marker='o', color='w',
               markerfacecolor=PLOTLY_COLORS[ci % len(PLOTLY_COLORS)],
               markersize=7, label=f"C{ci}")
               for ci in range(n_clusters)]
    fig.legend(handles=handles, loc="lower center", ncol=min(n_clusters, 5),
               fontsize=8, framealpha=0.9, bbox_to_anchor=(0.5, -0.01))

    fig.suptitle("Volcano Plots — colored by GMM cluster", fontsize=13, fontweight="bold", y=0.98)
    plt.tight_layout(rect=[0, 0.03, 0.97, 0.96])
    path = out_dir / "volcano_clusters.png"
    fig.savefig(path, dpi=170, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path.name}")


# ── Main ───────────────────────────────────────────────────────────────
def main():
    out_dir = OUT_DIR
    df_diff = pd.read_csv(DIFF_CSV)
    compound_meta = pd.read_csv(COMPOUND_CSV)
    print(f"Loaded {len(df_diff)} differential rows, {len(compound_meta)} compounds")

    # Build combined 18-feature matrix
    print("\nBuilding combined feature matrix (3 conditions x 3 timepoints x 2 measures = 18 features)...")
    feat_df, feature_cols = build_combined_feature_matrix(df_diff)
    X = feat_df[feature_cols].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    print(f"  {len(feat_df)} metabolites, {len(feature_cols)} features")

    # GMM clustering with BIC selection (k=2..9)
    print("\nFitting GMM (k=2..9)...")
    gmm, best_k = fit_gmm(X_scaled)
    named_labels, cluster_ids, semantic_map = label_gmm_clusters(gmm, X_scaled, scaler, feature_cols)

    # Print cluster distribution
    print(f"\nCluster breakdown (k={best_k}):")
    for ci in range(best_k):
        n = (cluster_ids == ci).sum()
        print(f"  Cluster {ci} ({semantic_map[ci]}): {n} metabolites")

    # Plots
    print("\nGenerating UMAP (4-panel, supervised by clusters)...")
    plot_umap_panels(X_scaled, cluster_ids, best_k, feat_df, out_dir)
    print("Generating volcano plot (cluster-colored)...")
    plot_volcano_clusters(df_diff, feat_df, cluster_ids, best_k, out_dir)

    # ── Merge cluster assignments into compound_metadata.csv ──
    print("\nMerging cluster assignments into compound_metadata.csv...")
    cluster_df = pd.DataFrame({
        "compound_id": feat_df["compound_id"],
        "cluster_id": cluster_ids,
        "cluster_label": named_labels,
    })
    # Drop existing cluster columns if re-running
    for col in ["cluster_id", "cluster_label"]:
        if col in compound_meta.columns:
            compound_meta = compound_meta.drop(columns=[col])
    compound_meta = compound_meta.merge(cluster_df, on="compound_id", how="left")
    compound_meta.to_csv(COMPOUND_CSV, index=False)
    n_clustered = compound_meta["cluster_id"].notna().sum()
    print(f"  {n_clustered}/{len(compound_meta)} compounds have cluster assignments")

    # ── Slim differential_results.csv ──
    print("\nSlimming differential_results.csv (dropping compound_name, kegg_id, inchi_key)...")
    drop_cols = [c for c in ["compound_name", "kegg_id", "inchi_key"] if c in df_diff.columns]
    if drop_cols:
        df_diff = df_diff.drop(columns=drop_cols)
        df_diff.to_csv(DIFF_CSV, index=False)
        print(f"  Dropped {drop_cols}, {len(df_diff.columns)} columns remain: {list(df_diff.columns)}")
    else:
        print("  No redundant columns found (already slimmed)")

    # ── Clean up old files ──
    for old_file in ["clustering_all_conditions.csv", "pca_combined.png"]:
        p = out_dir / old_file
        if p.exists():
            p.unlink()
            print(f"  Deleted {old_file}")

    print("\nDone!")


if __name__ == "__main__":
    main()
