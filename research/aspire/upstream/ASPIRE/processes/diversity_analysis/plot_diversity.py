#!/usr/bin/env python3
"""
diversity_analytics.py
Comprehensive diversity analysis: alpha diversity (Shannon), beta diversity (PERMANOVA),
and UMAP visualization from precomputed distance matrices with flexible metadata support.

Features:
- Alpha diversity comparisons (t-tests, boxplots)
- Global and pairwise PERMANOVA
- UMAP projections from precomputed distances
- Flexible metadata columns and palettes
- Multiple modes (micro, mito, custom)
- Outlier visualization support

Example:
--------
python diversity_analytics.py \
  --metadata metadata.tsv \
  --sample-col sample \
  --group-col type_group \
  --color-col Color \
  --alpha-table shannon.tsv \
  --distance-bray bray.tsv \
  --distance-jaccard jaccard.tsv \
  --exclude-groups "Skin Brush,Scope Flush" \
  --group-order "Oral Rinse,BAL,Bronchial Brush" \
  --output-dir diversity_results
"""

from __future__ import annotations
import argparse
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from itertools import combinations

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import umap
from skbio import DistanceMatrix
from skbio.stats.distance import permanova
from scipy.stats import ttest_ind, wilcoxon
from statsmodels.stats.multitest import multipletests
from statannotations.Annotator import Annotator

# Global aesthetics
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['svg.fonttype'] = 'none'
mpl.rcParams['savefig.dpi'] = 600
plt.rcParams.update({'font.size': 12})
plt.rcParams['font.family'] = 'Source Sans Pro'
sns.set_theme()
sns.set_style("white")

# Default fallback palette
DEFAULT_PALETTE = sns.color_palette("husl", 20).as_hex()


# ==================== Utility Functions ====================

def ensure_dir(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def read_tsv(path: Path, index_col: Optional[int] = None) -> pd.DataFrame:
    """Read TSV file with error handling."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    try:
        return pd.read_csv(path, sep='\t', header=0, index_col=index_col)
    except Exception as e:
        raise ValueError(f"Failed to read {path}: {e}")


def normalize_sample_id_value(value: Any) -> str:
    """Normalize sample IDs to stable string keys."""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        try:
            num = float(text)
            if np.isfinite(num) and num.is_integer():
                return str(int(num))
        except ValueError:
            pass
    return text


def normalize_sample_id_series(series: pd.Series) -> pd.Series:
    """Normalize a sample-ID series to string values."""
    return series.map(normalize_sample_id_value).astype(str)


def normalize_distance_matrix_ids(dist_matrix: pd.DataFrame) -> pd.DataFrame:
    """Normalize row/column sample IDs of a square distance matrix."""
    dm = dist_matrix.copy()
    dm.index = pd.Index([normalize_sample_id_value(v) for v in dm.index], name=dm.index.name)
    dm.columns = [normalize_sample_id_value(v) for v in dm.columns]
    return dm


def safe_merge(left: pd.DataFrame, right: pd.DataFrame, on: str, 
               how: str = 'left', suffixes: Tuple[str, str] = ('', '_drop')) -> pd.DataFrame:
    """Safely merge DataFrames, avoiding column conflicts."""
    if right.empty:
        return left
    merged = left.merge(right, on=on, how=how, suffixes=suffixes)
    # Drop duplicate columns with suffix
    drop_cols = [c for c in merged.columns if c.endswith(suffixes[1])]
    return merged.drop(columns=drop_cols)


def parse_list_arg(arg: str) -> List[str]:
    """Parse comma-separated string into list."""
    return [x.strip() for x in arg.split(',') if x.strip()] if arg else []


def parse_palette_arg(arg: str) -> Dict[str, str]:
    palette: Dict[str, str] = {}
    for item in parse_list_arg(arg):
        if "=" not in item:
            raise ValueError(f"Invalid palette entry '{item}'; expected label=#RRGGBB")
        label, color = (part.strip() for part in item.split("=", 1))
        if label and color:
            palette[label] = color
    return palette


def create_palette_from_metadata(meta: pd.DataFrame, group_col: str, 
                                 color_col: Optional[str] = None) -> Dict[str, str]:
    """
    Create color palette from metadata.
    If color_col exists, use it; otherwise auto-assign colors.
    """
    # Get unique groups and ensure consistent type (str)
    group_values = meta[group_col].dropna()
    groups = group_values.astype(str).unique()
    
    if color_col and color_col in meta.columns:
        # Use provided colors, ensuring keys are strings
        palette_df = meta[[group_col, color_col]].drop_duplicates()
        palette_df[group_col] = palette_df[group_col].astype(str)
        palette = dict(zip(palette_df[group_col], palette_df[color_col]))
    else:
        # Auto-assign colors
        n_groups = len(groups)
        colors = sns.color_palette("husl", n_groups).as_hex()
        palette = dict(zip(groups, colors))
    
    return palette


def sort_groups_numeric_aware(groups: List[str]) -> List[str]:
    """Sort groups, treating numeric values as numbers."""
    try:
        return sorted(groups, key=lambda x: float(x))
    except (ValueError, TypeError):
        return sorted(groups)


# ==================== Alpha Diversity Functions ====================

def pairwise_ttests_fdr(df: pd.DataFrame, group_col: str, 
                       value_col: str) -> pd.DataFrame:
    """
    Perform pairwise t-tests with FDR correction.
    
    Args:
        df: DataFrame with groups and values
        group_col: Column name for grouping
        value_col: Column name for values to test
    
    Returns:
        DataFrame with test results
    """
    results = []
    groups = [g for g in df[group_col].dropna().unique()]
    
    for group1, group2 in combinations(groups, 2):
        vals1 = df.loc[df[group_col] == group1, value_col].dropna()
        vals2 = df.loc[df[group_col] == group2, value_col].dropna()
        
        if len(vals1) < 2 or len(vals2) < 2:
            continue
        
        stat, pval = ttest_ind(vals1, vals2, equal_var=False)
        
        results.append({
            'group1': group1,
            'group2': group2,
            'mean1': vals1.mean(),
            'mean2': vals2.mean(),
            'tstat': stat,
            'pval': pval,
            'n1': len(vals1),
            'n2': len(vals2)
        })
    
    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df['qval'] = multipletests(result_df['pval'], method='fdr_bh')[1]
        result_df['significant'] = result_df['qval'] < 0.05
    
    return result_df


def _paired_signflip_pvalue(diffs: np.ndarray, n_perm: int = 10000,
                            random_state: int = 42) -> float:
    """
    Two-sided sign-flip permutation p-value for paired differences.
    """
    diffs = np.asarray(diffs, dtype=float)
    diffs = diffs[np.isfinite(diffs)]
    n = len(diffs)
    if n == 0:
        return np.nan
    if np.allclose(diffs, 0.0):
        return 1.0

    observed = float(np.abs(np.mean(diffs)))
    rng = np.random.default_rng(random_state)

    # Exact sign-flip when feasible.
    if n <= 15:
        signs = np.array(np.meshgrid(*[[-1.0, 1.0]] * n), dtype=float).T.reshape(-1, n)
    else:
        signs = rng.choice([-1.0, 1.0], size=(int(max(1, n_perm)), n), replace=True)

    perm_stats = np.abs((signs * diffs).mean(axis=1))
    pval = (1.0 + np.sum(perm_stats >= observed)) / (1.0 + len(perm_stats))
    return float(pval)


def pairwise_blocked_alpha_fdr(df: pd.DataFrame, group_col: str,
                               value_col: str, block_col: str,
                               random_state: int = 42) -> pd.DataFrame:
    """
    Block-aware pairwise alpha testing with FDR correction.

    For each group pair, values are averaged per (block, group) and compared
    using a paired Wilcoxon signed-rank test across blocks with both groups.
    If Wilcoxon fails (e.g., degenerate ties), falls back to sign-flip test.
    """
    results = []
    groups = [g for g in df[group_col].dropna().unique()]

    for group1, group2 in combinations(groups, 2):
        sub = df[df[group_col].isin([group1, group2])].copy()
        sub = sub[sub[value_col].notna() & sub[block_col].notna()]
        if sub.empty:
            continue

        # Average repeated samples within a patient/block and group.
        block_means = (
            sub.groupby([block_col, group_col], dropna=True)[value_col]
            .mean()
            .unstack(group_col)
        )
        if group1 not in block_means.columns or group2 not in block_means.columns:
            continue

        paired = block_means[[group1, group2]].dropna()
        n_blocks = int(len(paired))
        if n_blocks < 2:
            continue

        diffs = (paired[group1] - paired[group2]).to_numpy(dtype=float)
        mean1 = float(paired[group1].mean())
        mean2 = float(paired[group2].mean())
        median_diff = float(np.median(diffs))

        if np.allclose(diffs, 0.0):
            stat = 0.0
            pval = 1.0
            test_name = "paired_wilcoxon"
        else:
            try:
                w = wilcoxon(diffs, alternative='two-sided', zero_method='wilcox', method='auto')
                stat = float(w.statistic)
                pval = float(w.pvalue)
                test_name = "paired_wilcoxon"
            except Exception:
                stat = float(np.mean(diffs))
                pval = _paired_signflip_pvalue(diffs, random_state=random_state)
                test_name = "paired_signflip"

        results.append({
            'group1': group1,
            'group2': group2,
            'mean1': mean1,
            'mean2': mean2,
            'effect_mean_diff': float(np.mean(diffs)),
            'effect_median_diff': median_diff,
            'tstat': stat,
            'pval': pval,
            'n1': int((sub[group_col] == group1).sum()),
            'n2': int((sub[group_col] == group2).sum()),
            'n_blocks': n_blocks,
            'test': test_name,
            'blocked_by': block_col,
        })

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df['qval'] = multipletests(result_df['pval'], method='fdr_bh')[1]
        result_df['significant'] = result_df['qval'] < 0.05

    return result_df


def plot_alpha_boxplot(df: pd.DataFrame, group_col: str, value_col: str,
                       group_order: List[str], palette: Dict[str, str],
                       output_path: Path, title: str = "Alpha Diversity") -> None:
    """Create annotated boxplot for alpha diversity."""
    # Ensure group column is string type to match palette keys
    df = df.copy()
    df[group_col] = df[group_col].astype(str)
    
    # Ensure group_order items are strings
    group_order = [str(g) for g in group_order]
    plot_df = df[df[group_col].isin(group_order) & df[value_col].notna()].copy()
    if plot_df.empty or not group_order:
        warnings.warn(
            f"No plottable rows for alpha boxplot '{value_col}' by '{group_col}'; skipping."
        )
        return
    
    fig, ax = plt.subplots(figsize=(max(8, len(group_order) * 1.2), 6))
    
    # Fix: explicitly set hue to avoid deprecation warning
    sns.boxplot(
        data=plot_df, x=group_col, y=value_col, hue=group_col,
        order=group_order, palette=palette,
        linewidth=1.5, ax=ax, legend=False  # Important: legend=False to avoid duplicate
    )
    
    # Statistical annotations
    #pairs = list(combinations(group_order, 2))
    #if len(pairs) > 0:
    #    try:
    #        annotator = Annotator(ax, pairs, data=df, x=group_col, y=value_col, order=group_order)
    #        annotator.configure(test='t-test_ind', text_format='star', loc='inside', verbose=0)
    #        annotator.apply_and_annotate()
    #    except Exception as e:
    #        warnings.warn(f"Could not add statistical annotations: {e}")
    
    ax.set_xlabel("")
    ax.set_ylabel(value_col)
    ax.set_title(title, fontweight='bold', fontsize=14)
    ax.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    
    for ext in ('svg', 'pdf', 'png'):
        fig.savefig(output_path.with_suffix(f'.{ext}'), bbox_inches='tight')
    plt.close(fig)


def plot_alpha_faceted(df: pd.DataFrame, group_col: str, facet_col: str,
                       value_col: str, facet_order: List[str],
                       facet_palette: Dict[str, str], output_path: Path) -> None:
    """Create faceted boxplots for alpha diversity by secondary variable."""
    df = df.copy()
    if df.empty:
        warnings.warn("No data available for faceted alpha plot; skipping.")
        return

    # Normalize types to stabilize palette/hue behavior in seaborn
    df[group_col] = df[group_col].astype(str)
    df[facet_col] = df[facet_col].astype(str)
    facet_order = [str(g) for g in facet_order]

    # Remove rows missing required values for plotting
    df = df[df[group_col].notna() & df[facet_col].notna() & df[value_col].notna()].copy()
    if df.empty:
        warnings.warn("No plottable rows for faceted alpha plot after filtering; skipping.")
        return

    def _facet_boxplot(data, x, y, **kwargs):
        if data.empty:
            return
        # Keep only x levels present in this facet to avoid seaborn edge-case failures
        present = set(data[x].dropna().astype(str))
        x_levels = [str(level) for level in facet_palette if str(level) in present]
        x_levels.extend(sorted(level for level in present if level not in x_levels))
        if not x_levels:
            return
        sns.boxplot(
            data=data,
            x=x,
            y=y,
            hue=x,
            order=x_levels,
            hue_order=x_levels,
            palette=facet_palette,
            linewidth=1.5,
            legend=False,
            ax=plt.gca(),
        )

    g = sns.FacetGrid(
        df, col=group_col, col_wrap=3, sharey=True,
        col_order=facet_order, height=4, aspect=1.2
    )
    
    g.map_dataframe(_facet_boxplot, x=facet_col, y=value_col)
    
    for ax in g.axes.flat:
        ax.set_xlabel("")
        ax.set_ylabel(value_col)
        ax.tick_params(axis='x', rotation=45)
    
    g.figure.tight_layout()
    
    for ext in ('svg', 'pdf', 'png'):
        g.figure.savefig(output_path.with_suffix(f'.{ext}'), bbox_inches='tight')
    plt.close(g.figure)


# ==================== Beta Diversity Functions ====================

def _permanova_result_to_dict(result) -> Dict[str, Any]:
    """Normalize skbio PERMANOVA return object to a plain dict."""
    if isinstance(result, dict):
        return dict(result)
    if hasattr(result, "to_dict"):
        return dict(result.to_dict())
    raise TypeError(f"Unexpected PERMANOVA result type: {type(result)}")


def _run_permanova_restricted(
    dm: DistanceMatrix,
    groups: pd.Series,
    blocks: pd.Series,
    permutations: int = 999,
    random_state: int = 42,
) -> Dict[str, Any]:
    """Restricted-permutation PERMANOVA by shuffling labels within blocks."""
    groups = groups.astype(str)
    blocks = blocks.astype(str)
    if groups.name is None:
        groups.name = "group"
    ids = pd.Index(dm.ids)
    groups = groups.loc[ids]
    blocks = blocks.loc[ids]

    # Observed statistic (no permutations)
    try:
        observed_res = permanova(dm, groups, permutations=0)
    except Exception:
        # Compatibility fallback for skbio versions that reject permutations=0.
        observed_res = permanova(dm, groups, permutations=1)
    observed = _permanova_result_to_dict(observed_res)
    observed_stat = float(observed.get("test statistic", np.nan))

    if not np.isfinite(observed_stat):
        observed["p-value"] = np.nan
        observed["permutations"] = int(max(permutations, 0))
        observed["method name"] = "PERMANOVA (restricted)"
        return observed

    rng = np.random.default_rng(random_state)
    group_vals = groups.to_numpy(dtype=object)
    block_vals = blocks.to_numpy(dtype=object)
    block_levels = pd.unique(block_vals)
    block_indices = [np.where(block_vals == b)[0] for b in block_levels]
    perm_stats: List[float] = []

    for _ in range(int(max(permutations, 0))):
        perm = group_vals.copy()
        for idx in block_indices:
            if len(idx) > 1:
                perm[idx] = perm[idx][rng.permutation(len(idx))]
        perm_series = pd.Series(perm, index=ids, name=groups.name)
        try:
            res = permanova(dm, perm_series, permutations=0)
        except Exception:
            res = permanova(dm, perm_series, permutations=1)
        stat = float(_permanova_result_to_dict(res).get("test statistic", np.nan))
        if np.isfinite(stat):
            perm_stats.append(stat)

    if len(perm_stats) == 0:
        p_val = np.nan
    else:
        perm_arr = np.asarray(perm_stats, dtype=float)
        p_val = float((1.0 + np.sum(perm_arr >= observed_stat)) / (1.0 + len(perm_arr)))

    observed["p-value"] = p_val
    observed["permutations"] = int(max(permutations, 0))
    observed["method name"] = "PERMANOVA (restricted)"
    return observed


def compute_permanova(
    dist_matrix: pd.DataFrame,
    groups: pd.Series,
    permutations: int = 999,
    blocks: Optional[pd.Series] = None,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute global and pairwise PERMANOVA.
    
    Args:
        dist_matrix: Square distance matrix (samples × samples)
        groups: Series mapping sample IDs to groups
        permutations: Number of permutations
    
    Returns:
        Tuple of (global_results, pairwise_results)
    """
    # Align samples
    common_ids = dist_matrix.index.intersection(groups.index)
    dist_aligned = dist_matrix.loc[common_ids, common_ids]
    groups_aligned = groups.loc[common_ids]
    
    blocks_aligned = None
    if blocks is not None:
        blocks_aligned = blocks.loc[common_ids]

    # Global PERMANOVA
    dm = DistanceMatrix(dist_aligned.values.copy(order='C'), ids=dist_aligned.index.tolist())
    if blocks_aligned is not None:
        global_dict = _run_permanova_restricted(
            dm, groups_aligned, blocks_aligned, permutations=permutations, random_state=random_state
        )
        global_result = pd.DataFrame([global_dict])
    else:
        global_result = permanova(dm, groups_aligned, permutations=permutations).to_frame().T
    global_result.insert(0, 'comparison', 'global')
    
    # Pairwise PERMANOVA
    pairwise_results = []
    unique_groups = groups_aligned.unique()
    
    for group1, group2 in combinations(unique_groups, 2):
        subset_ids = groups_aligned[groups_aligned.isin([group1, group2])].index
        counts = groups_aligned.loc[subset_ids].value_counts()
        
        # Need at least 2 samples per group
        if (counts < 2).any():
            continue
        
        subset_dm = DistanceMatrix(
            dist_aligned.loc[subset_ids, subset_ids].values.copy(order='C'),
            ids=subset_ids.tolist()
        )
        
        if blocks_aligned is not None:
            pw_dict = _run_permanova_restricted(
                subset_dm,
                groups_aligned.loc[subset_ids],
                blocks_aligned.loc[subset_ids],
                permutations=permutations,
                random_state=random_state,
            )
        else:
            pw_result = permanova(subset_dm, groups_aligned.loc[subset_ids], permutations=permutations)
            pw_dict = _permanova_result_to_dict(pw_result)
        pw_dict['group1'] = group1
        pw_dict['group2'] = group2
        pairwise_results.append(pw_dict)
    
    pairwise_df = pd.DataFrame(pairwise_results)
    if not pairwise_df.empty:
        pairwise_df['q-value'] = multipletests(pairwise_df['p-value'], method='fdr_bh')[1]
        pairwise_df['significant'] = pairwise_df['q-value'] < 0.05
    
    return global_result, pairwise_df


def plot_permanova_heatmap(pairwise_df: pd.DataFrame, output_path: Path,
                           title: str = "Pairwise PERMANOVA (q-values)") -> None:
    """Create heatmap of pairwise PERMANOVA q-values."""
    if pairwise_df.empty:
        warnings.warn("No pairwise PERMANOVA results to plot")
        return
    
    # Pivot to matrix
    matrix = pairwise_df.pivot(index='group1', columns='group2', values='q-value')
    matrix = matrix.combine_first(matrix.T)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(
        matrix, annot=True, fmt='.3f', cmap='coolwarm_r',
        vmin=0, vmax=0.1, cbar_kws={'label': 'q-value'},
        linewidths=0.5, linecolor='lightgray', ax=ax
    )
    
    ax.set_title(title, fontweight='bold', fontsize=14)
    plt.tight_layout()
    
    for ext in ('svg', 'pdf', 'png'):
        fig.savefig(output_path.with_suffix(f'.{ext}'), bbox_inches='tight')
    plt.close(fig)


# ==================== UMAP Functions ====================

def compute_umap_embedding(dist_matrix: pd.DataFrame, n_neighbors: int = 30,
                          min_dist: float = 0.01, random_state: int = 42) -> pd.DataFrame:
    """
    Compute UMAP embedding from precomputed distance matrix.
    
    Args:
        dist_matrix: Square distance matrix
        n_neighbors: UMAP n_neighbors parameter
        min_dist: UMAP min_dist parameter
        random_state: Random seed
    
    Returns:
        DataFrame with UMAP1, UMAP2 columns
    """
    if dist_matrix.shape[0] != dist_matrix.shape[1]:
        raise ValueError("Distance matrix must be square")
    
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric='precomputed',
        random_state=random_state
    )
    
    embedding = reducer.fit_transform(dist_matrix.values)
    
    return pd.DataFrame(
        embedding,
        index=dist_matrix.index,
        columns=['UMAP1', 'UMAP2']
    )


def plot_umap_scatter(df: pd.DataFrame, x_col: str, y_col: str,
                     hue_col: str, hue_palette: Dict[str, str],
                     size_col: Optional[str] = None,
                     style_col: Optional[str] = None,
                     output_path: Path = None,
                     title: str = "UMAP Projection",
                     size_range: Tuple[int, int] = (50, 400)) -> None:
    """Create UMAP scatter plot with flexible styling."""
    # Ensure hue column matches palette key types (string)
    df = df.copy()
    df[hue_col] = df[hue_col].astype(str)
    
    # Ensure palette keys are strings
    hue_palette = {str(k): v for k, v in hue_palette.items()}
    
    # Get hue order from palette keys and sort numerically if possible
    hue_order = sort_groups_numeric_aware(list(hue_palette.keys()))
    
    # Filter to only groups present in data
    hue_order = [h for h in hue_order if h in df[hue_col].unique()]
    
    fig, ax = plt.subplots(figsize=(12, 10))
    
    sns.scatterplot(
        data=df, x=x_col, y=y_col,
        hue=hue_col, size=size_col, style=style_col,
        palette=hue_palette, hue_order=hue_order,
        sizes=size_range if size_col else None,
        alpha=0.8, edgecolor='lightgray', linewidth=0.5,
        ax=ax
    )
    
    ax.set_title(title, fontweight='bold', fontsize=14)
    ax.set_xlabel(x_col, fontweight='bold')
    ax.set_ylabel(y_col, fontweight='bold')
    
    # Legend outside plot with sorted order
    handles, labels = ax.get_legend_handles_labels()
    
    # Separate by type (hue, size, style)
    if size_col and style_col:
        # Find where size/style legends start (after hue items)
        n_hue = len(hue_order)
        hue_handles = handles[:n_hue]
        hue_labels = labels[:n_hue]
        other_handles = handles[n_hue:]
        other_labels = labels[n_hue:]
        
        # Sort hue items numerically
        hue_sorted = sorted(zip(hue_labels, hue_handles), 
                           key=lambda x: float(x[0]) if x[0].replace('.','').replace('-','').isdigit() else x[0])
        hue_labels, hue_handles = zip(*hue_sorted) if hue_sorted else ([], [])
        
        # Recombine
        handles = list(hue_handles) + other_handles
        labels = list(hue_labels) + other_labels
    
    ax.legend(handles, labels, bbox_to_anchor=(1.05, 1), loc='upper left', 
              frameon=True, fontsize=10, title_fontsize=11)
    
    plt.tight_layout()
    
    if output_path:
        for ext in ('svg', 'pdf', 'png'):
            fig.savefig(output_path.with_suffix(f'.{ext}'), bbox_inches='tight')
        plt.close(fig)


# ==================== Main Analysis Pipeline ====================

def run_analysis_pipeline(
    metadata: pd.DataFrame,
    sample_col: str,
    group_col: str,
    alpha_table: Optional[pd.DataFrame],
    distance_matrices: Dict[str, pd.DataFrame],
    output_dir: Path,
    group_order: Optional[List[str]] = None,
    exclude_groups: Optional[List[str]] = None,
    group_palette: Optional[Dict[str, str]] = None,
    secondary_col: Optional[str] = None,
    secondary_palette: Optional[Dict[str, str]] = None,
    filter_col: Optional[str] = None,
    filter_exclude: Optional[List[str]] = None,
    size_col: Optional[str] = None,
    style_cols: Optional[List[str]] = None,
    umap_params: Optional[Dict[str, Any]] = None,
    permanova_perms: int = 999,
    block_col: Optional[str] = None,
    random_state: int = 42,
    output_prefix: str = "",
    verbose: bool = False
) -> None:
    """
    Run complete diversity analysis pipeline.
    
    Args:
        metadata: Sample metadata DataFrame
        sample_col: Column name for sample IDs
        group_col: Column name for primary grouping variable
        alpha_table: Alpha diversity table (sample × metrics)
        distance_matrices: Dict of distance matrix names to DataFrames
        output_dir: Output directory
        group_order: Order for groups (None = auto-sort)
        exclude_groups: Groups to exclude
        group_palette: Color palette for groups
        secondary_col: Secondary grouping variable for faceted plots
        secondary_palette: Color palette for secondary variable
        filter_col: Column for filtering (e.g., pass_filter)
        filter_exclude: Values to exclude in filter_col
        size_col: Column for point sizes in UMAP
        style_cols: Columns for point styles in UMAP
        umap_params: UMAP parameters dict
        permanova_perms: Number of PERMANOVA permutations
        output_prefix: Prefix for output files
        verbose: Verbose output
    """
    ensure_dir(output_dir)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Running analysis: {output_prefix or 'main'}")
        print(f"{'='*60}")
    
    # Validate required columns
    required = [sample_col, group_col]
    missing = [c for c in required if c not in metadata.columns]
    if missing:
        raise ValueError(f"Missing required columns in metadata: {missing}")
    
    # Prepare working dataframe
    df = metadata.copy()
    df[sample_col] = normalize_sample_id_series(df[sample_col])
    df[group_col] = df[group_col].astype(str)
    df = df[df[group_col].notna() & (df[group_col].str.strip() != "") & (df[group_col] != "nan")].copy()
    exclude_groups = [str(g) for g in exclude_groups] if exclude_groups else []
    group_order = [str(g) for g in group_order] if group_order else None

    # Apply filters
    if filter_col and filter_col in df.columns:
        if filter_exclude:
            df = df[~df[filter_col].isin(filter_exclude)].copy()
            if verbose:
                print(f"[FILTER] Excluded {filter_col} values: {filter_exclude}")
    
    if exclude_groups:
        df = df[~df[group_col].isin(exclude_groups)].copy()
        if verbose:
            print(f"[FILTER] Excluded {group_col} values: {exclude_groups}")
    
    # Determine group order
    if not group_order:
        group_order = sort_groups_numeric_aware(df[group_col].dropna().unique().tolist())
    else:
        # Keep only groups present in data
        group_order = [g for g in group_order if g in df[group_col].unique()]
    
    if verbose:
        print(f"[INFO] Groups ({len(group_order)}): {group_order}")
    if not group_order:
        warnings.warn(
            f"No valid groups remain for '{group_col}' after filtering; skipping diversity analysis."
        )
        return
    
    # Create palette if not provided
    if not group_palette:
        group_palette = create_palette_from_metadata(df, group_col)
    
    # ========== Alpha Diversity ==========
    if alpha_table is not None and not alpha_table.empty:
        if verbose:
            print("\n[ALPHA] Running alpha diversity analysis...")
        
        # Merge alpha diversity
        alpha_df = alpha_table.reset_index().rename(columns={'sampleID': sample_col})
        alpha_df[sample_col] = normalize_sample_id_series(alpha_df[sample_col])

        df = safe_merge(df, alpha_df, on=sample_col)
        
        alpha_cols = [c for c in alpha_df.columns if c != sample_col]
        
        for alpha_metric in alpha_cols:
            if alpha_metric not in df.columns:
                continue
            
            # Pairwise alpha statistics: block-aware when requested.
            if block_col and block_col in df.columns:
                ttest_results = pairwise_blocked_alpha_fdr(
                    df, group_col, alpha_metric, block_col, random_state=random_state
                )
            elif block_col and block_col not in df.columns:
                warnings.warn(
                    f"Requested block_col '{block_col}' not found in metadata; "
                    "falling back to unpaired t-tests for alpha diversity."
                )
                ttest_results = pairwise_ttests_fdr(df, group_col, alpha_metric)
            else:
                ttest_results = pairwise_ttests_fdr(df, group_col, alpha_metric)
            ttest_path = output_dir / f"{output_prefix}alpha_ttest_{alpha_metric}.tsv"
            ttest_results.to_csv(ttest_path, sep='\t', index=False)
            if verbose:
                if block_col and block_col in df.columns:
                    print(f"  Saved paired/block-aware alpha results: {ttest_path.name}")
                else:
                    print(f"  Saved t-test results: {ttest_path.name}")
            
            # Boxplot
            plot_alpha_boxplot(
                df, group_col, alpha_metric, group_order, group_palette,
                output_dir / f"{output_prefix}alpha_boxplot_{alpha_metric}",
                title=f"{alpha_metric} by {group_col}"
            )
            
            # Faceted plot if secondary variable provided
            if secondary_col and secondary_col in df.columns:
                if not secondary_palette:
                    secondary_palette = create_palette_from_metadata(df, secondary_col)
                
                plot_alpha_faceted(
                    df, group_col, secondary_col, alpha_metric,
                    group_order, secondary_palette,
                    output_dir / f"{output_prefix}alpha_faceted_{alpha_metric}_{secondary_col}"
                )
    
    # ========== Beta Diversity (PERMANOVA) ==========
    for dist_name, dist_matrix in distance_matrices.items():
        if dist_matrix is None or dist_matrix.empty:
            continue
        
        if verbose:
            print(f"\n[BETA] Running PERMANOVA on {dist_name}...")
        
        # Align groups to distance matrix samples
        common_samples = dist_matrix.index.intersection(df[sample_col])
        groups_series = df.drop_duplicates(subset=[sample_col]).set_index(sample_col).loc[common_samples, group_col]
        
        # Compute PERMANOVA
        block_series = None
        if block_col and block_col in df.columns:
            block_series = df.drop_duplicates(subset=[sample_col]).set_index(sample_col).loc[common_samples, block_col]

        global_perm, pairwise_perm = compute_permanova(
            dist_matrix,
            groups_series,
            permutations=permanova_perms,
            blocks=block_series,
            random_state=random_state,
        )
        
        # Save results
        global_path = output_dir / f"{output_prefix}permanova_global_{dist_name}.tsv"
        pairwise_path = output_dir / f"{output_prefix}permanova_pairwise_{dist_name}.tsv"
        
        global_perm.to_csv(global_path, sep='\t', index=False)
        pairwise_perm.to_csv(pairwise_path, sep='\t', index=False)
        
        if verbose:
            print(f"  Global R²: {global_perm['test statistic'].iloc[0]:.4f}, p={global_perm['p-value'].iloc[0]:.4f}")
            print(f"  Saved: {global_path.name}, {pairwise_path.name}")
        
        # Heatmap
        plot_permanova_heatmap(
            pairwise_perm,
            output_dir / f"{output_prefix}permanova_heatmap_{dist_name}",
            title=f"Pairwise PERMANOVA ({dist_name})"
        )
    
    # ========== UMAP ==========
    umap_params = umap_params or {}
    n_neighbors = umap_params.get('n_neighbors', 30)
    min_dist = umap_params.get('min_dist', 0.01)
    random_state = umap_params.get('random_state', 42)

    for dist_name, dist_matrix in distance_matrices.items():
        if dist_matrix is None or dist_matrix.empty:
            continue
        
        if verbose:
            print(f"\n[UMAP] Computing UMAP from {dist_name}...")
        
        # Compute UMAP
        umap_df = compute_umap_embedding(
            dist_matrix, n_neighbors=n_neighbors,
            min_dist=min_dist, random_state=random_state
        )
        
        # Rename columns with distance name
        umap_cols = {
            'UMAP1': f'UMAP1_{dist_name}',
            'UMAP2': f'UMAP2_{dist_name}'
        }
        umap_df = umap_df.rename(columns=umap_cols)
        
        # Merge with metadata - IMPORTANT: ensure sample IDs match
        umap_df_reset = umap_df.reset_index().rename(columns={'sampleID': sample_col})
        df = safe_merge(df, umap_df_reset, on=sample_col)
        
        # ===== SAVE UMAP + METADATA TABLE =====
        umap_output_path = output_dir / f"{output_prefix}umap_coordinates_{dist_name}.tsv"
        
        # Select relevant columns: sample ID, UMAP coords, and all metadata
        x_col, y_col = umap_cols['UMAP1'], umap_cols['UMAP2']
        
        # Get samples with UMAP coordinates
        umap_samples_df = df.dropna(subset=[x_col, y_col]).copy()
        
        # Reorder columns: sample ID first, then UMAP coords, then everything else
        ordered_cols = [sample_col, x_col, y_col] + [c for c in umap_samples_df.columns 
                                                    if c not in [sample_col, x_col, y_col]]
        umap_samples_df = umap_samples_df[ordered_cols]
        
        # Save
        umap_samples_df.to_csv(umap_output_path, sep='\t', index=False)
        
        if verbose:
            print(f"  Saved UMAP coordinates + metadata: {umap_output_path.name}")
            print(f"  Shape: {umap_samples_df.shape[0]} samples × {umap_samples_df.shape[1]} columns")
        # ===== END SAVE =====
        
        # Filter to samples that have UMAP coordinates for plotting
        plot_df = umap_samples_df.copy()
        
        if plot_df.empty:
            warnings.warn(f"No samples with UMAP coordinates for {dist_name}")
            continue
        
        # Basic UMAP by group
        plot_umap_scatter(
            plot_df, x_col, y_col, group_col, group_palette,
            size_col=size_col,
            output_path=output_dir / f"{output_prefix}umap_{dist_name}_{group_col}",
            title=f"UMAP ({dist_name}) colored by {group_col}"
        )

    if verbose:
        print(f"\n{'='*60}")
        print(f"Analysis complete: {output_dir}")
        print(f"{'='*60}\n")


# ==================== CLI ====================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Comprehensive diversity analysis pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required inputs
    required = parser.add_argument_group("Required Inputs")
    required.add_argument(
        "--metadata", "-m", required=True, type=Path,
        help="Sample metadata TSV"
    )
    required.add_argument(
        "--output-dir", "-o", required=True, type=Path,
        help="Output directory"
    )
    
    # Column specifications
    cols = parser.add_argument_group("Column Names")
    cols.add_argument(
        "--sample-col", default="sample",
        help="Sample ID column in metadata"
    )
    cols.add_argument(
        "--group-col", default="type_group",
        help="Primary grouping column"
    )
    cols.add_argument(
        "--color-col", default=None,
        help="Color column in metadata (auto-detect if not provided)"
    )
    cols.add_argument(
        "--secondary-col", default=None,
        help="Secondary grouping column for faceted plots"
    )
    cols.add_argument(
        "--filter-col", default=None,
        help="Column for filtering samples"
    )
    cols.add_argument(
        "--size-col", default=None,
        help="Column for UMAP point sizes"
    )
    
    # Data tables
    data = parser.add_argument_group("Data Tables")
    data.add_argument(
        "--alpha-table", type=Path,
        help="Alpha diversity table (sample × metrics)"
    )
    data.add_argument(
        "--distance-bray", type=Path,
        help="Bray-Curtis distance matrix"
    )
    data.add_argument(
        "--distance-jaccard", type=Path,
        help="Jaccard distance matrix"
    )
    data.add_argument(
        "--distance-other", type=Path, nargs="+",
        help="Additional distance matrices (provide name:path pairs)"
    )
    
    # Filtering
    filt = parser.add_argument_group("Filtering")
    filt.add_argument(
        "--exclude-groups", default="",
        help="Comma-separated groups to exclude"
    )
    filt.add_argument(
        "--group-order", default="",
        help="Comma-separated group order (auto-sort if not provided)"
    )
    filt.add_argument(
        "--filter-exclude", default="",
        help="Comma-separated values to exclude in filter-col"
    )
    
    # Style columns
    style = parser.add_argument_group("Styling")
    style.add_argument(
        "--style-cols", default="",
        help="Comma-separated columns to use as styles in UMAP plots"
    )
    style.add_argument("--group-palette", default="", help="Primary label=#hex palette.")
    style.add_argument("--secondary-palette", default="", help="Secondary label=#hex palette.")
    
    # Parameters
    params = parser.add_argument_group("Analysis Parameters")
    params.add_argument(
        "--umap-neighbors", type=int, default=30,
        help="UMAP n_neighbors parameter"
    )
    params.add_argument(
        "--umap-min-dist", type=float, default=0.01,
        help="UMAP min_dist parameter"
    )
    params.add_argument(
        "--permanova-perms", type=int, default=999,
        help="Number of PERMANOVA permutations"
    )
    params.add_argument(
        "--block-col", default=None,
        help="Optional metadata block column for restricted PERMANOVA permutations (e.g., Participant_ID)"
    )
    params.add_argument(
        "--random-state", type=int, default=42,
        help="Random seed"
    )
    
    # Additional modes
    modes = parser.add_argument_group("Additional Modes")
    modes.add_argument(
        "--mito-mode", action="store_true",
        help="Run separate analysis for mitochondrial data"
    )
    modes.add_argument(
        "--mito-alpha", type=Path,
        help="Mitochondrial alpha diversity table"
    )
    modes.add_argument(
        "--mito-bray", type=Path,
        help="Mitochondrial Bray-Curtis matrix"
    )
    modes.add_argument(
        "--mito-jaccard", type=Path,
        help="Mitochondrial Jaccard matrix"
    )
    modes.add_argument(
        "--mito-output-dir", type=Path,
        help="Mitochondrial output directory (default: <output-dir>/mito)"
    )
    
    # Misc
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose output"
    )
    
    return parser.parse_args()


def main():
    """Main execution."""
    args = parse_args()
    
    # Read metadata
    try:
        metadata = read_tsv(args.metadata)
        metadata[args.sample_col] = normalize_sample_id_series(metadata[args.sample_col])
    except Exception as e:
        print(f"[ERROR] Failed to read metadata: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Parse lists
    exclude_groups = parse_list_arg(args.exclude_groups)
    group_order = parse_list_arg(args.group_order) if args.group_order else None
    filter_exclude = parse_list_arg(args.filter_exclude)
    style_cols = parse_list_arg(args.style_cols)
    
    # Create palette
    auto_primary = create_palette_from_metadata(metadata, args.group_col, args.color_col)
    palette = {**auto_primary, **parse_palette_arg(args.group_palette)}
    observed_groups = set(metadata[args.group_col].dropna().astype(str))
    palette = {key: value for key, value in palette.items() if key in observed_groups}
    secondary_palette = parse_palette_arg(args.secondary_palette)
    if args.secondary_col and args.secondary_col in metadata.columns:
        observed_secondary = set(metadata[args.secondary_col].dropna().astype(str))
        auto_secondary = create_palette_from_metadata(metadata, args.secondary_col)
        secondary_palette = {**auto_secondary, **secondary_palette}
        secondary_palette = {
            key: value for key, value in secondary_palette.items() if key in observed_secondary
        }
    
    # Load alpha diversity
    alpha_table = None
    if args.alpha_table:
        try:
            alpha_table = read_tsv(args.alpha_table, index_col=0)
            alpha_table.index = pd.Index(
                [normalize_sample_id_value(v) for v in alpha_table.index],
                name=alpha_table.index.name
            )
        except Exception as e:
            warnings.warn(f"Could not read alpha table: {e}")
    
    # Load distance matrices
    distance_matrices = {}
    
    if args.distance_bray:
        try:
            distance_matrices['bray'] = normalize_distance_matrix_ids(
                read_tsv(args.distance_bray, index_col=0)
            )
        except Exception as e:
            warnings.warn(f"Could not read Bray matrix: {e}")
    
    if args.distance_jaccard:
        try:
            distance_matrices['jaccard'] = normalize_distance_matrix_ids(
                read_tsv(args.distance_jaccard, index_col=0)
            )
        except Exception as e:
            warnings.warn(f"Could not read Jaccard matrix: {e}")
    
    # UMAP parameters
    umap_params = {
        'n_neighbors': args.umap_neighbors,
        'min_dist': args.umap_min_dist,
        'random_state': args.random_state
    }
    
    # ========== Main analysis ==========
    print("\n[INFO] Running main analysis...")
    
    run_analysis_pipeline(
        metadata=metadata,
        sample_col=args.sample_col,
        group_col=args.group_col,
        alpha_table=alpha_table,
        distance_matrices=distance_matrices,
        output_dir=args.output_dir,
        group_order=group_order,
        exclude_groups=exclude_groups,
        group_palette=palette,
        secondary_col=args.secondary_col,
        secondary_palette=secondary_palette or None,
        filter_col=args.filter_col,
        filter_exclude=filter_exclude,
        size_col=args.size_col,
        style_cols=style_cols,
        umap_params=umap_params,
        permanova_perms=args.permanova_perms,
        block_col=args.block_col,
        random_state=args.random_state,
        output_prefix="",
        verbose=args.verbose
    )
    
    # ========== Mitochondrial mode ==========
    if args.mito_mode:
        print("\n[INFO] Running mitochondrial analysis...")
        
        mito_output_dir = args.mito_output_dir or (args.output_dir.parent / "mito" / args.output_dir.name)
        
        mito_alpha = None
        if args.mito_alpha:
            try:
                mito_alpha = read_tsv(args.mito_alpha, index_col=0)
                mito_alpha.index = pd.Index(
                    [normalize_sample_id_value(v) for v in mito_alpha.index],
                    name=mito_alpha.index.name
                )
            except Exception as e:
                warnings.warn(f"Could not read mito alpha table: {e}")
        
        mito_distances = {}
        if args.mito_bray:
            try:
                mito_distances['bray'] = normalize_distance_matrix_ids(
                    read_tsv(args.mito_bray, index_col=0)
                )
            except Exception as e:
                warnings.warn(f"Could not read mito Bray matrix: {e}")
        
        if args.mito_jaccard:
            try:
                mito_distances['jaccard'] = normalize_distance_matrix_ids(
                    read_tsv(args.mito_jaccard, index_col=0)
                )
            except Exception as e:
                warnings.warn(f"Could not read mito Jaccard matrix: {e}")
        
        run_analysis_pipeline(
            metadata=metadata,
            sample_col=args.sample_col,
            group_col=args.group_col,
            alpha_table=mito_alpha,
            distance_matrices=mito_distances,
            output_dir=mito_output_dir,
            group_order=group_order,
            exclude_groups=exclude_groups,
            group_palette=palette,
            secondary_col=args.secondary_col,
            secondary_palette=secondary_palette or None,
            filter_col=args.filter_col,
            filter_exclude=filter_exclude,
            size_col=args.size_col,
            style_cols=style_cols,
            umap_params=umap_params,
            permanova_perms=args.permanova_perms,
            block_col=args.block_col,
            random_state=args.random_state,
            output_prefix="mito_",
            verbose=args.verbose
        )
    
    print("\n[OK] Analysis complete!")


if __name__ == "__main__":
    main()
