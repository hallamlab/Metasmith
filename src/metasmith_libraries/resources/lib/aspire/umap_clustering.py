#!/usr/bin/env python3
"""
asv_umap_clustering.py
---------------------
Perform UMAP dimensionality reduction and HDBSCAN clustering on ASV count data.
Generates scatter plots colored by primary group, secondary group, and cluster assignments.

Features:
- UMAP dimensionality reduction of ASV count matrix
- HDBSCAN clustering
- Multiple visualization options
- Rich metadata integration
- Configurable parameters

Example:
--------
python asv_umap_clustering.py \
  --input asv_data.tsv \
  --asv-col ASV_ID \
  --sample-col sampleID \
  --count-col count \
  --group1-col type_group \
  --color-col Color \
  --group2-col Case \
  --output-prefix umap_clustering \
  --formats pdf,png
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, List, Tuple
import warnings

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# UMAP and HDBSCAN
try:
    import umap
except ImportError:
    print("[ERROR] umap-learn not installed. Install with: pip install umap-learn")
    sys.exit(1)

try:
    import hdbscan
except ImportError:
    print("[ERROR] hdbscan not installed. Install with: pip install hdbscan")
    sys.exit(1)

from sklearn.preprocessing import StandardScaler

# Suppress warnings
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

BIOCHEM_COLOR_MAP = {
    'Oxygen': 'black',
    'Nitrogen Oxides': "#E7298A",
    'Nitrate': "#1B9E77",
    'Nitrite': "#66A61E",
    'Nitrous Oxide': "#0C5196",
    'Ammonium': "#7570B3",
    'Hydrogen Sulfide': "#D95F02",
    'Methane': "violet"
}

BIOCHEM_METADATA = list(BIOCHEM_COLOR_MAP.keys())

def load_and_pivot_data(
    input_file: Path,
    asv_col: str,
    sample_col: str,
    count_col: str,
    metadata_cols: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load ASV data and pivot to sample × ASV matrix.
    
    Args:
        input_file: Path to input TSV
        asv_col: ASV ID column
        sample_col: Sample ID column
        count_col: Count column
        metadata_cols: Metadata columns to preserve
    
    Returns:
        Tuple of (count_matrix, metadata_df)
        - count_matrix: samples as rows, ASVs as columns
        - metadata_df: metadata for each sample
    """
    print(f"[INFO] Reading {input_file}...")
    df = pd.read_csv(input_file, sep='\t')

    print(f"[INFO] Loaded {len(df)} rows")
    print(f"[INFO] Columns: {list(df.columns)}")
    
    # Validate required columns
    required = [asv_col, sample_col, count_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Extract metadata (one row per sample)
    available_metadata = [col for col in metadata_cols if col in df.columns]
    if available_metadata:
        metadata_df = df[[sample_col] + available_metadata].drop_duplicates(subset=[sample_col])
        metadata_df = metadata_df.set_index(sample_col)
        print(f"[INFO] Preserved metadata columns: {available_metadata}")
    else:
        metadata_df = pd.DataFrame(index=df[sample_col].unique())
        print("[WARN] No metadata columns found")
    
    # Pivot to sample × ASV matrix
    print("[INFO] Pivoting to sample × ASV matrix...")
    count_matrix = df.pivot_table(
        index=sample_col,
        columns=asv_col,
        values=count_col,
        aggfunc='sum',
        fill_value=0
    )
    
    print(f"[INFO] Count matrix shape: {count_matrix.shape} (samples × ASVs)")
    print(f"[INFO] Metadata shape: {metadata_df.shape}")
    
    # Align metadata with count matrix
    metadata_df = metadata_df.reindex(count_matrix.index)
    
    return count_matrix, metadata_df


def preprocess_counts(
    count_matrix: pd.DataFrame,
    normalize: str = 'total',
    transform: str = 'log1p',
    scale: bool = True
) -> np.ndarray:
    """
    Preprocess count matrix for UMAP.
    
    Args:
        count_matrix: Sample × ASV count matrix
        normalize: Normalization method ('total', 'clr', 'none')
        transform: Transformation method ('log1p', 'sqrt', 'none')
        scale: Whether to standardize features (Z-score)
    
    Returns:
        Preprocessed matrix as numpy array
    """
    print("[INFO] Preprocessing count matrix...")
    X = count_matrix.values.copy().astype(float)
    
    print(f"[INFO] Input shape: {X.shape}")
    print(f"[INFO] Input range: [{X.min():.2f}, {X.max():.2f}]")
    print(f"[INFO] Zeros: {(X == 0).sum()} / {X.size} ({100*(X == 0).sum()/X.size:.1f}%)")
    
    # Normalization
    if normalize == 'total':
        # Total-sum normalization (relative abundance)
        row_sums = X.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1  # Avoid division by zero
        X = X / row_sums
        print("[INFO] Applied total-sum normalization")
    elif normalize == 'clr':
        # Centered log-ratio transformation
        # Add pseudocount to avoid log(0)
        pseudocount = 1
        X = X + pseudocount
        
        # Calculate geometric mean per sample (row)
        # Use log space to avoid overflow
        log_X = np.log(X)
        geometric_mean = np.exp(log_X.mean(axis=1, keepdims=True))
        
        # Avoid division by zero
        geometric_mean[geometric_mean == 0] = 1
        
        X = np.log(X / geometric_mean)
        
        # Check for NaN/inf
        if np.any(~np.isfinite(X)):
            print("[WARN] CLR produced non-finite values, replacing with zeros")
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        print("[INFO] Applied CLR transformation")
    
    # Transformation
    if transform == 'log1p':
        # Ensure non-negative before log1p
        if X.min() < 0:
            print("[WARN] Negative values detected, shifting to non-negative")
            X = X - X.min()
        X = np.log1p(X)
        print("[INFO] Applied log1p transformation")
    elif transform == 'sqrt':
        # Ensure non-negative before sqrt
        if X.min() < 0:
            print("[WARN] Negative values detected, shifting to non-negative")
            X = X - X.min()
        X = np.sqrt(X)
        print("[INFO] Applied sqrt transformation")
    
    # Final check for NaN/inf
    if np.any(~np.isfinite(X)):
        print("[WARN] Non-finite values detected after transformation, replacing with zeros")
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    
    # Remove zero-variance features (constant columns)
    col_std = X.std(axis=0)
    zero_var_cols = col_std == 0
    if zero_var_cols.any():
        n_zero_var = zero_var_cols.sum()
        print(f"[INFO] Removing {n_zero_var} zero-variance features")
        X = X[:, ~zero_var_cols]
    
    # Scaling
    if scale:
        scaler = StandardScaler()
        X = scaler.fit_transform(X)
        
        # Final check after scaling
        if np.any(~np.isfinite(X)):
            print("[WARN] Non-finite values after scaling, replacing with zeros")
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        print("[INFO] Applied standard scaling (Z-score)")
    
    print(f"[INFO] Final shape: {X.shape}")
    print(f"[INFO] Final range: [{X.min():.2f}, {X.max():.2f}]")
    
    return X


def run_umap(
    X: np.ndarray,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    n_components: int = 2,
    metric: str = 'euclidean',
    random_state: int = 42
) -> np.ndarray:
    """
    Run UMAP dimensionality reduction.
    
    Args:
        X: Preprocessed count matrix
        n_neighbors: UMAP n_neighbors parameter
        min_dist: UMAP min_dist parameter
        n_components: Number of UMAP components
        metric: Distance metric
        random_state: Random seed
    
    Returns:
        UMAP embeddings (n_samples × n_components)
    """
    print(f"[INFO] Running UMAP (n_neighbors={n_neighbors}, min_dist={min_dist})...")
    
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        n_components=n_components,
        metric=metric,
        random_state=random_state,
        verbose=False
    )
    
    embedding = reducer.fit_transform(X)
    
    print(f"[INFO] UMAP embedding shape: {embedding.shape}")
    
    return embedding


def run_hdbscan(
    embedding: np.ndarray,
    min_cluster_size: int = 5,
    min_samples: int = 3,
    metric: str = 'euclidean'
) -> np.ndarray:
    """
    Run HDBSCAN clustering on UMAP embeddings.
    
    Args:
        embedding: UMAP embeddings
        min_cluster_size: Minimum cluster size
        min_samples: Minimum samples for core point
        metric: Distance metric
    
    Returns:
        Cluster labels (noise points = -1)
    """
    print(f"[INFO] Running HDBSCAN (min_cluster_size={min_cluster_size}, min_samples={min_samples})...")
    
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric=metric,
        cluster_selection_method='eom'
    )
    
    labels = clusterer.fit_predict(embedding)
    
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = list(labels).count(-1)
    
    print(f"[INFO] Found {n_clusters} clusters")
    print(f"[INFO] Noise points: {n_noise} ({100*n_noise/len(labels):.1f}%)")
    
    return labels


def parse_list_csv(arg: str) -> List[str]:
    if not arg:
        return []
    return [x.strip() for x in str(arg).split(",") if x and x.strip()]


def parse_palette(arg: str) -> dict[str, str]:
    palette: dict[str, str] = {}
    for item in parse_list_csv(arg):
        if "=" not in item:
            raise ValueError(f"Invalid palette entry '{item}'; expected label=#RRGGBB")
        label, color = (part.strip() for part in item.split("=", 1))
        palette[label] = color
    return palette


def safe_slug(value: object) -> str:
    return "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in str(value).strip().lower()).strip("_") or "group"


def resolve_category_order(values: pd.Series, requested: Optional[List[str]] = None, noise_last: bool = False) -> List[str]:
    present = [str(v) for v in pd.Series(values).dropna().astype(str).unique().tolist()]
    if requested:
        specified = [str(v).strip() for v in requested if str(v).strip()]
        ordered = [v for v in specified if v in present]
        ordered += [v for v in present if v not in ordered]
    else:
        try:
            ordered = sorted(present, key=lambda x: float(x))
        except (ValueError, TypeError):
            ordered = sorted(present, key=str)
    if noise_last and "-1" in ordered:
        ordered = [v for v in ordered if v != "-1"] + ["-1"]
    return ordered


def plot_umap_scatter(
    embedding: np.ndarray,
    colors: pd.Series,
    color_map: Optional[dict],
    title: str,
    xlabel: str,
    ylabel: str,
    legend_title: str,
    output_file: Path,
    figsize: Tuple[float, float] = (10, 8),
    dpi: int = 300,
    alpha: float = 0.7,
    point_size: float = 50,
    value_order: Optional[List[str]] = None,
) -> None:
    """
    Create UMAP scatter plot with custom coloring.
    """
    fig, ax = plt.subplots(figsize=figsize)
    
    # Handle different color types
    colors = colors.astype(str)
    if color_map is not None:
        # Use provided color map - sort values numerically if possible
        unique_vals_sorted = resolve_category_order(colors, value_order)
        cmap = {str(k): v for k, v in color_map.items()} if color_map else {}
        
        for val in unique_vals_sorted:
            mask = colors == val
            color = cmap.get(str(val), 'gray')
            ax.scatter(
                embedding[mask, 0],
                embedding[mask, 1],
                c=color,
                label=str(val),
                alpha=alpha,
                s=point_size,
                edgecolors='black',
                linewidth=0.5
            )
    else:
        # Auto-assign categorical colors
        unique_vals_sorted = resolve_category_order(colors, value_order, noise_last=True)
        
        palette = sns.color_palette('tab20', n_colors=len(unique_vals_sorted))
        color_dict = dict(zip(unique_vals_sorted, palette))
        
        for val in unique_vals_sorted:
            mask = colors == val
            color = color_dict[val]
            if str(val) == "-1":
                label = "Noise"
            else:
                label = str(val)
            ax.scatter(
                embedding[mask, 0],
                embedding[mask, 1],
                c=[color],
                label=label,
                alpha=alpha if str(val) != "-1" else 0.3,
                s=point_size,
                edgecolors='black',
                linewidth=0.5
            )
    
    ax.set_xlabel(xlabel, fontsize=14, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=14, fontweight='bold')
    ax.set_title(title, fontsize=16, fontweight='bold')
    
    # Legend
    legend = ax.legend(
        title=legend_title,
        bbox_to_anchor=(1.05, 1),
        loc='upper left',
        frameon=True,
        fontsize=10
    )
    plt.setp(legend.get_title(), fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    
    # Save
    if output_file.suffix == '.png':
        plt.savefig(output_file, dpi=dpi, bbox_inches='tight')
    else:
        plt.savefig(output_file, bbox_inches='tight')
    
    print(f"[OK] Saved: {output_file}")
    plt.close()


def plot_biochem_scatter(
    embedding: np.ndarray,
    values: pd.Series,
    meta: str,
    color: str,
    output_file: Path,
    figsize: Tuple[float, float],
    dpi: int,
    alpha: float,
    size_min: float = 20,
    size_max: float = 200,
    missing_size: float = 12
) -> None:
    """Plot UMAP scatter with marker size encoding biomeasurements."""
    numeric_values = pd.to_numeric(values, errors='coerce')
    plot_df = pd.DataFrame({
        'UMAP1': embedding[:, 0],
        'UMAP2': embedding[:, 1],
        'value': numeric_values
    })
    
    valid_mask = plot_df['value'].notna()
    missing_mask = ~valid_mask
    
    if valid_mask.sum() == 0:
        print(f"[WARN] Skipping {meta} plot: no non-missing values")
        return
    
    valid_values = plot_df.loc[valid_mask, 'value'].astype(float)
    val_min = valid_values.min()
    val_max = valid_values.max()
    
    if np.isclose(val_min, val_max):
        plot_df.loc[valid_mask, 'size'] = (size_min + size_max) / 2
    else:
        norm = (valid_values - val_min) / (val_max - val_min)
        plot_df.loc[valid_mask, 'size'] = size_min + norm * (size_max - size_min)
    
    plot_df.loc[missing_mask, 'size'] = missing_size
    
    fig, ax = plt.subplots(figsize=figsize)
    
    ax.scatter(
        plot_df.loc[valid_mask, 'UMAP1'],
        plot_df.loc[valid_mask, 'UMAP2'],
        c=color,
        s=plot_df.loc[valid_mask, 'size'],
        alpha=alpha,
        edgecolors='gray',
        linewidths=0.5,
        label=meta
    )
    
    if missing_mask.any():
        ax.scatter(
            plot_df.loc[missing_mask, 'UMAP1'],
            plot_df.loc[missing_mask, 'UMAP2'],
            c='lightgray',
            s=plot_df.loc[missing_mask, 'size'],
            alpha=alpha,
            edgecolors='gray',
            linewidths=0.5,
            label='Missing'
        )
    
    ax.set_xlabel("UMAP1", fontsize=14, fontweight='bold')
    ax.set_ylabel("UMAP2", fontsize=14, fontweight='bold')
    ax.set_title(f"UMAP Colored by {meta} (size encodes value)", fontsize=16, fontweight='bold')
    
    size_labels = []
    size_handles = []
    
    if np.isclose(val_min, val_max):
        value_levels = [val_min]
    else:
        value_levels = [val_min, (val_min + val_max) / 2, val_max]
    
    for val in value_levels:
        if np.isclose(val_min, val_max):
            size = (size_min + size_max) / 2
        else:
            size = size_min + ((val - val_min) / (val_max - val_min)) * (size_max - size_min)
        handle = ax.scatter([], [], c=color, alpha=alpha, s=size)
        size_handles.append(handle)
        size_labels.append(f"{val:.2f}")
    
    if missing_mask.any():
        handle = ax.scatter([], [], c='lightgray', alpha=alpha, s=missing_size)
        size_handles.append(handle)
        size_labels.append("Missing")
    
    legend = ax.legend(
        size_handles,
        size_labels,
        title=meta,
        scatterpoints=1,
        labelspacing=1,
        title_fontsize=13,
        fontsize=11,
        loc='upper left',
        bbox_to_anchor=(1.02, 1.0)
    )
    
    plt.setp(legend.get_title(), fontweight='bold')
    plt.tight_layout()
    
    if output_file.suffix == '.png':
        plt.savefig(output_file, dpi=dpi, bbox_inches='tight')
    else:
        plt.savefig(output_file, bbox_inches='tight')
    
    print(f"[OK] Saved: {output_file}")
    plt.close()


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    ap = argparse.ArgumentParser(
        description="UMAP dimensionality reduction and HDBSCAN clustering of ASV data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Input/output
    io = ap.add_argument_group("Input/Output")
    io.add_argument("--input", "-i", required=True, type=Path,
                    help="Input TSV file with ASV counts")
    io.add_argument("--output-prefix", "-o", default="umap_clustering",
                    help="Output file prefix")
    io.add_argument("--formats", default="pdf,png,svg",
                    help="Comma-separated output formats (pdf,png,svg)")
    io.add_argument("--dpi", type=int, default=300,
                    help="DPI for raster outputs")
    
    # Column names
    cols = ap.add_argument_group("Column Names")
    cols.add_argument("--asv-col", default="ASV_ID",
                      help="ASV ID column")
    cols.add_argument("--sample-col", default="sampleID",
                      help="Sample ID column")
    cols.add_argument("--count-col", default="count",
                      help="Count column")
    cols.add_argument("--group1-col", dest="depth_col", default="Depth",
                      help="Primary grouping column for coloring")
    cols.add_argument("--group2-col", dest="secondary_col", default="Month",
                      help="Secondary grouping column for coloring")
    # Backward-compatible aliases
    cols.add_argument("--depth-col", dest="depth_col", help=argparse.SUPPRESS)
    cols.add_argument("--secondary-col", dest="secondary_col", help=argparse.SUPPRESS)
    cols.add_argument("--month-col", dest="secondary_col", help=argparse.SUPPRESS)
    cols.add_argument("--color-col", default="Color",
                      help="Color mapping column for depths")
    cols.add_argument("--group1-order", default="",
                      help="Comma-separated explicit order for primary grouping legend.")
    cols.add_argument("--group2-order", default="",
                      help="Comma-separated explicit order for secondary grouping legend.")
    cols.add_argument("--group1-palette", default="", help="Primary label=#hex palette.")
    cols.add_argument("--group2-palette", default="", help="Secondary label=#hex palette.")
    
    # Preprocessing
    prep = ap.add_argument_group("Preprocessing")
    prep.add_argument("--normalize", choices=['total', 'clr', 'none'], default='total',
                      help="Normalization method")
    prep.add_argument("--transform", choices=['log1p', 'sqrt', 'none'], default='log1p',
                      help="Transformation method")
    prep.add_argument("--no-scale", action="store_true",
                      help="Skip standard scaling")
    
    # UMAP parameters
    umap_group = ap.add_argument_group("UMAP Parameters")
    umap_group.add_argument("--n-neighbors", type=int, default=15,
                            help="UMAP n_neighbors parameter")
    umap_group.add_argument("--min-dist", type=float, default=0.1,
                            help="UMAP min_dist parameter")
    umap_group.add_argument("--umap-metric", default="euclidean",
                            help="UMAP distance metric")
    umap_group.add_argument("--n-components", type=int, default=2,
                            help="Number of UMAP dimensions")
    
    # HDBSCAN parameters
    hdb = ap.add_argument_group("HDBSCAN Parameters")
    hdb.add_argument("--min-cluster-size", type=int, default=5,
                     help="Minimum cluster size")
    hdb.add_argument("--min-samples", type=int, default=3,
                     help="Minimum samples for core point")
    hdb.add_argument("--hdbscan-metric", default="euclidean",
                     help="HDBSCAN distance metric")
    
    # Plotting
    plot = ap.add_argument_group("Plotting")
    plot.add_argument("--figsize", default="10,8",
                      help="Figure size as 'width,height' in inches")
    plot.add_argument("--point-size", type=float, default=50,
                      help="Scatter point size")
    plot.add_argument("--alpha", type=float, default=0.7,
                      help="Point transparency (0-1)")
    
    # Other
    ap.add_argument("--random-state", type=int, default=42,
                    help="Random seed for reproducibility")
    
    return ap.parse_args()


def main():
    """Main execution function."""
    args = parse_args()
    
    # Validate input
    if not args.input.exists():
        print(f"[ERROR] Input file not found: {args.input}")
        sys.exit(1)
    
    # Parse parameters
    formats = [f.strip().lstrip('.') for f in args.formats.split(',')]
    figsize = tuple(map(float, args.figsize.split(',')))
    
    print("="*60)
    print("ASV UMAP Clustering Pipeline")
    print("="*60)
    print(f"Input: {args.input}")
    print(f"Output prefix: {args.output_prefix}")
    print("="*60)
    group1_order = parse_list_csv(args.group1_order)
    group2_order = parse_list_csv(args.group2_order)
    group1_palette = parse_palette(args.group1_palette)
    group2_palette = parse_palette(args.group2_palette)
    
    # Metadata columns to preserve
    metadata_cols = [
        args.depth_col, args.secondary_col, args.color_col,
        'Year', 'Cruise', 'Salinity (PSU)', 'sample_code',
        'Domain', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species',
        'plateID', 'Temperature', 'Oxygen',
        'Phosphate', 'Silicate', 'Nitrogen Oxides', 'Nitrate',
        'Nitrite', 'Ammonium', 'Hydrogen Sulfide', 'Fe (nM)', 'Methane',
        'Nitrous Oxide', 'Dimethyl Sulfide'
        ]

    # Load and pivot data
    count_matrix, metadata_df = load_and_pivot_data(
        input_file=args.input,
        asv_col=args.asv_col,
        sample_col=args.sample_col,
        count_col=args.count_col,
        metadata_cols=metadata_cols
    )
    
    # Preprocess
    X = preprocess_counts(
        count_matrix=count_matrix,
        normalize=args.normalize,
        transform=args.transform,
        scale=not args.no_scale
    )
    
    # Run UMAP
    embedding = run_umap(
        X=X,
        n_neighbors=args.n_neighbors,
        min_dist=args.min_dist,
        n_components=args.n_components,
        metric=args.umap_metric,
        random_state=args.random_state
    )
    
    # Run HDBSCAN
    cluster_labels = run_hdbscan(
        embedding=embedding,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
        metric=args.hdbscan_metric
    )
    
    # Create output directory
    output_prefix = Path(args.output_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    
    # Save results
    print("\n[INFO] Saving results...")
    results_df = pd.DataFrame({
        'sample': count_matrix.index,
        'UMAP1': embedding[:, 0],
        'UMAP2': embedding[:, 1],
        'cluster': cluster_labels
    })
    
    # Add metadata
    for col in metadata_df.columns:
        results_df[col] = metadata_df[col].values
    
    results_file = output_prefix.parent / f"{output_prefix.stem}_results.tsv"
    results_df.to_csv(results_file, sep='\t', index=False)
    print(f"[OK] Saved results: {results_file}")
    
    # Generate plots
    print("\n" + "="*60)
    print("[INFO] Generating plots...")
    print("="*60)
    
    # Plot 1: Colored by Depth (using Color column)
    if args.depth_col in metadata_df.columns and args.color_col in metadata_df.columns:
        print(f"\n[INFO] Creating UMAP plot colored by {args.depth_col}...")
        
        # Build color map from metadata
        depth_color_df = metadata_df[[args.depth_col, args.color_col]].dropna().drop_duplicates()
        color_map = dict(zip(depth_color_df[args.depth_col].astype(str), depth_color_df[args.color_col]))
        color_map.update(group1_palette)
        group1_suffix = safe_slug(args.depth_col)
        
        for fmt in formats:
            output_file = output_prefix.parent / f"{output_prefix.stem}_{group1_suffix}.{fmt}"
            plot_umap_scatter(
                embedding=embedding,
                colors=metadata_df[args.depth_col],
                color_map=color_map,
                title=f"UMAP Projection Colored by {args.depth_col}",
                xlabel="UMAP1",
                ylabel="UMAP2",
                legend_title=args.depth_col,
                output_file=output_file,
                figsize=figsize,
                dpi=args.dpi,
                alpha=args.alpha,
                point_size=args.point_size,
                value_order=group1_order if group1_order else None
            )
    else:
        print(f"[WARN] Skipping depth plot: {args.depth_col} or {args.color_col} not in metadata")
    
    # Plot 2: Colored by secondary grouping column
    if args.secondary_col in metadata_df.columns:
        print(f"\n[INFO] Creating UMAP plot colored by {args.secondary_col}...")
        secondary_suffix = safe_slug(args.secondary_col)
        
        for fmt in formats:
            output_file = output_prefix.parent / f"{output_prefix.stem}_{secondary_suffix}.{fmt}"
            plot_umap_scatter(
                embedding=embedding,
                colors=metadata_df[args.secondary_col],
                color_map=group2_palette or None,
                title=f"UMAP Projection Colored by {args.secondary_col}",
                xlabel="UMAP1",
                ylabel="UMAP2",
                legend_title=args.secondary_col,
                output_file=output_file,
                figsize=figsize,
                dpi=args.dpi,
                alpha=args.alpha,
                point_size=args.point_size,
                value_order=group2_order if group2_order else None
            )
    else:
        print(f"[WARN] Skipping secondary plot: {args.secondary_col} not in metadata")
    
    # Plot 3: Colored by HDBSCAN clusters
    print("\n[INFO] Creating UMAP plot colored by HDBSCAN clusters...")
    
    cluster_series = pd.Series(cluster_labels, index=count_matrix.index)
    
    for fmt in formats:
        output_file = output_prefix.parent / f"{output_prefix.stem}_clusters.{fmt}"
        plot_umap_scatter(
            embedding=embedding,
            colors=cluster_series,
            color_map=None,  # Auto-assign colors
            title="UMAP Projection with HDBSCAN Clusters",
            xlabel="UMAP1",
            ylabel="UMAP2",
            legend_title="Cluster",
            output_file=output_file,
            figsize=figsize,
            dpi=args.dpi,
            alpha=args.alpha,
            point_size=args.point_size
        )
    
    # Plot 4: Biochemistry metadata with size encoding
    biochem_available = [col for col in BIOCHEM_METADATA if col in metadata_df.columns]
    
    if biochem_available:
        print("\n[INFO] Creating UMAP scatters for biochemistry metadata...")
        
        for meta in biochem_available:
            series = metadata_df[meta]
            if series.notna().sum() == 0:
                print(f"[WARN] Skipping {meta} plot: column has only missing values")
                continue
            
            color = BIOCHEM_COLOR_MAP.get(meta, 'gray')
            slug = (
                meta.lower()
                .replace(' ', '_')
                .replace('(', '')
                .replace(')', '')
                .replace('/', '_')
            )
            
            for fmt in formats:
                output_file = output_prefix.parent / f"{output_prefix.stem}_{slug}.{fmt}"
                plot_biochem_scatter(
                    embedding=embedding,
                    values=series,
                    meta=meta,
                    color=color,
                    output_file=output_file,
                    figsize=figsize,
                    dpi=args.dpi,
                    alpha=args.alpha
                )
    else:
        print("\n[WARN] Skipping biochemistry plots: none of the configured columns present")
    
    print("\n" + "="*60)
    print("[OK] Pipeline complete!")
    print("="*60)
    print(f"Results saved to: {results_file}")
    print(f"Plots saved with prefix: {output_prefix}")


if __name__ == "__main__":
    main()
