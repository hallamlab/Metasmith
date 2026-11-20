#!/usr/bin/env python3
"""
asv_taxonomy_bubble_plot.py
---------------------------
Generate bubble plots showing ASV counts by taxonomy across user-defined groups.

Features:
- Hierarchical taxonomy visualization (Order > Family > Genus)
- Bubble sizes represent transformed ASV counts
- Faceted plots by primary group
- Summary plots: Taxonomy × Group1 and Taxonomy × Group2
- Automatic taxonomy hierarchy labeling with brackets
- Color-coded Group1 values from metadata
- Fully configurable via CLI

Example:
--------
python asv_taxonomy_bubble_plot.py \
  --input asv_data.tsv \
  --group1-col type_group \
  --color-col Color \
  --asv-col ASV_ID \
  --count-col count \
  --sample-col sampleID \
  --exclude-genus Ralstonia \
  --output-prefix bubble_plot_asv \
  --formats pdf,png
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import warnings

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Suppress warnings
warnings.filterwarnings('ignore')

# ---------- Taxonomy Functions ----------
def classify_taxonomy(order: str, family: str, genus: str, 
                      unclassified_label: str = 'Unclassified') -> str:
    """
    Classify taxonomy hierarchically with ALL ranks always populated.
    Handles special placeholder terms like 'Incertae_Sedis', 'unidentified', etc.
    
    Args:
        order: Order-level taxonomy
        family: Family-level taxonomy
        genus: Genus-level taxonomy
        unclassified_label: Label to use for completely unclassified taxa
    
    Returns:
        Pipe-delimited string: 'Order|Family|Genus' (all always populated)
    """
    # List of placeholder terms that should be treated as missing
    PLACEHOLDER_TERMS = {
        'incertae_sedis', 'incertae sedis', 'incertae-sedis',
        'unidentified', 'unassigned', 'uncultured', 'unknown',
        'undefined', 'unclassified', 'na', 'none', '', 'nan'
    }
    
    def is_placeholder(value):
        """Check if a value is a placeholder that should be treated as missing."""
        if not value or pd.isna(value):
            return True
        val_str = str(value).strip().lower()
        if val_str in PLACEHOLDER_TERMS:
            return True
        # Check if it's ONLY a placeholder (not "Family_unclassified")
        if val_str == 'incertae_sedis' or val_str.startswith('incertae'):
            return True
        return False
    
    # Clean and validate order
    if is_placeholder(order):
        order = unclassified_label
        order_base = unclassified_label
    else:
        order = str(order).strip()
        order_base = order.replace('_unclassified', '').strip()
        if not order_base or is_placeholder(order_base):
            order = unclassified_label
            order_base = unclassified_label
    
    # Clean and validate family - ALWAYS populate
    if is_placeholder(family):
        family = f'{order_base}_unclassified'
        family_base = order_base
    else:
        family = str(family).strip()
        family_base = family.replace('_unclassified', '').strip()
        if not family_base or is_placeholder(family_base):
            family = f'{order_base}_unclassified'
            family_base = order_base
    
    # Clean and validate genus - ALWAYS populate
    if is_placeholder(genus):
        genus = f'{family_base}_unclassified'
    else:
        genus = str(genus).strip()
        genus_base = genus.replace('_unclassified', '').strip()
        if not genus_base or is_placeholder(genus_base):
            genus = f'{family_base}_unclassified'
    
    # Final cleanup: ensure no double "_unclassified" patterns
    order = order.replace('_unclassified_unclassified', '_unclassified')
    family = family.replace('_unclassified_unclassified', '_unclassified')
    genus = genus.replace('_unclassified_unclassified', '_unclassified')
    
    # CRITICAL: Ensure nothing is empty or a placeholder
    if not order or order == '' or is_placeholder(order):
        order = unclassified_label
    if not family or family == '' or is_placeholder(family):
        family = f'{order}_unclassified'
    if not genus or genus == '' or is_placeholder(genus):
        genus = f'{family}_unclassified'
    
    return f'{order}|{family}|{genus}'


def apply_taxonomy_classification(df: pd.DataFrame, 
                                  order_col: str = 'Order',
                                  family_col: str = 'Family', 
                                  genus_col: str = 'Genus',
                                  unclassified_label: str = 'Unclassified') -> pd.DataFrame:
    """
    Apply taxonomy classification to entire dataframe with validation.
    Ensures ALL taxonomy ranks are always populated.
    
    Args:
        df: Input dataframe with Order, Family, Genus columns
        order_col: Name of order column
        family_col: Name of family column
        genus_col: Name of genus column
        unclassified_label: Label for unclassified taxa
    
    Returns:
        DataFrame with added 'Order_Family_Genus' column and updated taxonomy columns
    """
    ofg_list = []
    o_list = []
    f_list = []
    g_list = []
    
    # Convert columns to string to handle mixed types
    orders = df[order_col].fillna('').astype(str)
    families = df[family_col].fillna('').astype(str)
    genera = df[genus_col].fillna('').astype(str)
    
    for o, f, g in zip(orders, families, genera):
        ofg = classify_taxonomy(o, f, g, unclassified_label)
        ofg_list.append(ofg)
        
        # Split and store
        new_o, new_f, new_g = ofg.split('|')
        o_list.append(new_o)
        f_list.append(new_f)
        g_list.append(new_g)
    
    df['Order_Family_Genus'] = ofg_list
    df[order_col] = o_list
    df[family_col] = f_list
    df[genus_col] = g_list
    
    # Validation check
    print(f"[INFO] Taxonomy validation:")
    print(f"  Total entries: {len(df)}")
    print(f"  Orders with '_unclassified': {sum('_unclassified' in str(x) for x in o_list)}")
    print(f"  Families with '_unclassified': {sum('_unclassified' in str(x) for x in f_list)}")
    print(f"  Genera with '_unclassified': {sum('_unclassified' in str(x) for x in g_list)}")
    
    # Check for empty values (should be ZERO)
    empty_orders = sum(1 for x in o_list if not x or str(x).strip() == '')
    empty_families = sum(1 for x in f_list if not x or str(x).strip() == '')
    empty_genera = sum(1 for x in g_list if not x or str(x).strip() == '')
    
    if empty_orders > 0:
        print(f"  [ERROR] Found {empty_orders} empty Orders!")
    if empty_families > 0:
        print(f"  [ERROR] Found {empty_families} empty Families!")
    if empty_genera > 0:
        print(f"  [ERROR] Found {empty_genera} empty Genera!")
    
    if empty_orders == 0 and empty_families == 0 and empty_genera == 0:
        print(f"  ✓ All taxonomy ranks populated")
    
    # Check for double unclassified
    double_unclassified = [x for x in ofg_list if '_unclassified_unclassified' in x]
    if double_unclassified:
        print(f"  [WARN] Found {len(double_unclassified)} entries with double '_unclassified':")
        for item in set(double_unclassified[:5]):
            print(f"      {item}")
    else:
        print(f"  ✓ No double '_unclassified' patterns found")
    
    return df


def draw_taxonomy_hierarchy(ax, data, family_col='Family', order_col='Order',
                           family_x=-0.62, order_x=-1.18,
                           linewidth=1.0):
    """
    Draw taxonomy hierarchy with ONLY vertical lines showing nested structure.
    Only draws lines when there are multiple children (>1).
    
    Args:
        ax: Matplotlib axis
        data: DataFrame with taxonomy and ofg_numeric columns
        family_x: X-position for family lines and labels
        order_x: X-position for order lines and labels
        linewidth: Line width for hierarchy lines
    """
    # Sort data by position for clean rendering
    data_sorted = data.sort_values('ofg_numeric')
    
    # Draw Order hierarchy - ONE LINE PER ORDER (not per family)
    orders = [o for o in data_sorted[order_col].unique() if o and o.strip()]
    
    for order in orders:
        order_data = data_sorted[data_sorted[order_col] == order]
        positions = sorted(order_data['ofg_numeric'].unique())
        
        if len(positions) == 0:
            continue
        
        start_pos = min(positions)
        end_pos = max(positions)
        mid_pos = (start_pos + end_pos) / 2
        
        # Add order label at midpoint
        ax.text(
            order_x - 0.08, mid_pos, order,
            ha='right', va='center',
            transform=ax.get_yaxis_transform(),
            fontsize=9, weight='bold'
        )
        
        # Only draw vertical line if order has MORE THAN ONE child position
        if len(positions) > 1:
            ax.plot(
                [order_x, order_x],
                [start_pos, end_pos],
                lw=linewidth + 0.5, 
                color='black', 
                solid_capstyle='butt',
                clip_on=False,
                transform=ax.get_yaxis_transform(),
                zorder=1
            )
    
    # Draw Family hierarchy - ONE LINE PER FAMILY
    families = [f for f in data_sorted[family_col].unique() if f]
    
    for family in families:
        family_data = data_sorted[data_sorted[family_col] == family]
        positions = sorted(family_data['ofg_numeric'].unique())
        
        if len(positions) == 0:
            continue
        
        start_pos = min(positions)
        end_pos = max(positions)
        mid_pos = (start_pos + end_pos) / 2
        
        # Add family label at midpoint
        ax.text(
            family_x - 0.08, mid_pos, family,
            ha='right', va='center',
            transform=ax.get_yaxis_transform(),
            fontsize=8, style='italic'
        )
        
        # Only draw vertical line if family has MORE THAN ONE child position
        if len(positions) > 1:
            ax.plot(
                [family_x, family_x],
                [start_pos, end_pos],
                lw=linewidth,
                color='black',
                solid_capstyle='butt',
                clip_on=False,
                transform=ax.get_yaxis_transform(),
                zorder=1
            )


def sort_depths(depths: List) -> List:
    """Sort depth values - numerically if possible, otherwise alphabetically."""
    try:
        # Try to sort as numbers
        return sorted(depths, key=lambda x: float(x))
    except (ValueError, TypeError):
        # Fall back to string sorting
        return sorted(depths, key=str)


def resolve_group_order(values: List, requested: Optional[List[str]] = None) -> List:
    """Resolve categorical order: requested (filtered) then remaining in canonical order."""
    present = [str(v) for v in pd.Series(values).dropna().astype(str).unique().tolist()]
    if not requested:
        return present
    specified = [str(v).strip() for v in requested if str(v).strip()]
    ordered = [v for v in specified if v in present]
    ordered += [v for v in present if v not in ordered]
    return ordered


def transform_counts(counts: pd.Series, method: str = 'sqrt',
                     min_count: float = 1.1) -> pd.Series:
    """
    Transform count values for better visualization.
    
    Args:
        counts: Raw count values
        method: Transformation method ('sqrt', 'cbrt', 'log', 'asinh', 'quantile')
        min_count: Minimum value to use
    
    Returns:
        Transformed values
    """
    counts_adj = counts.apply(lambda x: x if x > 1 else min_count)
    
    if method == 'sqrt':
        return np.sqrt(counts_adj)
    elif method == 'cbrt':
        return np.cbrt(counts_adj)
    elif method == 'log':
        return np.log(counts_adj)
    elif method == 'asinh':
        return np.arcsinh(counts)
    elif method == 'quantile':
        return counts.rank(method='average')
    else:
        raise ValueError(f"Unknown method: {method}")


# ---------- Data Processing ----------

def load_and_process_data(
    input_file: Path,
    asv_col: str,
    count_col: str,
    depth_col: str,
    color_col: str,
    order_col: str,
    family_col: str,
    genus_col: str,
    stat_col: Optional[str],
    exclude_genera: Optional[List[str]],
    unclassified_label: str,
    sample_col: str
) -> Tuple[pd.DataFrame, List[str], Dict[str, str]]:
    """
    Load input data and process taxonomy.
    
    Returns:
        Tuple of (processed_dataframe, list_of_isa_orders, depth_color_palette)
    """
    # Load data
    print(f"[INFO] Reading {input_file}...")
    df = pd.read_csv(input_file, sep='\t')
    
    # Validate required columns
    required_cols = [asv_col, count_col, depth_col, order_col, family_col, genus_col]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    print(f"[INFO] Loaded {len(df)} rows")
    print(f"[INFO] Columns: {list(df.columns)}")
    
    # Get ISA orders if stat column provided
    order_isa_list = []
    if stat_col and stat_col in df.columns:
        order_isa_list = list(set([o for o, s in zip(df[order_col], df[stat_col]) 
                                   if pd.notna(o) and pd.notna(s) and s > 0]))
        print(f"[INFO] ISA orders (will be marked with *): {order_isa_list}")
    
    # Filter out excluded genera
    if exclude_genera:
        before_count = len(df)
        for genus in exclude_genera:
            df = df[df[genus_col] != genus]
        after_count = len(df)
        print(f"[INFO] Excluded {before_count - after_count} rows with genera: {exclude_genera}")
    
    # Apply taxonomy classification
    print("[INFO] Classifying taxonomy...")
    df = apply_taxonomy_classification(
        df, 
        order_col=order_col,
        family_col=family_col,
        genus_col=genus_col,
        unclassified_label=unclassified_label
    )
    
    # Build depth color palette from metadata
    palette = {}
    if color_col in df.columns:
        palette_df = df[[depth_col, color_col]].drop_duplicates()
        palette = dict(zip(palette_df[depth_col].astype(str), palette_df[color_col]))
        print(f"[INFO] Depth color palette: {palette}")
    
    return df, order_isa_list, palette


def aggregate_data(
    df: pd.DataFrame,
    asv_col: str,
    sample_col: str,
    count_col: str,
    depth_col: str,
    order_isa_list: List[str],
    min_count: float = 1.1
) -> pd.DataFrame:
    """
    Aggregate data by sample, taxonomy, and depth (summing across ASVs).
    
    Args:
        df: Input dataframe
        asv_col: ASV ID column name
        sample_col: Sample ID column name
        count_col: Count column name
        depth_col: Depth column name
        order_isa_list: List of ISA marker orders to highlight
        min_count: Minimum count to use (for log transformation)
    
    Returns:
        Aggregated dataframe
    """
    print("[INFO] Aggregating counts by sample, taxonomy, and depth...")
    
    # Group and sum counts across ASVs - now grouping by SAMPLE instead
    grouped_df = df.groupby(
        [sample_col, 'Order', 'Family', 'Genus', depth_col, 'Order_Family_Genus']
    )[count_col].sum().reset_index()
    
    print(f"[INFO] Aggregated to {len(grouped_df)} unique sample × taxonomy combinations")
    
    # Transform counts
    grouped_df['transformed_count'] = transform_counts(
        grouped_df[count_col],
        method='sqrt',
        min_count=min_count
    )
    
    print(f"[INFO] Unique samples: {len(grouped_df[sample_col].unique())}")
    print(f"[INFO] Unique taxonomies: {len(grouped_df['Order_Family_Genus'].unique())}")
    print(f"[INFO] Unique depths: {len(grouped_df[depth_col].unique())}")
    print("[INFO] Skipping zero-filling (not needed for bubble plots)")
    
    # Mark ISA orders with asterisk
    if order_isa_list:
        grouped_df['Order'] = grouped_df['Order'].apply(
            lambda o: f'* {o}' if o in order_isa_list else f'  {o}'
        )
        print(f"[INFO] Marked {len(order_isa_list)} ISA orders with asterisk")
    
    # Sort
    grouped_df = grouped_df.sort_values(by=[depth_col, 'Order_Family_Genus', sample_col])
    
    return grouped_df


def create_summary_data(
    df: pd.DataFrame,
    group_col: str,
    count_col: str,
    min_count: float = 1.1
) -> pd.DataFrame:
    """
    Create summary data aggregated by taxonomy and a grouping column (depth/month).
    Averages counts across samples within each group.
    
    Args:
        df: Input dataframe (already aggregated by sample)
        group_col: Column to group by (e.g., 'Depth', 'Month')
        count_col: Count column name
        min_count: Minimum count for transformation
    
    Returns:
        Aggregated dataframe with transformed counts
    """
    print(f"[INFO] Creating summary data grouped by {group_col}...")
    
    # Average counts across samples for each taxonomy × group combination
    summary_df = df.groupby(
        [group_col, 'Order', 'Family', 'Genus', 'Order_Family_Genus']
    )[count_col].mean().reset_index()
    
    print(f"[INFO] Summary aggregated to {len(summary_df)} unique combinations")
    print(f"[INFO] Unique {group_col} values: {len(summary_df[group_col].unique())}")
    
    # Transform counts
    summary_df['transformed_count'] = transform_counts(
        summary_df[count_col],
        method='sqrt',
        min_count=min_count
    )
    
    return summary_df


def prepare_taxonomy_ordering(
    df: pd.DataFrame,
    unclassified_label: str = 'Unclassified',
    add_spacing: bool = True,
    order_gap: float = 2.0,
    family_gap: float = 0.8
) -> Tuple[List[str], List[str], Dict[str, int], Dict[str, int]]:
    """
    Create ordered taxonomy lists and mappings for plotting with optional spacing.
    
    Args:
        add_spacing: Add vertical gaps between orders and families
        order_gap: Gap size between different orders
        family_gap: Gap size between families within same order
    
    Returns:
        Tuple of (ofg_list, genera_list, ofg_mapping, genus_mapping)
    """
    # Get unique OFG values and sort (put unclassified at top)
    val = f'{unclassified_label}||'
    ofg_updated = list(sorted(df['Order_Family_Genus'].unique()))[::-1]
    
    if val in ofg_updated:
        ofg_updated.remove(val)
        ofg_updated.insert(0, val)
    
    # Extract genera
    genera_updated = [x.rsplit('|', 1)[1] for x in ofg_updated]
    
    # Create mappings with optional spacing
    if add_spacing:
        ofg_mapping_updated = {}
        genus_mapping_updated = {}
        
        current_pos = 0
        prev_order = None
        prev_family = None
        
        for ofg, genus in zip(ofg_updated, genera_updated):
            order, family, _ = ofg.split('|')
            
            # Add gap between different orders
            if prev_order is not None and order != prev_order:
                current_pos += order_gap
            # Add smaller gap between families within same order
            elif prev_family is not None and family != prev_family and order == prev_order:
                current_pos += family_gap
            
            ofg_mapping_updated[ofg] = current_pos
            genus_mapping_updated[genus] = current_pos
            
            prev_order = order
            prev_family = family
            current_pos += 1
    else:
        # Original behavior - sequential numbering
        ofg_mapping_updated = {ofg: i for i, ofg in enumerate(ofg_updated)}
        genus_mapping_updated = {genus: i for i, genus in enumerate(genera_updated)}
    
    print(f"[INFO] Taxonomy ordering prepared: {len(genera_updated)} genera")
    if add_spacing:
        max_pos = max(ofg_mapping_updated.values())
        print(f"[INFO] Y-axis span with spacing: 0 to {max_pos:.1f}")
    
    return ofg_updated, genera_updated, ofg_mapping_updated, genus_mapping_updated


def calculate_figsize(
    n_x: int,
    n_y: int,
    x_labels: Optional[List[str]] = None,
    y_labels: Optional[List[str]] = None,
    width_per_x: float = 0.55,
    height_per_y: float = 0.26,
    min_width: float = 16,
    max_width: float = 180,
    min_height: float = 12,
    max_height: float = 180,
    x_rotation: float = 0.0,
    include_legend: bool = True,
    include_taxonomy_gutters: bool = True
) -> Tuple[float, float]:
    """
    Calculate appropriate figure size based on data dimensions.
    
    Args:
        n_x: Number of x-axis entries
        n_y: Number of y-axis entries
        x_labels: X-axis label values
        y_labels: Y-axis label values
        width_per_x: Width in inches per x-axis entry
        height_per_y: Height in inches per y-axis entry
        min_width/max_width: Bounds for width
        min_height/max_height: Bounds for height
        x_rotation: Rotation angle for x-axis labels in degrees
        include_legend: Reserve space for legend on the right
        include_taxonomy_gutters: Reserve extra left margin for Order/Family labels
    
    Returns:
        Tuple of (width, height) in inches
    """
    x_labels = [str(x) for x in (x_labels or [])]
    y_labels = [str(y) for y in (y_labels or [])]

    max_x_len = max((len(x) for x in x_labels), default=0)
    max_y_len = max((len(y) for y in y_labels), default=0)

    label_rotation_factor = 1.0 if abs(x_rotation) >= 45 else 0.45

    # Main plotting area
    width = (n_x * width_per_x) + (max_x_len * 0.11 * label_rotation_factor)
    height = (n_y * height_per_y) + (max_y_len * 0.035) + (max_x_len * 0.03 * label_rotation_factor)

    # Margins for taxonomy gutters and legends
    left_margin = 2.5 + (max_y_len * 0.12)
    if include_taxonomy_gutters:
        left_margin += 4.0
    right_margin = 4.5 if include_legend else 1.5
    top_bottom_margin = 3.5

    width += left_margin + right_margin
    height += top_bottom_margin
    
    # Apply bounds
    width = max(min_width, min(width, max_width))
    height = max(min_height, min(height, max_height))
    
    return (width, height)


# ---------- Plotting ----------


def plot_depth_bubble(
    depth_data: pd.DataFrame,
    depth_value: str,
    sample_col: str,
    count_col: str,
    genera_list: List[str],
    genus_mapping: Dict[str, int],
    base_figsize: Tuple[int, int],
    bubble_scale: float,
    output_prefix: Path,
    formats: List[str],
    dpi: int,
    show_legend: bool,
    auto_size: bool = True
) -> None:
    """
    Create and save bubble plot for a single depth.
    """
    # Get samples
    samples = sorted(depth_data[sample_col].unique())
    n_samples = len(samples)
    n_genera = len(genera_list)
    
    # Calculate figure size
    if auto_size:
        figsize = calculate_figsize(
            n_x=n_samples,
            n_y=n_genera,
            x_labels=samples,
            y_labels=genera_list,
            width_per_x=0.58,
            height_per_y=0.48,
            min_width=18,
            max_width=220,
            min_height=12,
            max_height=180,
            x_rotation=90,
            include_legend=show_legend,
            include_taxonomy_gutters=True
        )
        print(f"[INFO] Auto-calculated figure size: {figsize[0]:.1f} × {figsize[1]:.1f} inches")
    else:
        figsize = base_figsize
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create sample to x-position mapping
    sample_positions = {sample: i for i, sample in enumerate(samples)}
    
    # Plot bubbles for each sample × taxonomy combination
    for sample in samples:
        sample_data = depth_data[depth_data[sample_col] == sample]
        x_pos = sample_positions[sample]
        
        ax.scatter(
            [x_pos] * len(sample_data), 
            sample_data['ofg_numeric'], 
            s=sample_data['transformed_count'] * bubble_scale,
            alpha=0.6, 
            edgecolors='white',
            linewidth=0.5,
            marker='o', 
            color='black'
        )
    
    # Set y-axis using actual OFG data from this depth
    # Get unique OFG values present in this depth's data
    present_ofg = depth_data['Order_Family_Genus'].unique()
    
    # Sort by their numeric positions and extract genus labels
    y_positions = []
    y_labels = []
    for ofg in sorted(present_ofg, key=lambda x: depth_data[depth_data['Order_Family_Genus'] == x]['ofg_numeric'].iloc[0]):
        pos = depth_data[depth_data['Order_Family_Genus'] == ofg]['ofg_numeric'].iloc[0]
        genus = ofg.split('|')[2] if '|' in ofg else ofg
        y_positions.append(pos)
        y_labels.append(genus)
    
    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=10)
    
    # Draw taxonomy hierarchy with clean vertical lines only
    draw_taxonomy_hierarchy(
        ax, depth_data,
        family_col='Family',
        order_col='Order',
        family_x=-0.62,
        order_x=-1.18,
        linewidth=1.0
    )
    
    # Set x-axis with sample names
    ax.set_xticks(list(sample_positions.values()))
    ax.set_xticklabels(samples, rotation=90, ha='right', fontsize=9)
    
    # Set limits with padding
    max_y = max(y_positions) if y_positions else len(genera_list)
    min_y = min(y_positions) if y_positions else 0
    ax.set_xlim(-1.5, len(samples))
    ax.set_ylim(min_y - 1, max_y + 1)
    ax.set_xlabel('Samples', fontsize=16, fontweight='bold')
    ax.tick_params(axis='x', labelsize=10)
    ax.tick_params(axis='y', labelsize=10)
    
    # Add legend for bubble sizes
    if show_legend:
        handles, labels = [], []
        raw_counts = [1, 10, 100, 1000, 10000, 100000]
        in_labels = ['1', '10', '100', '1K', '10K', '100K']
        
        # Apply same transformation as the data
        transformed_counts = [np.sqrt(c) for c in raw_counts]
        
        for size, label in zip(transformed_counts, in_labels):
            handle = ax.scatter([], [], s=size * bubble_scale, color='grey', alpha=0.5, label=label)
            handles.append(handle)
            labels.append(label)
        
        legend = ax.legend(
            handles=handles, labels=labels, title="Total ASV count",
            bbox_to_anchor=(1, 1), loc='upper left', fontsize=14,
            labelspacing=1.2, handlelength=4, handleheight=2.5
        )
        plt.setp(legend.get_title(), fontsize=14, fontweight='bold')
    
    # Add taxonomy level labels at bottom - ALIGNED with their respective columns
    label_y = min_y - 0.8
    ax.text(-0.62, label_y, 'Family', transform=ax.get_yaxis_transform(),
            ha='center', va='top', fontsize=13, fontweight='bold', style='italic')
    ax.text(-1.18, label_y, 'Order', transform=ax.get_yaxis_transform(),
            ha='center', va='top', fontsize=13, fontweight='bold')
    ax.text(0, label_y, 'Genus', transform=ax.get_yaxis_transform(), 
            ha='left', va='top', fontsize=13, fontweight='bold')
    
    plt.title(f'Bubble Plot of ASV Counts by Taxonomy at Depth {depth_value}', 
              fontsize=18, fontweight='bold', pad=20)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.3)
    plt.tight_layout()
    
    # Save outputs
    safe_depth = str(depth_value).replace('.', '_').replace('/', '_')
    for fmt in formats:
        output_file = output_prefix.parent / f"{output_prefix.stem}_depth_{safe_depth}.{fmt}"
        if fmt == 'png':
            plt.savefig(output_file, format=fmt, bbox_inches='tight', dpi=dpi)
        else:
            plt.savefig(output_file, format=fmt, bbox_inches='tight')
        print(f"[OK] Saved: {output_file}")
    
    plt.close()


def plot_summary_bubble(
    summary_data: pd.DataFrame,
    group_col: str,
    count_col: str,
    genera_list: List[str],
    ofg_mapping: Dict[str, int],
    bubble_scale: float,
    output_prefix: Path,
    formats: List[str],
    dpi: int,
    show_legend: bool,
    title: str,
    group_order: Optional[List[str]] = None
) -> None:
    """
    Create summary bubble plot showing taxonomy vs depth/month.
    """
    summary_data = summary_data.copy()
    summary_data[group_col] = summary_data[group_col].astype(str)

    # Add numeric mapping for y-axis
    summary_data['ofg_numeric'] = summary_data['Order_Family_Genus'].map(ofg_mapping)
    
    # Get unique group values and sort
    if group_order:
        groups = resolve_group_order(summary_data[group_col].astype(str).tolist(), group_order)
    elif group_col == 'Month':
        # Sort months chronologically
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        groups = [m for m in month_order if m in summary_data[group_col].astype(str).unique()]
        if not groups:  # If month names don't match, try numeric
            groups = sorted(summary_data[group_col].astype(str).unique(), key=lambda x: int(x) if str(x).isdigit() else 0)
    else:
        groups = sort_depths(summary_data[group_col].astype(str).unique())
    
    n_groups = len(groups)
    n_genera = len(genera_list)
    
    # Calculate figure size
    figsize = calculate_figsize(
        n_x=n_groups,
        n_y=n_genera,
        x_labels=groups,
        y_labels=genera_list,
        width_per_x=1.1,
        height_per_y=0.48,
        min_width=18,
        max_width=180,
        min_height=14,
        max_height=180,
        x_rotation=45,
        include_legend=show_legend,
        include_taxonomy_gutters=True
    )
    
    print(f"[INFO] Summary plot size: {figsize[0]:.1f} × {figsize[1]:.1f} inches")
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create group to x-position mapping
    group_positions = {grp: i for i, grp in enumerate(groups)}
    
    # Plot bubbles
    for grp in groups:
        grp_data = summary_data[summary_data[group_col] == grp]
        x_pos = group_positions[grp]
        
        ax.scatter(
            [x_pos] * len(grp_data),
            grp_data['ofg_numeric'],
            s=grp_data['transformed_count'] * bubble_scale,
            alpha=0.6,
            edgecolors='white',
            linewidth=0.5,
            marker='o',
            color='darkblue'
        )
    
    # Set y-axis - get unique OFG values in the summary data
    present_ofg = summary_data['Order_Family_Genus'].unique()
    
    # Get their positions and corresponding genera
    y_positions = []
    y_labels = []
    for ofg in sorted(present_ofg, key=lambda x: ofg_mapping.get(x, 0)):
        if ofg in ofg_mapping:
            y_positions.append(ofg_mapping[ofg])
            genus = ofg.split('|')[2] if '|' in ofg else ofg
            y_labels.append(genus)
    
    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=10)
    
    # Draw taxonomy hierarchy with clean vertical lines only
    draw_taxonomy_hierarchy(
        ax, summary_data,
        family_col='Family',
        order_col='Order',
        family_x=-0.62,
        order_x=-1.18,
        linewidth=1.0
    )
    
    # Set x-axis
    ax.set_xticks(list(group_positions.values()))
    ax.set_xticklabels(groups, rotation=45, ha='right', fontsize=11)
    
    # Set limits with padding
    max_y = max(y_positions) if y_positions else len(genera_list)
    min_y = min(y_positions) if y_positions else 0
    ax.set_xlim(-2.0, len(groups))
    ax.set_ylim(min_y - 1, max_y + 1)
    ax.set_xlabel(group_col, fontsize=16, fontweight='bold')
    ax.tick_params(axis='x', labelsize=12)
    ax.tick_params(axis='y', labelsize=10)
    
    # Add legend
    if show_legend:
        handles, labels_list = [], []
        raw_counts = [1, 10, 100, 1000, 10000, 100000]
        in_labels = ['1', '10', '100', '1K', '10K', '100K']
        
        transformed_counts = [np.sqrt(c) for c in raw_counts]
        
        for size, label in zip(transformed_counts, in_labels):
            handle = ax.scatter([], [], s=size * bubble_scale, color='darkblue', alpha=0.6, label=label)
            handles.append(handle)
            labels_list.append(label)
        
        legend = ax.legend(
            handles=handles, labels=labels_list, title="Avg ASV count",
            bbox_to_anchor=(1, 1), loc='upper left', fontsize=14,
            labelspacing=1.2, handlelength=4, handleheight=2.5
        )
        plt.setp(legend.get_title(), fontsize=14, fontweight='bold')
    
    # Add taxonomy level labels - ALIGNED with their respective columns
    label_y = min_y - 0.8
    ax.text(-0.62, label_y, 'Family', transform=ax.get_yaxis_transform(),
            ha='center', va='top', fontsize=13, fontweight='bold', style='italic')
    ax.text(-1.18, label_y, 'Order', transform=ax.get_yaxis_transform(),
            ha='center', va='top', fontsize=13, fontweight='bold')
    ax.text(0, label_y, 'Genus', transform=ax.get_yaxis_transform(),
            ha='left', va='top', fontsize=13, fontweight='bold')
    
    plt.title(title, fontsize=18, fontweight='bold', pad=20)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.3)
    plt.tight_layout()
    
    # Save outputs
    safe_name = group_col.lower()
    for fmt in formats:
        output_file = output_prefix.parent / f"{output_prefix.stem}_summary_{safe_name}.{fmt}"
        if fmt == 'png':
            plt.savefig(output_file, format=fmt, bbox_inches='tight', dpi=dpi)
        else:
            plt.savefig(output_file, format=fmt, bbox_inches='tight')
        print(f"[OK] Saved: {output_file}")
    
    plt.close()


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    ap = argparse.ArgumentParser(
        description="Generate taxonomy bubble plots from ASV count data across depths",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Input/output
    io = ap.add_argument_group("Input/Output")
    io.add_argument("--input", "-i", required=True, type=Path,
                    help="Input TSV file with ASV counts and taxonomy")
    io.add_argument("--output-prefix", "-o", default="bubble_plot_asv",
                    help="Output file prefix")
    io.add_argument("--formats", default="pdf,png,svg",
                    help="Comma-separated output formats (pdf,png,svg)")
    io.add_argument("--dpi", type=int, default=300,
                    help="DPI for raster outputs (PNG)")
    
    # Column names
    cols = ap.add_argument_group("Column Names")
    cols.add_argument("--asv-col", default="ASV_ID",
                      help="Column containing ASV IDs")
    cols.add_argument("--count-col", default="count",
                      help="Column containing count values")
    cols.add_argument("--sample-col", default="sampleID",
                      help="Column containing sample IDs")
    cols.add_argument("--group1-col", dest="depth_col", default="Depth",
                      help="Primary grouping column")
    cols.add_argument("--group2-col", dest="month_col", default="Month",
                      help="Secondary grouping column")
    cols.add_argument("--group1-order", default="",
                      help="Comma-separated order for primary grouping levels.")
    cols.add_argument("--group2-order", default="",
                      help="Comma-separated order for secondary grouping levels.")
    # Backward-compatible aliases
    cols.add_argument("--depth-col", dest="depth_col", help=argparse.SUPPRESS)
    cols.add_argument("--month-col", dest="month_col", help=argparse.SUPPRESS)
    cols.add_argument("--color-col", default="Color",
                      help="Column containing color values for depths")
    cols.add_argument("--order-col", default="Order",
                      help="Column containing Order taxonomy")
    cols.add_argument("--family-col", default="Family",
                      help="Column containing Family taxonomy")
    cols.add_argument("--genus-col", default="Genus",
                      help="Column containing Genus taxonomy")
    cols.add_argument("--stat-col", default=None,
                      help="Optional column for ISA statistics (orders with stat>0 marked with *)")
    
    # Filtering
    filt = ap.add_argument_group("Filtering")
    filt.add_argument("--exclude-genus", "--exclude-genera", dest="exclude_genera",
                      action="append", default=[],
                      help="Genus name(s) to exclude (can be used multiple times)")
    filt.add_argument("--depths", default=None,
                      help="Comma-separated list of depths to plot (default: all depths)")
    filt.add_argument("--unclassified-label", default="Unclassified",
                      help="Label for unclassified taxa")
    
    # Plotting
    plot = ap.add_argument_group("Plotting Options")
    plot.add_argument("--figsize", default="24,24",
                      help="Figure size as 'width,height' in inches (used if auto-size disabled)")
    plot.add_argument("--bubble-scale", type=float, default=50,
                      help="Scaling factor for bubble sizes")
    plot.add_argument("--min-count", type=float, default=1.1,
                      help="Minimum count for transformation")
    plot.add_argument("--no-legend", action="store_true",
                      help="Hide the legend")
    plot.add_argument("--no-auto-size", action="store_true",
                      help="Disable automatic figure size calculation")
    plot.add_argument("--skip-summaries", action="store_true",
                      help="Skip summary plots (Taxonomy × Depth and Taxonomy × Month)")
    
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
    figsize = tuple(map(int, args.figsize.split(',')))
    group1_order = [x.strip() for x in str(args.group1_order).split(',') if x and x.strip()]
    group2_order = [x.strip() for x in str(args.group2_order).split(',') if x and x.strip()]
    
    print(f"[INFO] ASV Taxonomy Bubble Plot Generator")
    print(f"[INFO] Input: {args.input}")
    
    # Load and process data
    df, order_isa_list, depth_palette = load_and_process_data(
        input_file=args.input,
        asv_col=args.asv_col,
        count_col=args.count_col,
        depth_col=args.depth_col,
        color_col=args.color_col,
        order_col=args.order_col,
        family_col=args.family_col,
        genus_col=args.genus_col,
        stat_col=args.stat_col,
        exclude_genera=args.exclude_genera if args.exclude_genera else None,
        unclassified_label=args.unclassified_label,
        sample_col=args.sample_col
    )
    
    print(f"[INFO] Found {len(df[args.asv_col].unique())} unique ASVs")
    print(f"[INFO] Found {len(df[args.depth_col].unique())} unique depths")
    
    # Aggregate data
    grouped_df = aggregate_data(
        df=df,
        asv_col=args.asv_col,
        sample_col=args.sample_col,
        count_col=args.count_col,
        depth_col=args.depth_col,
        order_isa_list=order_isa_list,
        min_count=args.min_count
    )
    
    # Prepare taxonomy ordering
    ofg_list, genera_list, ofg_mapping, genus_mapping = prepare_taxonomy_ordering(
        grouped_df, 
        unclassified_label=args.unclassified_label
    )
    
    # Add numeric mapping for plotting
    grouped_df['ofg_numeric'] = grouped_df['Order_Family_Genus'].map(ofg_mapping)
    
    # Determine depths to plot
    if args.depths:
        depths = [d.strip() for d in args.depths.split(',')]
    else:
        depths = resolve_group_order(grouped_df[args.depth_col].astype(str).tolist(), group1_order) if group1_order else sort_depths(grouped_df[args.depth_col].astype(str).unique())
    
    print(f"[INFO] Plotting depths: {depths}")
    
    # Generate plots for each depth
    output_prefix = Path(args.output_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    
    for depth in depths:
        print(f"\n[INFO] Generating plot for depth: {depth}")
        depth_data = grouped_df[grouped_df[args.depth_col].astype(str) == str(depth)]
        
        if depth_data.empty:
            print(f"[WARN] No data for depth '{depth}', skipping")
            continue
        
        plot_depth_bubble(
            depth_data=depth_data,
            depth_value=depth,
            sample_col=args.sample_col,
            count_col=args.count_col,
            genera_list=genera_list,
            genus_mapping=genus_mapping,
            base_figsize=figsize,
            bubble_scale=args.bubble_scale,
            output_prefix=output_prefix,
            formats=formats,
            dpi=args.dpi,
            show_legend=not args.no_legend,
            auto_size=not args.no_auto_size
        )
    
    # Generate summary plots
    if not args.skip_summaries:
        print("\n" + "="*60)
        print("[INFO] Creating summary plots...")
        print("="*60)
        
        # Summary by Depth
        if args.depth_col in grouped_df.columns:
            print("\n[INFO] Creating Taxonomy × Depth summary...")
            depth_summary = create_summary_data(
                df=grouped_df,
                group_col=args.depth_col,
                count_col=args.count_col,
                min_count=args.min_count
            )
            
            plot_summary_bubble(
                summary_data=depth_summary,
                group_col=args.depth_col,
                count_col=args.count_col,
                genera_list=genera_list,
                ofg_mapping=ofg_mapping,
                bubble_scale=args.bubble_scale,
                output_prefix=output_prefix,
                formats=formats,
                dpi=args.dpi,
                show_legend=not args.no_legend,
                title='Summary: Taxonomy × Depth (Averaged ASV Counts)',
                group_order=group1_order if group1_order else None
            )
        
        # Summary by Month
        if args.month_col in df.columns:
            print("\n[INFO] Creating Taxonomy × Month summary...")
            # Re-aggregate from original df with Month column
            grouped_with_month = df.groupby(
                [args.sample_col, 'Order', 'Family', 'Genus', args.month_col, 'Order_Family_Genus']
            )[args.count_col].sum().reset_index()
            
            # Apply same ISA marking as depth data
            if order_isa_list:
                grouped_with_month['Order'] = grouped_with_month['Order'].apply(
                    lambda o: f'* {o}' if o in order_isa_list else f'  {o}'
                )
            
            # Create month summary
            month_summary = grouped_with_month.groupby(
                [args.month_col, 'Order', 'Family', 'Genus', 'Order_Family_Genus']
            )[args.count_col].mean().reset_index()
            
            print(f"[INFO] Month summary: {len(month_summary)} unique combinations")
            print(f"[INFO] Unique months: {len(month_summary[args.month_col].unique())}")
            
            # Transform counts
            month_summary['transformed_count'] = transform_counts(
                month_summary[args.count_col],
                method='sqrt',
                min_count=args.min_count
            )
            
            plot_summary_bubble(
                summary_data=month_summary,
                group_col=args.month_col,
                count_col=args.count_col,
                genera_list=genera_list,
                ofg_mapping=ofg_mapping,
                bubble_scale=args.bubble_scale,
                output_prefix=output_prefix,
                formats=formats,
                dpi=args.dpi,
                show_legend=not args.no_legend,
                title='Summary: Taxonomy × Month (Averaged ASV Counts)',
                group_order=group2_order if group2_order else None
            )
        else:
            print(f"[INFO] Skipping Month summary (column '{args.month_col}' not found)")
    
    print("\n[OK] All plots generated successfully!")


if __name__ == "__main__":
    main()
