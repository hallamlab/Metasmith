#!/usr/bin/env python3
"""
plot_power_curves.py

Create publication-quality power curve visualizations from power analysis results.
Follows ASPIRE plotting style.

Outputs:
- power_curves_cancer_vs_control.pdf/svg - PERMANOVA + Shannon by sample type (full)
- power_curves_cancer_vs_control_simple.pdf/svg - Observed + null only (no artificial inflation)
- power_curves_sample_type.pdf/svg - Sample type comparisons
- power_curves_taxonomic_cancer.pdf/svg - Taxonomic differential abundance (full)
- power_curves_taxonomic_cancer_simple.pdf/svg - Taxonomic observed + null only
- power_curves_taxonomic_stype.pdf/svg - Taxonomic by sample type
- Summary tables with key sample size recommendations

Usage:
------
python plot_power_curves.py \
  --results-dir output/power_results_FINAL \
  --outdir output/power_results_FINAL/figures
"""

import argparse
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings('ignore')

# ----------------------- Matplotlib/Seaborn Style ------------------------
mpl.rcParams['pdf.fonttype'] = 42      # Keep text as text in PDF
mpl.rcParams['svg.fonttype'] = 'none'  # Keep text as text in SVG
mpl.rcParams['savefig.dpi'] = 600
plt.rcParams.update({'font.size': 11})
plt.rcParams['font.family'] = 'Source Sans Pro'
plt.rcParams['axes.linewidth'] = 0.8   # Thinner panel borders
sns.set_theme()
sns.set_style("white")

# Color palettes matching the ASPIRE plotting style
PALETTE_TYPES = {
    'Bronchial Brush':  '#009E73',
    'BAL':         '#0072B2',
    'Oral Rinse':  '#6A3D9A',
}

PALETTE_SCENARIOS = {
    'True_Null': '#999999',      # Gray for null
    'Observed': '#000000',        # Black for observed
    'Weak': '#E69F00',           # Orange
    'Moderate': '#56B4E9',       # Light blue
    'Strong': '#D55E00',         # Red-orange
}

# Line styles
LINE_STYLES = {
    'True_Null': ':',
    'Observed': '-',
    'Weak': '--',
    'Moderate': '-.',
    'Strong': '-',
}


def ensure_dir(p: Path) -> None:
    """Create directory if it doesn't exist."""
    p.mkdir(parents=True, exist_ok=True)


def load_tsv_if_nonempty(path: Path):
    """Load a TSV if it exists and has parseable tabular content."""
    if not path.exists() or path.stat().st_size <= 1:
        return None
    try:
        df = pd.read_csv(path, sep='\t')
    except pd.errors.EmptyDataError:
        return None
    if df.empty and len(df.columns) == 0:
        return None
    return df


def add_power_threshold(ax, threshold=0.8):
    """Add horizontal line at power threshold."""
    ax.axhline(threshold, color='gray', linestyle='--', linewidth=1,
               alpha=0.5, zorder=0)
    ax.text(ax.get_xlim()[1] * 0.98, threshold + 0.02, f'{threshold:.0%} power',
            ha='right', va='bottom', fontsize=9, color='gray')


def add_alpha_threshold(ax, alpha=0.05):
    """Add horizontal line at alpha level (for True_Null)."""
    ax.axhline(alpha, color='red', linestyle=':', linewidth=1,
               alpha=0.4, zorder=0)
    ax.text(ax.get_xlim()[0] * 1.02, alpha + 0.005, f'α={alpha}',
            ha='left', va='bottom', fontsize=8, color='red')


def find_sample_size_for_power(df, target_power=0.8):
    """Find minimum sample size that achieves target power (no interpolation)."""
    if df['Power'].max() < target_power:
        return None

    # Find first sample size that achieves target power
    above = df[df['Power'] >= target_power]

    if len(above) == 0:
        return None

    return int(above['n_cancer'].min())


def plot_cancer_vs_control(results_dir: Path, outdir: Path):
    """
    Plot power curves for Cancer vs Control analyses (PERMANOVA + Shannon).

    Creates multi-panel figure with sample types as columns, analysis type as rows.
    Generates both full version (all scenarios) and simplified version (observed + null only).
    """
    # Load data
    permanova_df = load_tsv_if_nonempty(results_dir / 'permanova_power_stratified.tsv')
    shannon_df = load_tsv_if_nonempty(results_dir / 'shannon_power_stratified.tsv')
    if permanova_df is None or shannon_df is None:
        raise ValueError("Cancer-vs-control power result tables are missing or empty.")

    sample_types = list(dict.fromkeys(
        permanova_df['Sample_type'].dropna().astype(str).tolist()
        + shannon_df['Sample_type'].dropna().astype(str).tolist()
    ))
    if not sample_types:
        raise ValueError("Cancer-vs-control power tables contain no sample types.")
    n_types = len(sample_types)

    # ========== FULL VERSION (all scenarios) ==========
    fig, axes = plt.subplots(2, n_types, figsize=(max(8, 4 * n_types), 7), sharex=True, squeeze=False)

    for col_idx, stype in enumerate(sample_types):
        # PERMANOVA (top row)
        ax_perm = axes[0, col_idx]
        df_perm = permanova_df[permanova_df['Sample_type'] == stype]

        for scenario in ['True_Null', 'Observed', 'Weak', 'Moderate', 'Strong']:
            df_scen = df_perm[df_perm['Scenario'] == scenario]
            if len(df_scen) > 0:
                ax_perm.plot(df_scen['n_cancer'], df_scen['Power'],
                           color=PALETTE_SCENARIOS[scenario],
                           linestyle=LINE_STYLES[scenario],
                           marker='o', markersize=4,
                           linewidth=2, label=scenario)

        add_power_threshold(ax_perm, 0.8)
        add_alpha_threshold(ax_perm, 0.05)
        ax_perm.set_title(f'{stype}', fontsize=12)
        ax_perm.set_ylim(-0.05, 1.05)
        ax_perm.grid(alpha=0.3, linewidth=0.5)

        if col_idx == 0:
            ax_perm.set_ylabel('Power (PERMANOVA, Bray-Curtis)', fontsize=11)

        # Shannon (bottom row)
        ax_shan = axes[1, col_idx]
        df_shan = shannon_df[shannon_df['Sample_type'] == stype]

        for scenario in ['True_Null', 'Observed']:
            df_scen = df_shan[df_shan['Scenario'] == scenario]
            if len(df_scen) > 0:
                ax_shan.plot(df_scen['n_cancer'], df_scen['Power'],
                           color=PALETTE_SCENARIOS[scenario],
                           linestyle=LINE_STYLES[scenario],
                           marker='o', markersize=4,
                           linewidth=2, label=scenario)

        add_power_threshold(ax_shan, 0.8)
        add_alpha_threshold(ax_shan, 0.05)
        ax_shan.set_xlabel('Cancer patients (n)', fontsize=11)
        ax_shan.set_ylim(-0.05, 1.05)
        ax_shan.grid(alpha=0.3, linewidth=0.5)

        if col_idx == 0:
            ax_shan.set_ylabel('Power (Shannon)', fontsize=11)

    # Add legend to top-right subplot
    handles, labels = axes[0, -1].get_legend_handles_labels()
    axes[0, -1].legend(handles, labels, loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True,
                     fontsize=9, title='Scenario')

    plt.suptitle('Power Analysis: Cancer vs Control',
                fontsize=14, y=0.995)
    plt.tight_layout()

    # Save full version
    for ext in ['pdf', 'svg']:
        fig.savefig(outdir / f'power_curves_cancer_vs_control.{ext}',
                   bbox_inches='tight', dpi=600)
    print(f"✓ Saved: power_curves_cancer_vs_control.pdf/svg")
    plt.close()

    # ========== SIMPLIFIED VERSION (observed + null only) ==========
    fig, axes = plt.subplots(2, n_types, figsize=(max(8, 4 * n_types), 7), sharex=True, squeeze=False)

    for col_idx, stype in enumerate(sample_types):
        # PERMANOVA (top row)
        ax_perm = axes[0, col_idx]
        df_perm = permanova_df[permanova_df['Sample_type'] == stype]

        for scenario in ['True_Null', 'Observed']:
            df_scen = df_perm[df_perm['Scenario'] == scenario]
            if len(df_scen) > 0:
                ax_perm.plot(df_scen['n_cancer'], df_scen['Power'],
                           color=PALETTE_SCENARIOS[scenario],
                           linestyle=LINE_STYLES[scenario],
                           marker='o', markersize=4,
                           linewidth=2, label=scenario)

        add_power_threshold(ax_perm, 0.8)
        add_alpha_threshold(ax_perm, 0.05)
        ax_perm.set_title(f'{stype}', fontsize=12)
        ax_perm.set_ylim(-0.05, 1.05)
        ax_perm.grid(alpha=0.3, linewidth=0.5)

        if col_idx == 0:
            ax_perm.set_ylabel('Power (PERMANOVA, Bray-Curtis)', fontsize=11)

        # Shannon (bottom row)
        ax_shan = axes[1, col_idx]
        df_shan = shannon_df[shannon_df['Sample_type'] == stype]

        for scenario in ['True_Null', 'Observed']:
            df_scen = df_shan[df_shan['Scenario'] == scenario]
            if len(df_scen) > 0:
                ax_shan.plot(df_scen['n_cancer'], df_scen['Power'],
                           color=PALETTE_SCENARIOS[scenario],
                           linestyle=LINE_STYLES[scenario],
                           marker='o', markersize=4,
                           linewidth=2, label=scenario)

        add_power_threshold(ax_shan, 0.8)
        add_alpha_threshold(ax_shan, 0.05)
        ax_shan.set_xlabel('Cancer patients (n)', fontsize=11)
        ax_shan.set_ylim(-0.05, 1.05)
        ax_shan.grid(alpha=0.3, linewidth=0.5)

        if col_idx == 0:
            ax_shan.set_ylabel('Power (Shannon)', fontsize=11)

    # Add legend to top-right subplot
    handles, labels = axes[0, -1].get_legend_handles_labels()
    axes[0, -1].legend(handles, labels, loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True,
                     fontsize=9, title='Scenario')

    plt.suptitle('Power Analysis: Cancer vs Control (Observed Effect)',
                fontsize=14, y=0.995)
    plt.tight_layout()

    # Save simplified version
    for ext in ['pdf', 'svg']:
        fig.savefig(outdir / f'power_curves_cancer_vs_control_simple.{ext}',
                   bbox_inches='tight', dpi=600)
    print(f"✓ Saved: power_curves_cancer_vs_control_simple.pdf/svg")
    plt.close()


def plot_sample_type_comparison(results_dir: Path, outdir: Path):
    """
    Plot power curves for Sample Type comparisons (PERMANOVA + Shannon).
    """
    # Load data - handle both old and new file naming conventions
    perm_file = results_dir / 'sample_type_permanova_power.tsv'
    perm_pairwise_file = results_dir / 'sample_type_permanova_power_pairwise.tsv'

    if load_tsv_if_nonempty(perm_pairwise_file) is not None:
        # New format: pairwise comparisons, need to aggregate
        permanova_raw = load_tsv_if_nonempty(perm_pairwise_file)
        comparison_types = list(dict.fromkeys(
            permanova_raw['group1'].dropna().astype(str).tolist()
            + permanova_raw['group2'].dropna().astype(str).tolist()
        ))
        # Compute max power across pairwise comparisons per sample size (any pairwise difference)
        permanova_df = permanova_raw.groupby('n_patients', as_index=False)['power'].max()
    elif load_tsv_if_nonempty(perm_file) is not None:
        # Old format: single power column
        permanova_df = load_tsv_if_nonempty(perm_file)
        comparison_types = []
    else:
        raise ValueError("Sample-type PERMANOVA result tables are missing or empty.")

    shannon_df = load_tsv_if_nonempty(results_dir / 'sample_type_shannon_power.tsv')
    if shannon_df is None:
        raise ValueError("Sample-type Shannon result table is missing or empty.")

    # Create figure: 1 row × 2 cols (PERMANOVA, Shannon)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # PERMANOVA
    ax_perm = axes[0]
    power_col = 'power' if 'power' in permanova_df.columns else 'power_any_difference'
    ax_perm.plot(permanova_df['n_patients'], permanova_df[power_col],
                color='#0072B2', marker='o', markersize=6, linewidth=2.5,
                label='Sample Type Effect')
    add_power_threshold(ax_perm, 0.8)
    ax_perm.set_xlabel('Patients (n)', fontsize=12)
    ax_perm.set_ylabel('Power', fontsize=12)
    comparison_label = " vs ".join(comparison_types) if comparison_types else "Configured sample types"
    ax_perm.set_title(f'PERMANOVA\n({comparison_label})',
                     fontsize=13)
    ax_perm.set_ylim(-0.05, 1.05)
    ax_perm.grid(alpha=0.3, linewidth=0.5)
    ax_perm.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True, fontsize=10)

    # Shannon - single omnibus test (any difference)
    ax_shan = axes[1]
    ax_shan.plot(shannon_df['n_patients'], shannon_df['power_any_difference'],
                color='#6A3D9A', marker='o', markersize=6, linewidth=2.5,
                label='Any Pairwise Difference')

    add_power_threshold(ax_shan, 0.8)
    ax_shan.set_xlabel('Patients (n)', fontsize=12)
    ax_shan.set_ylabel('Power', fontsize=12)
    ax_shan.set_title('Shannon Diversity\n(Any Pairwise Difference)',
                     fontsize=13)
    ax_shan.set_ylim(-0.05, 1.05)
    ax_shan.grid(alpha=0.3, linewidth=0.5)
    ax_shan.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True, fontsize=10)

    plt.suptitle('Power Analysis: Sample Type Comparisons',
                fontsize=14, y=0.98)
    plt.tight_layout()

    # Save
    for ext in ['pdf', 'svg']:
        fig.savefig(outdir / f'power_curves_sample_type.{ext}',
                   bbox_inches='tight', dpi=600)
    print(f"✓ Saved: power_curves_sample_type.pdf/svg")
    plt.close()


def plot_taxonomic_abundance(results_dir: Path, outdir: Path):
    """
    Plot power curves for taxonomic differential abundance analyses.

    Shows phylum and family level separately, stratified by sample type.
    Generates both full version (all scenarios) and simplified version (observed + null only).
    """
    # Load data
    tax_cancer_df = load_tsv_if_nonempty(results_dir / 'taxonomic_abundance_power.tsv')
    tax_stype_df = load_tsv_if_nonempty(results_dir / 'taxonomic_sample_type_power.tsv')
    if tax_cancer_df is None or tax_stype_df is None:
        raise ValueError("Taxonomic power result tables are missing or empty.")

    sample_types = list(dict.fromkeys(tax_cancer_df['sample_type'].dropna().astype(str).tolist()))
    if not sample_types:
        raise ValueError("Taxonomic power table contains no sample types.")
    n_types = len(sample_types)
    tax_levels = ['Phylum', 'Family']

    # ========== FULL VERSION (all scenarios) ==========
    fig, axes = plt.subplots(2, n_types, figsize=(max(8, 4 * n_types), 7), sharex=True, squeeze=False)

    for row_idx, tax_level in enumerate(tax_levels):
        for col_idx, stype in enumerate(sample_types):
            ax = axes[row_idx, col_idx]

            df_subset = tax_cancer_df[
                (tax_cancer_df['tax_level'] == tax_level) &
                (tax_cancer_df['sample_type'] == stype)
            ]

            for scenario in ['True_Null', 'Observed', 'Weak', 'Moderate', 'Strong']:
                df_scen = df_subset[df_subset['scenario'] == scenario]
                if len(df_scen) > 0:
                    ax.plot(df_scen['n_cancer'], df_scen['power'],
                           color=PALETTE_SCENARIOS[scenario],
                           linestyle=LINE_STYLES[scenario],
                           marker='o', markersize=4, linewidth=2,
                           label=scenario)

            add_power_threshold(ax, 0.8)
            add_alpha_threshold(ax, 0.05)

            if row_idx == 0:
                ax.set_title(f'{stype}', fontsize=12)
            if col_idx == 0:
                ax.set_ylabel(f'Power ({tax_level})', fontsize=11)
            if row_idx == 1:
                ax.set_xlabel('Cancer patients (n)', fontsize=11)

            ax.set_ylim(-0.05, 1.05)
            ax.grid(alpha=0.3, linewidth=0.5)

    # Add legend
    handles, labels = axes[0, -1].get_legend_handles_labels()
    axes[0, -1].legend(handles, labels, loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True,
                     fontsize=8, title='Scenario')

    plt.suptitle('Power Analysis: Taxonomic Differential Abundance (Cancer vs Control)',
                fontsize=13, y=0.995)
    plt.tight_layout()

    # Save full version
    for ext in ['pdf', 'svg']:
        fig.savefig(outdir / f'power_curves_taxonomic_cancer.{ext}',
                   bbox_inches='tight', dpi=600)
    print(f"✓ Saved: power_curves_taxonomic_cancer.pdf/svg")
    plt.close()

    # ========== SIMPLIFIED VERSION (observed + null only) ==========
    fig, axes = plt.subplots(2, n_types, figsize=(max(8, 4 * n_types), 7), sharex=True, squeeze=False)

    for row_idx, tax_level in enumerate(tax_levels):
        for col_idx, stype in enumerate(sample_types):
            ax = axes[row_idx, col_idx]

            df_subset = tax_cancer_df[
                (tax_cancer_df['tax_level'] == tax_level) &
                (tax_cancer_df['sample_type'] == stype)
            ]

            for scenario in ['True_Null', 'Observed']:
                df_scen = df_subset[df_subset['scenario'] == scenario]
                if len(df_scen) > 0:
                    ax.plot(df_scen['n_cancer'], df_scen['power'],
                           color=PALETTE_SCENARIOS[scenario],
                           linestyle=LINE_STYLES[scenario],
                           marker='o', markersize=4, linewidth=2,
                           label=scenario)

            add_power_threshold(ax, 0.8)
            add_alpha_threshold(ax, 0.05)

            if row_idx == 0:
                ax.set_title(f'{stype}', fontsize=12)
            if col_idx == 0:
                ax.set_ylabel(f'Power ({tax_level})', fontsize=11)
            if row_idx == 1:
                ax.set_xlabel('Cancer patients (n)', fontsize=11)

            ax.set_ylim(-0.05, 1.05)
            ax.grid(alpha=0.3, linewidth=0.5)

    # Add legend
    handles, labels = axes[0, -1].get_legend_handles_labels()
    axes[0, -1].legend(handles, labels, loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True,
                     fontsize=8, title='Scenario')

    plt.suptitle('Power Analysis: Taxonomic Differential Abundance (Cancer vs Control, Observed Effect)',
                fontsize=13, y=0.995)
    plt.tight_layout()

    # Save simplified version
    for ext in ['pdf', 'svg']:
        fig.savefig(outdir / f'power_curves_taxonomic_cancer_simple.{ext}',
                   bbox_inches='tight', dpi=600)
    print(f"✓ Saved: power_curves_taxonomic_cancer_simple.pdf/svg")
    plt.close()

    # Sample Type Comparison: separate by tax level
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))

    for idx, tax_level in enumerate(tax_levels):
        ax = axes[idx]
        df_subset = tax_stype_df[tax_stype_df['tax_level'] == tax_level]

        ax.plot(df_subset['n_patients'], df_subset['power'],
               color='#0072B2', marker='o', markersize=6, linewidth=2.5,
               label='Sample Type Effect')

        add_power_threshold(ax, 0.8)
        ax.set_xlabel('Patients (n)', fontsize=12)
        ax.set_ylabel('Power', fontsize=12)
        tax_comparisons = list(dict.fromkeys(tax_stype_df.get('comparison', pd.Series(dtype=str)).dropna().astype(str)))
        tax_comparison_label = " / ".join(tax_comparisons) if tax_comparisons else "Configured sample types"
        ax.set_title(f'{tax_level} Level\n({tax_comparison_label})',
                    fontsize=13)
        ax.set_ylim(-0.05, 1.05)
        ax.grid(alpha=0.3, linewidth=0.5)
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), frameon=True, fontsize=10)

    plt.suptitle('Power Analysis: Taxonomic Abundance by Sample Type',
                fontsize=14, y=0.98)
    plt.tight_layout()

    # Save
    for ext in ['pdf', 'svg']:
        fig.savefig(outdir / f'power_curves_taxonomic_stype.{ext}',
                   bbox_inches='tight', dpi=600)
    print(f"✓ Saved: power_curves_taxonomic_stype.pdf/svg")
    plt.close()


def generate_summary_table(results_dir: Path, outdir: Path):
    """
    Generate summary table with sample size recommendations for 80% power.
    """
    # Load data
    permanova_df = load_tsv_if_nonempty(results_dir / 'permanova_power_stratified.tsv')
    shannon_df = load_tsv_if_nonempty(results_dir / 'shannon_power_stratified.tsv')
    if permanova_df is None or shannon_df is None:
        raise ValueError("Cancer-vs-control power result tables are missing or empty.")

    # Load taxonomic data if available
    taxonomic_file = results_dir / 'taxonomic_abundance_power.tsv'
    taxonomic_df = load_tsv_if_nonempty(taxonomic_file)
    if taxonomic_df is not None:
        taxonomic_df = taxonomic_df
    else:
        taxonomic_df = None

    summary_rows = []

    sample_types = list(dict.fromkeys(
        permanova_df['Sample_type'].dropna().astype(str).tolist()
        + shannon_df['Sample_type'].dropna().astype(str).tolist()
    ))

    for stype in sample_types:
        # PERMANOVA - Observed scenario
        df_perm_obs = permanova_df[
            (permanova_df['Sample_type'] == stype) &
            (permanova_df['Scenario'] == 'Observed')
        ].sort_values('n_cancer')

        n_80_perm = find_sample_size_for_power(df_perm_obs, 0.8)
        max_n_perm = int(df_perm_obs['n_cancer'].max())
        current_n_perm = int(df_perm_obs['n_cancer'].max())
        current_power_perm = df_perm_obs[df_perm_obs['n_cancer'] == current_n_perm]['Power'].values
        current_power_perm = current_power_perm[0] if len(current_power_perm) > 0 else np.nan

        # Shannon - Observed scenario
        df_shan_obs = shannon_df[
            (shannon_df['Sample_type'] == stype) &
            (shannon_df['Scenario'] == 'Observed')
        ].sort_values('n_cancer')

        n_80_shan = find_sample_size_for_power(df_shan_obs, 0.8)
        max_n_shan = int(df_shan_obs['n_cancer'].max())
        current_n_shan = int(df_shan_obs['n_cancer'].max())
        current_power_shan = df_shan_obs[df_shan_obs['n_cancer'] == current_n_shan]['Power'].values
        current_power_shan = current_power_shan[0] if len(current_power_shan) > 0 else np.nan

        summary_rows.append({
            'Sample_Type': stype,
            'Analysis': 'PERMANOVA',
            'Current_n': current_n_perm,
            'Current_Power': f'{current_power_perm:.3f}' if not np.isnan(current_power_perm) else 'N/A',
            'n_for_80%_Power': str(n_80_perm) if n_80_perm else f'>{max_n_perm}',
            'Status': 'Adequate' if current_power_perm >= 0.8 else 'Underpowered'
        })

        summary_rows.append({
            'Sample_Type': stype,
            'Analysis': 'Shannon',
            'Current_n': current_n_shan,
            'Current_Power': f'{current_power_shan:.3f}' if not np.isnan(current_power_shan) else 'N/A',
            'n_for_80%_Power': str(n_80_shan) if n_80_shan else f'>{max_n_shan}',
            'Status': 'Adequate' if current_power_shan >= 0.8 else 'Underpowered'
        })

        # Taxonomic abundance - Phylum and Family levels
        if taxonomic_df is not None:
            for tax_level in ['Phylum', 'Family']:
                df_tax_obs = taxonomic_df[
                    (taxonomic_df['sample_type'] == stype) &
                    (taxonomic_df['tax_level'] == tax_level) &
                    (taxonomic_df['scenario'] == 'Observed')
                ].sort_values('n_cancer')

                if not df_tax_obs.empty:
                    n_80_tax = find_sample_size_for_power(df_tax_obs.rename(columns={'power': 'Power'}), 0.8)
                    max_n_tax = int(df_tax_obs['n_cancer'].max())
                    current_n_tax = int(df_tax_obs['n_cancer'].max())
                    current_power_tax = df_tax_obs[df_tax_obs['n_cancer'] == current_n_tax]['power'].values
                    current_power_tax = current_power_tax[0] if len(current_power_tax) > 0 else np.nan

                    summary_rows.append({
                        'Sample_Type': stype,
                        'Analysis': f'Taxonomic ({tax_level})',
                        'Current_n': current_n_tax,
                        'Current_Power': f'{current_power_tax:.3f}' if not np.isnan(current_power_tax) else 'N/A',
                        'n_for_80%_Power': str(n_80_tax) if n_80_tax else f'>{max_n_tax}',
                        'Status': 'Adequate' if current_power_tax >= 0.8 else 'Underpowered'
                    })

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(outdir / 'power_summary_cancer_vs_control.tsv',
                      sep='\t', index=False)

    print("\n" + "="*60)
    print("Power Summary: Cancer vs Control (Observed Effect)")
    print("="*60)
    print(summary_df.to_string(index=False))
    print(f"\n✓ Saved: power_summary_cancer_vs_control.tsv")


def main():
    parser = argparse.ArgumentParser(
        description="Generate power curve visualizations"
    )
    parser.add_argument('--results-dir', type=Path, required=True,
                       help='Directory with power analysis TSV results')
    parser.add_argument('--outdir', type=Path, required=True,
                       help='Output directory for figures')
    args = parser.parse_args()

    ensure_dir(args.outdir)

    print("="*60)
    print("Generating Power Curve Visualizations")
    print("="*60)

    # Generate plots
    print("\n1. Cancer vs Control (PERMANOVA + Shannon)...")
    if load_tsv_if_nonempty(args.results_dir / 'permanova_power_stratified.tsv') is not None and \
       load_tsv_if_nonempty(args.results_dir / 'shannon_power_stratified.tsv') is not None:
        try:
            plot_cancer_vs_control(args.results_dir, args.outdir)
        except ValueError as exc:
            print(f"  ⊘ Skipped: {exc}")
    else:
        print("  ⊘ Skipped: cancer-vs-control power result tables missing or empty")

    print("\n2. Sample Type Comparisons...")
    perm_file = args.results_dir / 'sample_type_permanova_power.tsv'
    perm_pairwise = args.results_dir / 'sample_type_permanova_power_pairwise.tsv'
    if load_tsv_if_nonempty(perm_file) is not None or load_tsv_if_nonempty(perm_pairwise) is not None:
        try:
            plot_sample_type_comparison(args.results_dir, args.outdir)
        except ValueError as exc:
            print(f"  ⊘ Skipped: {exc}")
    else:
        print("  ⊘ Skipped: sample type power result tables missing or empty")

    print("\n3. Taxonomic Differential Abundance...")
    if load_tsv_if_nonempty(args.results_dir / 'taxonomic_abundance_power.tsv') is not None:
        try:
            plot_taxonomic_abundance(args.results_dir, args.outdir)
        except ValueError as exc:
            print(f"  ⊘ Skipped: {exc}")
    else:
        print("  ⊘ Skipped: taxonomic power result tables missing or empty")

    print("\n4. Summary Table...")
    if load_tsv_if_nonempty(args.results_dir / 'permanova_power_stratified.tsv') is not None and \
       load_tsv_if_nonempty(args.results_dir / 'shannon_power_stratified.tsv') is not None:
        try:
            generate_summary_table(args.results_dir, args.outdir)
        except ValueError as exc:
            print(f"  ⊘ Skipped: {exc}")
    else:
        print("  ⊘ Skipped: cancer-vs-control summary inputs missing or empty")

    print("\n" + "="*60)
    print("✓ All visualizations complete!")
    print(f"Output directory: {args.outdir}")
    print("="*60)


if __name__ == '__main__':
    main()
