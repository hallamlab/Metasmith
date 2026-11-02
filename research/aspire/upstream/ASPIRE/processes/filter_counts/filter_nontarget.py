#!/usr/bin/env python3
"""
filter_nontarget_asvs.py
------------------------
Filter ASV count table by removing non-target sequences, applying abundance thresholds,
and filtering samples based on group membership (e.g., minimum samples per depth).

Features:
- Remove off-target sequences (mitochondria, contaminants)
- Separate microbial and mitochondrial ASVs
- Apply abundance thresholds
- Filter by taxonomy quality
- Remove samples from groups with insufficient replicates
- Fully configurable via CLI

Example:
--------
python filter_nontarget_asvs.py \
  --count-table ASV_filtered.tsv \
  --nontarget-table nontarget.master.tsv \
  --taxonomy-table ASV_SILVA_tax.tsv \
  --metadata metadata.tsv \
  --output ASV_target.tsv \
  --group-col Depth \
  --min-group-size 3 \
  --abundance-threshold 0.005 \
  --mito-output-dir ../mito/ASVs
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple

import pandas as pd


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Filter ASV count table by removing non-targets and low-abundance ASVs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required inputs
    required = parser.add_argument_group("Required Inputs")
    required.add_argument(
        "--count-table", "-c",
        type=Path,
        required=True,
        help="Input ASV count table (TSV, ASVs as rows, samples as columns)"
    )
    required.add_argument(
        "--nontarget-table", "-n",
        type=Path,
        required=True,
        help="Non-target classification table (TSV, ASVs as rows)"
    )
    required.add_argument(
        "--taxonomy-table", "-t",
        type=Path,
        required=True,
        help="Taxonomy assignment table (TSV, ASVs as rows)"
    )
    
    # Optional inputs
    optional = parser.add_argument_group("Optional Inputs")
    optional.add_argument(
        "--metadata", "-m",
        type=Path,
        default=None,
        help="Sample metadata table (TSV, sampleID as index or first column)"
    )
    
    # Outputs
    output = parser.add_argument_group("Outputs")
    output.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Output filtered count table"
    )
    output.add_argument(
        "--mito-output-dir",
        type=Path,
        default=None,
        help="Directory for mitochondrial ASV output (default: auto-detect from output path)"
    )
    output.add_argument(
        "--save-intermediates",
        action="store_true",
        help="Save intermediate filtered tables (decon, micro, mito)"
    )
    
    # Filtering parameters
    filtering = parser.add_argument_group("Filtering Parameters")
    filtering.add_argument(
        "--abundance-threshold",
        type=float,
        default=0.005,
        help="Minimum relative abundance threshold (percent within a sample; ASV kept if threshold is met in >=1 sample)"
    )
    filtering.add_argument(
        "--min-consensus",
        type=float,
        default=0.0,
        help="Minimum taxonomy consensus score (0.0 = disabled)"
    )
    filtering.add_argument(
        "--exclude-taxon",
        action="append",
        default=[],
        metavar="RANK:VALUE",
        help=(
            "Exclude ASVs whose taxonomy has an exact rank/value match. "
            "May be repeated; accepts ':' or '=' separators, e.g. "
            "'Species:Homo sapiens' or 'Class=Mammalia'."
        )
    )
    filtering.add_argument(
        "--biofactorial-col",
        default="BioFactorial",
        help="Column name for BioFactorial filtering in nontarget table"
    )
    filtering.add_argument(
        "--mito-cols",
        nargs="+",
        default=["MITOMASTER", "BLAST_mito"],
        help="Column names for mitochondrial classification in nontarget table"
    )
    
    # Group filtering
    group = parser.add_argument_group("Group-Based Sample Filtering")
    group.add_argument(
        "--group-col",
        default=None,
        help="Metadata column for grouping samples (e.g., 'Depth', 'Month')"
    )
    group.add_argument(
        "--min-group-size",
        type=int,
        default=1,
        help="Minimum number of samples required per group (groups with fewer samples are removed)"
    )
    
    # Column names
    cols = parser.add_argument_group("Column Names")
    cols.add_argument(
        "--sample-id-col",
        default="sampleID",
        help="Column name for sample IDs in metadata"
    )
    cols.add_argument(
        "--taxon-col",
        default="Taxon",
        help="Column name for taxon assignment in taxonomy table"
    )
    cols.add_argument(
        "--consensus-col",
        default="Consensus",
        help="Column name for consensus score in taxonomy table"
    )
    
    return parser.parse_args()


def load_table(file_path: Path, index_col: int = 0) -> pd.DataFrame:
    """Load TSV table with error handling."""
    if not file_path.exists():
        print(f"[ERROR] File not found: {file_path}")
        sys.exit(1)
    
    try:
        df = pd.read_csv(file_path, sep='\t', header=0, index_col=index_col)
        print(f"[INFO] Loaded {file_path.name}: {df.shape[0]} rows × {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load {file_path}: {e}")
        sys.exit(1)


def clean_index(df: pd.DataFrame) -> pd.DataFrame:
    """Remove semicolon-delimited suffixes from index (e.g., 'ASV_1;size=100' -> 'ASV_1')."""
    df.index = [str(i).split(';', 1)[0] for i in df.index.values]
    return df


def filter_samples_by_group(
    count_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    group_col: str,
    min_group_size: int,
    sample_id_col: str
) -> pd.DataFrame:
    """
    Filter samples to keep only those in groups with sufficient size.
    
    Args:
        count_df: ASV count dataframe (columns = samples)
        metadata_df: Sample metadata
        group_col: Column in metadata to group by
        min_group_size: Minimum samples per group
        sample_id_col: Column name for sample IDs in metadata
    
    Returns:
        Filtered count dataframe
    """
    # Ensure sample_id_col is in index
    if sample_id_col in metadata_df.columns:
        metadata_df = metadata_df.set_index(sample_id_col)
    
    # Check if group_col exists
    if group_col not in metadata_df.columns:
        print(f"[ERROR] Group column '{group_col}' not found in metadata")
        print(f"[INFO] Available columns: {list(metadata_df.columns)}")
        sys.exit(1)
    
    # Count samples per group
    group_counts = metadata_df[group_col].value_counts()
    
    print(f"\n[INFO] Group sizes for '{group_col}':")
    for group, count in group_counts.sort_index().items():
        status = "✓ KEEP" if count >= min_group_size else "✗ REMOVE"
        print(f"  {group}: {count} samples [{status}]")
    
    # Get groups that meet minimum size
    valid_groups = group_counts[group_counts >= min_group_size].index
    
    # Filter metadata to valid groups
    valid_metadata = metadata_df[metadata_df[group_col].isin(valid_groups)]
    valid_samples = valid_metadata.index.tolist()
    
    # Filter count table to keep only valid samples
    available_samples = [s for s in valid_samples if s in count_df.columns]
    removed_samples = set(count_df.columns) - set(available_samples)
    
    if removed_samples:
        print(f"\n[INFO] Removing {len(removed_samples)} samples from groups with < {min_group_size} samples:")
        for sample in sorted(removed_samples):
            if sample in metadata_df.index:
                group = metadata_df.loc[sample, group_col]
                print(f"  - {sample} (from {group_col}={group})")
    
    filtered_df = count_df[available_samples]
    
    print(f"\n[INFO] Samples: {len(count_df.columns)} → {len(filtered_df.columns)} (removed {len(removed_samples)})")
    
    return filtered_df


def filter_nontarget_asvs(
    count_df: pd.DataFrame,
    nontarget_df: pd.DataFrame,
    biofactorial_col: str,
    mito_cols: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Filter ASVs by BioFactorial and separate micro/mito.
    
    Returns:
        Tuple of (decon_count_df, micro_count_df, mito_count_df)
    """
    # Filter by BioFactorial
    if biofactorial_col not in nontarget_df.columns:
        print(f"[ERROR] Column '{biofactorial_col}' not found in nontarget table")
        print(f"[INFO] Available columns: {list(nontarget_df.columns)}")
        sys.exit(1)

    # Align nontarget table to all ASVs in count_df.
    # Missing ASVs are treated as pass (=1) to avoid over-filtering when nontarget annotations are partial.
    status_df = nontarget_df.reindex(count_df.index)
    annotated_n = int(status_df[biofactorial_col].notna().sum())
    coverage = (annotated_n / len(count_df) * 100) if len(count_df) else 0.0
    print(
        f"[INFO] Nontarget coverage: {annotated_n}/{len(count_df)} ASVs "
        f"({coverage:.1f}%) have '{biofactorial_col}' annotations"
    )
    if annotated_n < len(count_df):
        print(
            "[WARN] Nontarget table is partial; ASVs missing from nontarget table "
            "are treated as pass (1)."
        )

    bio_pass = status_df[biofactorial_col].fillna(1).eq(1)
    decon_keep = bio_pass

    # Separate microbial vs mitochondrial within decontaminated ASVs
    mito_mask = pd.Series(False, index=count_df.index)
    for col in mito_cols:
        if col in status_df.columns:
            mito_mask |= status_df[col].fillna(1).eq(0)
        else:
            print(f"[WARN] Mitochondrial column '{col}' not found, skipping")

    micro_keep = decon_keep & (~mito_mask)
    mito_keep = decon_keep & mito_mask

    print(f"[INFO] BioFactorial filtering: {len(count_df)} → {int(decon_keep.sum())} ASVs")
    print(f"[INFO] Separated: {int(micro_keep.sum())} microbial, {int(mito_keep.sum())} mitochondrial ASVs")

    # Filter count tables
    decon_cnt_df = count_df.loc[decon_keep]
    micro_cnt_df = count_df.loc[micro_keep]
    mito_cnt_df = count_df.loc[mito_keep]
    
    return decon_cnt_df, micro_cnt_df, mito_cnt_df


def filter_by_abundance(
    count_df: pd.DataFrame,
    threshold_pct: float
) -> pd.DataFrame:
    """
    Filter ASVs by per-sample relative abundance threshold.
    
    Args:
        count_df: ASV count dataframe
        threshold_pct: Minimum percent within a sample
    
    Returns:
        Filtered count dataframe
    """
    if count_df.empty:
        print(f"[INFO] Abundance filtering skipped: input table is empty")
        return count_df

    sample_totals = count_df.sum(axis=0)
    nonzero_samples = sample_totals > 0

    if not nonzero_samples.any():
        print("[WARN] All sample totals are zero; abundance filtering removed all ASVs")
        return count_df.iloc[0:0]

    per_sample_rel_abund = count_df.loc[:, nonzero_samples].div(
        sample_totals[nonzero_samples], axis=1
    ) * 100
    keep_asvs = per_sample_rel_abund.ge(threshold_pct).any(axis=1)
    filtered_df = count_df.loc[keep_asvs]
    
    removed = len(count_df) - len(filtered_df)
    print(
        f"[INFO] Abundance filtering (>= {threshold_pct}% in >=1 sample): "
        f"{len(count_df)} → {len(filtered_df)} ASVs (removed {removed})"
    )
    
    return filtered_df


TAXONOMY_LEVELS = ["Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"]


def normalize_taxon_value(value) -> str:
    """Normalize taxonomy values for exact, case-insensitive matching."""
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).replace("_", " ").strip().lower().split())


def split_taxa_string(taxa_str: str, delimiter: str = ";") -> Dict[str, str]:
    """Split a SILVA-style taxonomy string into named ranks."""
    if pd.isna(taxa_str) or str(taxa_str).strip() == "":
        parts = ["Unassigned"]
    elif str(taxa_str).strip() == "Unassigned":
        parts = ["Unassigned"]
    else:
        parts = [
            part.strip().split("__", 1)[1] if "__" in part else part.strip()
            for part in str(taxa_str).split(delimiter)
        ]
    return {
        level: (parts[i] if i < len(parts) else "")
        for i, level in enumerate(TAXONOMY_LEVELS)
    }


def parse_exclude_taxa(items: Sequence[str]) -> Dict[str, Set[str]]:
    """Parse RANK:VALUE/RANK=VALUE entries for taxonomy exclusion."""
    rank_lookup = {level.lower(): level for level in TAXONOMY_LEVELS}
    filters: Dict[str, Set[str]] = {}

    for raw in items or []:
        if raw is None or not str(raw).strip():
            continue
        text = str(raw).strip()
        separator = ":" if ":" in text else "=" if "=" in text else None
        if separator is None:
            raise ValueError(f"--exclude-taxon expects RANK:VALUE or RANK=VALUE, got '{raw}'")
        rank, value = text.split(separator, 1)
        rank_key = rank_lookup.get(rank.strip().lower())
        if rank_key is None:
            raise ValueError(
                f"Unknown taxonomy rank '{rank}'. Expected one of: {', '.join(TAXONOMY_LEVELS)}"
            )
        clean_value = normalize_taxon_value(value)
        if not clean_value:
            raise ValueError(f"Missing taxon value in --exclude-taxon entry '{raw}'")
        filters.setdefault(rank_key, set()).add(clean_value)

    return filters


def filter_by_taxonomy(
    count_df: pd.DataFrame,
    tax_df: pd.DataFrame,
    taxon_col: str,
    consensus_col: str,
    min_consensus: float,
    exclude_taxa: Dict[str, Set[str]]
) -> pd.DataFrame:
    """
    Filter ASVs by taxonomy quality.
    
    Args:
        count_df: ASV count dataframe
        tax_df: Taxonomy table
        taxon_col: Column name for taxon assignment
        consensus_col: Column name for consensus score
        min_consensus: Minimum consensus score
    
    Returns:
        Filtered count dataframe
    """
    # Filter taxonomy table
    tax_filter = (tax_df[taxon_col] != 'Unassigned')
    
    if min_consensus > 0 and consensus_col in tax_df.columns:
        tax_filter &= (tax_df[consensus_col] >= min_consensus)
        print(f"[INFO] Taxonomy filter: Taxon != 'Unassigned' AND Consensus >= {min_consensus}")
    else:
        print(f"[INFO] Taxonomy filter: Taxon != 'Unassigned'")
    
    qual_tax_df = tax_df.loc[tax_filter]

    excluded_asvs: Set[str] = set()
    if exclude_taxa:
        for asv_id, taxon in qual_tax_df[taxon_col].items():
            ranks = split_taxa_string(taxon)
            for rank, excluded_values in exclude_taxa.items():
                if normalize_taxon_value(ranks.get(rank, "")) in excluded_values:
                    excluded_asvs.add(asv_id)
                    break

        if excluded_asvs:
            qual_tax_df = qual_tax_df.loc[~qual_tax_df.index.isin(excluded_asvs)]

        rendered = ", ".join(
            f"{rank}={value}"
            for rank, values in exclude_taxa.items()
            for value in sorted(values)
        )
        print(f"[INFO] Explicit taxonomy exclusions: {rendered}")
        print(f"[INFO] Explicit taxonomy exclusion removed {len(excluded_asvs)} annotated ASVs")
    
    annotated_n = int(count_df.index.isin(tax_df.index).sum())
    coverage = (annotated_n / len(count_df) * 100) if len(count_df) else 0.0
    print(f"[INFO] Taxonomy coverage: {annotated_n}/{len(count_df)} ASVs ({coverage:.1f}%)")
    if annotated_n < len(count_df):
        print(
            "[WARN] Taxonomy table is partial; ASVs missing taxonomy are retained "
            "and taxonomy filtering is applied only to annotated ASVs."
        )

    # Apply taxonomy filtering to annotated ASVs; keep unannotated ASVs.
    keep_mask = (~count_df.index.isin(tax_df.index)) | (count_df.index.isin(qual_tax_df.index))
    filtered_df = count_df.loc[keep_mask]

    removed = len(count_df) - len(filtered_df)
    print(f"[INFO] Taxonomy filtering: {len(count_df)} → {len(filtered_df)} ASVs (removed {removed})")
    
    return filtered_df


def save_output(
    df: pd.DataFrame,
    output_path: Path,
    description: str
) -> None:
    """Save dataframe to TSV with status message."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, sep='\t', header=True, index=True)
    print(f"[OK] Saved {description}: {output_path}")


def main():
    """Main execution function."""
    args = parse_args()
    try:
        exclude_taxa = parse_exclude_taxa(args.exclude_taxon)
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
    
    print("="*70)
    print("ASV NON-TARGET FILTERING PIPELINE")
    print("="*70)
    
    # Load input tables
    print("\n[STEP 1] Loading input tables...")
    count_df = load_table(args.count_table)
    #count_df.columns = [x.split('_')[0] + '_' + x.split('_')[1] for x in count_df.columns]
    nontarget_df = load_table(args.nontarget_table)
    tax_df = load_table(args.taxonomy_table)
    
    # Clean indices (remove ;size= etc.)
    nontarget_df = clean_index(nontarget_df)
    tax_df = clean_index(tax_df)
    
    # Load metadata if provided
    metadata_df = None
    if args.metadata:
        metadata_df = load_table(args.metadata, index_col=None)
        print(f"[INFO] Metadata columns: {list(metadata_df.columns)}")
    
    # Filter samples by group size
    if args.group_col and metadata_df is not None:
        print(f"\n[STEP 2] Filtering samples by group size (min={args.min_group_size})...")
        count_df = filter_samples_by_group(
            count_df=count_df,
            metadata_df=metadata_df,
            group_col=args.group_col,
            min_group_size=args.min_group_size,
            sample_id_col=args.sample_id_col
        )
    else:
        print("\n[STEP 2] Skipping group-based sample filtering (no metadata or group-col specified)")
    
    # Filter non-target ASVs
    print(f"\n[STEP 3] Filtering non-target ASVs...")
    decon_cnt_df, micro_cnt_df, mito_cnt_df = filter_nontarget_asvs(
        count_df=count_df,
        nontarget_df=nontarget_df,
        biofactorial_col=args.biofactorial_col,
        mito_cols=args.mito_cols
    )
    
    # Save intermediate decon table
    if args.save_intermediates:
        decon_output = args.output.parent / args.output.name.replace('.tsv', '.decon.tsv')
        save_output(decon_cnt_df, decon_output, "decontaminated ASVs")
    
    # Save mitochondrial ASVs
    if args.mito_output_dir:
        mito_dir = args.mito_output_dir
    else:
        # Auto-detect: go up two levels, then into mito/ASVs
        mito_dir = args.output.parent.parent / 'mito' / 'ASVs'
    
    mito_output = mito_dir / args.output.name.replace('.tsv', '.mito.tsv')
    save_output(mito_cnt_df, mito_output, "mitochondrial ASVs")
    
    # Filter by abundance
    print(f"\n[STEP 4] Filtering by abundance threshold...")
    abund_filter_df = filter_by_abundance(
        count_df=micro_cnt_df,
        threshold_pct=args.abundance_threshold
    )
    
    # Filter by taxonomy
    print(f"\n[STEP 5] Filtering by taxonomy quality...")
    final_df = filter_by_taxonomy(
        count_df=abund_filter_df,
        tax_df=tax_df,
        taxon_col=args.taxon_col,
        consensus_col=args.consensus_col,
        min_consensus=args.min_consensus,
        exclude_taxa=exclude_taxa
    )
    
    # Save final output
    print(f"\n[STEP 6] Saving outputs...")
    
    if args.save_intermediates:
        micro_output = args.output.parent / args.output.name.replace('.tsv', '.micro.tsv')
        save_output(micro_cnt_df, micro_output, "microbial ASVs (before filtering)")
    
    save_output(final_df, args.output, "final filtered ASVs")
    
    # Summary
    print("\n" + "="*70)
    print("FILTERING SUMMARY")
    print("="*70)
    print(f"Input ASVs:              {len(count_df)}")
    print(f"After decontamination:   {len(decon_cnt_df)}")
    print(f"  ├─ Microbial:          {len(micro_cnt_df)}")
    print(f"  └─ Mitochondrial:      {len(mito_cnt_df)}")
    print(f"After abundance filter:  {len(abund_filter_df)}")
    print(f"After taxonomy filter:   {len(final_df)}")
    print(f"\nFinal ASVs:              {len(final_df)} ({100*len(final_df)/len(count_df):.1f}% of input)")
    print(f"Final samples:           {len(final_df.columns)}")
    print("="*70)
    print("[OK] Filtering complete!")


if __name__ == "__main__":
    main()
