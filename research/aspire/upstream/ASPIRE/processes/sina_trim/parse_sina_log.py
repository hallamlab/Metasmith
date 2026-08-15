#!/usr/bin/env python3
"""
parse_sina_log.py

Parse SINA alignment log files to extract alignment positions, quality metrics,
and identify 16S rRNA variable regions covered by each ASV.

Features:
- Extract alignment start/end positions from SINA logs
- Identify V-regions (V1-V9) covered by each sequence
- Calculate alignment quality metrics
- Output results as TSV with optional summary statistics

Example:
--------
python parse_sina_log.py \
  --log sina_alignment.log \
  --output asv_v_regions.tsv \
  --verbose

Custom V-region boundaries:
python parse_sina_log.py \
  --log sina_alignment.log \
  --output asv_v_regions.tsv \
  --v-regions "V3-V4:6388-25319,V4:13862-23444" \
  --verbose
"""

from __future__ import annotations
import argparse
import sys
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd

# Default SILVA V-region boundaries (based on E. coli/SILVA alignment positions)
DEFAULT_V_REGIONS = {
    'V1': (1044, 1423),
    'V2': (2573, 6423),
    'V3': (6388, 10459),
    'V4': (13862, 23444),
    'V5': (23444, 27659),
    'V6': (28180, 28986),
    'V7': (30534, 32202),
    'V8': (35380, 36188),
    'V9': (38097, 38551),
    # Common combined regions
    'V1-V2': (1044, 6423),
    'V1-V3': (1044, 10459),
    'V3-V4': (6388, 25319),
    'V3-V5': (6388, 27659),
    'V4-V5': (13862, 27659),
    'V6-V8': (28180, 36188),
    'V1-V9': (1044, 43116),  # Full-length
}


def parse_v_region_string(v_region_str: str) -> Dict[str, Tuple[int, int]]:
    """
    Parse V-region definitions from string.
    
    Format: "V3-V4:6388-25319,V4:13862-23444"
    
    Args:
        v_region_str: Comma-separated region definitions
    
    Returns:
        Dictionary mapping region names to (start, end) tuples
    """
    regions = {}
    
    for item in v_region_str.split(','):
        item = item.strip()
        if not item:
            continue
        
        if ':' not in item or '-' not in item.split(':')[1]:
            raise ValueError(f"Invalid V-region format: {item}. Expected 'Name:start-end'")
        
        name, positions = item.split(':', 1)
        start, end = positions.split('-', 1)
        
        try:
            regions[name.strip()] = (int(start.strip()), int(end.strip()))
        except ValueError as e:
            raise ValueError(f"Invalid V-region positions in '{item}': {e}")
    
    return regions


def identify_v_regions(start: int, end: int, 
                       v_regions: Dict[str, Tuple[int, int]],
                       tolerance: int = 100,
                       min_coverage: float = 0.8) -> Tuple[str, List[str]]:
    """
    Identify which V-regions are covered by an alignment.
    
    Args:
        start: Alignment start position
        end: Alignment end position
        v_regions: Dictionary of V-region boundaries
        tolerance: Position tolerance for exact matches (bp)
        min_coverage: Minimum fraction of region that must be covered
    
    Returns:
        Tuple of (primary_region, list_of_covered_regions)
    """
    if not start or not end:
        return 'Unknown', []
    
    # First, try exact matching with tolerance (for combined regions like V3-V4)
    best_match = None
    best_score = float('inf')
    
    for region_name, (r_start, r_end) in v_regions.items():
        start_diff = abs(start - r_start)
        end_diff = abs(end - r_end)
        total_diff = start_diff + end_diff
        
        if start_diff < tolerance and end_diff < tolerance:
            if total_diff < best_score:
                best_score = total_diff
                best_match = region_name
    
    if best_match:
        return best_match, [best_match]
    
    # If no exact match, identify individual regions covered
    individual_regions = ['V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9']
    covered = []
    
    for region in individual_regions:
        if region not in v_regions:
            continue
        
        r_start, r_end = v_regions[region]
        
        # Calculate overlap
        overlap_start = max(start, r_start)
        overlap_end = min(end, r_end)
        
        if overlap_end > overlap_start:
            overlap_len = overlap_end - overlap_start
            region_len = r_end - r_start
            coverage = overlap_len / region_len
            
            if coverage >= min_coverage:
                covered.append(region)
    
    if len(covered) >= 2:
        primary = f"{covered[0]}-{covered[-1]}"
        return primary, covered
    elif len(covered) == 1:
        return covered[0], covered
    else:
        return f'Partial ({start}-{end})', []


def parse_sina_log(log_path: Path, v_regions: Dict[str, Tuple[int, int]],
                   tolerance: int = 100, min_coverage: float = 0.8,
                   verbose: bool = False) -> List[Dict]:
    """
    Parse SINA log file to extract alignment information.
    """
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")
    
    results = []
    current_asv = {}
    line_count = 0
    asv_count = 0
    
    if verbose:
        print(f"[INFO] Parsing log file: {log_path}")
    
    with open(log_path, 'r') as f:
        for line in f:
            line_count += 1
            original_line = line.strip()
            
            # Skip empty lines
            if not original_line:
                continue
            
            # Remove timestamp and log prefixes
            # Format: "[2025-10-26 02:57:20.056] [log] [info] field: value"
            line = original_line
            
            # Remove timestamp: [YYYY-MM-DD HH:MM:SS.mmm]
            line = re.sub(r'^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\]', '', line).strip()
            
            # Remove [log] and [info]/[warn]/[error] tags
            line = re.sub(r'^\[log\]\s*', '', line).strip()
            line = re.sub(r'^\[(info|warn|error|debug)\]\s*', '', line).strip()
            
            # New sequence identifier
            if line.startswith('sequence_identifier:'):
                # Save previous ASV if it exists
                if current_asv:
                    # Add V-region info before saving
                    if 'align_start' in current_asv and 'align_end' in current_asv:
                        primary, covered = identify_v_regions(
                            current_asv['align_start'],
                            current_asv['align_end'],
                            v_regions,
                            tolerance,
                            min_coverage
                        )
                        current_asv['v_region'] = primary
                        current_asv['covered_regions'] = ','.join(covered) if covered else ''
                        current_asv['span'] = current_asv['align_end'] - current_asv['align_start'] + 1
                    else:
                        current_asv['v_region'] = 'Unknown'
                        current_asv['covered_regions'] = ''
                        current_asv['span'] = 0
                    
                    results.append(current_asv.copy())
                    asv_count += 1
                    
                    # Progress indicator
                    if verbose and asv_count % 10000 == 0:
                        print(f"[INFO]   Processed {asv_count:,} ASVs...")
                
                # Start new ASV
                asv_id = line.split('sequence_identifier:', 1)[1].strip()
                # Remove size annotation if present
                asv_id = asv_id.split(';')[0]
                current_asv = {'ASV_ID': asv_id}
            
            # Extract fields
            elif line.startswith('align_startpos_slv:'):
                try:
                    value = line.split(':', 1)[1].strip()
                    current_asv['align_start'] = int(value)
                except (ValueError, IndexError) as e:
                    if verbose:
                        print(f"[WARN] Could not parse align_startpos from: {original_line}")
            
            elif line.startswith('align_stoppos_slv:'):
                try:
                    value = line.split(':', 1)[1].strip()
                    current_asv['align_end'] = int(value)
                except (ValueError, IndexError) as e:
                    if verbose:
                        print(f"[WARN] Could not parse align_stoppos from: {original_line}")
            
            elif line.startswith('nuc:') and 'nuc_' not in line:
                try:
                    value = line.split(':', 1)[1].strip()
                    current_asv['seq_length'] = int(value)
                except (ValueError, IndexError):
                    pass
            
            elif line.startswith('align_quality_slv:'):
                try:
                    value = line.split(':', 1)[1].strip()
                    current_asv['align_quality'] = int(value)
                except (ValueError, IndexError):
                    pass
            
            elif line.startswith('align_bp_score_slv:'):
                try:
                    value = line.split(':', 1)[1].strip()
                    current_asv['align_bp_score'] = int(value)
                except (ValueError, IndexError):
                    pass
            
            elif line.startswith('sequence_number:'):
                try:
                    value = line.split(':', 1)[1].strip()
                    current_asv['sequence_number'] = int(value)
                except (ValueError, IndexError):
                    pass
    
    # Don't forget the last ASV
    if current_asv:
        if 'align_start' in current_asv and 'align_end' in current_asv:
            primary, covered = identify_v_regions(
                current_asv['align_start'],
                current_asv['align_end'],
                v_regions,
                tolerance,
                min_coverage
            )
            current_asv['v_region'] = primary
            current_asv['covered_regions'] = ','.join(covered) if covered else ''
            current_asv['span'] = current_asv['align_end'] - current_asv['align_start'] + 1
        else:
            current_asv['v_region'] = 'Unknown'
            current_asv['covered_regions'] = ''
            current_asv['span'] = 0
        
        results.append(current_asv)
        asv_count += 1
    
    if verbose:
        print(f"[INFO] Processed {line_count:,} lines")
        print(f"[INFO] Found {asv_count:,} ASVs")
        
        # Debug: Check how many have position data
        with_positions = sum(1 for r in results if 'align_start' in r and 'align_end' in r)
        with_quality = sum(1 for r in results if 'align_quality' in r)
        with_length = sum(1 for r in results if 'seq_length' in r)
        
        print(f"[INFO] ASVs with position data: {with_positions:,} ({with_positions/asv_count*100:.1f}%)")
        print(f"[INFO] ASVs with quality data: {with_quality:,} ({with_quality/asv_count*100:.1f}%)")
        print(f"[INFO] ASVs with length data: {with_length:,} ({with_length/asv_count*100:.1f}%)")
        
        if with_positions == 0:
            print(f"[WARN] No position data found! Check log format.")
    
    return results


def save_results(results: List[Dict], output_path: Path, verbose: bool = False) -> None:
    """Save results to TSV file, including all ASVs even with missing data."""
    df = pd.DataFrame(results)
    
    # Define column order with defaults
    col_order = [
        'ASV_ID', 'align_start', 'align_end', 'span', 'seq_length',
        'v_region', 'covered_regions', 'align_quality', 'align_bp_score',
        'sequence_number'
    ]
    
    # Ensure all expected columns exist (fill with NA for missing)
    for col in col_order:
        if col not in df.columns:
            if col == 'v_region':
                df[col] = 'Unknown'
            elif col == 'covered_regions':
                df[col] = ''
            else:
                df[col] = pd.NA
    
    # Keep columns in order
    df = df[col_order]
    
    # Sort by ASV_ID (natural sort if possible)
    try:
        # Try numeric sort on ASV numbers
        df['_sort_key'] = df['ASV_ID'].str.extract(r'(\d+)').astype(float)
        df = df.sort_values('_sort_key').drop(columns=['_sort_key'])
    except (ValueError, KeyError, TypeError):
        # Fall back to string sort
        df = df.sort_values('ASV_ID')
    
    # Save - use empty string for NA in string columns
    df.to_csv(output_path, sep='\t', index=False, na_rep='')
    
    if verbose:
        print(f"[INFO] Saved results to: {output_path}")
        print(f"[INFO] Output shape: {df.shape[0]} rows × {df.shape[1]} columns")
        
        # Report on data completeness
        n_unknown = (df['v_region'] == 'Unknown').sum()
        n_with_positions = df['align_start'].notna().sum()
        
        print(f"[INFO] ASVs with V-region classification: {df.shape[0] - n_unknown:,}")
        print(f"[INFO] ASVs marked as Unknown: {n_unknown:,}")
        print(f"[INFO] ASVs with alignment positions: {n_with_positions:,}")


def print_summary(results: List[Dict], v_regions: Dict[str, Tuple[int, int]]) -> None:
    """Print summary statistics."""
    df = pd.DataFrame(results)
    
    print(f"\n{'='*70}")
    print(f"SUMMARY STATISTICS")
    print(f"{'='*70}\n")
    
    print(f"Total ASVs: {len(df):,}")
    
    # Count unknowns
    n_unknown = sum(1 for r in results if r.get('v_region') == 'Unknown')
    n_classified = len(df) - n_unknown
    print(f"  Classified:  {n_classified:,} ({n_classified/len(df)*100:.1f}%)")
    print(f"  Unknown:     {n_unknown:,} ({n_unknown/len(df)*100:.1f}%)\n")
    
    # Sequence length statistics (only for those with data)
    if 'seq_length' in df.columns:
        seq_data = df['seq_length'].dropna()
        if len(seq_data) > 0:
            print("Sequence Length (for ASVs with data):")
            stats = seq_data.describe()
            print(f"  Mean:   {stats['mean']:7.1f} bp")
            print(f"  Median: {stats['50%']:7.1f} bp")
            print(f"  Min:    {stats['min']:7.0f} bp")
            print(f"  Max:    {stats['max']:7.0f} bp")
            print(f"  N:      {len(seq_data):,}")
            print()
    
    # Alignment quality (only for those with data)
    if 'align_quality' in df.columns:
        qual_data = df['align_quality'].dropna()
        if len(qual_data) > 0:
            print("Alignment Quality (for ASVs with data):")
            stats = qual_data.describe()
            print(f"  Mean:   {stats['mean']:6.1f}")
            print(f"  Median: {stats['50%']:6.1f}")
            print(f"  Min:    {stats['min']:6.0f}")
            print(f"  Max:    {stats['max']:6.0f}")
            print(f"  N:      {len(qual_data):,}")
            print()
    
    # Alignment position ranges (only for those with data)
    if 'align_start' in df.columns and 'align_end' in df.columns:
        pos_data = df.dropna(subset=['align_start', 'align_end'])
        if len(pos_data) > 0:
            print("Alignment Positions (for ASVs with data):")
            print(f"  Start: {pos_data['align_start'].min():,.0f} - {pos_data['align_start'].max():,.0f}")
            print(f"  End:   {pos_data['align_end'].min():,.0f} - {pos_data['align_end'].max():,.0f}")
            print(f"  N:     {len(pos_data):,}")
            print()
    
    # V-region distribution (including Unknown)
    if 'v_region' in df.columns:
        print("V-Region Distribution:")
        region_counts = df['v_region'].value_counts()
        
        # Sort: known regions first, then Unknown
        known_regions = [(r, c) for r, c in region_counts.items() if r != 'Unknown']
        unknown_count = region_counts.get('Unknown', 0)
        
        # Sort known regions
        known_regions.sort(key=lambda x: x[1], reverse=True)
        
        for region, count in known_regions:
            pct = (count / len(df)) * 100
            
            # Add position info if available
            region_info = ""
            if region in v_regions:
                start, end = v_regions[region]
                region_info = f" ({start:,}-{end:,})"
            
            print(f"  {region:20s}{region_info:25s}: {count:6,} ({pct:5.1f}%)")
        
        # Show Unknown separately
        if unknown_count > 0:
            pct = (unknown_count / len(df)) * 100
            print(f"  {'Unknown':20s}{'(no position data)':25s}: {unknown_count:6,} ({pct:5.1f}%)")
        print()
    
    # Examples - show both classified and unknown
    print("Example ASVs:")
    cols = ['ASV_ID', 'align_start', 'align_end', 'seq_length', 'v_region']
    display_cols = [c for c in cols if c in df.columns]
    
    print("\n  Classified (first 5):")
    classified = df[df['v_region'] != 'Unknown']
    if len(classified) > 0:
        print(classified[display_cols].head(5).to_string(index=False, na_rep='NA'))
    else:
        print("  (none)")
    
    print("\n  Unknown (first 5):")
    unknown = df[df['v_region'] == 'Unknown']
    if len(unknown) > 0:
        print(unknown[display_cols].head(5).to_string(index=False, na_rep='NA'))
    else:
        print("  (none)")
    
    print(f"\n{'='*70}\n")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Parse SINA alignment logs to identify 16S V-regions",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required arguments
    required = parser.add_argument_group("Required Arguments")
    required.add_argument(
        "--log", "-l", required=True, type=Path,
        help="SINA log file (created with --log-file)"
    )
    required.add_argument(
        "--output", "-o", required=True, type=Path,
        help="Output TSV file"
    )
    
    # V-region definitions
    regions = parser.add_argument_group("V-Region Configuration")
    regions.add_argument(
        "--v-regions", type=str,
        help="Custom V-region boundaries (format: 'V3-V4:6388-25319,V4:13862-23444')"
    )
    regions.add_argument(
        "--tolerance", type=int, default=100,
        help="Position tolerance for exact region matching (bp)"
    )
    regions.add_argument(
        "--min-coverage", type=float, default=0.8,
        help="Minimum fraction of region that must be covered (0.0-1.0)"
    )
    
    # Output options
    output_opts = parser.add_argument_group("Output Options")
    output_opts.add_argument(
        "--no-summary", action="store_true",
        help="Skip printing summary statistics"
    )
    output_opts.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose output"
    )
    
    return parser.parse_args()


def main():
    """Main execution."""
    args = parse_args()
    
    # Validate inputs
    if not args.log.exists():
        print(f"[ERROR] Log file not found: {args.log}", file=sys.stderr)
        sys.exit(1)
    
    if args.min_coverage < 0 or args.min_coverage > 1:
        print(f"[ERROR] --min-coverage must be between 0 and 1", file=sys.stderr)
        sys.exit(1)
    
    # Parse V-regions
    if args.v_regions:
        try:
            v_regions = parse_v_region_string(args.v_regions)
            if args.verbose:
                print(f"[INFO] Using custom V-region definitions:")
                for name, (start, end) in sorted(v_regions.items()):
                    print(f"  {name:15s}: {start:6,} - {end:6,}")
        except ValueError as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            sys.exit(1)
    else:
        v_regions = DEFAULT_V_REGIONS
        if args.verbose:
            print(f"[INFO] Using default SILVA V-region boundaries")
    
    # Parse log file
    try:
        results = parse_sina_log(
            args.log,
            v_regions,
            tolerance=args.tolerance,
            min_coverage=args.min_coverage,
            verbose=args.verbose
        )
    except Exception as e:
        print(f"[ERROR] Failed to parse log file: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    if not results:
        print(f"[ERROR] No ASVs found in log file", file=sys.stderr)
        sys.exit(1)
    
    # Save results
    try:
        save_results(results, args.output, verbose=args.verbose)
    except Exception as e:
        print(f"[ERROR] Failed to save results: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    # Print summary
    if not args.no_summary:
        print_summary(results, v_regions)
    
    if args.verbose:
        print("[OK] Analysis complete!")


if __name__ == "__main__":
    main()