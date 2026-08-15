#!/usr/bin/env python3
"""
Trim aligned sequences to specific V-regions based on SINA alignment metadata.
Memory-optimized version with batch processing and streaming output.
"""

import argparse
import pandas as pd
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import multiprocessing
from itertools import islice


def parse_args():
    parser = argparse.ArgumentParser(
        description='Trim aligned sequences to specific V-regions based on SINA metadata',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Keep only V4 sequences, trimmed to V4 region
  python trim_v_regions.py -m metadata.tsv -f aligned.fasta -r V4 -o trimmed_V4.fasta
  
  # Keep V4 and V4-V5 sequences, trim both to V4 coordinates
  python trim_v_regions.py -m metadata.tsv -f aligned.fasta -r V4,V4-V5 -t V4 -o trimmed_V4.fasta
  
  # Keep V4 and V4-V5 sequences, trim to union of all coordinates
  python trim_v_regions.py -m metadata.tsv -f aligned.fasta -r V4,V4-V5 -o trimmed_union.fasta
        """
    )
    parser.add_argument('-m', '--metadata', required=True,
                        help='SINA metadata table (TSV/CSV with align_start, align_end, v_region columns)')
    parser.add_argument('-f', '--fasta', required=True,
                        help='Aligned FASTA file from SINA (with gaps)')
    parser.add_argument('-r', '--regions', required=True,
                        help='Comma-separated V-regions to keep (e.g., "V4" or "V4,V4-V5")')
    parser.add_argument('-t', '--trim-to', default=None,
                        help='Specific region to trim to (e.g., "V4"). If not specified, uses union of all selected regions.')
    parser.add_argument('-o', '--output', required=True,
                        help='Output FASTA file (degapped, trimmed sequences)')
    parser.add_argument('--id-column', default='ASV_ID',
                        help='Column name for sequence IDs (default: ASV_ID)')
    parser.add_argument('--keep-gaps', action='store_true',
                        help='Keep gaps in output sequences (default: remove gaps)')
    parser.add_argument('--threads', type=int, default=None,
                        help='Number of threads for parallel processing (default: number of CPUs)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of sequences to process per batch (default: 1000, lower = less RAM)')
    
    return parser.parse_args()


def load_metadata(metadata_file, id_column):
    """Load SINA metadata table."""
    # Try to detect delimiter
    with open(metadata_file) as f:
        first_line = f.readline()
        delimiter = '\t' if '\t' in first_line else ','
    
    df = pd.read_csv(metadata_file, sep=delimiter)
    
    # Check required columns
    required_cols = [id_column, 'align_start', 'align_end', 'v_region']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    return df


def filter_sequences(df, target_regions, id_column):
    """Filter sequences to those containing target V-regions."""
    target_list = [r.strip() for r in target_regions.split(',')]
    
    # Filter to sequences with target regions
    mask = df['v_region'].isin(target_list)
    filtered_df = df[mask].copy()
    
    if len(filtered_df) == 0:
        raise ValueError(f"No sequences found with regions: {target_list}")
    
    print(f"Found {len(filtered_df)} sequences with regions: {target_list}")
    print(f"Region breakdown:")
    print(filtered_df['v_region'].value_counts())
    
    return filtered_df, target_list


def get_trim_coordinates(filtered_df, trim_to=None):
    """Get alignment coordinates for trimming."""
    # If trim_to is specified, use only sequences from that region
    if trim_to:
        trim_df = filtered_df[filtered_df['v_region'] == trim_to]
        if len(trim_df) == 0:
            raise ValueError(f"No sequences found with region '{trim_to}' to use for trimming coordinates")
        print(f"\nUsing {len(trim_df)} sequences from region '{trim_to}' to determine trim coordinates")
    else:
        trim_df = filtered_df
        print(f"\nUsing all {len(trim_df)} selected sequences to determine trim coordinates")
    
    # Find the range that covers all selected sequences
    min_start = trim_df['align_start'].min()
    max_end = trim_df['align_end'].max()
    
    print(f"Alignment coordinate range:")
    print(f"  Start: {min_start}")
    print(f"  End: {max_end}")
    print(f"  Span: {max_end - min_start}")
    
    return int(min_start), int(max_end)


def trim_aligned_sequence(aligned_seq, start_pos, end_pos):
    """
    Trim an aligned sequence to specific alignment coordinates.
    
    aligned_seq: string with gaps
    start_pos: alignment start position (1-based, SILVA coordinates)
    end_pos: alignment end position (1-based, SILVA coordinates)
    """
    # SILVA coordinates are 1-based, convert to 0-based for Python
    # The alignment string position corresponds to SILVA position + 1
    # We need to find which positions in the aligned string correspond to our range
    
    # Count non-gap positions to find where start_pos and end_pos fall
    position = 0  # Current SILVA position (0-based)
    start_idx = None
    end_idx = None
    
    for i, base in enumerate(aligned_seq):
        if base != '-':
            position += 1
        
        if position == start_pos and start_idx is None:
            start_idx = i
        
        if position == end_pos:
            end_idx = i + 1  # +1 for Python slicing
            break
    
    if start_idx is None:
        start_idx = 0
    if end_idx is None:
        end_idx = len(aligned_seq)
    
    return aligned_seq[start_idx:end_idx]


def process_single_sequence(record, keep_ids, start_pos, end_pos, keep_gaps):
    """Process a single sequence record."""
    seq_id = record.id
    sub_id = record.id.rsplit(';', 1)[0]
    if sub_id not in keep_ids:
        return None
    
    # Trim to alignment coordinates
    aligned_seq = str(record.seq)
    trimmed_aligned = trim_aligned_sequence(aligned_seq, start_pos, end_pos)
    
    # Remove gaps unless user wants to keep them
    if keep_gaps:
        final_seq = trimmed_aligned
    else:
        final_seq = trimmed_aligned.replace('-', '')
    
    # Convert any RNA (U) to DNA (T)
    final_seq = final_seq.replace('U', 'T').replace('u', 't')
    # Create new record
    new_record = SeqRecord(
        Seq(final_seq),
        id=seq_id,
        description=record.description
    )
    # Force DNA molecule type
    new_record.annotations["molecule_type"] = "DNA"
    
    return new_record


def batch_iterator(iterator, batch_size):
    """Yield successive batches from an iterator."""
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def process_sequences_streaming(fasta_file, filtered_df, start_pos, end_pos, 
                                id_column, keep_gaps, num_threads, batch_size, 
                                output_file):
    """Process and write sequences in batches with streaming I/O."""
    # Get set of IDs to keep
    keep_ids = set(filtered_df[id_column])
    
    if num_threads is None:
        num_threads = multiprocessing.cpu_count()
    
    print(f"\nProcessing sequences from {fasta_file}...")
    print(f"Using {num_threads} threads")
    print(f"Batch size: {batch_size} sequences")
    print(f"Memory-efficient mode: streaming input and output\n")
    
    # Statistics
    total_processed = 0
    total_written = 0
    found_ids = set()
    lengths = []
    
    # Open output file
    with open(output_file, 'w') as out_handle:
        # Process in batches
        seq_parser = SeqIO.parse(fasta_file, 'fasta')
        
        # Use tqdm to show overall progress
        with tqdm(desc="Processing", unit="seq") as pbar:
            for batch in batch_iterator(seq_parser, batch_size):
                total_processed += len(batch)
                
                # Process batch in parallel
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    # Submit batch
                    futures = {
                        executor.submit(process_single_sequence, record, keep_ids, 
                                      start_pos, end_pos, keep_gaps): record
                        for record in batch
                    }
                    
                    # Write results as they complete
                    for future in as_completed(futures):
                        result = future.result()
                        if result is not None:
                            SeqIO.write(result, out_handle, 'fasta')
                            found_ids.add(result.id)
                            lengths.append(len(result.seq))
                            total_written += 1
                        
                        pbar.update(1)
                
                # Explicitly clear batch to free memory
                batch.clear()
    
    # Report results
    print(f"\nProcessed {total_processed} total sequences")
    print(f"Wrote {total_written} sequences to {output_file}")
    
    # Check for missing sequences
    '''
    missing = keep_ids - found_ids
    if missing:
        print(f"\nWARNING: {len(missing)} sequences from metadata not found in FASTA:")
        for seq_id in list(missing)[:5]:
            print(f"  {seq_id}")
        if len(missing) > 5:
            print(f"  ... and {len(missing) - 5} more")
    '''
    
    # Print length statistics
    if lengths:
        print(f"\nSequence length statistics:")
        print(f"  Min: {min(lengths)}")
        print(f"  Max: {max(lengths)}")
        print(f"  Mean: {sum(lengths)/len(lengths):.1f}")
    
    return total_written


def main():
    args = parse_args()
    
    try:
        # Load metadata
        print(f"Loading metadata from {args.metadata}...")
        df = load_metadata(args.metadata, args.id_column)
        print(f"Loaded {len(df)} sequences")
        
        # Filter to target regions
        filtered_df, target_regions = filter_sequences(df, args.regions, args.id_column)
        
        # Get trim coordinates
        start_pos, end_pos = get_trim_coordinates(filtered_df, args.trim_to)
        
        # Process sequences with streaming
        process_sequences_streaming(
            args.fasta, 
            filtered_df, 
            start_pos, 
            end_pos, 
            args.id_column,
            args.keep_gaps,
            args.threads,
            args.batch_size,
            args.output
        )
        
        print("\n✓ Complete!")
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()