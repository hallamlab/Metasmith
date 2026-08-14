#!/usr/bin/env python
"""Merge per-chunk PromoTech results and remap to original contig coordinates.

PromoTech concatenates all input sequences and reports flat positions.
This script uses the chunk manifest to map each prediction back to its
original contig and position.

Output: TSV with columns chrom, start, end, score, strand, sequence
where chrom is a single contig ID (not comma-joined).
"""
import argparse
import csv
import json
import os
import sys


def load_manifest(path):
    """Load chunk manifest JSON."""
    with open(path) as f:
        return json.load(f)


def remap_predictions(predictions_path, regions):
    """Remap flat PromoTech coordinates to original contig positions.

    PromoTech concatenates all sequences in the FASTA and reports positions
    in the concatenated string. We walk through the regions to find which
    region each prediction falls in, then compute the original coordinate.

    Args:
        predictions_path: Path to genome_predictions.csv (TSV)
        regions: List of {"contig", "start", "end", "length"} dicts

    Yields:
        (contig, orig_start, orig_end, score, strand, sequence) tuples
    """
    # Build cumulative offset table for the concatenated sequence
    offsets = []  # (cum_start, cum_end, region)
    cum = 0
    for region in regions:
        offsets.append((cum, cum + region["length"], region))
        cum += region["length"]

    with open(predictions_path) as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)  # skip header
        for row in reader:
            if len(row) < 6:
                continue
            # chrom, start, end, score, strand, sequence
            flat_start = int(row[1])
            flat_end = int(row[2])
            score = row[3]
            strand = row[4]
            sequence = row[5]

            # Find which region this falls in
            for cum_start, cum_end, region in offsets:
                if cum_start <= flat_start < cum_end:
                    # Position within this region
                    offset_in_region = flat_start - cum_start
                    orig_start = region["start"] + offset_in_region
                    orig_end = orig_start + (flat_end - flat_start)
                    yield (region["contig"], orig_start, orig_end,
                           score, strand, sequence)
                    break


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="Chunk manifest JSON")
    parser.add_argument("--results-dir", required=True, help="Directory with per-chunk results")
    parser.add_argument("--output", required=True, help="Output TSV path")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)

    total_predictions = 0
    with open(args.output, "w") as out:
        out.write("chrom\tstart\tend\tscore\tstrand\tsequence\n")

        for chunk_info in manifest:
            chunk_name = chunk_info["chunk"].replace(".fna", "")
            predictions_path = os.path.join(
                args.results_dir, chunk_name, "genome_predictions.csv")

            if not os.path.exists(predictions_path):
                print("WARNING: No predictions for chunk {}".format(chunk_name),
                      file=sys.stderr)
                continue

            for contig, start, end, score, strand, seq in remap_predictions(
                    predictions_path, chunk_info["regions"]):
                out.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(
                    contig, start, end, score, strand, seq))
                total_predictions += 1

    print("Merged {} predictions from {} chunks".format(
        total_predictions, len(manifest)))


if __name__ == "__main__":
    main()
