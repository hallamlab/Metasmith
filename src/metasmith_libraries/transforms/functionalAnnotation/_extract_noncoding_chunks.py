#!/usr/bin/env python
"""Extract non-coding intervals from an assembly using a Prodigal GFF.

Reads a FASTA assembly and its Prodigal GFF annotation, computes
intergenic (non-coding) intervals, extracts sequences, and writes
them as chunk FASTA files no larger than --max-chunk-bp each.

Each FASTA header encodes the original coordinate:
  >{contig_id}:{start}-{end}   (0-based, half-open)

Also writes a manifest.json mapping chunk files to their regions.
"""
import argparse
import json
import os
import sys
from collections import defaultdict


def parse_fasta(path):
    """Parse FASTA file into {contig_id: sequence} dict."""
    contigs = {}
    current_id = None
    parts = []
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith(">"):
                if current_id is not None:
                    contigs[current_id] = "".join(parts)
                current_id = line[1:].split()[0]
                parts = []
            else:
                parts.append(line)
    if current_id is not None:
        contigs[current_id] = "".join(parts)
    return contigs


def parse_gff_cds(path):
    """Parse Prodigal GFF for CDS intervals.

    Returns {contig_id: [(start, end), ...]} with 0-based half-open coords.
    """
    cds = defaultdict(list)
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 9:
                continue
            if parts[2] != "CDS":
                continue
            contig = parts[0]
            # GFF is 1-based inclusive; convert to 0-based half-open
            start = int(parts[3]) - 1
            end = int(parts[4])
            cds[contig].append((start, end))
    return cds


def noncoding_intervals(contig_len, cds_intervals):
    """Compute non-coding intervals as complement of sorted/merged CDS."""
    if not cds_intervals:
        return [(0, contig_len)]

    # Sort and merge overlapping CDS intervals
    sorted_cds = sorted(cds_intervals)
    merged = [sorted_cds[0]]
    for start, end in sorted_cds[1:]:
        if start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    # Compute complement
    noncoding = []
    prev_end = 0
    for cds_start, cds_end in merged:
        if prev_end < cds_start:
            noncoding.append((prev_end, cds_start))
        prev_end = max(prev_end, cds_end)
    if prev_end < contig_len:
        noncoding.append((prev_end, contig_len))

    return noncoding


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fasta", required=True, help="Assembly FASTA")
    parser.add_argument("--gff", required=True, help="Prodigal GFF")
    parser.add_argument("--outdir", required=True, help="Output directory for chunks")
    parser.add_argument("--max-chunk-bp", type=int, default=1000000,
                        help="Max base pairs per chunk FASTA (default: 1000000)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    contigs = parse_fasta(args.fasta)
    cds_map = parse_gff_cds(args.gff)

    print("Assembly: {} contigs, {:.1f} Mbp total".format(
        len(contigs), sum(len(s) for s in contigs.values()) / 1e6))
    print("GFF: {} contigs with CDS annotations".format(len(cds_map)))

    # Collect all non-coding regions
    all_regions = []  # (contig_id, start, end, sequence)
    total_nc_bp = 0
    for contig_id, seq in contigs.items():
        intervals = noncoding_intervals(len(seq), cds_map.get(contig_id, []))
        for start, end in intervals:
            region_seq = seq[start:end]
            if len(region_seq) < 40:
                continue  # PromoTech needs at least 40nt
            all_regions.append((contig_id, start, end, region_seq))
            total_nc_bp += len(region_seq)

    print("Non-coding: {} regions, {:.1f} Mbp ({:.0f}% of assembly)".format(
        len(all_regions), total_nc_bp / 1e6,
        100.0 * total_nc_bp / max(1, sum(len(s) for s in contigs.values()))))

    # Write chunks
    manifest = []
    chunk_idx = 0
    chunk_bp = 0
    chunk_regions = []
    chunk_fh = None

    def start_chunk():
        nonlocal chunk_idx, chunk_bp, chunk_regions, chunk_fh
        if chunk_fh is not None:
            chunk_fh.close()
            manifest.append({
                "chunk": "chunk_{:03d}.fna".format(chunk_idx),
                "regions": chunk_regions,
            })
            chunk_idx += 1
        chunk_bp = 0
        chunk_regions = []
        path = os.path.join(args.outdir, "chunk_{:03d}.fna".format(chunk_idx))
        chunk_fh = open(path, "w")

    start_chunk()

    for contig_id, start, end, seq in all_regions:
        # Start new chunk if this region would exceed limit
        if chunk_bp > 0 and chunk_bp + len(seq) > args.max_chunk_bp:
            start_chunk()

        chunk_fh.write(">{}:{}-{}\n".format(contig_id, start, end))
        # Write sequence in 80-char lines
        for i in range(0, len(seq), 80):
            chunk_fh.write(seq[i:i+80] + "\n")

        chunk_regions.append({
            "contig": contig_id,
            "start": start,
            "end": end,
            "length": len(seq),
        })
        chunk_bp += len(seq)

    # Close last chunk
    if chunk_fh is not None:
        chunk_fh.close()
        manifest.append({
            "chunk": "chunk_{:03d}.fna".format(chunk_idx),
            "regions": chunk_regions,
        })

    # Write manifest
    manifest_path = os.path.join(args.outdir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print("Wrote {} chunks to {}".format(len(manifest), args.outdir))


if __name__ == "__main__":
    main()
