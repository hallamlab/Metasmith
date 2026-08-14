#!/bin/bash
# Generate subsampled test data for MetasmithLibraries E2E tests
#
# This script creates small test datasets from real metagenome data.
# The resulting files are suitable for quick E2E tests.
#
# Prerequisites:
#   - seqkit: conda install -c bioconda seqkit
#   - minimap2 & samtools: conda install -c bioconda minimap2 samtools
#
# Usage:
#   ./generate_test_data.sh [source_dir] [output_dir]

set -e

SOURCE_DIR="${1:-.}"
OUTPUT_DIR="${2:-./test_data}"

mkdir -p "$OUTPUT_DIR"

echo "Generating test data in $OUTPUT_DIR..."

# Small reads (1000 reads)
if [ -f "$SOURCE_DIR/reads.fq.gz" ]; then
    echo "Creating small_reads.fq.gz (1000 reads)..."
    seqkit head -n 1000 "$SOURCE_DIR/reads.fq.gz" | gzip > "$OUTPUT_DIR/small_reads.fq.gz"
fi

# Small assembly (10 contigs)
if [ -f "$SOURCE_DIR/assembly.fna" ]; then
    echo "Creating small_assembly.fna (10 contigs)..."
    seqkit head -n 10 "$SOURCE_DIR/assembly.fna" > "$OUTPUT_DIR/small_assembly.fna"
fi

# Small ORFs (50 proteins)
if [ -f "$SOURCE_DIR/orfs.faa" ]; then
    echo "Creating small_orfs.faa (50 ORFs)..."
    seqkit head -n 50 "$SOURCE_DIR/orfs.faa" > "$OUTPUT_DIR/small_orfs.faa"
fi

# Small BAM (aligned reads to small assembly)
if [ -f "$OUTPUT_DIR/small_assembly.fna" ] && [ -f "$OUTPUT_DIR/small_reads.fq.gz" ]; then
    echo "Creating small_bam.bam..."
    minimap2 -ax sr "$OUTPUT_DIR/small_assembly.fna" "$OUTPUT_DIR/small_reads.fq.gz" | \
        samtools sort -o "$OUTPUT_DIR/small_bam.bam"
    samtools index "$OUTPUT_DIR/small_bam.bam"
fi

# Small long reads (100 reads)
if [ -f "$SOURCE_DIR/long_reads.fq.gz" ]; then
    echo "Creating small_long_reads.fq.gz (100 reads)..."
    seqkit head -n 100 "$SOURCE_DIR/long_reads.fq.gz" | gzip > "$OUTPUT_DIR/small_long_reads.fq.gz"
fi

# Small ASVs (20 sequences)
if [ -f "$SOURCE_DIR/asvs.fasta" ]; then
    echo "Creating small_asvs.fasta (20 ASVs)..."
    seqkit head -n 20 "$SOURCE_DIR/asvs.fasta" > "$OUTPUT_DIR/small_asvs.fasta"
fi

echo "Test data generation complete!"
echo ""
echo "Generated files:"
ls -la "$OUTPUT_DIR"
