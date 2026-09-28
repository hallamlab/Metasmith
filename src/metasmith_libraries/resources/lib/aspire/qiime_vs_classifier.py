#!/usr/bin/env python3

import os
import argparse
import time
import tempfile
import shutil
import qiime2
import pandas as pd
from qiime2.plugins.feature_classifier.pipelines import classify_consensus_vsearch
from Bio import SeqIO

def parse_args():
    parser = argparse.ArgumentParser(description="Chunked QIIME2 VSEARCH Classifier with Checkpointing")
    parser.add_argument('--input-fasta', '-i', required=True)
    parser.add_argument('--ref-taxonomy', '-t', required=True)   # .qza
    parser.add_argument('--ref-seqs', '-r', required=True)       # .qza
    parser.add_argument('--output-tsv', '-o', default='classifications.tsv')
    parser.add_argument('--stats-output', '-s', default='classification_stats.tsv')
    parser.add_argument('--chunk-size', type=int, default=500)
    parser.add_argument(
        '--threads',
        type=int,
        default=int(os.environ.get('NXF_TASK_CPUS', '1')),
        help="Threads for QIIME2 classify-consensus-vsearch (default: $NXF_TASK_CPUS or 1)."
    )
    return parser.parse_args()

def split_fasta(fasta_path, chunk_size, outdir):
    os.makedirs(outdir, exist_ok=True)
    with open(fasta_path) as handle:
        records = list(SeqIO.parse(handle, "fasta"))
    for i in range(0, len(records), chunk_size):
        chunk_records = records[i:i+chunk_size]
        chunk_file = os.path.join(outdir, f"chunk_{i//chunk_size:04d}.fasta")
        SeqIO.write(chunk_records, chunk_file, "fasta")

def load_sequences_to_artifact(fasta_path, artifact_path):
    if os.path.exists(artifact_path):
        return qiime2.Artifact.load(artifact_path)
    artifact = qiime2.Artifact.import_data('FeatureData[Sequence]', fasta_path)
    artifact.save(artifact_path)
    return artifact

def classify_with_vsearch(query_seqs, ref_seqs, ref_taxonomy, result_path, threads: int):
    if os.path.exists(result_path):
        return qiime2.Artifact.load(result_path).view(pd.DataFrame)

    result = classify_consensus_vsearch(
        query=query_seqs,
        reference_reads=ref_seqs,
        reference_taxonomy=ref_taxonomy,
        threads=int(threads)
    )
    result.classification.save(result_path)
    return result.classification.view(pd.DataFrame)

def save_classifications(df, output_path):
    df.to_csv(output_path, sep='\t', index=True)

def calculate_statistics(df, output_path):
    stats = df['Taxon'].value_counts()
    stats.to_csv(output_path, sep='\t', header=['Count'])

def main():
    args = parse_args()

    work_dir = os.path.join(os.path.dirname(args.output_tsv), "intermediate")
    chunk_dir = os.path.join(work_dir, "chunks")
    qza_dir = os.path.join(work_dir, "artifacts")
    tsv_dir = os.path.join(work_dir, "tsvs")
    os.makedirs(chunk_dir, exist_ok=True)
    os.makedirs(qza_dir, exist_ok=True)
    os.makedirs(tsv_dir, exist_ok=True)

    print("🔹 Splitting input FASTA...")
    split_fasta(args.input_fasta, args.chunk_size, chunk_dir)

    ref_tax = qiime2.Artifact.load(args.ref_taxonomy)
    ref_seqs = qiime2.Artifact.load(args.ref_seqs)

    all_chunks = []
    for chunk_file in sorted(os.listdir(chunk_dir)):
        if not chunk_file.endswith(".fasta"):
            continue
        base = os.path.splitext(chunk_file)[0]
        chunk_path = os.path.join(chunk_dir, chunk_file)
        qza_path = os.path.join(qza_dir, f"{base}.qza")
        result_path = os.path.join(qza_dir, f"{base}_result.qza")
        tsv_path = os.path.join(tsv_dir, f"{base}.tsv")

        if os.path.exists(tsv_path):
            print(f"[✓] Skipping {base} (already processed)")
            continue

        print(f"⚙️  Processing {base}...")
        query = load_sequences_to_artifact(chunk_path, qza_path)
        df = classify_with_vsearch(query, ref_seqs, ref_tax, result_path, threads=args.threads)
        save_classifications(df, tsv_path)
        all_chunks.append(tsv_path)

    print("📦 Concatenating all results...")
    all_dfs = [pd.read_csv(path, sep='\t', index_col=0) for path in sorted(all_chunks)]
    final_df = pd.concat(all_dfs)
    final_df.to_csv(args.output_tsv, sep='\t')

    print("📊 Generating classification statistics...")
    calculate_statistics(final_df, args.stats_output)

    print("🧹 Cleaning up intermediates...")
    shutil.rmtree(work_dir)

    print("✅ Done.")

if __name__ == '__main__':
    main()
