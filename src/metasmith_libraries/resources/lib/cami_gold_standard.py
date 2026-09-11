"""Builds an AMBER gold-standard table over one sample's OWN assembly contigs.

CAMI's published binning_gs.tsv/gsa_mapping.tsv.gz key on the CAMI-provided
gold-standard-assembly's own contig ids (e.g. S9C933009), which share no id
space with a participant's own assembly (e.g. megahit's k141_N). Comparing a
self-assembled contig_to_bin_table against binning_gs.tsv directly would join
on nothing. This script instead majority-votes each of OUR contigs against
CAMISIM's per-read truth table (reads_mapping.tsv.gz), using the same sample's
own alignment::bam to say which reads landed on which contig -- the same idea
CAMI's own gsa_mapping.tsv.gz used its "number_reads" column for, just applied
to a different assembly.

Read ids are matched with a trailing /1 or /2 stripped on both sides: whether
an aligner keeps that suffix in QNAME is aligner- and version-dependent, and
neither side can be trusted to already agree with the other.

argv: read_contig.tsv  reads_mapping.tsv.gz  contig_lengths.tsv  sample_id  out.tsv

read_contig.tsv      -- two columns, no header: read_id, contig_id. One row per
                         primary mapped alignment (samtools view -F 0x904).
reads_mapping.tsv.gz -- CAMISIM's own read-truth table, gzip-compressed tsv
                         with header "#anonymous_read_id genome_id tax_id read_id".
contig_lengths.tsv   -- two columns, no header: contig_id, length (bp).
"""
import sys

import polars as pl

read_contig_path, reads_mapping_path, contig_lengths_path, sample_id, out_path = sys.argv[1:]


def _mate_stripped(col: str) -> pl.Expr:
    return pl.col(col).str.replace(r"/[12]$", "")


read_contig = pl.read_csv(
    read_contig_path, separator="\t", has_header=False,
    new_columns=["read_id", "contig_id"],
).with_columns(_mate_stripped("read_id").alias("read_key"))

truth = pl.read_csv(reads_mapping_path, separator="\t", has_header=True)
truth = truth.rename({truth.columns[0]: "anonymous_read_id"})
truth = (
    truth.with_columns(_mate_stripped("anonymous_read_id").alias("read_key"))
    .select(["read_key", "genome_id", "tax_id"])
    .unique(subset=["read_key"])
)

joined = read_contig.join(truth, on="read_key", how="inner")

# Majority vote: the (genome_id, tax_id) with the most supporting reads per
# contig. Sorting the whole vote table by count before grouping, with
# maintain_order, is what makes group_by().first() pick the top vote rather
# than an arbitrary row -- polars does not otherwise guarantee group order.
votes = (
    joined.group_by(["contig_id", "genome_id", "tax_id"])
    .agg(pl.len().alias("n_reads"))
    .sort(["contig_id", "n_reads"], descending=[False, True])
)
winners = votes.group_by("contig_id", maintain_order=True).first()

lengths = pl.read_csv(
    contig_lengths_path, separator="\t", has_header=False,
    new_columns=["contig_id", "length"],
)
out = winners.join(lengths, on="contig_id", how="left")

n_contigs_total = lengths.height
n_contigs_scored = out.height
n_reads_total = read_contig.height
n_reads_matched = joined.height
print(
    f"cami_gold_standard: {n_contigs_scored}/{n_contigs_total} contigs voted, "
    f"{n_reads_matched}/{n_reads_total} mapped reads matched to truth",
    file=sys.stderr,
)
assert n_contigs_scored > 0, (
    "no contig received a single truth vote -- the read-id join between the "
    "BAM and reads_mapping.tsv.gz found no overlap at all. Check whether the "
    "aligner is stripping/keeping the /1 /2 mate suffix differently than "
    "assumed here."
)

with open(out_path, "w") as f:
    f.write(f"@Version:0.9.1\n@SampleID:{sample_id}\n\n")
    f.write("@@SEQUENCEID\tBINID\tTAXID\t_LENGTH\n")
    for row in out.iter_rows(named=True):
        f.write(f"{row['contig_id']}\t{row['genome_id']}\t{row['tax_id']}\t{row['length']}\n")
