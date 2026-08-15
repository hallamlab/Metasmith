import pandas as pd
import sys
from Bio import SeqIO

# Inputs
count_table = sys.argv[1]
filtered_table = sys.argv[2]
count_threshold = int(sys.argv[3])
asv_abund_threshold = float(sys.argv[4])
input_fasta = sys.argv[5]
filtered_fasta = sys.argv[6]

# Load count table
count_df = pd.read_csv(count_table, sep='\t', header=0, index_col=0)

# Filter samples by total count
low_filter_df = count_df.loc[:, count_df.sum() >= count_threshold]

# Filter ASVs by per-sample relative abundance (%).
# Keep an ASV if it reaches the threshold in at least one retained sample.
sample_totals = low_filter_df.sum(axis=0)
nonzero_samples = sample_totals > 0

if nonzero_samples.any():
    per_sample_rel_abund = low_filter_df.loc[:, nonzero_samples].div(
        sample_totals[nonzero_samples], axis=1
    ) * 100
    keep_asvs = per_sample_rel_abund.ge(asv_abund_threshold).any(axis=1)
    abund_filter_df = low_filter_df.loc[keep_asvs]
else:
    abund_filter_df = low_filter_df.iloc[0:0]

# Filter ASVs that are all 0s
filter_0s = low_filter_df > 0
print(f"Number of ASVs before filtering: {abund_filter_df.shape[0]}")
abund_filter_df = abund_filter_df.loc[filter_0s.any(axis=1)]
print(f"Number of ASVs after filtering: {abund_filter_df.shape[0]}")

# Save filtered count table
abund_filter_df.to_csv(filtered_table, sep='\t', header=True, index=True)

# Subset FASTA
filtered_asvs = set(abund_filter_df.index)

with open(filtered_fasta, "w") as out_fa:
    for record in SeqIO.parse(input_fasta, "fasta"):
        if record.id.split(';')[0] in filtered_asvs:
            SeqIO.write(record, out_fa, "fasta")
