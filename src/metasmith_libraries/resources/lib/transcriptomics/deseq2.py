import sys
import pandas as pd
import numpy as np
from pydeseq2.dds import DeseqDataSet
from pydeseq2.ds import DeseqStats

count_file  = sys.argv[1]
output_file = sys.argv[2]
n_cpus      = int(sys.argv[3])

counts = pd.read_csv(count_file, index_col="gene_id")
if "gene_name" in counts.columns:
    counts = counts.drop(columns=["gene_name"])

# Infer condition from sample name: everything before the trailing replicate number
def get_condition(name):
    parts = name.split("-")
    try:
        int(parts[-1])
        return "-".join(parts[:-1])
    except ValueError:
        return name

conditions = {s: get_condition(s) for s in counts.columns}
print(f"Conditions: {sorted(set(conditions.values()))}", flush=True)

# DESeq2 requires integer counts; drop all-zero genes
counts_int = counts.round().astype(int)
counts_int = counts_int[counts_int.sum(axis=1) > 0]
print(f"Genes after zero-count filter: {len(counts_int)}", flush=True)

samples = pd.DataFrame({"condition": conditions})
dds = DeseqDataSet(
    counts=counts_int.T,
    metadata=samples,
    design_factors="condition",
    refit_cooks=True,
    n_cpus=n_cpus,
    quiet=True,
)
dds.deseq2()

cond_names = sorted(set(conditions.values()))
de_frames = []
for ref in cond_names:
    for alt in cond_names:
        if alt <= ref:
            continue
        label = f"{alt}_vs_{ref}"
        stat = DeseqStats(dds, contrast=["condition", alt, ref], alpha=0.05, quiet=True)
        stat.summary()
        df = stat.results_df.copy()
        df.columns = [f"{label}_{c}" for c in df.columns]
        de_frames.append(df)
        sig = (df[f"{label}_padj"].fillna(1) < 0.05) & (df[f"{label}_log2FoldChange"].abs() > 1)
        print(f"  {label}: {sig.sum()} DEGs (padj<0.05, |log2FC|>1)", flush=True)

de_df = pd.concat(de_frames, axis=1)
de_df.index.name = "gene_id"
de_df.to_csv(output_file)
print(f"Wrote {output_file} ({len(de_df)} genes, {len(de_frames)} comparisons)", flush=True)
