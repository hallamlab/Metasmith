from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
qgtf    = model.AddRequirement(lib.GetType("transcriptomics::stringtie_quant_gtf"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::pydeseq2.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::diff_count_table"))

def protocol(context: ExecutionContext):
    qgtf_paths=context.InputGroup(qgtf)
    iout=context.Output(out)

    # Write sample manifest mapping sample names to quantified GTF paths
    manifest = Path("sample_manifest.tsv")
    with open(manifest, "w") as f:
        for i, p in enumerate(qgtf_paths):
            sample_name = p.local.parent.name if p.local.parent.name != "." else f"sample_{i}"
            f.write(f"{sample_name}\t{p.container}\n")

    # Python script to build raw count matrix from StringTie GTFs and normalize with PyDESeq2
    script = Path("pydeseq2_normalize.py")
    with open(script, "w") as f:
        f.write("""\
import csv
import re
import sys

import numpy as np
import pandas as pd
from pydeseq2.dds import DeseqDataSet

manifest = sys.argv[1]
output = sys.argv[2]

samples = []
with open(manifest) as m:
    for line in m:
        name, path = line.strip().split("\\t")
        samples.append((name, path))

# Parse each quantified GTF to extract gene-level integer read counts
gene_counts = {}  # gene_id -> {sample: count}
for sample_name, gtf_path in samples:
    with open(gtf_path) as gtf:
        for line in gtf:
            if line.startswith("#"):
                continue
            fields = line.strip().split("\\t")
            if len(fields) < 9:
                continue
            if fields[2] != "transcript":
                continue
            attrs = fields[8]
            gene_id_m = re.search(r'gene_id "([^"]+)"', attrs)
            cov_m = re.search(r'cov "([^"]+)"', attrs)
            if not gene_id_m or not cov_m:
                continue
            gene_id = gene_id_m.group(1)
            cov = float(cov_m.group(1))
            # Approximate read count: coverage * transcript_length / read_length
            start = int(fields[3])
            end = int(fields[4])
            length = end - start + 1
            read_count = cov * length / 100.0
            if gene_id not in gene_counts:
                gene_counts[gene_id] = {}
            gene_counts[gene_id][sample_name] = (
                gene_counts[gene_id].get(sample_name, 0) + read_count
            )

# Build raw count matrix (genes x samples) with integer counts
sample_names = [s[0] for s in samples]
gene_ids = sorted(gene_counts.keys())
count_matrix = np.zeros((len(gene_ids), len(sample_names)), dtype=int)
for i, gid in enumerate(gene_ids):
    for j, sn in enumerate(sample_names):
        count_matrix[i, j] = round(gene_counts[gid].get(sn, 0))

counts_df = pd.DataFrame(count_matrix, index=gene_ids, columns=sample_names)

# Filter out genes with zero counts across all samples
counts_df = counts_df.loc[counts_df.sum(axis=1) > 0]

# PyDESeq2 expects samples as rows
counts_df = counts_df.T

# Minimal metadata — single condition (normalization only, no DE testing)
metadata = pd.DataFrame({"condition": ["A"] * len(sample_names)}, index=sample_names)

dds = DeseqDataSet(counts=counts_df, metadata=metadata, design="~1")
dds.deseq2()

# Extract normalized counts (samples x genes) and transpose back to genes x samples
normed = pd.DataFrame(
    dds.layers["normed_counts"],
    index=counts_df.index,
    columns=counts_df.columns,
).T

normed.index.name = "gene_id"
normed.to_csv(output, sep="\\t")
""")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python pydeseq2_normalize.py {manifest} {iout.container}",
    )
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    )
)
