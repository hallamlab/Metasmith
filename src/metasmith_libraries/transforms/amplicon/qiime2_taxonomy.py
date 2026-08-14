"""Classify ASV taxonomy using hybrid QIIME2 approach: NB + VSEARCH consensus.

Uses naive Bayes classifier as primary, falls back to VSEARCH consensus
for ASVs that NB cannot classify. This improves recall for short/divergent sequences.
"""

from pathlib import Path
from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()
image      = model.AddRequirement(lib.GetType("env::qiime2.env"))
asvs       = model.AddRequirement(lib.GetType("amplicon::asv_seqs"))
classifier = model.AddRequirement(lib.GetType("amplicon::silva_nb_classifier"))
tax        = model.AddProduct(lib.GetType("amplicon::asv_taxonomy"))

# SILVA reference URLs for VSEARCH fallback
SILVA_SEQS_URL = "https://data.qiime2.org/2024.10/common/silva-138-99-seqs.qza"
SILVA_TAX_URL = "https://data.qiime2.org/2024.10/common/silva-138-99-tax.qza"

def protocol(context: ExecutionContext):
    iasvs       = context.Input(asvs)
    iclassifier = context.Input(classifier)
    itax        = context.Output(tax)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
set -e

# Import ASV sequences as QIIME2 artifact
echo "Importing ASV sequences..."
qiime tools import \
    --type 'FeatureData[Sequence]' \
    --input-path {iasvs.container} \
    --output-path asv_seqs.qza

# === Method 1: Naive Bayes classifier ===
echo "Running NB classifier..."
qiime feature-classifier classify-sklearn \
    --i-classifier {iclassifier.container} \
    --i-reads asv_seqs.qza \
    --o-classification taxonomy_nb.qza

qiime tools export \
    --input-path taxonomy_nb.qza \
    --output-path taxonomy_nb_export

# === Method 2: VSEARCH consensus (for fallback) ===
echo "Downloading SILVA reference for VSEARCH..."
wget -q -O silva_seqs.qza "{SILVA_SEQS_URL}"
wget -q -O silva_tax.qza "{SILVA_TAX_URL}"

echo "Running VSEARCH consensus classifier..."
qiime feature-classifier classify-consensus-vsearch \
    --i-query asv_seqs.qza \
    --i-reference-reads silva_seqs.qza \
    --i-reference-taxonomy silva_tax.qza \
    --p-perc-identity 0.70 \
    --p-min-consensus 0.51 \
    --p-maxaccepts 10 \
    --p-maxrejects 10 \
    --p-threads 4 \
    --o-classification taxonomy_vsearch.qza \
    --o-search-results vsearch_hits.qza

qiime tools export \
    --input-path taxonomy_vsearch.qza \
    --output-path taxonomy_vsearch_export

# === Combine: prefer NB, fallback to VSEARCH ===
echo "Combining results (NB preferred, VSEARCH fallback)..."

cat > combine_tax.py << 'ENDSCRIPT'
import pandas as pd
import sys

nb = pd.read_csv("taxonomy_nb_export/taxonomy.tsv", sep="\t")
nb.columns = ["Feature ID", "nb_taxon", "nb_confidence"]

vs = pd.read_csv("taxonomy_vsearch_export/taxonomy.tsv", sep="\t")
vs.columns = ["Feature ID", "vs_taxon", "vs_confidence"]

merged = nb.merge(vs, on="Feature ID", how="outer")

def combine(row):
    nb_tax = row.get("nb_taxon", "Unassigned")
    nb_conf = row.get("nb_confidence", 0)
    vs_tax = row.get("vs_taxon", "Unassigned")
    vs_conf = row.get("vs_confidence", 0)
    if pd.notna(nb_tax) and nb_tax not in ["Unassigned", "Unclassified"]:
        return nb_tax, nb_conf, "nb"
    elif pd.notna(vs_tax) and vs_tax not in ["Unassigned", "Unclassified"]:
        return vs_tax, vs_conf, "vsearch"
    else:
        return "Unassigned", 0.0, "none"

merged[["Taxon", "Confidence", "Source"]] = merged.apply(lambda r: pd.Series(combine(r)), axis=1)
output = merged[["Feature ID", "Taxon", "Confidence", "Source"]]
output.to_csv(sys.argv[1], sep="\t", index=False)

total = len(output)
assigned = (output["Taxon"] != "Unassigned").sum()
print(f"Total ASVs: {{total}}")
print(f"Assigned: {{assigned}} ({{100*assigned/total:.1f}}%)")
print("Source breakdown:")
print(output["Source"].value_counts().to_string())
ENDSCRIPT

python3 combine_tax.py {itax.container}
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                tax: itax.local,
            },
        ],
        success=itax.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asvs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    )
)
