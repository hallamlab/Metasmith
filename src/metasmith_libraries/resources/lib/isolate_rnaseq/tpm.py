"""TPM within one genome, from an annotated raw count table.

    tpm.py <raw_count_table.tsv> <tpm_table.tsv>

TPM is computed over protein-coding genes only. rRNA left after depletion varies by library
and would otherwise set every other gene's scale.
"""
import sys

import pandas as pd

ANNOTATION = ["Length", "gene_biotype", "gene_name", "product", "protein_id", "copies", "multimap_fraction"]


def main(raw: str, out: str) -> None:
    table = pd.read_csv(raw, sep="\t", index_col="gene_id")
    libraries = [c for c in table.columns if c not in ANNOTATION]
    coding = table[table.gene_biotype == "protein_coding"]
    rate = coding[libraries].div(coding.Length / 1e3, axis=0)
    tpm = (rate / rate.sum() * 1e6).round(3)
    result = coding[ANNOTATION].join(tpm.add_prefix("TPM_")).join(coding[libraries].add_prefix("count_"))
    result.sort_values(f"TPM_{libraries[0]}", ascending=False).to_csv(out, sep="\t")
    print(f"{len(result)} protein-coding genes, {len(libraries)} libraries")


if __name__ == "__main__":
    main(*sys.argv[1:3])
