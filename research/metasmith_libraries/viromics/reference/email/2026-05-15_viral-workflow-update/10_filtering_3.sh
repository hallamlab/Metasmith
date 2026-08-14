#!/bin/bash
#SBATCH --job-name=09_filter_large_mobile
#SBATCH --output=09_filter_large_mobile_%j.out
#SBATCH --error=09_filter_large_mobile_%j.err
#SBATCH --partition=albaicin


set -euo pipefail

BASE="/SCRATCH/RNM270/ach/Viromics"

VOTU_FASTA="${BASE}/08_vOTUs/vOTU_representatives.fna"
GENES_TSV="${BASE}/06_geNomad/viral_precluster_rep_seq.renamed_summary/viral_precluster_rep_seq.renamed_virus_genes.tsv"

OUT="${BASE}/09_vOTUs_filter3_large_mobile"
mkdir -p "${OUT}"

FILTERED_FASTA="${OUT}/vOTUs.filter3.no_large_mobile.fasta"

python <<EOF
import re
from pathlib import Path

votu_fasta = Path("${VOTU_FASTA}")
genes_tsv = Path("${GENES_TSV}")
outdir = Path("${OUT}")
filtered_fasta = Path("${FILTERED_FASTA}")

bad_patterns = re.compile(
    r"transposase|transposon|"
    r"glycosyltransferase|glycosyl transferase|"
    r"nucleotidyl.?transferase|"
    r"carbohydrate kinase|"
    r"nucleotide sugar epimerase|"
    r"lipopolysaccharide|lps|"
    r"endonuclease|"
    r"integrase|"
    r"plasmid stability|partition protein|parA|parB|"
    r"toxin.?antitoxin|relE|hipA|stability",
    re.IGNORECASE
)

seqs = {}
current_id = None
current_seq = []

with open(votu_fasta) as f:
    for line in f:
        line = line.rstrip()
        if line.startswith(">"):
            if current_id is not None:
                seqs[current_id] = "".join(current_seq)
            current_id = line[1:].split()[0]
            current_seq = []
        else:
            current_seq.append(line)

if current_id is not None:
    seqs[current_id] = "".join(current_seq)

lengths = {k: len(v) for k, v in seqs.items()}

bad_contigs = set()

with open(genes_tsv) as f:
    header = f.readline().rstrip("\\n").split("\\t")
    col = {name: i for i, name in enumerate(header)}

    if "gene" not in col:
        raise SystemExit("Missing column in geNomad genes table: gene")

    annot_cols = [
        c for c in [
            "marker",
            "annotation_description",
            "annotation_accessions",
            "annotation_conjscan",
            "annotation_amr",
            "taxname"
        ]
        if c in col
    ]

    for line in f:
        fields = line.rstrip("\\n").split("\\t")
        gene = fields[col["gene"]]
        contig = re.sub(r"_[0-9]+$", "", gene)

        text = " ".join(
            fields[col[c]] for c in annot_cols
            if col[c] < len(fields)
        )

        if bad_patterns.search(text):
            bad_contigs.add(contig)

remove = set()
keep = set()

for contig, length in lengths.items():
    if length >= 100000 and contig in bad_contigs:
        remove.add(contig)
    else:
        keep.add(contig)

with open(filtered_fasta, "w") as out:
    for contig, seq in seqs.items():
        if contig in keep:
            out.write(f">{contig}\\n")
            for i in range(0, len(seq), 80):
                out.write(seq[i:i+80] + "\\n")

large_total = sum(1 for x in lengths.values() if x >= 100000)
removed_pct = (len(remove) / large_total * 100) if large_total > 0 else 0

with open(outdir / "removed_large_mobile_vOTUs.txt", "w") as out:
    for contig in sorted(remove):
        out.write(contig + "\\n")

with open(outdir / "filter3_summary.txt", "w") as out:
    out.write(f"Input vOTUs: {len(seqs)}\\n")
    out.write(f"vOTUs >=100 kb: {large_total}\\n")
    out.write(f"vOTUs >=100 kb removed: {len(remove)}\\n")
    out.write(f"Percent removed among >=100 kb vOTUs: {removed_pct:.2f}%\\n")
    out.write(f"vOTUs retained: {len(keep)}\\n")

print("Input vOTUs:", len(seqs))
print("vOTUs >=100 kb:", large_total)
print("Removed large mobile/artifact-like vOTUs:", len(remove))
print("Percent removed among >=100 kb vOTUs:", f"{removed_pct:.2f}%")
print("Retained vOTUs:", len(keep))
print("Filtered FASTA:", filtered_fasta)
EOF

echo "Done."
echo "Filtered FASTA:"
echo "${FILTERED_FASTA}"

echo "Summary:"
cat "${OUT}/filter3_summary.txt"