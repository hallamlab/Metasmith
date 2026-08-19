import csv
import re
import sys

manifest = sys.argv[1]
output = sys.argv[2]

samples = []
with open(manifest) as m:
    for line in m:
        name, path = line.strip().split("\t")
        samples.append((name, path))

# Parse each quantified GTF to extract gene-level read counts
gene_counts = {}  # gene_id -> {sample: count}
for sample_name, gtf_path in samples:
    with open(gtf_path) as gtf:
        for line in gtf:
            if line.startswith("#"):
                continue
            fields = line.strip().split("\t")
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

# Write gene count matrix CSV
sample_names = [s[0] for s in samples]
with open(output, "w", newline="") as out:
    writer = csv.writer(out)
    writer.writerow(["gene_id"] + sample_names)
    for gene_id in sorted(gene_counts.keys()):
        row = [gene_id]
        for sn in sample_names:
            row.append(f"{gene_counts[gene_id].get(sn, 0):.1f}")
        writer.writerow(row)
