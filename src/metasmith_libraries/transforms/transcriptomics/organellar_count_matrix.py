"""Build a gene-level count matrix for organellar genes from BAM alignments and GFF3 annotations."""

from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
exp   = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
bam   = model.AddRequirement(lib.GetType("transcriptomics::organellar_bam"), parents={exp})
gff   = model.AddRequirement(lib.GetType("transcriptomics::organellar_gff"))
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("transcriptomics::organellar_gene_count_table"))

def protocol(context: ExecutionContext):
    bam_paths = context.InputGroup(bam)
    gff_paths = context.InputGroup(gff)
    iout      = context.Output(out)

    # Write BAM manifest
    manifest = Path("sample_manifest.tsv")
    with open(manifest, "w") as f:
        for p in bam_paths:
            f.write(f"{p.container}\n")

    # Write GFF3 manifest (multiple organellar genomes)
    gff_manifest = Path("gff_manifest.tsv")
    with open(gff_manifest, "w") as f:
        for p in gff_paths:
            f.write(f"{p.container}\n")

    script = Path("count_organellar.py")
    with open(script, "w") as f:
        f.write("""\
import csv
import re
import sys
from collections import defaultdict

import pysam

bam_manifest_file = sys.argv[1]
gff_manifest_file = sys.argv[2]
output_file       = sys.argv[3]

# Read GFF3 manifest (one or more GFF3 files)
gff_files = [l.strip() for l in open(gff_manifest_file) if l.strip()]

# Parse all GFF3 files to get gene intervals
genes = []  # (seqid, start, end, gene_id, gene_name)
for gff_file in gff_files:
    print(f"Parsing GFF3: {gff_file}", flush=True)
    with open(gff_file) as gf:
        for line in gf:
            if line.startswith("#"):
                continue
            fields = line.strip().split("\\t")
            if len(fields) < 9 or fields[2] not in ("gene", "CDS"):
                continue
            seqid = fields[0]
            start = int(fields[3])
            end   = int(fields[4])
            attrs = fields[8]
            m_id = re.search(r"ID=([^;]+)", attrs)
            gene_id = m_id.group(1) if m_id else f"{seqid}_{start}_{end}"
            m_name = re.search(r"Name=([^;]+)", attrs)
            gene_name = m_name.group(1) if m_name else ""
            genes.append((seqid, start, end, gene_id, gene_name))

# Deduplicate: keep gene-level over CDS
gene_ids_seen = set()
deduped = []
for seqid, start, end, gene_id, gene_name in genes:
    base_id = gene_id.replace("cds_", "", 1) if gene_id.startswith("cds_") else gene_id
    if base_id not in gene_ids_seen:
        gene_ids_seen.add(base_id)
        deduped.append((seqid, start, end, base_id, gene_name))
genes = deduped

print(f"Loaded {len(genes)} gene intervals from {len(gff_files)} GFF3 file(s)", flush=True)

# Read BAM manifest
bam_files = [l.strip() for l in open(bam_manifest_file) if l.strip()]

# Count reads per gene per sample
# Multiple BAMs may share a sample (e.g., chloroplast + mitochondrion alignments)
gene_counts = defaultdict(lambda: defaultdict(int))
sample_names_ordered = []
sample_names_seen = set()

for i, bam_path in enumerate(bam_files):
    bamfile = pysam.AlignmentFile(bam_path, "rb")
    # Extract sample name from @RG SM tag
    rg = bamfile.header.to_dict().get("RG", [])
    sample_name = rg[0]["SM"] if rg and "SM" in rg[0] else f"sample_{i}"
    if sample_name not in sample_names_seen:
        sample_names_ordered.append(sample_name)
        sample_names_seen.add(sample_name)
    refs = set(bamfile.references)
    print(f"  Counting {bam_path} (sample={sample_name}, refs={refs})...", flush=True)
    total = 0
    for seqid, start, end, gene_id, gene_name in genes:
        if seqid not in refs:
            continue
        count = 0
        for read in bamfile.fetch(seqid, start - 1, end):
            if read.is_unmapped or read.is_secondary or read.is_supplementary:
                continue
            count += 1
        gene_counts[gene_id][sample_name] += count
        total += count
    bamfile.close()
    print(f"    {sample_name}: {total} reads mapped to genes", flush=True)

# Write CSV
name_lookup = {g[3]: g[4] for g in genes}

with open(output_file, "w", newline="") as out:
    writer = csv.writer(out)
    writer.writerow(["gene_id", "gene_name"] + sample_names_ordered)
    for gene_id in sorted(gene_counts.keys()):
        row = [gene_id, name_lookup.get(gene_id, "")]
        for sn in sample_names_ordered:
            row.append(gene_counts[gene_id][sn])
        writer.writerow(row)

print(f"Wrote {output_file} ({len(gene_counts)} genes x {len(sample_names_ordered)} samples)", flush=True)
""")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"pip install -q pysam && python count_organellar.py {manifest} {gff_manifest} {iout.container}",
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
