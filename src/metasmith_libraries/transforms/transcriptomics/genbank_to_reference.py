"""Extract organellar genome FASTA and GFF3 annotations from a GenBank file."""

from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
gbk   = model.AddRequirement(lib.GetType("sequences::gbk"))
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
fasta = model.AddProduct(lib.GetType("transcriptomics::organellar_reference"))
gff   = model.AddProduct(lib.GetType("transcriptomics::organellar_gff"))

def protocol(context: ExecutionContext):
    igbk   = context.Input(gbk)
    ifasta = context.Output(fasta)
    igff   = context.Output(gff)

    script = Path("gbk_to_ref.py")
    with open(script, "w") as f:
        f.write("""\
import sys
from Bio import SeqIO

gbk_file   = sys.argv[1]
fasta_file = sys.argv[2]
gff_file   = sys.argv[3]

records = list(SeqIO.parse(gbk_file, "genbank"))
print(f"Parsed {len(records)} record(s) from {gbk_file}", flush=True)

# Write FASTA
with open(fasta_file, "w") as fout:
    for rec in records:
        fout.write(f">{rec.id} {rec.description}\\n")
        seq = str(rec.seq)
        for i in range(0, len(seq), 80):
            fout.write(seq[i:i+80] + "\\n")
print(f"Wrote FASTA: {fasta_file}", flush=True)

# Write GFF3
with open(gff_file, "w") as gout:
    gout.write("##gff-version 3\\n")
    for rec in records:
        seq_len = len(rec.seq)
        gout.write(f"##sequence-region {rec.id} 1 {seq_len}\\n")

        gene_counter = 0
        for feat in rec.features:
            if feat.type not in ("gene", "CDS", "tRNA", "rRNA", "tmRNA"):
                continue

            # Extract location — handles complement and join
            start = int(feat.location.start) + 1  # 1-based
            end = int(feat.location.end)
            strand = "+" if feat.location.strand == 1 else "-"

            # Check for circular wrapping (parts spanning origin)
            if hasattr(feat.location, "parts") and len(feat.location.parts) > 1:
                # For multi-part features, report full span
                starts = [int(p.start) + 1 for p in feat.location.parts]
                ends = [int(p.end) for p in feat.location.parts]
                start = min(starts)
                end = max(ends)

            qualifiers = feat.qualifiers
            gene_name = qualifiers.get("gene", [""])[0]
            product = qualifiers.get("product", [""])[0]
            locus_tag = qualifiers.get("locus_tag", [""])[0]
            transl_table = qualifiers.get("transl_table", [""])[0]

            # Build GFF3 attributes
            attrs = []
            if feat.type == "gene":
                gene_counter += 1
                gene_id = locus_tag or gene_name or f"gene_{gene_counter}"
                attrs.append(f"ID={gene_id}")
                if gene_name:
                    attrs.append(f"Name={gene_name}")
                if product:
                    attrs.append(f"product={product}")
                if locus_tag:
                    attrs.append(f"locus_tag={locus_tag}")
            elif feat.type == "CDS":
                gene_id = locus_tag or gene_name or f"cds_{gene_counter}"
                parent_id = locus_tag or gene_name or f"gene_{gene_counter}"
                attrs.append(f"ID=cds_{gene_id}")
                attrs.append(f"Parent={parent_id}")
                if gene_name:
                    attrs.append(f"Name={gene_name}")
                if product:
                    attrs.append(f"product={product}")
                if transl_table:
                    attrs.append(f"transl_table={transl_table}")
            else:
                # tRNA, rRNA, tmRNA
                rna_id = locus_tag or gene_name or f"{feat.type}_{gene_counter}"
                parent_id = locus_tag or gene_name or f"gene_{gene_counter}"
                attrs.append(f"ID={feat.type}_{rna_id}")
                attrs.append(f"Parent={parent_id}")
                if gene_name:
                    attrs.append(f"Name={gene_name}")
                if product:
                    attrs.append(f"product={product}")

            score = "."
            phase = "0" if feat.type == "CDS" else "."
            attr_str = ";".join(attrs)

            gout.write(f"{rec.id}\\t"
                       f"GenBank\\t"
                       f"{feat.type}\\t"
                       f"{start}\\t"
                       f"{end}\\t"
                       f"{score}\\t"
                       f"{strand}\\t"
                       f"{phase}\\t"
                       f"{attr_str}\\n")

        print(f"  {rec.id}: {gene_counter} genes written to GFF3", flush=True)

print(f"Wrote GFF3: {gff_file}", flush=True)
""")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python gbk_to_ref.py {igbk.container} {ifasta.container} {igff.container}",
    )
    return ExecutionResult(
        manifest=[
            {fasta: ifasta.local},
            {gff: igff.local},
        ],
        success=ifasta.local.exists() and igff.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=gbk,
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(minutes=30),
    ),
)
