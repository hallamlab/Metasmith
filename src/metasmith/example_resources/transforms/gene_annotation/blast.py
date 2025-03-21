from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
orfs    = model.AddRequirement(node=lib.GetType("genomics::aa_sequences"))
refdb   = model.AddRequirement(node=lib.GetType("genomics::protein_reference_fasta"))
image   = model.AddRequirement(node=lib.GetType("genomics::oci_image_blast"))
annot   = model.AddProduct(lib.GetType("genomics::orf_annotations"))

def protocol(context: ExecutionContext):
    out_path = context.Get(annot)
    COLUMNS = f"qseqid sseqid bitscore evalue pident"
    context.ExecWithContainer(
        image = image,
        cmd = f"""\
            blastp \
                -query {context.Get(orfs).container} \
                -subject {context.Get(refdb).container} \
                -outfmt "6 {COLUMNS}" \
                -out {out_path.container} \
            """,
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        annot: "annotations.csv",
    },
)
