from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
contigs = model.AddRequirement(lib.GetType("genomics::contigs"))
image   = model.AddRequirement(lib.GetType("genomics::oci_image_prodigal"))
orfs    = model.AddProduct(lib.GetType("genomics::aa_sequences"))

def protocol(context: ExecutionContext):
    out_path = context.Get(orfs)
    context.ExecWithContainer(
        image = image,
        cmd = f"""\
            prodigal \
                -i {context.Get(contigs).container} \
                -a {out_path.container} \
                -f gff \
                -o {out_path.container.with_suffix('.gff')} \
            """,
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        orfs: "orfs.faa",
    },
)
