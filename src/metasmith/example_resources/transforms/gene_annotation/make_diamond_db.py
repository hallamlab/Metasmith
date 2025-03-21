from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
raw     = model.AddRequirement(node=lib.GetType("genomics::protein_reference_fasta"))
image   = model.AddRequirement(node=lib.GetType("genomics::oci_image_diamond"))
refdb   = model.AddProduct(lib.GetType("genomics::protein_reference_diamond"))

def protocol(context: ExecutionContext):
    out_path = context.Get(refdb)
    context.ExecWithContainer(
        image = image,
        cmd = f"makedb --in {context.Get(raw).container} --db {out_path.container}",
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        refdb: "ref.dmnd",
    },
)
