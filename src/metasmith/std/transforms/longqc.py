from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
reads     = model.AddRequirement(node=lib.GetType("qc::short_reads"))
image     = model.AddRequirement(node=lib.GetType("qc::oci_image_longqc"))
out       = model.AddProduct(lib.GetType("qc::read_stats"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""\
            longqc sampleqc \
                -x pb-sequel
                -o {out_path.container}longqc \
                {context.Get(reads).container}
            """,
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "longqc",
    },
)
