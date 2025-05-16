from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::short_reads"))
image   = model.AddRequirement(lib.GetType("std::oci_image_fastqc"))
out     = model.AddProduct(lib.GetType("std::read_stats"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    reads_path = context.Get(reads)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                mkdir fastqc_out
                fastqc \
                    --noextract \
                    -o fastqc_out \
                    {reads_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "fastqc_out/",
    },
)
