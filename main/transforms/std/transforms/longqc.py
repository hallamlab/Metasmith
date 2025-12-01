from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::long_reads"))
image   = model.AddRequirement(lib.GetType("std::oci_image_longqc"))
out     = model.AddProduct(lib.GetType("std::read_stats"))

def protocol(context: ExecutionContext):
    out_path = context.Input(out)
    reads_path = context.Input(reads)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                MPLBACKEND=Agg longQC.py sampleqc \
                    -x pb-sequel \
                    -o longqc_out \
                    {reads_path.container}
            """,
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    output_signature = {
        out: "longqc_out/",
    },
)
