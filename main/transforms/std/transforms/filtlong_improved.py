from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads      = model.AddRequirement(lib.GetType("std::long_reads"))
estimate   = model.AddRequirement(lib.GetType("std::miniasm_estimate"))
image      = model.AddRequirement(lib.GetType("std::oci_image_filtlong"))
out        = model.AddProduct(lib.GetType("std::long_reads_filtered"))

def protocol(context: ExecutionContext):
    reads_path = context.Input(reads)
    estimate_path = context.Input(estimate)
    out_path = context.Output(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                filtlong \
                    {reads_path.container} \
                    --min_length 1000 \
                    --target_bases $(< {estimate_path.container}) \
                    > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    output_signature = {
        out: "long_reads_filtered.fastq",
    },
)
