from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::long_reads"))
image   = model.AddRequirement(lib.GetType("std::oci_image_filtlong"))
out     = model.AddProduct(lib.GetType("std::long_reads_filtered"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    reads_path = context.Get(reads)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                filtlong \
                    {reads_path.container} \
                    --keep_percent 90 \
                    > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "long_reads_filtered.fasta",
    },
)
