from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::short_reads_trimmed"))
image   = model.AddRequirement(lib.GetType("std::oci_image_megahit"))
out     = model.AddProduct(lib.GetType("std::short_reads_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    reads_path = context.Get(reads)
    # https://github.com/voutcn/megahit
    # --12          interleaved
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                megahit \
                    --12 {reads_path.container} \
                    -o {out_path.container.parent}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "short_reads_assembly/final.contigs.fa",
    },
)
