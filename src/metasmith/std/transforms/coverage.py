from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

bam             = model.AddRequirement(lib.GetType("std::binary_alignment_map"))
image_samtools  = model.AddRequirement(lib.GetType("std::oci_image_samtools"))
out             = model.AddProduct(lib.GetType("std::per_contig_coverage"))

def protocol(context: ExecutionContext):
    bam_path = context.Get(bam)
    out_path = context.Get(out)

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                samtools coverage -b {bam_path.container} > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "contig_coverage",
    },
)
