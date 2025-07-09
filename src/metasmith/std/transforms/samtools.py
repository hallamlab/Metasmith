from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

sam   = model.AddRequirement(lib.GetType("std::sequence_alignment_map"))
image   = model.AddRequirement(lib.GetType("std::oci_image_samtools"))
out     = model.AddProduct(lib.GetType("std::binary_alignment_map"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    sam_path = context.Get(sam)
    out_bam = out_path.container / "alignments.sorted.bam"

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-@ {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                mkdir -p {out_path.container}
                samtools view {cpus_string} -b {sam_path.container} | samtools sort {cpus_string} -o {out_bam}
                samtools index {out_bam}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "bam_alignments/",
    },
)
