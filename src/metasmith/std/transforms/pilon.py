from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly   = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
bam   = model.AddRequirement(lib.GetType("std::binary_alignment_map"))
image   = model.AddRequirement(lib.GetType("std::oci_image_pilon"))
out     = model.AddProduct(lib.GetType("std::hybrid_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    assembly_path = context.Get(assembly).container / "00-assembly/draft_assembly.fasta"
    bam_dir = context.Get(bam).container

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                mkdir -p {out_path.container}
                cd {out_path.container}
                java -jar \
                    /pilon/pilon.jar \
                        --genome {assembly_path} \
                        --frags {bam_dir / "alignments.sorted.bam"} \
                        --output pilon_out \
                        --fix all
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "pilon_out/",
    },
)
