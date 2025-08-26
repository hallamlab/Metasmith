from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly  = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
bam       = model.AddRequirement(lib.GetType("std::binary_alignment_map_hybrid"))
csi       = model.AddRequirement(lib.GetType("std::binary_alignment_map_csi_hybrid"))
image     = model.AddRequirement(lib.GetType("std::oci_image_pilon"))
out       = model.AddProduct(lib.GetType("std::hybrid_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    assembly_path = context.Get(assembly).container
    bam_path = context.Get(bam).container
    csi_path = context.Get(csi).container

    memory = context.params.get("memory")
    memory_string = ""
    if memory is not None:
        memory = int(memory * 0.9)
        memory_string = f"-Xmx{memory}G"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                mkdir bam/
                cp {bam_path} bam/alignments.bam
                cp {csi_path} bam/alignments.bam.csi
                java {memory_string} -jar \
                    /pilon/pilon.jar \
                        --genome {assembly_path} \
                        --frags bam/alignments.bam \
                        --fix all
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "pilon.fasta",
    },
)
