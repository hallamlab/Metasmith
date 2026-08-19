from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly    = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
short_reads = model.AddRequirement(lib.GetType("std::short_reads_trimmed"))
bam         = model.AddRequirement(lib.GetType("std::binary_alignment_map"), parents={assembly, short_reads})
csi         = model.AddRequirement(lib.GetType("std::binary_alignment_map_csi"), parents={assembly, short_reads})
image       = model.AddRequirement(lib.GetType("std::oci_image_pilon"))
out         = model.AddProduct(lib.GetType("std::hybrid_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    assembly_path = context.Input(assembly).container
    bam_path = context.Input(bam).container
    csi_path = context.Input(csi).container

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
    group_by=assembly,
    model = model,
    output_signature = {
        out: "pilon.fasta",
    },
)
