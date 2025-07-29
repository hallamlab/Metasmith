from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly   = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
bam   = model.AddRequirement(lib.GetType("std::binary_alignment_map"))
image   = model.AddRequirement(lib.GetType("std::oci_image_pilon"))
out     = model.AddProduct(lib.GetType("std::hybrid_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    assembly_path = context.Get(assembly).container
    bam_dir = context.Get(bam).container

    cpus = context.params.get("cpus")
    cpus_string = ""
    memory = context.params.get("cpus")
    memory_string = ""
    if cpus is not None:
        cpus_string = f"--threads {cpus}"
    if memory is not None:
        memory *= 0.9
        memory_string = f"-Xmx{memory}G"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                java {memory_string} -jar \
                    /pilon/pilon.jar \
                        --genome {assembly_path} \
                        --frags {bam_dir / "alignments.sorted.bam"} \
                        {cpus_string} \
                        --fix all
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "pilon_out/pilon.fasta",
    },
)
