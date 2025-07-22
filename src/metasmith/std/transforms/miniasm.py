from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::long_reads"))
image_minimap2  = model.AddRequirement(lib.GetType("std::oci_image_minimap2"))
image_miniasm   = model.AddRequirement(lib.GetType("std::oci_image_miniasm"))
out             = model.AddProduct(lib.GetType("std::miniasm_estimate"))

def protocol(context: ExecutionContext):
    reads_path = context.Get(reads)
    out_path = context.Get(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-t{cpus}"

    context.ExecWithContainer(
        image = image_minimap2,
        cmd = f"""
                cd {out_path.container.parent}
                minimap2 \
                    -x ava-pb \
                    {cpus_string} \
                    {reads_path.container} {reads_path.container} | gzip -1 > self_mappings.gzip
        """
    )
    context.ExecWithContainer(
        image = image_miniasm,
        cmd = f"""
                cd {out_path.container.parent}
                miniasm \
                    -f {reads_path.container} \
                    self_mappings.gzip \
                    > miniasm.gfa

                GENOME_SIZE=$(awk '/^S/ {{ sum += length($3) }} END {{ print sum }}' miniasm.gfa); \
                TARGET_BASES=$(( GENOME_SIZE * 100 )); \
                echo $TARGET_BASES > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "miniasm_estimate"
    },
)
