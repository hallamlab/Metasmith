from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::long_reads"))
mappings        = model.AddRequirement(lib.GetType("std::self_mappings"))
image_miniasm   = model.AddRequirement(lib.GetType("std::oci_image_miniasm"))
out             = model.AddProduct(lib.GetType("std::miniasm_estimate"))

def protocol(context: ExecutionContext):
    reads_path = context.Get(reads)
    mappings_path = context.Get(mappings)
    out_path = context.Get(out)

    context.ExecWithContainer(
        image = image_miniasm,
        cmd = f"""
                miniasm \
                    -f {reads_path.container} \
                    {mappings_path.container} \
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
