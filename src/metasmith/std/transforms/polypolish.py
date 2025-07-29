from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly   = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
sam   = model.AddRequirement(lib.GetType("std::sequence_alignment_map"))
image   = model.AddRequirement(lib.GetType("std::oci_image_polypolish"))
out     = model.AddProduct(lib.GetType("std::assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    assembly_path = context.Get(assembly).container
    sam_path = context.Get(sam)

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            cd {out_path.container.parent}
            polypolish filter --in1 {sam_path.container} --out1 filtered.sam
            polypolish polish {assembly_path} filtered.sam > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "polished.fasta",
    },
)
