from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads      = model.AddRequirement(lib.GetType("std::short_reads_filtered"))
assembly   = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
image      = model.AddRequirement(lib.GetType("std::oci_image_bwa"))
out        = model.AddProduct(lib.GetType("std::sequence_alignment_map"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    reads_path = context.Get(reads)
    assembly_path_container = context.Get(assembly).container / "assembly/assembly.fasta"
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                bwa index {assembly_path_container}
                bwa mem {assembly_path_container} {reads_path.container} > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "alignments.sam",
    },
)
