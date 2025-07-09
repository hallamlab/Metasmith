from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::long_reads"))
image_minimap2  = model.AddRequirement(lib.GetType("std::oci_image_minimap2"))
mappings        = model.AddProduct(lib.GetType("std::long_reads_self_mappings"))
image_miniasm   = model.AddRequirement(lib.GetType("std::oci_image_miniasm"))
gfa             = model.AddProduct(lib.GetType("std::long_reads_gfa"))
image_gfatools  = model.AddRequirement(lib.GetType("std::oci_image_gfatools"))
out             = model.AddProduct(lib.GetType("std::long_reads_miniasm"))

def protocol(context: ExecutionContext):
    reads_path = context.Get(reads)
    mappings_path = context.Get(mappings)
    gfa_path = context.Get(gfa)
    out_path = context.Get(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-t{cpus}"

    context.ExecWithContainer(
        image = image_minimap2,
        cmd = f"""
                minimap2 \
                    -x ava-pb \
                    {cpus_string} \
                    {reads_path.container} {reads_path.container} | gzip -1 > {mappings_path.container}
        """
    )
    context.ExecWithContainer(
        image = image_miniasm,
        cmd = f"""
                miniasm \
                    -f {reads_path.container} \
                    {mappings_path.container} \
                    > {gfa_path.container}
        """
    )
    context.ExecWithContainer(
        image = image_gfatools,
        cmd = f"""
                gfatools gfa2fa \
                    {gfa_path.container} \
                    > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        mappings: "mappings.paf.gz",
        gfa: "reads.gfa",
        out: "assembly.fasta"
    },
)
