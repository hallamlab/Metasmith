from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reference = model.AddRequirement(lib.GetType("std::cazy_ref"))
cds  = model.AddRequirement(lib.GetType("std::coding_sequences"))
image   = model.AddRequirement(lib.GetType("std::oci_image_fast_aligner"))
out     = model.AddProduct(lib.GetType("std::cazy_annotations"))

def protocol(context: ExecutionContext):
    cds_path = context.Get(cds)
    reference_path = context.Get(reference)
    out_path = context.Get(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-P {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                fastdb \
                    -p CAZy_db \
                    {reference_path.container}
                fastal \
                    {cpus_string} \
                    -o {out_path.container} \
                    CAZy_db \
                    {cds_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "CAZy_alignment.tsv",
    },
)
