from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reference = model.AddRequirement(lib.GetType("std::busco_ref"))
assembly  = model.AddRequirement(lib.GetType("std::assembly"))
image   = model.AddRequirement(lib.GetType("std::oci_image_fast_aligner"))
out     = model.AddProduct(lib.GetType("std::busco_annotations"))

def protocol(context: ExecutionContext):
    assembly_path = context.Get(assembly)
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
                    -p BUSCO_db \
                    {reference_path.container}
                fastal \
                    {cpus_string} \
                    -o {out_path.container} \
                    BUSCO_db \
                    {assembly_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "BUSCO_alignment.tsv",
    },
)
