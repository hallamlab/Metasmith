from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::long_reads"))
image_minimap2  = model.AddRequirement(lib.GetType("std::oci_image_minimap2"))
out             = model.AddProduct(lib.GetType("std::self_mappings"))

def protocol(context: ExecutionContext):
    reads_path = context.Get(reads)
    out_path = context.Get(out)

    cpus_string = ""
    # cpus = context.params.get("cpus")
    # if cpus is not None:
    #     cpus_string = f"-t{cpus}"

    context.ExecWithContainer(
        image = image_minimap2,
        cmd = f"""
                minimap2 \
                    -x ava-pb \
                    {cpus_string} \
                    {reads_path.container} {reads_path.container} | gzip -1 > {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "self_mappings.gzip"
    },
)
