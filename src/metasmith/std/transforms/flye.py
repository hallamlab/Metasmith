from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

via_miniasm = model.AddRequirement(lib.GetType("std::miniasm_estimate"))
reads   = model.AddRequirement(lib.GetType("std::long_reads_filtered"), parents={via_miniasm})
image   = model.AddRequirement(lib.GetType("std::oci_image_flye"))
out     = model.AddProduct(lib.GetType("std::long_reads_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    reads_path = context.Get(reads)

    cpus_string = ""
    # cpus = context.params.get("cpus")
    # if cpus is not None:
    #     cpus_string = f"--threads {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            flye \
                --pacbio-raw \
                {reads_path.container} \
                --out-dir {out_path.container} \
                {cpus_string}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "long_reads_assembly/",
    },
)
