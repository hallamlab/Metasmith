from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

forward     = model.AddRequirement(lib.GetType("std::paired_reads_forward"))
reverse     = model.AddRequirement(lib.GetType("std::paired_reads_reverse"))
image       = model.AddRequirement(lib.GetType("std::oci_image_bbtools"))
out         = model.AddProduct(lib.GetType("std::short_reads"))

def protocol(context: ExecutionContext):
    forward_path    = context.Get(forward)
    reverse_path    = context.Get(reverse)
    out_path        = context.Get(out)

    mem_gb = context.params.get("mem_gb")
    mem_str = f"-Xmx{mem_gb}g" if mem_gb is not None else ""

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            reformat.sh -eoom {mem_str} in1={forward_path.container} in2={reverse_path.container} out={out_path.container}
        """,
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "interleaved_reads.fq.gz",
    },
)
