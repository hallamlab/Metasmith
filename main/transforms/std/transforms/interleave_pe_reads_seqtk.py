from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

forward     = model.AddRequirement(lib.GetType("std::paired_reads_forward"))
reverse     = model.AddRequirement(lib.GetType("std::paired_reads_reverse"))
image       = model.AddRequirement(lib.GetType("std::oci_image_seqtk"))
out         = model.AddProduct(lib.GetType("std::short_reads"))

def protocol(context: ExecutionContext):
    forward_path    = context.Input(forward)
    reverse_path    = context.Input(reverse)
    out_path        = context.Input(out)

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            seqtk mergepe {forward_path.container} {reverse_path.container} | gzip > {out_path.container}
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
