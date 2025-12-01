from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
out    = model.AddProduct(lib.GetType("std::cazy_ref"))

def protocol(context: ExecutionContext):
    out_path = context.Input(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                curl -L -o {out_path.container} https://bcb.unl.edu/dbCAN2/download/CAZyDB.07142024.fa
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "cazy_ref.fasta",
    },
)
