from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("std::oci_image_ubuntu"))
flye_image = model.AddRequirement(lib.GetType("std::oci_image_flye"))
out        = model.AddProduct(lib.GetType("std::assembly"))

# You would expect two different containers, but you actually end up with SINGULARITY_CONTAINER being the same for both.
def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
            echo $SINGULARITY_CONTAINER > {out_path.container}
        """
    )
    context.ExecWithContainer(
        image = flye_image,
        cmd = f"""
            echo $SINGULARITY_CONTAINER >> {out_path.container}
            flye --help
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "otherfile.txt",
    },
)