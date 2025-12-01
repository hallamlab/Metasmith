from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
out    = model.AddProduct(lib.GetType("std::kofamscan_profile"))

def protocol(context: ExecutionContext):
    out_path = context.Input(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                curl -L -o profiles.tar.gz https://www.genome.jp/ftp/db/kofam/profiles.tar.gz
                mkdir {out_path.container}
                tar -xzvf profiles.tar.gz -C {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    group_by=image,
    model = model,
    output_signature = {
        out: "profiles/",
    },
)
