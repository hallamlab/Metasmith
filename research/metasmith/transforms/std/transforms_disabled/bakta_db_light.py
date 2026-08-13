from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
out    = model.AddProduct(lib.GetType("std::bakta_database_light"))

def protocol(context: ExecutionContext):
    out_path = context.Input(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                curl -L -o bakta_db.tar.xz https://zenodo.org/records/14916843/files/db-light.tar.xz
                mkdir {out_path.container}
                tar -xJvf bakta_db.tar.xz -C {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "bakta_db/",
    },
)
