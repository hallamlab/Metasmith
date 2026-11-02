from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
out    = model.AddProduct(lib.GetType("std::bakta_database_full"))

def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                curl -L -o bakta_db.tar.xz https://zenodo.org/records/14916843/files/db.tar.xz
                mkdir {out_path.container}
                tar -xJvf bakta_db.tar.xz -C {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    group_by=image,
    model = model,
    output_signature = {
        out: "bakta_db/",
    },
)
