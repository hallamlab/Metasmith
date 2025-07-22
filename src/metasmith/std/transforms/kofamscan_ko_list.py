from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("std::oci_image_biocontainers"))
out    = model.AddProduct(lib.GetType("std::kofamscan_ko_list"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parents}
                curl -L -o ko_list.txt.gz https://www.genome.jp/ftp/db/kofam/ko_list.gz
                gunzip ko_list.txt.gz
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "ko_list.txt",
    },
)
