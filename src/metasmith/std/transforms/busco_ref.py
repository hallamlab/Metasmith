from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("std::oci_image_biocontainers"))
out_map    = model.AddProduct(lib.GetType("std::busco_ref"))
out_ref    = model.AddProduct(lib.GetType("std::busco_map"))

def protocol(context: ExecutionContext):
    out_ref_path = context.Get(out_ref)
    out_map_path = context.Get(out_map)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_ref_path.container.parent}
                curl -L -o bacteria.tar.gz https://busco-data.ezlab.org/v5/data/lineages/bacteria_odb12.2025-05-14.tar.gz
                mkdir out/
                tar -xzvf bacteria.tar.gz -C out/
                gunzip out/bacteria_odb12/refseq_db.faa.gz
                cp out/bacteria_odb12/refseq_db.faa {out_ref_path.container}
                cp out/bacteria_odb12/info/species.info {out_map_path.container}
        """
    )
    return ExecutionResult(success=out_ref_path.local.exists() and out_map_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out_ref: "busco_ref.fasta",
        out_map: "busco_map.info",
    },
)
