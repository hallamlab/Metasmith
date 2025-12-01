from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

profile   = model.AddRequirement(lib.GetType("std::kofamscan_profile"))
ko_list   = model.AddRequirement(lib.GetType("std::kofamscan_ko_list"))
cds       = model.AddRequirement(lib.GetType("std::coding_sequences"))
image     = model.AddRequirement(lib.GetType("std::oci_image_kofamscan"))
out       = model.AddProduct(lib.GetType("std::kofamscan_annotations"))

def protocol(context: ExecutionContext):
    profile_path = context.Input(profile)
    ko_list_path = context.Input(ko_list)
    cds_path = context.Input(cds)
    out_path = context.Input(out)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"--cpu={cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                exec_annotation \
                    --profile={profile_path.container} \
                    --ko-list={ko_list_path.container} \
                    {cpus_string} \
                    --format detail \
                    -o {out_path.container} \
                    {cds_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "kofamscan_out.tsv",
    },
)
