from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

database = model.AddRequirement(lib.GetType("std::bakta_database"))
assembly = model.AddRequirement(lib.GetType("std::assembly"))
features = model.AddRequirement(lib.GetType("std::gene_features"), parents={assembly})
cds      = model.AddRequirement(lib.GetType("std::coding_sequences"), parents={assembly})
image    = model.AddRequirement(lib.GetType("std::oci_image_bakta"))
out      = model.AddProduct(lib.GetType("std::bakta_annotations"))

def protocol(context: ExecutionContext):
    db_path = context.Get(database)
    features_path = context.Get(features)
    cds_path = context.Get(cds)
    assembly_path = context.Get(assembly)
    out_path = context.Get(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"--threads {cpus}"
    cpus_string = ""
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                bakta \
                    --db {db_path.container} \
                    {cpus_string} \
                    --skip-cds \
                    --regions {features_path.container} \
                    --proteins {cds_path.container} \
                    --prefix bakta \
                    --output {out_path.container} \
                    {assembly_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "bakta_out/",
    },
)
