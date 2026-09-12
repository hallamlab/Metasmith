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
    db_path = context.Input(database)
    features_path = context.Input(features)
    cds_path = context.Input(cds)
    assembly_path = context.Input(assembly)
    out_path = context.Output(out)

    cpus = context.params.get("cpus") or 8
    context.ExecWithContainer(
        image = image,
        cmd = f"""
            export MPLBACKEND=Agg
            bakta \
                --db {db_path.container} \
                --threads {cpus} \
                --skip-trna --skip-tmrna --skip-rrna --skip-ncrna \
                --skip-ncrna-region --skip-crispr --skip-sorf \
                --skip-gap --skip-ori --skip-plot \
                --prefix bakta \
                --output {out_path.container} \
                {assembly_path.container}
        """
    )
    return ExecutionResult(
        manifest=[{
            out: out_path.local,
        }],
        success=out_path.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=assembly,
    model = model,
)
