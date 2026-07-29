from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly = model.AddRequirement(lib.GetType("std::assembly"))
image    = model.AddRequirement(lib.GetType("std::oci_image_prodigal"))
cds     = model.AddProduct(lib.GetType("std::coding_sequences"))
gff     = model.AddProduct(lib.GetType("std::gene_features"))

def protocol(context: ExecutionContext):
    cds_path = context.Output(cds)
    gff_path = context.Output(gff)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"-T {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""\
            pprodigal \
                {cpus_string} \
                -C 10 \
                -p meta \
                -i {context.Input(assembly).container} \
                -a {cds_path.container} \
                -f gff \
                -o {gff_path.container}
            """,
    )
    return ExecutionResult(
        manifest=[{
            cds: cds_path.local,
            gff: gff_path.local,
        }],
        success=cds_path.local.exists() and gff_path.local.exists()
    )

TransformInstance(
    protocol = protocol,
    group_by=assembly,
    model = model,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    ),
)
