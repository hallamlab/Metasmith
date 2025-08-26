from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly = model.AddRequirement(lib.GetType("std::assembly"))
image    = model.AddRequirement(lib.GetType("std::oci_image_prodigal"))
cds     = model.AddProduct(lib.GetType("std::coding_sequences"))
gff     = model.AddProduct(lib.GetType("std::gene_features"))

def protocol(context: ExecutionContext):
    cds_path = context.Get(cds)
    gff_path = context.Get(gff)
    context.ExecWithContainer(
        image = image,
        cmd = f"""\
            prodigal \
                -T {2} \
                -C 10 \
                -p meta \
                -i {context.Get(assembly).container} \
                -a {cds_path.container} \
                -f gff \
                -o {gff_path.container}
            """,
    )
    return ExecutionResult(success=cds_path.local.exists() and gff_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        cds: "cds.faa",
        gff: "output.gff"
    },
)
