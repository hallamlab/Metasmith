from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly = model.AddRequirement(lib.GetType("std::assembly"))
db       = model.AddRequirement(lib.GetType("std::bakta_database"))
image    = model.AddRequirement(lib.GetType("std::oci_image_bakta"))
cds      = model.AddProduct(lib.GetType("std::coding_sequences"))
gff      = model.AddProduct(lib.GetType("std::gene_features"))

def protocol(context: ExecutionContext):
    assembly_path  = context.Input(assembly)
    db_path  = context.Input(db)
    cds_path  = context.Input(cds)
    gff_path  = context.Input(gff)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {cds_path}
                bakta \
                    --db {db_path.container} \
                    --skip-trna \
                    --skip-tmrna \
                    --skip-rrna \
                    --skip-ncrna \
                    --skip-ncrna-region \
                    --skip-crispr \
                    --skip-pseudo \
                    --skip-sorf \
                    --skip-gap \
                    --skip-ori \
                    --skip-plot \
                    --output out/ \
                    --prefix bakta-prodigal \
                    {assembly_path.container}
            """,
    )
    return ExecutionResult(success=cds_path.local.exists() and gff_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        cds: "out/bakta-prodigal.faa",
        gff: "out/bakta-prodigal.gff3"
    },
)
