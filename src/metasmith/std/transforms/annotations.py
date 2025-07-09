from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

bakta     = model.AddRequirement(lib.GetType("std::bakta_annotations"))
cds       = model.AddRequirement(lib.GetType("std::coding_sequences"))
gff       = model.AddRequirement(lib.GetType("std::gene_features"))
kofamscan = model.AddRequirement(lib.GetType("std::kofamscan_annotations"))
cazy      = model.AddRequirement(lib.GetType("std::cazy_annotations"))
busco     = model.AddRequirement(lib.GetType("std::busco_annotations"))
out       = model.AddProduct(lib.GetType("std::functional_annotations"))

def protocol(context: ExecutionContext):
    bakta_path     = context.Get(bakta)
    cds_path       = context.Get(cds)
    gff_path       = context.Get(gff)
    kofamscan_path = context.Get(kofamscan)
    cazy_path      = context.Get(cazy)
    busco_path     = context.Get(busco)
    out_path       = context.Get(out)

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            TODO
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "annotations/",
    },
)
