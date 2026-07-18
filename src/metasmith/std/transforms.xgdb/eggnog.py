from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

annotations = model.AddRequirement(lib.GetType("std::bakta_annotations"))
database    = model.AddRequirement(lib.GetType("std::eggnog_database"))
image       = model.AddRequirement(lib.GetType("std::oci_image_eggnog_mapper"))
out         = model.AddProduct(lib.GetType("std::eggnog_annotations"))

def protocol(context: ExecutionContext):
    annotations_path = context.Input(annotations)
    db_path          = context.Input(database)
    out_path         = context.Output(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"--cpu {cpus}"

    # bakta writes amino-acid CDS to <prefix>.faa (prefix "bakta") inside its
    # output directory; eggNOG-mapper annotates those proteins and writes
    # <out_dir>/eggnog.emapper.annotations, which we expose as the product.
    context.ExecWithContainer(
        image = image,
        cmd = f"""
            cd {out_path.container.parent}
            emapper.py \
                -i {annotations_path.container}/bakta.faa \
                --itype proteins \
                {cpus_string} \
                --data_dir {db_path.container} \
                --output_dir {out_path.container.parent} \
                --output eggnog \
                --override
            cp {out_path.container.parent}/eggnog.emapper.annotations {out_path.container}
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
    group_by=annotations,
    model = model,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=8),
    ),
)
