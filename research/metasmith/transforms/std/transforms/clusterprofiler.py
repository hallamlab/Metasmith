from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

annotations = model.AddRequirement(lib.GetType("std::eggnog_annotations"))
image       = model.AddRequirement(lib.GetType("std::oci_image_clusterprofiler"))
script      = model.AddRequirement(lib.GetType("std::clusterprofiler_enrich_script"))
table       = model.AddProduct(lib.GetType("std::enrichment_table"))
plot        = model.AddProduct(lib.GetType("std::enrichment_plot"))

def protocol(context: ExecutionContext):
    annotations_path = context.Input(annotations)
    script_path      = context.Input(script)
    table_path       = context.Output(table)
    plot_path        = context.Output(plot)

    # KEGG/GO over-representation from the eggNOG annotations, run inside the
    # bioconductor-clusterprofiler container.
    context.ExecWithContainer(
        image = image,
        cmd = f"""
            Rscript {script_path.container} \
                {annotations_path.container} \
                {table_path.container} \
                {plot_path.container}
        """
    )
    return ExecutionResult(
        manifest=[{
            table: table_path.local,
            plot: plot_path.local,
        }],
        success=table_path.local.exists() and plot_path.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=annotations,
    model = model,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
