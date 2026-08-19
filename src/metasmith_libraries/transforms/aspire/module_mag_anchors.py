from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run      = model.AddRequirement(lib.GetType("aspire::run"))
mall     = model.AddRequirement(lib.GetType("aspire::network_modules_all"), parents={run})
nf       = model.AddRequirement(lib.GetType("aspire::network_node_features"), parents={run})
tax      = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={run})
counts   = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={run})
md       = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={run})
pairing  = model.AddRequirement(lib.GetType("aspire::asv_mag_pairing"), parents={run})
net      = model.AddRequirement(lib.GetType("aspire::network_outputs"), parents={run})
anchors  = model.AddProduct(lib.GetType("aspire::module_asv_anchor_table"))
summary  = model.AddProduct(lib.GetType("aspire::module_mag_anchor_summary"))
scores   = model.AddProduct(lib.GetType("aspire::sample_module_scores"))
top      = model.AddProduct(lib.GetType("aspire::sample_top_modules"))
matrix   = model.AddProduct(lib.GetType("aspire::sample_module_matrix"))
heatmaps = model.AddProduct(lib.GetType("aspire::sample_module_heatmaps"))

def protocol(context: ExecutionContext):
    made = {
        anchors: context.Output(anchors),
        summary: context.Output(summary),
        scores: context.Output(scores),
        top: context.Output(top),
        matrix: context.Output(matrix),
        heatmaps: context.Output(heatmaps),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = {heatmaps}

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=run,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
