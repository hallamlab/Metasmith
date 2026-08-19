from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run        = model.AddRequirement(lib.GetType("aspire::run"))
policy     = model.AddRequirement(lib.GetType("aspire::batch_correction_on"), parents={run})
md         = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={run})
am         = model.AddRequirement(lib.GetType("aspire::stage_asv_meta"), parents={run})
counts     = model.AddRequirement(lib.GetType("aspire::asv_final_micro"), parents={run})
out_counts = model.AddProduct(lib.GetType("aspire::analysis_counts"))
out_am     = model.AddProduct(lib.GetType("aspire::analysis_asv_meta"))
clr_sel    = model.AddProduct(lib.GetType("aspire::asv_clr_selected"))
clr_after  = model.AddProduct(lib.GetType("aspire::asv_clr_after"))
corr       = model.AddProduct(lib.GetType("aspire::asv_corrected_counts"))
corr_int   = model.AddProduct(lib.GetType("aspire::asv_corrected_counts_int"))
sel        = model.AddProduct(lib.GetType("aspire::asv_selected_counts"))
decision   = model.AddProduct(lib.GetType("aspire::correction_decision"))
cs_plot    = model.AddProduct(lib.GetType("aspire::correction_countspace_plot"))
cs_metrics = model.AddProduct(lib.GetType("aspire::correction_countspace_metrics"))
umap_plot  = model.AddProduct(lib.GetType("aspire::correction_umap_plot"))
stats      = model.AddProduct(lib.GetType("aspire::correction_stats"))
umap_res   = model.AddProduct(lib.GetType("aspire::umap_hdbscan_results"))

def protocol(context: ExecutionContext):
    made = {
        out_counts: context.Output(out_counts),
        out_am: context.Output(out_am),
        clr_sel: context.Output(clr_sel),
        clr_after: context.Output(clr_after),
        corr: context.Output(corr),
        corr_int: context.Output(corr_int),
        sel: context.Output(sel),
        decision: context.Output(decision),
        cs_plot: context.Output(cs_plot),
        cs_metrics: context.Output(cs_metrics),
        umap_plot: context.Output(umap_plot),
        stats: context.Output(stats),
        umap_res: context.Output(umap_res),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = set()

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=run,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
