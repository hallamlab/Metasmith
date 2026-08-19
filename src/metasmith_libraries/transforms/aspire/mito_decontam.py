from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run       = model.AddRequirement(lib.GetType("aspire::run"))
master    = model.AddRequirement(lib.GetType("aspire::mitomaster_table"), parents={run})
mhits     = model.AddRequirement(lib.GetType("aspire::mito_blast6"), parents={run})
chits     = model.AddRequirement(lib.GetType("aspire::contaminant_blast6"), parents={run})
tax       = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={run})
nontarget = model.AddProduct(lib.GetType("aspire::nontarget_table"))
summaries = model.AddProduct(lib.GetType("aspire::mito_summary_tables"))
plots     = model.AddProduct(lib.GetType("aspire::mito_plots"))

def protocol(context: ExecutionContext):
    made = {
        nontarget: context.Output(nontarget),
        summaries: context.Output(summaries),
        plots: context.Output(plots),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = {summaries, plots}

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=run,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
