from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run       = model.AddRequirement(lib.GetType("aspire::run"))
fcounts   = model.AddRequirement(lib.GetType("aspire::asv_filtered_counts"), parents={run})
fseqs     = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={run})
tax       = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={run})
nontarget = model.AddRequirement(lib.GetType("aspire::nontarget_table"), parents={run})
filtered  = model.AddProduct(lib.GetType("aspire::counts_filtered"))
micro     = model.AddProduct(lib.GetType("aspire::counts_micro"))
mito      = model.AddProduct(lib.GetType("aspire::counts_mito"))
decon     = model.AddProduct(lib.GetType("aspire::counts_decon"))

def protocol(context: ExecutionContext):
    made = {
        filtered: context.Output(filtered),
        micro: context.Output(micro),
        mito: context.Output(mito),
        decon: context.Output(decon),
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
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
