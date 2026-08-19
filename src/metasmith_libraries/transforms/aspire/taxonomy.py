from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run     = model.AddRequirement(lib.GetType("aspire::run"))
trimmed = model.AddRequirement(lib.GetType("aspire::sina_trimmed_seqs"), parents={run})
refseqs = model.AddRequirement(lib.GetType("amplicon::silva_db"))
reftax  = model.AddRequirement(lib.GetType("aspire::silva_ref_taxonomy"))
tax     = model.AddProduct(lib.GetType("amplicon::asv_taxonomy"))
upper   = model.AddProduct(lib.GetType("aspire::taxonomy_uppercase_seqs"))
stats   = model.AddProduct(lib.GetType("aspire::taxonomy_stats"))

def protocol(context: ExecutionContext):
    made = {
        tax: context.Output(tax),
        upper: context.Output(upper),
        stats: context.Output(stats),
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
        memory=Size.GB(16),
        duration=Duration(hours=6),
    ),
)
