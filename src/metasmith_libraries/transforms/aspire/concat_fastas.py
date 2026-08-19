from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run       = model.AddRequirement(lib.GetType("aspire::run"))
sid       = model.AddRequirement(lib.GetType("aspire::sample_id"), parents={run})
filtered  = model.AddRequirement(lib.GetType("aspire::filtered_fasta"), parents={sid})
concat    = model.AddProduct(lib.GetType("aspire::concat_fasta"))
counts_fa = model.AddProduct(lib.GetType("aspire::concat_counts_fasta"))

def protocol(context: ExecutionContext):
    made = {
        concat: context.Output(concat),
        counts_fa: context.Output(counts_fa),
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
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
