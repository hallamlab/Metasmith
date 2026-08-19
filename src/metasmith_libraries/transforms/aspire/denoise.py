from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run       = model.AddRequirement(lib.GetType("aspire::run"))
derep     = model.AddRequirement(lib.GetType("aspire::derep_fasta"), parents={run})
centroids = model.AddProduct(lib.GetType("aspire::centroids_fasta"))

def protocol(context: ExecutionContext):
    made = {
        centroids: context.Output(centroids),
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
        duration=Duration(hours=4),
    ),
)
