from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
am     = model.AddRequirement(lib.GetType("aspire::analysis_asv_meta"), parents={run})
counts = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={run})
out    = model.AddProduct(lib.GetType("aspire::taxonomy_group_association_outputs"))

def protocol(context: ExecutionContext):
    made = {
        out: context.Output(out),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = {out}

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
