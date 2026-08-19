from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
counts = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={run})
md     = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={run})
out    = model.AddProduct(lib.GetType("aspire::collectors_outputs"))

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
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
