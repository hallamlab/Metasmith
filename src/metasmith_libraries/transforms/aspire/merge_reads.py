from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
sid    = model.AddRequirement(lib.GetType("aspire::sample_id"), parents={run})
fwd    = model.AddRequirement(lib.GetType("aspire::qc_reads_fwd"), parents={sid})
rev    = model.AddRequirement(lib.GetType("aspire::qc_reads_rev"), parents={sid})
merged = model.AddProduct(lib.GetType("aspire::merged_reads"))

def protocol(context: ExecutionContext):
    made = {
        merged: context.Output(merged),
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
    group_by=sid,
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
