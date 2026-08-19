from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
policy = model.AddRequirement(lib.GetType("aspire::indicspecies_off"), parents={run})
tables = model.AddProduct(lib.GetType("aspire::indicspecies_tables"))
g1sum  = model.AddProduct(lib.GetType("aspire::indicspecies_group1_summary"))

def protocol(context: ExecutionContext):
    made = {
        tables: context.Output(tables),
        g1sum: context.Output(g1sum),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = {tables}

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
