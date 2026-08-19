from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run     = model.AddRequirement(lib.GetType("aspire::run"))
policy  = model.AddRequirement(lib.GetType("aspire::network_modules_on"), parents={run})
all     = model.AddRequirement(lib.GetType("aspire::network_graph_all"), parents={run})
thr     = model.AddRequirement(lib.GetType("aspire::network_graph_thr"), parents={run})
sub     = model.AddProduct(lib.GetType("aspire::network_modules_sub"))
mall    = model.AddProduct(lib.GetType("aspire::network_modules_all"))
summary = model.AddProduct(lib.GetType("aspire::network_modules_summary"))
runs    = model.AddProduct(lib.GetType("aspire::network_modules_runs"))

def protocol(context: ExecutionContext):
    made = {
        sub: context.Output(sub),
        mall: context.Output(mall),
        summary: context.Output(summary),
        runs: context.Output(runs),
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
        duration=Duration(hours=6),
    ),
)
