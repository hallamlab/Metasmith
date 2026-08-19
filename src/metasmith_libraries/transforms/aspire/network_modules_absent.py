from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
policy = model.AddRequirement(lib.GetType("aspire::network_modules_off"), parents={run})
sub    = model.AddProduct(lib.GetType("aspire::network_modules_sub"))
mall   = model.AddProduct(lib.GetType("aspire::network_modules_all"))

def protocol(context: ExecutionContext):
    made = {
        sub: context.Output(sub),
        mall: context.Output(mall),
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
