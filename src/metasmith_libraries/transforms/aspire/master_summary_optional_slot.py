from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run = model.AddRequirement(lib.GetType("aspire::run"))
opt = model.AddProduct(lib.GetType("aspire::optional_outputs"))

def protocol(context: ExecutionContext):
    made = {
        opt: context.Output(opt),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = {opt}

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
