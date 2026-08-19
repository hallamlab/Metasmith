from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
md     = model.AddRequirement(lib.GetType("aspire::metadata_micro"), parents={run})
counts = model.AddRequirement(lib.GetType("aspire::asv_final_micro"), parents={run})
out    = model.AddProduct(lib.GetType("aspire::grouping_diagnostics_outputs"))
assign = model.AddProduct(lib.GetType("aspire::soft_assignments"))
valid  = model.AddProduct(lib.GetType("aspire::soft_validation"))
vsum   = model.AddProduct(lib.GetType("aspire::soft_validation_summary"))

def protocol(context: ExecutionContext):
    made = {
        out: context.Output(out),
        assign: context.Output(assign),
        valid: context.Output(valid),
        vsum: context.Output(vsum),
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
