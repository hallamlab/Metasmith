from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run    = model.AddRequirement(lib.GetType("aspire::run"))
policy = model.AddRequirement(lib.GetType("aspire::augmentation_on"), parents={run})
md     = model.AddRequirement(lib.GetType("aspire::metadata_micro"), parents={run})
am     = model.AddRequirement(lib.GetType("aspire::asv_meta_micro"), parents={run})
assign = model.AddRequirement(lib.GetType("aspire::soft_assignments"), parents={run})
vsum   = model.AddRequirement(lib.GetType("aspire::soft_validation_summary"), parents={run})
out_md = model.AddProduct(lib.GetType("aspire::analysis_metadata"))
out_am = model.AddProduct(lib.GetType("aspire::stage_asv_meta"))
audit  = model.AddProduct(lib.GetType("aspire::augmentation_audit"))

def protocol(context: ExecutionContext):
    made = {
        out_md: context.Output(out_md),
        out_am: context.Output(out_am),
        audit: context.Output(audit),
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
