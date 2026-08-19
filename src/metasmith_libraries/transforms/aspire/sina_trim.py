from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run     = model.AddRequirement(lib.GetType("aspire::run"))
fseqs   = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={run})
ref     = model.AddRequirement(lib.GetType("aspire::sina_arb_reference"))
trimmed = model.AddProduct(lib.GetType("aspire::sina_trimmed_seqs"))
aligned = model.AddProduct(lib.GetType("aspire::sina_aligned_seqs"))
log     = model.AddProduct(lib.GetType("aspire::sina_log"))
vreg    = model.AddProduct(lib.GetType("aspire::sina_v_regions"))

def protocol(context: ExecutionContext):
    made = {
        trimmed: context.Output(trimmed),
        aligned: context.Output(aligned),
        log: context.Output(log),
        vreg: context.Output(vreg),
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
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
