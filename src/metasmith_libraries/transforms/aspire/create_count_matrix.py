from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run       = model.AddRequirement(lib.GetType("aspire::run"))
counts_fa = model.AddRequirement(lib.GetType("aspire::concat_counts_fasta"), parents={run})
nochi     = model.AddRequirement(lib.GetType("aspire::nochimera_fasta"), parents={run})
counts    = model.AddProduct(lib.GetType("amplicon::asv_table"))
seqs      = model.AddProduct(lib.GetType("amplicon::asv_seqs"))

def protocol(context: ExecutionContext):
    made = {
        counts: context.Output(counts),
        seqs: context.Output(seqs),
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
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
