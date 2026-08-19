from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run       = model.AddRequirement(lib.GetType("aspire::run"))
counts_fa = model.AddRequirement(lib.GetType("aspire::concat_counts_fasta"), parents={run})
fastq     = model.AddProduct(lib.GetType("aspire::fastq_stats"))
fastp     = model.AddProduct(lib.GetType("aspire::fastp_stats"))
filtered  = model.AddProduct(lib.GetType("aspire::filtered_stats"))
concat    = model.AddProduct(lib.GetType("aspire::concat_stats"))

def protocol(context: ExecutionContext):
    made = {
        fastq: context.Output(fastq),
        fastp: context.Output(fastp),
        filtered: context.Output(filtered),
        concat: context.Output(concat),
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
