from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::samtools.env"))
bam     = model.AddRequirement(lib.GetType("alignment::bam"))
out     = model.AddProduct(lib.GetType("alignment::alignment_stats"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   samtools stats -@ $cpus {ibam.container} > {iout.container}
    #
    # Mapping rate, depth and insert size -- the readout that separates a wrong reference
    # from a wrong sample from a sample that is simply quiet, and the one every alignment
    # in this library currently has no way to report.
    made = {out: context.Output(out)}
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
    group_by=bam,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
