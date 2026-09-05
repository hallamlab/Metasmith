from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bcftools.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
bam     = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
out     = model.AddProduct(lib.GetType("sequences::variant_calls"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   bcftools mpileup -f {iasm.container} {ibam.container} \
    #     | bcftools call -mv -Ov -o {iout.container}
    #
    # Recovers the strain variation an assembly consensus collapses to one base. The bam is
    # required to descend from the assembly it was called against -- a VCF is meaningless
    # without knowing which reference its coordinates are in.
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
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
