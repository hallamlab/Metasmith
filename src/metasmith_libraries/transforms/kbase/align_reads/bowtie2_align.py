from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bowtie2.env"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={exp})
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={exp})
out     = model.AddProduct(lib.GetType("transcriptomics::bowtie2_bam"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   bowtie2-build {iasm.container} ref
    #   bowtie2 -p $cpus -x ref -U {ireads.container} | samtools sort -o {iout.container}
    #
    # A bacterial transcript is colinear with the genome, so a spliced aligner invents
    # introns to raise its own score. That is why this exists beside `star_align.py`
    # rather than reusing it.
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
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
