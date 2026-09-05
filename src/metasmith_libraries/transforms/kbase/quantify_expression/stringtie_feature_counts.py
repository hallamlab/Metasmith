from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::stringtie.env"))
bam     = model.AddRequirement(lib.GetType("transcriptomics::bowtie2_bam"))
gff     = model.AddRequirement(lib.GetType("annotation::bakta_gff"))
out     = model.AddProduct(lib.GetType("transcriptomics::stringtie_quant_gtf"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   stringtie -e -B -p $cpus -G {igff.container} -o {iout.container} {ibam.container}
    #
    # This is `stringtie_quant.py` with a bacterial bam and a bacterial annotation in place
    # of a STAR bam and a braker merged GTF, and it produces the SAME
    # `transcriptomics::stringtie_quant_gtf` -- so `stringtie_count_matrix.py`, `deseq2.py`
    # and `pydeseq2.py` serve it unchanged and no subread environment is needed.
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
        duration=Duration(hours=2),
    ),
)
