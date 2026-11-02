from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::stringtie.env"))
bam     = model.AddRequirement(lib.GetType("transcriptomics::bowtie2_bam"))
gff     = model.AddRequirement(lib.GetType("annotation::bakta_gff"))
out     = model.AddProduct(lib.GetType("transcriptomics::stringtie_quant_gtf"))

def protocol(context: ExecutionContext):
    ibam=context.Input(bam)
    igff=context.Input(gff)
    iout=context.Output(out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    # -e restricts assembly to the reference transcripts, which is what makes the
    # output a count table rather than a discovery run -- and it reports nothing at
    # all without the -G those transcripts come from.
    _cmd = f"""\
            stringtie -e -B {threads} -G {igff.container} -o {iout.container} {ibam.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

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
