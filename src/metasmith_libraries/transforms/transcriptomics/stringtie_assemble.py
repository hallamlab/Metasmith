from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
bam     = model.AddRequirement(lib.GetType("transcriptomics::star_bam"))
image   = model.AddRequirement(lib.GetType("env::stringtie.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::stringtie_gtf"))

def protocol(context: ExecutionContext):
    ibam=context.Input(bam)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = 4 if threads is None else threads
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            stringtie {ibam.container} \
                -o output.gtf \
                -l SAMPLE \
                -p {threads}
        """,
    )
    context.LocalShell(f"mv output.gtf {iout.local}")
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bam,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    )
)
