from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
bam     = model.AddRequirement(lib.GetType("transcriptomics::star_bam"))
mgtf    = model.AddRequirement(lib.GetType("transcriptomics::merged_gtf"))
image   = model.AddRequirement(lib.GetType("env::stringtie.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::stringtie_quant_gtf"))

def protocol(context: ExecutionContext):
    ibam=context.Input(bam)
    imgtf=context.Input(mgtf)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = 4 if threads is None else threads
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            stringtie -e -B -p {threads} \
                -G {imgtf.container} \
                -o quant.gtf \
                {ibam.container}
        """,
    )
    context.LocalShell(f"mv quant.gtf {iout.local}")
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
        memory=Size.GB(4),
        duration=Duration(hours=1),
    )
)
