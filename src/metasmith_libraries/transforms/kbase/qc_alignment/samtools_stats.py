from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::samtools.env"))
bam     = model.AddRequirement(lib.GetType("alignment::bam"))
out     = model.AddProduct(lib.GetType("alignment::alignment_stats"))

def protocol(context: ExecutionContext):
    ibam=context.Input(bam)
    iout=context.Output(out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-@ {threads}"
    _cmd = f"""\
            samtools stats {threads} {ibam.container} > {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

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
