from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::mafft.env"))
seqs    = model.AddRequirement(lib.GetType("sequences::orfs"))
out     = model.AddProduct(lib.GetType("comparative::msa"))

def protocol(context: ExecutionContext):
    iseqs=context.Input(seqs)
    iout=context.Output(out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--thread {threads}"
    _cmd = f"""\
            mafft --auto {threads} {iseqs.container} > {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=seqs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
