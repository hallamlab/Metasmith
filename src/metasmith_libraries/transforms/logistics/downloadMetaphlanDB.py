from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::metaphlan.env"))
out   = model.AddProduct(lib.GetType("ref::metaphlan_db"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--nproc {threads}"

    _cmd = f"""
            mkdir -p {iout.container}
            metaphlan --install --bowtie2db {iout.container} {threads_arg}
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    ),
)
