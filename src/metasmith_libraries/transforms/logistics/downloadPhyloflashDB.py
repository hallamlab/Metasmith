from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::phyloflash.env"))
out   = model.AddProduct(lib.GetType("ref::phyloflash_db"))

SILVA_RELEASE = "138.2"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-CPUs {threads}"

    _cmd = f"""
            mkdir -p {iout.container}
            cd {iout.container}
            phyloFlash_makedb.pl --remote_dbsource={SILVA_RELEASE} {threads_arg}
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
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)
