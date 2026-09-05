from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::ganon.env"))
out   = model.AddProduct(lib.GetType("ref::ganon2_db"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--threads {threads}"

    _cmd = f"""
            mkdir -p $(dirname {iout.container})
            ganon build --db-prefix {iout.container} \
                --source refseq --organism-group archaea bacteria \
                --top 1 {threads_arg}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    ),
)
