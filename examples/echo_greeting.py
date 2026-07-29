from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("containers::metasmith.env"))
name = model.AddRequirement(lib.GetType("examples::name"))
out = model.AddProduct(lib.GetType("examples::greeting"))


def protocol(context: ExecutionContext):
    name_path = context.Input(name)
    out_path = context.Output(out)
    # `echo`/`cat` are on PATH in both worlds and every ContextPath view is the
    # right path for the arm that runs, so the two commands coincide here. That
    # is a property of this tool, not of tools in general -- most of the library
    # needs genuinely different commands, which is why the arms are separate.
    cmd = f'echo "hello $(cat {name_path.container})" > {out_path.container}'
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)
    return ExecutionResult(
        manifest=[{out: out_path.local}],
        success=out_path.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=name,
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(minutes=5)),
)
