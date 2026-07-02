from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("containers::metasmith.oci"))
name = model.AddRequirement(lib.GetType("examples::name"))
out = model.AddProduct(lib.GetType("examples::greeting"))


def protocol(context: ExecutionContext):
    name_path = context.Input(name)
    out_path = context.Output(out)
    context.ExecWithContainer(
        image=image,
        cmd=f'echo "hello $(cat {name_path.container})" > {out_path.container}',
    )
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
