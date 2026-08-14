from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::metabuli.env"))
ref   = model.AddProduct(lib.GetType("ref::metabuli_ref"))


def protocol(context: ExecutionContext):
    idb = context.Output(ref)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd="metabuli databases GTDB . tmp",
    )
    Path("gtdb").rename(idb.local)

    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=8),
    ),
)
