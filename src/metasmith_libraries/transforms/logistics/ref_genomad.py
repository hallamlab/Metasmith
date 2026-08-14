from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::genomad.env"))
ref = model.AddProduct(lib.GetType("ref::genomad"))


def protocol(context: ExecutionContext):
    idb = context.Output(ref)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd="/usr/local/bin/_entrypoint.sh genomad download-database .",
    )
    Path("genomad_db").rename(idb.local)

    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    )
)
