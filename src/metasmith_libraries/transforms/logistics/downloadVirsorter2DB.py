from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::virsorter2.env"))
db    = model.AddProduct(lib.GetType("annotation::virsorter2_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    cpus = context.params.get("cpus", 4)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export HOME=/tmp
            virsorter setup -d vs2_db -j {cpus}
        """,
    )

    Path("vs2_db").rename(idb.local)

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
