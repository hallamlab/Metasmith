from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::amrfinderplus.env"))
db    = model.AddProduct(lib.GetType("annotation::amrfinderplus_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # amrfinder_update fetches the latest DB compatible with this binary into
    # the given dir; `amrfinder -d <dir>` consumes it. The DB version is pinned
    # by whatever the bundled binary supports, so keep the image tag fixed.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd="""
            mkdir -p amrfinderdb
            amrfinder_update --database amrfinderdb
        """,
    )

    Path("amrfinderdb").rename(idb.local)

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
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
