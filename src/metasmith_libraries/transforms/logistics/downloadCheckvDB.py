from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::checkv.env"))
db    = model.AddProduct(lib.GetType("ref::checkv_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # `checkv download_database <dest>` resolves the current release from
    # portal.nersc.gov/CheckV/CURRENT_RELEASE.txt and unpacks it as
    # <dest>/checkv-db-vX.Y, so the version is in the name and is not ours to
    # predict. Glob for it rather than hard-coding v1.5 -- a pinned name is a
    # rename away from a download that succeeds and a product that is missing.
    _cmd = "checkv download_database ."
    context.ExecWithEnv(env=image, cmd=_cmd)

    found = sorted(Path(".").glob("checkv-db-v*"))
    assert len(found) == 1, f"expected one checkv-db-v* directory, got {found}"
    found[0].rename(idb.local)

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
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
