from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::vibrant.env"))
db    = model.AddProduct(lib.GetType("ref::vibrant_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # One directory carries both of VIBRANT's reference arguments, and that is
    # the script's doing rather than a choice. `download-db.sh <dest>` first
    # copies all of $VIBRANT_DATA_PATH out of the image -- which is where the
    # complete files/ tree already lives, the -m argument -- and only then runs
    # VIBRANT_setup.py inside <dest>/databases to fetch the HMMs, the -d
    # argument. Splitting this into two reference types would mean unpicking a
    # script that deliberately hands back one tree.
    #
    # The fetch is the expensive half: KEGG, Pfam and VOG from three third-party
    # mirrors of 2019-era archives, roughly 20 GB of working space for 11 GB kept.
    _cmd = "download-db.sh ./vibrant_db"
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    staged = Path("vibrant_db")
    for half in ("databases", "files"):
        assert (staged/half).is_dir(), f"vibrant reference is missing {half}/"
    staged.rename(idb.local)

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
        memory=Size.GB(8),
        duration=Duration(hours=8),
    ),
)
