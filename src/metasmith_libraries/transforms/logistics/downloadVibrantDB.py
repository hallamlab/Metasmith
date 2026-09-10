from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::vibrant.env"))
db    = model.AddProduct(lib.GetType("ref::vibrant_db"))

# Where the image keeps its half of the reference. A literal, because the image's
# own environment variable for it is unreachable from a non-login shell and the
# env file's digest is what holds this path still.
VIBRANT_DATA_PATH = "/usr/local/share/vibrant-1.2.1/db"


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
    # VIBRANT_DATA_PATH must be exported here, and this is not belt-and-braces.
    # download-db.sh opens with `cp -r $VIBRANT_DATA_PATH/* $1/`, and the image
    # sets that variable through a conda activate.d hook that only a LOGIN shell
    # sources. The container exec is not one, so unset it expands to `cp -r /*`
    # and the script copies the entire container root -- including every bind
    # mount, so the repo and its DVC data get recursively copied into the output
    # until the filesystem fills. Observed, not theorised.
    _cmd = f"""
        export VIBRANT_DATA_PATH={VIBRANT_DATA_PATH}
        test -d "$VIBRANT_DATA_PATH/files" || {{ echo "VIBRANT files/ absent at $VIBRANT_DATA_PATH" >&2; exit 3; }}
        download-db.sh ./vibrant_db
    """
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
