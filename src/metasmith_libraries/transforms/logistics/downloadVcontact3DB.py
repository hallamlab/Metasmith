from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::vcontact3.env"))
db    = model.AddProduct(lib.GetType("ref::vcontact3_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # This must run in vcontact3's OWN image, which is why the env is a
    # requirement rather than a convenience. From 3.0.5 `prepare_databases` only
    # offers releases the installed version can read, so fetching the same
    # database from anywhere else can stage one 3.1.4 will refuse at run time.
    _cmd = 'vcontact3 prepare_databases --get-version "latest" --set-location ./vcontact3_db'
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    staged = Path("vcontact3_db")
    assert staged.is_dir() and any(staged.iterdir()), "vcontact3 staged nothing"
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
        duration=Duration(hours=4),
    ),
)
