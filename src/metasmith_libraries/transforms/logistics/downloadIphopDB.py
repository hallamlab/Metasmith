from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::iphop.env"))
db    = model.AddProduct(lib.GetType("ref::iphop_db"))

# The database version is pinned here rather than left at the tool's default,
# and the pin is a taxonomy decision. iPHoP_db_Aug23_rw is built on GTDB r214,
# which is the release research/viromics/pipeline_steps.yml runs GTDB-Tk at, so
# host calls and MAG taxonomy stay on one release. The current default,
# iPHoP_db_Jun25_rw, is r226 and needs iphop >= 1.4.1 -- taking it would move
# both the tool and the answers. iPHoP_db_for-test is the small one, for wiring
# this transform up before paying for the real fetch.
DB_VERSION = "iPHoP_db_Aug23_rw"

# Size it before running it anywhere: Aug23_rw arrives as SEVENTEEN 10 GiB chunks
# and is then concatenated and unpacked in place, so peak disk is roughly three
# times the download rather than one. Measured from the release's own md5 manifest
# after seven chunks landed. This does not fit beside anything else on a
# workstation; stage it on cluster scratch.


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # --no_prompt because the download asks for confirmation and nothing is
    # attached to answer it; --split because this is a multi-gigabyte fetch that
    # skips chunks it already has, so an interrupted download resumes instead of
    # restarting.
    # `iphop download` refuses a --db_dir that does not exist rather than
    # creating one: "Pblm, I could not find folder ./iphop_db".
    _cmd = f"""
        mkdir -p ./iphop_db
        iphop download --db_dir ./iphop_db -dbv {DB_VERSION} --split --no_prompt
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    staged = Path("iphop_db")
    assert staged.is_dir() and any(staged.iterdir()), "iphop staged nothing"
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
        duration=Duration(hours=12),
    ),
)
