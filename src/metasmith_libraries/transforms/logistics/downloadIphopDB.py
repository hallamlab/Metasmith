from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::iphop.env"))
db    = model.AddProduct(lib.GetType("ref::iphop_db"))

# iPHoP_db_Jun25_rw is GTDB r226 and is what iphop.env's version reads; see there for
# why no r232 host database is possible. A run param, so Aug23_rw (r214, iphop 1.3.3)
# or the small `iPHoP_db_rw_for-test` can be named at launch.
#
# Use `iPHoP_db_rw_for-test` to wire this up, NOT the `iPHoP_db_for-test` the tool's
# own --help prints: that name 404s, and the downloader answers a 404 by printing
# "We could not identify the database version you are trying to download" and
# exiting 0. Check the product, never the exit code.
DB_VERSION = "iPHoP_db_Jun25_rw"

# Size it before running it anywhere: Jun25_rw is 299 GB in 29 chunks, md5-checked
# one by one and skipped when already present, then concatenated and unpacked in
# place, so peak disk is roughly three times the download. It runs on the login node
# because fir's compute nodes fetched at 574 KB/s, about five hours per chunk.


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    version = context.params.get("iphop_db_version", DB_VERSION)

    # --no_prompt because the download asks for confirmation and nothing is
    # attached to answer it; --split so an interrupted download resumes by chunk.
    # `iphop download` refuses a --db_dir that does not exist rather than
    # creating one: "Pblm, I could not find folder ./iphop_db".
    context.ExecWithEnv(env=image, cmd=f"""
        mkdir -p ./iphop_db
        iphop download --db_dir ./iphop_db -dbv {version} --split --no_prompt
        rm -f ./iphop_db/*.tar.gz ./iphop_db/*.tar.gz.[0-9]*
    """)

    # The archive unpacks to one folder (Jun_2025_pub_rw, Test_db_rw, ...), and that
    # folder is what predict and add_to_db take as --db_dir.
    staged = [d for d in Path("iphop_db").iterdir()
              if (d/"db").is_dir() and (d/"db_infos").is_dir()]
    assert len(staged) == 1, (
        f"expected one database folder holding db/ and db_infos/ under iphop_db/, "
        f"found {[p.name for p in Path('iphop_db').iterdir()]}"
    )
    staged[0].rename(idb.local)

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=(idb.local/"db"/"wish_data").is_dir(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=24),
    ),
)
