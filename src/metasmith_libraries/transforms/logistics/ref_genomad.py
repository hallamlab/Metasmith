from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::genomad.env"))
ref = model.AddProduct(lib.GetType("ref::genomad"))

# `genomad download-database` takes the newest release in releases.txt that lists
# the package's minor version; for genomad 1.11 (genomad.env) that is v1.9. Pinned
# here so a new release does not move the product under an unchanged image.
DB_VERSION = "1.9"
BASE_URL = "https://portal.nersc.gov/genomad/__data__"


def protocol(context: ExecutionContext):
    idb = context.Output(ref)
    version = str(context.params.get("genomad_db_version", DB_VERSION))
    base = context.params.get("genomad_base_url", BASE_URL).rstrip("/")
    archive = f"genomad_db_v{version}.tar.gz"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/{archive}", archive, label=f"geNomad database v{version}"),
        "tar -xzf " + archive,
        f"mv genomad_db {idb.local}",
        f"rm -f {archive}",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    stamp = idb.local/"version.txt"
    found = stamp.read_text().strip() if stamp.exists() else None
    Log.Info(f"geNomad database version.txt reads [{found}], asked for [{version}]")
    whole = found == version and all(
        (idb.local/name).is_file() and (idb.local/name).stat().st_size > 0
        for name in ("genomad_db", "genomad_integrase_db",
                     "genomad_marker_metadata.tsv", "names.dmp", "nodes.dmp")
    )
    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=whole,
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
    )
)
