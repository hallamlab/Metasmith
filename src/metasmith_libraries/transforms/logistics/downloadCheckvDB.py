from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::checkv.env"))
db    = model.AddProduct(lib.GetType("ref::checkv_db"))

# `checkv download_database` takes whatever CURRENT_RELEASE.txt names (v1.5 since
# 2023-01), and its fetch gives up on any error other than a request error while
# leaving the partial archive behind. Pinned and fetched here instead; the DIAMOND
# build below is the step the tool runs after unpacking.
DB_VERSION = "1.5"
BASE_URL = "https://portal.nersc.gov/CheckV"


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    version = str(context.params.get("checkv_db_version", DB_VERSION))
    base = context.params.get("checkv_base_url", BASE_URL).rstrip("/")
    name = f"checkv-db-v{version}"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/{name}.tar.gz", f"{name}.tar.gz", label=f"CheckV database v{version}"),
        f"tar -xzf {name}.tar.gz",
        f"rm -f {name}.tar.gz",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            diamond makedb \
                --in {name}/genome_db/checkv_reps.faa \
                --db {name}/genome_db/checkv_reps \
                --threads 1
        """,
    )

    Path(name).rename(idb.local)

    dmnd = idb.local/"genome_db"/"checkv_reps.dmnd"
    n_hmm = len(list((idb.local/"hmm_db"/"checkv_hmms").glob("*.hmm")))
    Log.Info(f"CheckV database v{version}: {n_hmm:,} HMM files")
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=dmnd.is_file() and dmnd.stat().st_size > 0 and n_hmm > 0,
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
