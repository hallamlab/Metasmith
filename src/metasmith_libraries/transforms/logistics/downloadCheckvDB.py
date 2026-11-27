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
URLS = {"checkv_db": f"{BASE_URL}/checkv-db-v{DB_VERSION}.tar.gz"}


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    name = f"checkv-db-v{DB_VERSION}"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(Rebase(URLS["checkv_db"], BASE_URL, context.params.get("checkv_base_url")), f"{name}.tar.gz", label=f"CheckV database v{DB_VERSION}"),
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
    Log.Info(f"CheckV database v{DB_VERSION}: {n_hmm:,} HMM files")
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=dmnd.is_file() and dmnd.stat().st_size > 0 and n_hmm > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(1.57),  # checkv-db-v1.5.tar.gz Content-Length 1,681,539,865 B, 2026-11-23
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
