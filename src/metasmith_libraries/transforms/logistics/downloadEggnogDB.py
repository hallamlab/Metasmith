from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::eggnog-mapper.env"))
# Given by the user, never produced: it opts a run into a ~12 GB fetch that unpacks
# to ~55 GB, and its content is not read.
src   = model.AddRequirement(lib.GetType("annotation::eggnog_source"))
out   = model.AddProduct(lib.GetType("annotation::eggnog_data"))

# The database release eggNOG-mapper 2.1.x reads; download_eggnog_data.py fetches
# the same three files from the same directory.
BASE_URL = "http://eggnog5.embl.de/download/emapperdb-5.0.2"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    base = context.params.get("eggnog_base_url", BASE_URL).rstrip("/")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/eggnog.db.gz", "eggnog.db.gz"),
        FetchCommand(f"{base}/eggnog_proteins.dmnd.gz", "eggnog_proteins.dmnd.gz"),
        FetchCommand(f"{base}/eggnog.taxa.tar.gz", "eggnog.taxa.tar.gz"),
        f"mkdir -p {iout.local}",
        f"gunzip -c eggnog.db.gz > {iout.local}/eggnog.db",
        f"gunzip -c eggnog_proteins.dmnd.gz > {iout.local}/eggnog_proteins.dmnd",
        f"tar -xzf eggnog.taxa.tar.gz -C {iout.local}",
        "rm -f eggnog.db.gz eggnog_proteins.dmnd.gz eggnog.taxa.tar.gz",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    db = iout.local/"eggnog.db"
    with open(db, "rb") as f:
        is_sqlite = f.read(16) == b"SQLite format 3\x00"
    whole = is_sqlite and all(
        (iout.local/name).is_file() and (iout.local/name).stat().st_size > 0
        for name in ("eggnog_proteins.dmnd", "eggnog.taxa.db", "eggnog.taxa.db.traverse.pkl")
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=src,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
