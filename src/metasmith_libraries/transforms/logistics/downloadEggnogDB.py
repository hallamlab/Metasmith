from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::eggnog-mapper.env"))
# Given by the user, never produced: it opts a run into a ~12 GB fetch that unpacks
# to ~55 GB, and its content is not read.
src   = model.AddRequirement(lib.GetType("annotation::eggnog_source"))
out   = model.AddProduct(lib.GetType("annotation::eggnog_packed"))

# The database release eggNOG-mapper 2.1.x reads; download_eggnog_data.py fetches
# the same three files from the same directory.
BASE_URL = "http://eggnog5.embl.de/download/emapperdb-5.0.2"


FILES = ("eggnog.db.gz", "eggnog_proteins.dmnd.gz", "eggnog.taxa.tar.gz")


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    staged = context.StagingDir()
    base = context.params.get("eggnog_base_url", BASE_URL).rstrip("/")

    # Fetch only. buildEggnogDB unpacks on a compute node.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{name}", staged.local/name) for name in FILES),
        f"mkdir -p {iout.local}",
        *(f"mv {staged.local/name} {iout.local}/" for name in FILES),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local/name).stat().st_size > 0 for name in FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=src,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=12),
    ),
)
