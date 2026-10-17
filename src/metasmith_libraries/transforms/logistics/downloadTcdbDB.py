from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::tcdb_diamond_db"))

# TCDB publishes no versioned release, so this moves; pass `tcdb_url` to hold a copy.
TCDB_URL = "https://tcdb.org/public/tcdb"


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    url = context.params.get("tcdb_url", TCDB_URL)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(url, "tcdb.fasta", label="TCDB FASTA"),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    with open("tcdb.fasta", errors="replace") as f:
        n_seqs = sum(1 for line in f if line.startswith(">"))
    Log.Info(f"TCDB FASTA from [{url}] holds {n_seqs:,} sequences")
    if n_seqs == 0:
        return ExecutionResult(manifest=[], success=False)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            diamond makedb --in tcdb.fasta -d tcdb
            mv tcdb.dmnd {idb.container}
        """,
    )

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.is_file() and idb.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
