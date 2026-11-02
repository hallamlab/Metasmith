from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
sources = model.AddRequirement(lib.GetType("annotation::eggnog_packed"))
out     = model.AddProduct(lib.GetType("annotation::eggnog_data"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    iout = context.Output(out)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        f"gunzip -c {isrc.local}/eggnog.db.gz > {iout.local}/eggnog.db",
        f"gunzip -c {isrc.local}/eggnog_proteins.dmnd.gz > {iout.local}/eggnog_proteins.dmnd",
        f"tar -xzf {isrc.local}/eggnog.taxa.tar.gz -C {iout.local}",
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
    group_by=sources,
    labels=["noscratch"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=6),
    ),
)
