from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::deeparg.env"))
sources = model.AddRequirement(lib.GetType("ref::deeparg_sources"))
db      = model.AddProduct(lib.GetType("ref::deeparg_db"))

# `deeparg predict -d <db>` reads these, for the LS model and model version v2.
LAYOUT = ("database/v2/features.dmnd", "database/v2/features.gene.length",
          "model/v2/model_LS.pkl", "model/v2/metadata_LS.pkl")


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)
    idb.local.mkdir(parents=True, exist_ok=True)

    # The zip holds one top-level deeparg/ directory; download_data moves its contents up.
    context.LocalShell(f"""
        set -euo pipefail
        unzip -q {isrc.local}/deeparg.zip -d unpacked
        mv unpacked/deeparg/* {idb.local}/
        rm -rf unpacked
    """)

    missing = [f for f in LAYOUT if not (idb.local/f).exists()]
    if missing:
        raise SystemExit(f"[deeparg] the unpacked data lacks {missing}")
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=True,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
