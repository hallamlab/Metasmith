from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::mobileog_diamond_db"))

URLS = {"mobileog": "https://zenodo.org/records/13241605/files/mobileOG-db_beatrix-1.6.All.faa?download=1"}


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            wget -q "{URLS['mobileog']}" -O mobileog.faa
            diamond makedb --in mobileog.faa -d mobileog
            mv mobileog.dmnd {idb.container}
        """,
    )

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(0.33),  # Content-Length 355,523,471 B, 2026-11-23
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
