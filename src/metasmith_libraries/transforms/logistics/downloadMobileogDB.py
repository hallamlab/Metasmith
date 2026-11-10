from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::mobileog_diamond_db"))

MOBILEOG_URL = "https://zenodo.org/records/13241605/files/mobileOG-db_beatrix-1.6.All.faa?download=1"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            wget -q "{MOBILEOG_URL}" -O mobileog.faa
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
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
