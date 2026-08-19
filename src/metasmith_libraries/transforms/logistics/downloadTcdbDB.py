from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::tcdb_diamond_db"))

TCDB_URL = "https://tcdb.org/public/tcdb"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q --no-check-certificate {TCDB_URL} -O tcdb.fasta
            diamond makedb --in tcdb.fasta -d tcdb
            mv tcdb.dmnd {idb.container}
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
