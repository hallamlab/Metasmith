from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
db      = model.AddProduct(lib.GetType("ref::uniref50_diamond_db"))

UNIREF50_URL = "https://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref50/uniref50.fasta.gz"

def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q {UNIREF50_URL} -O uniref50.fasta.gz
            diamond makedb --in uniref50.fasta.gz -d uniref50
            mv uniref50.dmnd {idb.container}
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
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
