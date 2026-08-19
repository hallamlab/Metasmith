from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
img_sqk = model.AddRequirement(lib.GetType("env::seqkit.env"))
db      = model.AddProduct(lib.GetType("annotation::megares_diamond_db"))

MEGARES_URL = "https://www.meglab.org/downloads/megares_v3.00/megares_database_v3.00.fasta"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"wget -q --no-check-certificate {MEGARES_URL} -O megares.fasta",
    )

    context.ExecWithEnv().ifContainerDo(
        env=img_sqk,
        cmd="seqkit translate --frame 1 --transl-table 1 --clean --trim "
            "megares.fasta -o megares_prot.fasta",
    )

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            diamond makedb --in megares_prot.fasta -d megares
            mv megares.dmnd {idb.container}
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
