from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::vfdb_diamond_db"))

# VFDB full dataset (setB) protein sequences. Use VFDB_setA_pro.fas.gz for the
# curated "core" subset instead.
VFDB_URL = "http://www.mgc.ac.cn/VFs/Down/VFDB_setB_pro.fas.gz"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q --no-check-certificate {VFDB_URL} -O vfdb.fasta.gz
            gunzip -f vfdb.fasta.gz
            diamond makedb --in vfdb.fasta -d vfdb
            mv vfdb.dmnd {idb.container}
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
