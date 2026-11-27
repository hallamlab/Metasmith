from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::vfdb_diamond_db"))

URLS = {"vfdb": "http://www.mgc.ac.cn/VFs/Down/VFDB_setB_pro.fas.gz"}


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            wget -q --no-check-certificate {URLS['vfdb']} -O vfdb.fasta.gz
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
    download_size=Size.GB(0.01),  # VFDB setB proteins
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
