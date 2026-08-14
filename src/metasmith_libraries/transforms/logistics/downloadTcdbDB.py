from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::tcdb_diamond_db"))

# TCDB publishes every transporter protein as one FASTA at this endpoint.
TCDB_URL = "https://tcdb.org/public/tcdb"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # diamond_tcdb.py mounts the .dmnd's *parent* at /db and references the
    # file by name, so the product is a single `.dmnd` file (ext: dmnd).
    # tcdb.org's TLS chain trips wget's verification, hence --no-check-certificate.
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
