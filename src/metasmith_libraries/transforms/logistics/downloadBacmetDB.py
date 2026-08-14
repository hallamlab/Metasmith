from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
db    = model.AddProduct(lib.GetType("annotation::bacmet_diamond_db"))

# BacMet2 "predicted" protein DB (~40k genes; metal + biocide resistance).
# Swap to BacMet2_EXP_database.fasta for the experimentally-confirmed subset.
BACMET_URL = "http://bacmet.biomedicine.gu.se/download/BacMet2_predicted_database.fasta.gz"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q --no-check-certificate {BACMET_URL} -O bacmet.fasta.gz
            gunzip -f bacmet.fasta.gz
            diamond makedb --in bacmet.fasta -d bacmet
            mv bacmet.dmnd {idb.container}
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
