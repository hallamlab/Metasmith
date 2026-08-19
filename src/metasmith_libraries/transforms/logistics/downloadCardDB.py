from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::rgi.env"))
db    = model.AddProduct(lib.GetType("annotation::card_db"))

CARD_URL = "https://card.mcmaster.ca/latest/data"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q --no-check-certificate {CARD_URL} -O card-data.tar.bz2
            mkdir -p card_raw
            tar xjf card-data.tar.bz2 -C card_raw
            rgi load --card_json card_raw/card.json --local
        """,
    )

    Path("localDB").rename(idb.local)

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
