import json
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::rgi.env"))
db    = model.AddProduct(lib.GetType("annotation::card_db"))

# CARD 4.0.2, which `https://card.mcmaster.ca/latest/data` served on 2026-10-17.
CARD_URL = "https://card.mcmaster.ca/download/0/broadstreet-v4.0.2.tar.bz2"


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    url = context.params.get("card_url", CARD_URL)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(url, "card-data.tar.bz2", label="CARD data"),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    context.ExecWithEnv(
        env=image,
        cmd="""
            mkdir -p card_raw
            tar xjf card-data.tar.bz2 -C card_raw
            rgi load --card_json card_raw/card.json --local
        """,
    )

    Path("localDB").rename(idb.local)

    card_json = idb.local/"card.json"
    loaded = idb.local/"loaded_databases.json"
    version = None
    if loaded.exists():
        version = json.loads(loaded.read_text()).get("card_json", {}).get("data_version")
    Log.Info(f"rgi loaded CARD data version [{version}] from [{url}]")
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=card_json.is_file() and card_json.stat().st_size > 0 and bool(version),
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
