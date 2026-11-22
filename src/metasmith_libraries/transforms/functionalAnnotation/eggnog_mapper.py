import os
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image    = model.AddRequirement(lib.GetType("env::eggnog-mapper.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
data_dir = model.AddRequirement(lib.GetType("annotation::eggnog_data"))
out      = model.AddProduct(lib.GetType("annotation::eggnog_results_chunk"))

# emapper opens eggnog.db and eggnog.taxa.db with a bare sqlite3.connect. SQLite
# then looks for a -journal and a -wal file before every read; mode=ro does not
# stop that, immutable=1 does.
IMMUTABLE_SQLITE = """
import sqlite3, urllib.parse
_connect = sqlite3.connect
def connect(database, *args, **kwargs):
    if isinstance(database, str) and database.endswith(".db") and not kwargs.get("uri"):
        database = "file:" + urllib.parse.quote(database) + "?immutable=1"
        kwargs["uri"] = True
    return _connect(database, *args, **kwargs)
sqlite3.connect = connect
"""

def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idata = context.Input(data_dir)
    iout  = context.Output(out)

    cpus = context.params.get("cpus")
    cpus = 16 if cpus is None else cpus

    shim = Path("sqlite_shim")
    shim.mkdir(exist_ok=True)
    (shim/"sitecustomize.py").write_text(IMMUTABLE_SQLITE)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            sed '/^[^>]/s/\\*//g' {iorfs.container} > ./proteins.clean.faa
            PYTHONPATH="$PWD/{shim}" emapper.py \
                --data_dir {idata.container} \
                -i ./proteins.clean.faa \
                -o results \
                -m diamond \
                --cpu {cpus}
        """,
    )

    context.LocalShell(f"cp results.emapper.annotations {iout.local}")

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=24),
    ),
)
