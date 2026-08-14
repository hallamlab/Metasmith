from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::pathologic.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"))
ann      = model.AddRequirement(lib.GetType("annotation::ptools_annotation_table"))
archive  = model.AddProduct(lib.GetType("annotation::pgdb_archive"))
tables   = model.AddProduct(lib.GetType("annotation::pgdb_csv_tables"))


def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    iann     = context.Input(ann)
    iarchive = context.Output(archive)
    itables  = context.Output(tables)

    # Build 0.pf from the generic annotation table.
    build_pf = f"""
import pandas as pd

orf_ids = []
with open("{iorfs.container}") as fh:
    for line in fh:
        if line.startswith(">"):
            orf_ids.append(line[1:].split()[0])

df = pd.read_parquet("{iann.container}")
by_orf = {{}}
for _, r in df.iterrows():
    by_orf.setdefault(str(r["orf_id"]), []).append((str(r["kind"]), str(r["value"])))

EMIT = {{
    "EC":       lambda v: ("EC",     v),
    "GO":       lambda v: ("DBLINK", "GO:" + v.split(":",1)[-1]),
    "KEGG":     lambda v: ("DBLINK", "KEGG:" + v),
    "UNIPROT":  lambda v: ("DBLINK", "UNIPROT:" + v),
    "FUNCTION": lambda v: ("FUNCTION", v),
}}

with open("/ws/0.pf", "w") as f:
    for oid in orf_ids:
        f.write(f"ID\\t{{oid}}\\n")
        f.write(f"NAME\\t{{oid}}\\n")
        f.write("PRODUCT-TYPE\\tP\\n")
        for kind, value in by_orf.get(oid, []):
            fn = EMIT.get(kind)
            if fn is None:
                continue
            field, payload = fn(value)
            f.write(f"{{field}}\\t{{payload}}\\n")
        f.write("//\\n")
"""

    context.LocalShell("mkdir -p ws")
    context.LocalShell("cat > _build_pf.py << 'PYEOF'\n" + build_pf + "\nPYEOF\n")

    # Single container invocation: build .pf, run pathologic, dump CSVs, tar both.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(context.external_cwd / "ws", "/ws")],
        cmd=(
            "cp _build_pf.py /ws/_build_pf.py && "
            "python3 /ws/_build_pf.py && "
            "pathologic -i /ws/0.pf -o /ws/cyc --silent && "
            "mkdir -p /ws/tables && "
            "dump_pgdb_csvs /ws/cyc /ws/tables && "
            "tar czf /ws/cyc.tgz -C /ws/cyc . && "
            "tar czf /ws/tables.tgz -C /ws/tables ."
        ),
    )

    context.LocalShell(f"cp ws/cyc.tgz {iarchive.local}")
    context.LocalShell(f"cp ws/tables.tgz {itables.local}")

    return ExecutionResult(
        manifest=[
            {
                archive: iarchive.local,
                tables:  itables.local,
            },
        ],
        success=iarchive.local.exists() and itables.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=4),
    ),
)
