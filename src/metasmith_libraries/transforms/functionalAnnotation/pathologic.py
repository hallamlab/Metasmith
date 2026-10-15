import os
import pwd
from pathlib import Path

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::pathologic.env"))
pyenv    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"))
ann      = model.AddRequirement(lib.GetType("annotation::ptools_annotation_table"))
archive  = model.AddProduct(lib.GetType("annotation::pgdb_archive"))
tables   = model.AddProduct(lib.GetType("annotation::pgdb_csv_tables"))

# PathoLogic reads the organism id from the .pf file name, and a Lisp reader takes
# an all-digit name as a number, so the name must start with a letter.
PF_NAME = "pgdb.pf"


def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    iann     = context.Input(ann)
    iarchive = context.Output(archive)
    itables  = context.Output(tables)

    build_pf = f"""
import polars as pl

orf_ids = []
with open("{iorfs.container}") as fh:
    for line in fh:
        if line.startswith(">"):
            orf_ids.append(line[1:].split()[0])

by_orf = {{}}
for r in pl.read_parquet("{iann.container}").iter_rows(named=True):
    by_orf.setdefault(str(r["orf_id"]), []).append((str(r["kind"]), str(r["value"])))

EMIT = {{
    "EC":       lambda v: ("EC",     v),
    "GO":       lambda v: ("DBLINK", "GO:" + v.split(":",1)[-1]),
    "KEGG":     lambda v: ("DBLINK", "KEGG:" + v),
    "UNIPROT":  lambda v: ("DBLINK", "UNIPROT:" + v),
    "FUNCTION": lambda v: ("FUNCTION", v),
}}

with open("ws/{PF_NAME}", "w") as f:
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

    # One row per frame, keyed the same way for every PGDB source, so PGDBs from
    # different pipelines flatten to comparable tables.
    dump = """
import csv, re
from pathlib import Path

cyc = Path("ws/cyc")
out = Path("ws/tables")
out.mkdir(exist_ok=True)

def frames(path):
    rec = {}
    if not path.exists():
        return
    for line in path.read_text(encoding="latin-1").splitlines():
        if line.startswith("#"):
            continue
        if line.startswith("//"):
            if rec:
                yield rec
            rec = {}
            continue
        if " - " in line and not line.startswith("/"):
            k, v = line.split(" - ", 1)
            rec.setdefault(k, []).append(v)
    if rec:
        yield rec

prot = {p["UNIQUE-ID"][0]: (p.get("GENE", []), [c.split()[0] for c in p.get("COMPONENTS", [])])
        for p in frames(cyc / "data" / "proteins.dat")}

def genes(pid, depth=0):
    g, comps = prot.get(pid, ([], []))
    out_ = list(g)
    if depth == 0:
        for c in comps:
            out_ += genes(c, 1)
    return out_

def write(name, header, rows):
    with open(out / name, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

write("reactions.csv", ["reaction", "ec"],
      [(r["UNIQUE-ID"][0], "|".join(r.get("EC-NUMBER", []))) for r in frames(cyc / "data" / "reactions.dat")])
write("reaction_genes.csv", ["reaction", "gene"],
      sorted({(r, g) for e in frames(cyc / "data" / "enzrxns.dat")
              for r in e.get("REACTION", []) for g in genes(e.get("ENZYME", [""])[0])}))
write("genes.csv", ["gene", "name", "product"],
      [(g["UNIQUE-ID"][0], "|".join(g.get("COMMON-NAME", [])), "|".join(g.get("PRODUCT", [])))
       for g in frames(cyc / "data" / "genes.dat")])
pwys = []
ev = cyc / "reports" / "pwy-evidence-list.dat"
if ev.exists():
    for line in ev.read_text(encoding="latin-1").splitlines():
        if line.startswith("("):
            ids = re.findall(r'"([^"]+)"', line)
            if ids:
                pwys.append((ids[0], "|".join(ids[1:])))
write("pathways.csv", ["pathway", "evidenced_reactions"], pwys)
"""

    context.LocalShell("mkdir -p ws/cyc ws/pgdbs")
    context.LocalShell("cat > _build_pf.py << 'PYEOF'\n" + build_pf + "\nPYEOF\n")
    context.LocalShell("cat > _dump.py << 'PYEOF'\n" + dump + "\nPYEOF\n")
    context.ExecWithEnv(env=pyenv, cmd="python3 _build_pf.py")

    # The engine already mounts the task directory at /ws, so the task's ws/ is /ws/ws.
    # The image is read-only, and PathoLogic builds the PGDB under
    # /data/ptools-local/pgdbs before the wrapper moves it to -o. Pathway Tools refuses
    # to start unless the passwd home exists, and --no-home leaves it out; $HOME does not
    # stand in for it.
    context.LocalShell("mkdir -p ws/home")
    context.ExecWithEnv(
        env=image,
        binds=[(context.external_cwd / "ws" / "pgdbs", "/data/ptools-local/pgdbs"),
               (context.external_cwd / "ws" / "home", pwd.getpwuid(os.getuid()).pw_dir)],
        cmd=f"pathologic -i /ws/ws/{PF_NAME} -o /ws/ws/cyc --silent",
    )
    # The wrapper exits 0 whether or not Pathway Tools built anything.
    built = Path("ws/cyc/data/reactions.dat").exists()

    context.ExecWithEnv(env=pyenv, cmd="python3 _dump.py")
    context.LocalShell(f"tar czf {iarchive.local} -C ws/cyc .")
    context.LocalShell(f"tar czf {itables.local} -C ws/tables .")

    return ExecutionResult(
        manifest=[
            {
                archive: iarchive.local,
                tables:  itables.local,
            },
        ],
        success=built and iarchive.local.exists() and itables.local.exists(),
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
