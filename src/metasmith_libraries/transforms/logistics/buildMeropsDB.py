import re

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
sources = model.AddRequirement(lib.GetType("ref::merops_sources"))
db      = model.AddProduct(lib.GetType("ref::merops_diamond_db"))

# DRAM reads the peptidase family out of the header with this, and so does the lane.
FAMILY = re.compile(r"#\w*.#")


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)
    threads = context.params.get("cpus", 8)

    with open(isrc.local/"pepunit.lib", errors="replace") as fh:
        headers = [line for _, line in zip(range(200), (l for l in fh if l.startswith(">")))]
    if not headers or not all(FAMILY.search(h) for h in headers):
        raise SystemExit("[merops] pepunit.lib headers do not carry the #family# tag "
                         "DRAM reads the peptidase family from; it is not MEROPS pepunit.lib")

    # MEROPS ships CRLF line ends and stray spaces inside sequences, which DIAMOND rejects.
    with open(isrc.local/"pepunit.lib", errors="replace") as src, open("pepunit.clean.fa", "w") as out:
        for line in src:
            line = line.rstrip("\r\n")
            out.write((line if line.startswith(">") else "".join(c for c in line if c.isalpha() or c == "*")) + "\n")

    context.ExecWithEnv(env=image, cmd=f"""
        diamond makedb --in pepunit.clean.fa -d merops --threads {threads}
        mv merops.dmnd {idb.container}
    """)

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.exists() and idb.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=8,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
