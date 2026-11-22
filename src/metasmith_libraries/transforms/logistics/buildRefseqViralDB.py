import gzip

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
sources = model.AddRequirement(lib.GetType("ref::refseq_viral_sources"))
db      = model.AddProduct(lib.GetType("ref::refseq_viral_diamond_db"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)
    threads = context.params.get("cpus", 8)

    # An HTML error page or a truncated download both fail here rather than in makedb.
    with gzip.open(isrc.local/"viral.1.protein.faa.gz", "rt", errors="replace") as fh:
        first = fh.readline()
    if not first.startswith(">"):
        raise SystemExit("[refseq_viral] viral.1.protein.faa.gz is not a protein FASTA")

    context.ExecWithEnv(env=image, cmd=f"""
        diamond makedb --in {isrc.container}/viral.1.protein.faa.gz -d refseq_viral --threads {threads}
        mv refseq_viral.dmnd {idb.container}
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
