from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
db      = model.AddProduct(lib.GetType("ref::uniref50_diamond_db"))

# ftp.uniprot.org has gone down whole-host before (every path 404s, not just
# this one) without warning -- pinning a single mirror just resets the same
# fragility clock. Try each in order and fall through on failure instead.
UNIREF50_URLS = [
    "https://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/uniref/uniref50/uniref50.fasta.gz",
    "https://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref50/uniref50.fasta.gz",
    "https://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref50/uniref50.fasta.gz",
]

def protocol(context: ExecutionContext):
    idb = context.Output(db)

    _cmd = "\n".join([
        FetchCommand(UNIREF50_URLS, "uniref50.fasta.gz", label="uniref50"),
        "diamond makedb --in uniref50.fasta.gz -d uniref50",
        f"mv uniref50.dmnd {idb.container}",
    ])
    context.ExecWithEnv(env=image, cmd=_cmd)

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
        cpus=8,
        memory=Size.GB(14),
        duration=Duration(hours=12),
    ),
)
