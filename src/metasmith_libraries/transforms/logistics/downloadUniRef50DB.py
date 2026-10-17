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
CPUS = 4

def protocol(context: ExecutionContext):
    idb = context.Output(db)
    url = context.params.get("uniref50_url")
    urls = UNIREF50_URLS if url is None else [url]

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(urls, "uniref50.fasta.gz", label="uniref50"),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # makedb holds one 1e9-letter block plus a 16-byte entry per sequence, whatever the
    # input's size: rebuilding the 2026-08 release (38.8 M sequences, 12.2 G letters)
    # peaked at 3.4 GB RSS with --threads 4, in 21 min. Unbounded, --threads is every
    # core on a shared login node.
    context.ExecWithEnv(env=image, cmd="\n".join([
        f"diamond makedb --in uniref50.fasta.gz -d uniref50 --threads {CPUS}",
        "diamond dbinfo -d uniref50.dmnd",
        f"mv uniref50.dmnd {idb.container}",
    ]))

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.is_file() and idb.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=CPUS,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
