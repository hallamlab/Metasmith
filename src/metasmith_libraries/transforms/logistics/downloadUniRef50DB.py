from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
fasta   = model.AddProduct(lib.GetType("ref::uniref50_fasta"))

# ftp.uniprot.org has gone down whole-host before (every path 404s, not just
# this one) without warning -- pinning a single mirror just resets the same
# fragility clock. Try each in order and fall through on failure instead.
UNIREF50_URLS = [
    "https://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/uniref/uniref50/uniref50.fasta.gz",
    "https://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref50/uniref50.fasta.gz",
    "https://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref50/uniref50.fasta.gz",
]

def protocol(context: ExecutionContext):
    ifasta = context.Output(fasta)
    # In the staging dir, so a retry resumes the 12 h transfer rather than restarting it.
    staged = context.StagingDir()
    url = context.params.get("uniref50_url")
    urls = UNIREF50_URLS if url is None else [url]

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(urls, staged.local/"uniref50.fasta.gz", label="uniref50"),
        f"mv {staged.local}/uniref50.fasta.gz {ifasta.local}",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{fasta: ifasta.local}],
        success=ifasta.local.is_file() and ifasta.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=12),
    ),
)
