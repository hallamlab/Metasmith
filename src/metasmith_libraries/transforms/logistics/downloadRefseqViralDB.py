from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
sources = model.AddProduct(lib.GetType("ref::refseq_viral_sources"))

# What DRAM-setup.py 1.5.0 fetches for `viral`: NUMBER_OF_VIRAL_FILES is 1. RefSeq replaces
# this file every release (238: 2026-11-13, 102 MB), so the release number is kept beside it.
BASE_URL = "https://ftp.ncbi.nlm.nih.gov/refseq/release"


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    base = context.params.get("refseq_base_url", BASE_URL).rstrip("/")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/viral/viral.1.protein.faa.gz", staged.local/"viral.1.protein.faa.gz",
                     label="RefSeq viral proteins"),
        FetchCommand(f"{base}/RELEASE_NUMBER", staged.local/"RELEASE_NUMBER", label="RefSeq release"),
        f"mkdir -p {isrc.local}",
        f"mv {staged.local}/viral.1.protein.faa.gz {staged.local}/RELEASE_NUMBER {isrc.local}/",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=(isrc.local/"viral.1.protein.faa.gz").stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(0.1),  # 102 MB, see above
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=2),
    ),
)
