from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
sources = model.AddProduct(lib.GetType("ref::merops_sources"))

# What DRAM-setup.py 1.5.0 fetches for `peptidase`. MEROPS has not released since 12.4,
# so current_release has held this one file since 2023-02-22 (457.5 MB).
URL = "https://ftp.ebi.ac.uk/pub/databases/merops/current_release/pepunit.lib"


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    url = context.params.get("merops_url", URL)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(url, staged.local/"pepunit.lib", label="MEROPS pepunit.lib"),
        f"mkdir -p {isrc.local}",
        f"mv {staged.local}/pepunit.lib {isrc.local}/",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=(isrc.local/"pepunit.lib").stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=2),
    ),
)
