from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
sources = model.AddProduct(lib.GetType("ref::interproscan_sources"))

IPRSCAN_DATA_URL = "https://ftp.ebi.ac.uk/pub/databases/interpro/iprscan/5/5.67-99.0/interproscan-5.67-99.0-64-bit.tar.gz"

def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(IPRSCAN_DATA_URL, staged.local/"interproscan-data.tar.gz"),
        f"mv {staged.local}/interproscan-data.tar.gz {isrc.local}",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=isrc.local.is_file() and isrc.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=8),
    ),
)
