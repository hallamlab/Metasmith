from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::deeparg.env"))
sources = model.AddProduct(lib.GetType("ref::deeparg_sources"))

# What `deeparg download_data` fetches (deeparg 1.0.4 entry.py), 1.88 GB.
URL = "https://zenodo.org/records/8280582/files/deeparg.zip"
MD5 = "52198e9d89d5e953ca498f6af1f1b374"


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(URL, staged.local/"deeparg.zip", label="DeepARG data"),
        f'echo "{MD5}  {staged.local}/deeparg.zip" | md5sum -c -',
        f"mkdir -p {isrc.local}",
        f"mv {staged.local}/deeparg.zip {isrc.local}/",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=(isrc.local/"deeparg.zip").stat().st_size > 0,
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
