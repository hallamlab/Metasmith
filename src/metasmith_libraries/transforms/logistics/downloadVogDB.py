from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddProduct(lib.GetType("ref::vogdb_sources"))

# The release downloadDramDB pins; DRAM's own default is `latest`.
BASE_URL = "https://fileshare.lisc.univie.ac.at/vog"
RELEASE = "vog237"
FILES = ("vog.hmm.tar.gz", "vog.annotations.tsv.gz")


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    base = context.params.get("vogdb_base_url", BASE_URL).rstrip("/")
    release = context.params.get("vogdb_release", RELEASE)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{release}/{name}", staged.local/name, label=f"VOGDB {name}")
          for name in FILES),
        f"mkdir -p {isrc.local}",
        *(f"mv {staged.local/name} {isrc.local}/" for name in FILES),
        f"echo {release} > {isrc.local}/release",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=all((isrc.local/name).stat().st_size > 0 for name in FILES),
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
