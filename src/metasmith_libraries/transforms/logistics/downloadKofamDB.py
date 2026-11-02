from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
sources = model.AddProduct(lib.GetType("ref::kofam_sources"))

# The top-level profiles.tar.gz and ko_list.gz are replaced at each KEGG release;
# archives/<date>/ keeps each pair, and the pair is what must agree.
BASE_URL = "https://www.genome.jp/ftp/db/kofam/archives"
RELEASE = "2026-08-02"

def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    base = context.params.get("kofam_base_url", BASE_URL).rstrip("/")
    release = context.params.get("kofam_release", RELEASE)
    Log.Info(f"KOfam release [{release}] from [{base}]")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/{release}/profiles.tar.gz", staged.local/"profiles.tar.gz", label="KOfam profiles"),
        FetchCommand(f"{base}/{release}/ko_list.gz", staged.local/"ko_list.gz", label="KOfam ko_list"),
        f"mkdir -p {isrc.local}",
        f"mv {staged.local}/profiles.tar.gz {staged.local}/ko_list.gz {isrc.local}/",
        f"echo {release} > {isrc.local}/release",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=all((isrc.local/f).stat().st_size > 0 for f in ("profiles.tar.gz", "ko_list.gz")),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=4),
    ),
)
