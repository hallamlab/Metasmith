from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddProduct(lib.GetType("ref::dbcan_sources"))

# The V11 HMMdb and family table downloadDramDB pins for DRAM 1.5.0, from the host that
# still serves them: DRAM's own bcb.unl.edu address answers with a web page.
BASE_URL = "https://pro.unl.edu/dbCAN2/download/Databases/V11"
FILES = {"hmm": "dbCAN-HMMdb-V11.txt", "activities": "CAZyDB.08062022.fam-activities.txt"}


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    base = context.params.get("dbcan_base_url", BASE_URL).rstrip("/")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{name}", staged.local/name, label=f"dbCAN {key}")
          for key, name in FILES.items()),
        f"mkdir -p {isrc.local}",
        *(f"mv {staged.local/name} {isrc.local}/" for name in FILES.values()),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=all((isrc.local/name).stat().st_size > 0 for name in FILES.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(1),  # dbCAN V11, template estimate
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=1),
    ),
)
