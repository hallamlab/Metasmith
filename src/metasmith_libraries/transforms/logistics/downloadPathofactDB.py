from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddProduct(lib.GetType("ref::pathofact_sources"))

# The databases PathoFact 2.0's README fetches (1.14 GB, md5 acb950e7994c561311a0a6ec36caa701)
# and its source at the `submission` branch head, which pathofact_ml runs the prediction
# scripts from: the feature encoders live in those scripts and nowhere else.
DB_URL = "https://zenodo.org/records/14192463/files/DATABASES.tar.gz"
DB_MD5 = "acb950e7994c561311a0a6ec36caa701"
SRC_REPO = "https://gitlab.com/uniluxembourg/lcsb/systems-ecology/pathofact2"
SRC_COMMIT = "bd5d527ede1516803500691536104a3cf41a5575"


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    commit = context.params.get("pathofact_commit", SRC_COMMIT)
    src_url = f"{SRC_REPO}/-/archive/{commit}/pathofact2-{commit}.tar.gz"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(DB_URL, staged.local/"DATABASES.tar.gz", label="PathoFact 2.0 databases"),
        FetchCommand(src_url, staged.local/"pathofact2.tar.gz", label="PathoFact 2.0 source"),
        f'echo "{DB_MD5}  {staged.local}/DATABASES.tar.gz" | md5sum -c -',
        f"mkdir -p {isrc.local}",
        f"mv {staged.local}/DATABASES.tar.gz {staged.local}/pathofact2.tar.gz {isrc.local}/",
        f"echo {commit} > {isrc.local}/commit",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=(isrc.local/"pathofact2.tar.gz").stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(1.07),  # DATABASES.tar.gz Content-Length 1,144,511,329 B, 2026-11-23
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=2),
    ),
)
