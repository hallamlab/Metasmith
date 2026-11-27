from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddProduct(lib.GetType("ref::dbcan_sources"))

# dbCAN's own S3 release, the host run_dbcan reads; pro.unl.edu and bcb.unl.edu answer 503.
# A dated folder, never a moving alias. The md5s are the objects' single-part ETags.
BASE_URL = "https://dbcan.s3.us-west-2.amazonaws.com/db_v5-2-9_11-3-2026"
FILES = {"hmm": "dbCAN.hmm", "families": "fam-substrate-mapping.tsv"}
MD5 = {"dbCAN.hmm": "d3deacef477a6efeff68c3478177e19d",
       "fam-substrate-mapping.tsv": "a3c76a623dab04f01f4dc3a07663f6b1"}
URLS = {name: f"{BASE_URL}/{name}" for name in FILES.values()}


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    mirror = context.params.get("dbcan_base_url")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(Rebase(URLS[name], BASE_URL, mirror), staged.local/name,
                       label=f"dbCAN {key}", md5=MD5[name])
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
    download_size=Size.MB(119),  # dbCAN db_v5-2-9_11-3-2026, S3 Content-Length
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=1),
    ),
)
