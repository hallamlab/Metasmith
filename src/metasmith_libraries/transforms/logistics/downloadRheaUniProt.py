from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::rhea_uniprot"))

# Rhea serves only its current release file by file; old_releases/ keeps whole-release
# archives, so rhea-release.properties is fetched beside the tables to name what landed.
BASE = "https://ftp.expasy.org/databases/rhea"
FILES = {"rhea-release.properties": f"{BASE}/rhea-release.properties",
         "rhea2uniprot.tsv": f"{BASE}/tsv/rhea2uniprot.tsv",
         "rhea2uniprot_trembl.tsv.gz": f"{BASE}/tsv/rhea2uniprot_trembl.tsv.gz"}
URLS = FILES


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        *(FetchCommand(url, iout.local/name) for name, url in FILES.items()),
        f"grep '^rhea.release' {iout.local}/rhea-release.properties",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)
    return ExecutionResult(manifest=[{out: iout.local}],
                           success=all((iout.local/f).stat().st_size > 0 for f in FILES))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(0.17),  # release 143, 164,837,681 B by Content-Length, 2026-11-26
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(hours=1)),
)
