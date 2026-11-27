from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::metanetx_reactions"))

RELEASE = "4.5"
BASE = f"https://www.metanetx.org/ftp/{RELEASE}"
# The release's own .md5 sidecars, read 2026-11-26.
MD5 = {"reac_prop.tsv": "24c8cf3371399919fc2668d31afd5091",
       "reac_xref.tsv": "26fe01a96bfbe18f05f5a8c76338544f"}
URLS = {name: f"{BASE}/{name}" for name in MD5}


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        *(FetchCommand(URLS[f], iout.local/f, md5=MD5[f]) for f in MD5),
        f"echo {RELEASE} > {iout.local}/release",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)
    return ExecutionResult(manifest=[{out: iout.local}],
                           success=all((iout.local/f).stat().st_size > 0 for f in MD5))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(0.09),  # 91,026,539 B by Content-Length, 2026-11-26
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(hours=1)),
)
